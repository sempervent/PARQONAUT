#!/usr/bin/env bash
# End-to-end server plugin scan job (HTTP → durable job → worker → plugin host).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

RUN_DIR="${REPO_ROOT}/target/plugin-server-demo"
STATE_DIR="${RUN_DIR}/state"
CONFIG="${RUN_DIR}/server.toml"
PID_FILE="${RUN_DIR}/server.pid"
LOG="${RUN_DIR}/server.log"
LISTEN="${PLUGIN_SERVER_LISTEN:-127.0.0.1:3857}"
BASE_URL="http://${LISTEN}"
FIXTURES="$(cd "${REPO_ROOT}/fixtures" && pwd)"
SCAN_FILE="${FIXTURES}/scan/tiny_parquet/micro.parquet"
PLUGINS_ROOT="${REPO_ROOT}/fixtures/plugins"

cleanup() {
  if [[ -f "${PID_FILE}" ]]; then
    pid="$(cat "${PID_FILE}")"
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
    rm -f "${PID_FILE}"
  fi
}
trap cleanup EXIT

rm -rf "${RUN_DIR}"
mkdir -p "${STATE_DIR}/database"
touch "${STATE_DIR}/database/parqonaut.sqlite"

cat >"${CONFIG}" <<EOF
[storage]
allowed_local_roots = ["${FIXTURES}"]
EOF

export PRQNT_BOOTSTRAP_ADMIN_TOKEN="${PRQNT_BOOTSTRAP_ADMIN_TOKEN:-plugin_server_demo_bootstrap}"
export PARQONAUT_SERVER_PLUGINS_ENABLED=true
export PARQONAUT_SERVER_PLUGIN_ROOTS="${PLUGINS_ROOT}"
export PARQONAUT_SERVER_ALLOWED_PLUGINS=example-rules
export PARQONAUT_PLUGIN_SDK_PATH="${REPO_ROOT}/python/parqonaut_plugins/src"
export PARQONAUT_PLUGIN_PYTHON="${PARQONAUT_PLUGIN_PYTHON:-python3}"
if [[ -x "${REPO_ROOT}/python/parqonaut_plugins/.venv/bin/python" ]]; then
  export PARQONAUT_PLUGIN_PYTHON="${REPO_ROOT}/python/parqonaut_plugins/.venv/bin/python"
fi

cargo build -q -p parqonaut-cli --bin prqnt
PRQNT="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/debug/prqnt"

: >"${LOG}"
(
  exec "${PRQNT}" serve \
    --listen "${LISTEN}" \
    --state-dir "${STATE_DIR}" \
    --workers 2 \
    --config "${CONFIG}" \
    >>"${LOG}" 2>&1
) &
echo $! >"${PID_FILE}"

for _ in $(seq 1 90); do
  if curl -fsS "${BASE_URL}/api/v1/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -fsS "${BASE_URL}/api/v1/health" | grep -q '"status"[[:space:]]*:[[:space:]]*"ok"'

auth="Authorization: Bearer ${PRQNT_BOOTSTRAP_ADMIN_TOKEN}"

plugins="$(curl -fsS -H "${auth}" "${BASE_URL}/api/v1/plugins")"
echo "${plugins}" | grep -q '"name"[[:space:]]*:[[:space:]]*"example-rules"'

body="$(python3 - <<PY
import json, os
print(json.dumps({
  "target": {"type": "local_file", "path": os.environ["SCAN_FILE"]},
  "profile": "quick",
  "plugins": ["example-rules"],
}))
PY
)"
export SCAN_FILE

job="$(curl -fsS -X POST "${BASE_URL}/api/v1/jobs/scans" \
  -H "${auth}" -H "content-type: application/json" \
  -d "${body}")"
job_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])' <<<"${job}")"

status=""
for _ in $(seq 1 120); do
  row="$(curl -fsS -H "${auth}" "${BASE_URL}/api/v1/jobs/${job_id}")"
  status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"${row}")"
  if [[ "${status}" == "succeeded" || "${status}" == "failed" || "${status}" == "canceled" ]]; then
    break
  fi
  sleep 0.25
done
[[ "${status}" == "succeeded" ]] || {
  echo "job did not succeed: ${status}" >&2
  tail -50 "${LOG}" >&2 || true
  exit 1
}

run_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])' <<<"${row}")"
report="$(curl -fsS -H "${auth}" "${BASE_URL}/api/v1/runs/${run_id}/report")"
echo "${report}" | grep -q 'plugin.example_rules'
echo "${report}" | grep -q '"plugin_version"'
echo "${report}" | grep -q '"plugin_digest"'
echo "${report}" | grep -q '"phase"'

echo "plugin-server-demo PASS (${BASE_URL})"
