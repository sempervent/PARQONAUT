#!/usr/bin/env bash
# End-to-end Operator Console demo: prqnt serve + API/SSE + UI browser smoke + restart recovery.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

RUN_DIR="${REPO_ROOT}/target/console-demo"
STATE_DIR="${RUN_DIR}/state"
CONFIG="${RUN_DIR}/server.toml"
PID_FILE="${RUN_DIR}/server.pid"
LOG="${RUN_DIR}/server.log"
LISTEN="${CONSOLE_DEMO_LISTEN:-127.0.0.1:3858}"
BASE_URL="http://${LISTEN}"
FIXTURES="$(cd "${REPO_ROOT}/fixtures" && pwd)"
SCAN_TARGET="${FIXTURES}/scan/single_parquet"
REPAIR_OUT="${RUN_DIR}/repaired_dataset"

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
allowed_local_roots = ["${FIXTURES}", "${RUN_DIR}"]
EOF

export PRQNT_BOOTSTRAP_ADMIN_TOKEN="${PRQNT_BOOTSTRAP_ADMIN_TOKEN:-console_demo_admin_tok}"

cargo build -q -p parqonaut-cli --bin prqnt
PRQNT="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/debug/prqnt"

start_server() {
  : >>"${LOG}"
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
      return 0
    fi
    sleep 0.2
  done
  echo "server failed to start" >&2
  tail -40 "${LOG}" >&2 || true
  return 1
}

start_server

export PARQONAUT_UI_URL="${BASE_URL}"
export CONSOLE_SCAN_TARGET="${SCAN_TARGET}"
export CONSOLE_REPAIR_OUT="${REPAIR_OUT}"

node --test scripts/ui-test.mjs
node scripts/ui-browser-smoke.mjs

# Verify durable history survives server restart on the same SQLite directory.
cleanup
start_server

auth="Authorization: Bearer ${PRQNT_BOOTSTRAP_ADMIN_TOKEN}"
jobs_json="$(curl -fsS -H "${auth}" "${BASE_URL}/api/v1/jobs")"
echo "${jobs_json}" | python3 -c '
import json, sys
data = json.load(sys.stdin)
assert data["total"] >= 1, f"expected persisted jobs after restart: {data}"
total = data["total"]
print(f"persisted jobs after restart: {total}")
'

echo "console-demo PASS (${BASE_URL}/ui/)"
