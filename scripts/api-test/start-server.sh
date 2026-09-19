#!/usr/bin/env bash
# Start `prqnt serve` against a fresh temp SQLite state dir (background).
set -euo pipefail
source "$(dirname "$0")/env.sh"

if [[ -f "${API_TEST_PID_FILE}" ]]; then
  old_pid="$(cat "${API_TEST_PID_FILE}")"
  if kill -0 "${old_pid}" 2>/dev/null; then
    echo "server already running (pid ${old_pid})" >&2
    exit 0
  fi
fi

: >"${API_TEST_LOG}"

if [[ "${API_TEST_FRESH_STATE:-0}" == "1" ]]; then
  rm -rf "${API_TEST_STATE_DIR}"
fi
mkdir -p "${API_TEST_STATE_DIR}/database"
touch "${API_TEST_STATE_DIR}/database/parqonaut.sqlite"

cat >"${API_TEST_RUN_ENV}" <<EOF
API_TEST_STATE_DIR=${API_TEST_STATE_DIR}
API_TEST_LISTEN=${API_TEST_LISTEN}
API_TEST_BASE_URL=${API_TEST_BASE_URL}
PRQNT_BOOTSTRAP_ADMIN_TOKEN=${PRQNT_BOOTSTRAP_ADMIN_TOKEN}
API_TEST_CONFIG=${API_TEST_CONFIG}
EOF

cd "${REPO_ROOT}"
cargo build -q -p parqonaut-cli --bin prqnt
prqnt_bin="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/debug/prqnt"
if [[ ! -x "${prqnt_bin}" ]]; then
  echo "missing prqnt binary at ${prqnt_bin}" >&2
  exit 1
fi

export PRQNT_BOOTSTRAP_ADMIN_TOKEN
(
  exec "${prqnt_bin}" serve \
    --listen "${API_TEST_LISTEN}" \
    --state-dir "${API_TEST_STATE_DIR}" \
    --workers 2 \
    --config "${API_TEST_CONFIG}" \
    >>"${API_TEST_LOG}" 2>&1
) &
pid=$!
echo "${pid}" >"${API_TEST_PID_FILE}"

echo "started prqnt serve pid=${pid} listen=${API_TEST_LISTEN} state=${API_TEST_STATE_DIR}"
