#!/usr/bin/env bash
# Shared environment for API integration scripts (deterministic token/port; ephemeral state dir).
# Usage: source "$(dirname "$0")/env.sh"

set -euo pipefail

_api_test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${_api_test_dir}/../.." && pwd)"

export API_TEST_LISTEN="${API_TEST_LISTEN:-127.0.0.1:3847}"
export API_TEST_BASE_URL="http://${API_TEST_LISTEN}"
export PRQNT_BOOTSTRAP_ADMIN_TOKEN="${PRQNT_BOOTSTRAP_ADMIN_TOKEN:-prqnt_api_test_bootstrap_secret}"

RUN_DIR="${REPO_ROOT}/target/api-test"
mkdir -p "${RUN_DIR}"

if [[ -z "${API_TEST_STATE_DIR:-}" && -f "${RUN_DIR}/run.env" ]]; then
  # shellcheck disable=SC1090
  source "${RUN_DIR}/run.env"
fi

if [[ -z "${API_TEST_STATE_DIR:-}" ]]; then
  API_TEST_STATE_DIR="${RUN_DIR}/state"
fi
export API_TEST_STATE_DIR

FIXTURES_ROOT="$(cd "${REPO_ROOT}/fixtures" && pwd)"
export API_TEST_CONFIG="${API_TEST_CONFIG:-${RUN_DIR}/server.toml}"
if [[ ! -f "${API_TEST_CONFIG}" ]]; then
  cat >"${API_TEST_CONFIG}" <<EOF
[storage]
allowed_local_roots = ["${FIXTURES_ROOT}"]
EOF
fi

export API_TEST_RUN_ENV="${RUN_DIR}/run.env"
export API_TEST_PID_FILE="${RUN_DIR}/server.pid"
export API_TEST_LOG="${RUN_DIR}/server.log"
