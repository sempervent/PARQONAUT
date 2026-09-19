#!/usr/bin/env bash
# Poll GET /api/v1/health until the server responds 200 or timeout.
set -euo pipefail
source "$(dirname "$0")/env.sh"

deadline="${1:-60}"
if [[ -f "${API_TEST_RUN_ENV}" ]]; then
  # shellcheck disable=SC1090
  source "${API_TEST_RUN_ENV}"
fi

for ((i = 1; i <= deadline; i++)); do
  if curl -fsS "${API_TEST_BASE_URL}/api/v1/health" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "server not ready at ${API_TEST_BASE_URL} after ${deadline}s" >&2
if [[ -f "${API_TEST_LOG}" ]]; then
  tail -40 "${API_TEST_LOG}" >&2 || true
fi
exit 1
