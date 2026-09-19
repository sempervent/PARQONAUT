#!/usr/bin/env bash
# End-to-end HTTP smoke against a running (or freshly started) local server.
set -euo pipefail
source "$(dirname "$0")/env.sh"

if [[ "${1:-}" == "--start" ]]; then
  API_TEST_FRESH_STATE=1 "$(dirname "$0")/start-server.sh"
fi

if [[ -f "${API_TEST_RUN_ENV}" ]]; then
  # shellcheck disable=SC1090
  source "${API_TEST_RUN_ENV}"
fi

"$(dirname "$0")/wait-ready.sh" 90

token="$("$(dirname "$0")/bootstrap-token.sh")"
auth="Authorization: Bearer ${token}"

curl -fsS "${API_TEST_BASE_URL}/api/v1/health" | grep -q '"status"[[:space:]]*:[[:space:]]*"ok"'

whoami="$(curl -fsS -H "${auth}" "${API_TEST_BASE_URL}/api/v1/whoami")"
echo "${whoami}" | grep -q '"role"[[:space:]]*:[[:space:]]*"admin"'

fixture="${FIXTURES_ROOT}/scan/single_parquet/data.parquet"
export FIXTURE_PATH="${fixture}"
body=$(python3 - <<'PY'
import json, os
print(json.dumps({
  "target": {"type": "local_file", "path": os.environ["FIXTURE_PATH"]},
  "profile": "standard",
  "options": {"mode": "full", "max_files": 100000, "format_hints": []},
}))
PY
)

created="$(curl -fsS -X POST "${API_TEST_BASE_URL}/api/v1/scans/sync" \
  -H "${auth}" -H "content-type: application/json" \
  -d "${body}")"
run_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])' <<<"${created}")"

curl -fsS -H "${auth}" "${API_TEST_BASE_URL}/api/v1/runs/${run_id}" >/dev/null

curl -fsS -H "${auth}" "${API_TEST_BASE_URL}/api/v1/openapi.json" | grep -q 'PARQONAUT API'

echo "API smoke PASS (${API_TEST_BASE_URL})"
