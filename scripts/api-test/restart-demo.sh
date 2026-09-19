#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "${ROOT}/scripts/api-test/env.sh"
"${ROOT}/scripts/api-test/start-server.sh"
"${ROOT}/scripts/api-test/wait-ready.sh" 90
token="$("${ROOT}/scripts/api-test/bootstrap-token.sh")"
auth="Authorization: Bearer ${token}"
fixture="${REPO_ROOT}/fixtures/scan/single_parquet/data.parquet"
body="$(FIXTURE_PATH="${fixture}" python3 - <<'PY'
import json, os
print(json.dumps({
  "target": {"type": "local_file", "path": os.environ["FIXTURE_PATH"]},
  "profile": "standard",
  "options": {"mode": "full", "max_files": 100000, "format_hints": []},
}))
PY
)"
created="$(curl -fsS -X POST "${API_TEST_BASE_URL}/api/v1/scans/sync" \
  -H "${auth}" -H "content-type: application/json" -d "${body}")"
run_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])' <<<"${created}")"
if [[ -f "${API_TEST_PID_FILE}" ]]; then
  kill "$(cat "${API_TEST_PID_FILE}")" 2>/dev/null || true
  rm -f "${API_TEST_PID_FILE}"
fi
sleep 1
"${ROOT}/scripts/api-test/start-server.sh"
"${ROOT}/scripts/api-test/wait-ready.sh" 90
curl -fsS -H "${auth}" "${API_TEST_BASE_URL}/api/v1/runs/${run_id}" | grep -q "${run_id}"
echo "API restart demo PASS (run ${run_id} survived restart)"
