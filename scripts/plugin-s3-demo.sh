#!/usr/bin/env bash
set -euo pipefail
export PARQONAUT_PLUGIN_ROOTS="${PARQONAUT_PLUGIN_ROOTS:-$PWD/fixtures/plugins}"
export PARQONAUT_PLUGIN_SDK_PATH="${PARQONAUT_PLUGIN_SDK_PATH:-$PWD/python/parqonaut_plugins/src}"

cleanup() {
  if [[ -n "${SPEC_TMP:-}" && -f "${SPEC_TMP:-}" ]]; then rm -f "$SPEC_TMP"; fi
  if [[ "${STARTED_S3:-0}" == "1" ]]; then just s3-down || true; fi
}
trap cleanup EXIT

STARTED_S3=0
if [[ -z "${PARQONAUT_S3_ENDPOINT:-}" ]]; then
  just s3-up
  STARTED_S3=1
fi
source scripts/s3-test/env.sh
export FOGBANK_ENDPOINT="${FOGBANK_ENDPOINT:-${PARQONAUT_S3_ENDPOINT:-http://127.0.0.1:9000}}"
export FOGBANK_BUCKET="${FOGBANK_BUCKET:-${PARQONAUT_S3_BUCKET:-fogbank}}"
export PARQONAUT_S3_ENDPOINT="${PARQONAUT_S3_ENDPOINT:-$FOGBANK_ENDPOINT}"
export PARQONAUT_S3_BUCKET="${PARQONAUT_S3_BUCKET:-$FOGBANK_BUCKET}"
just s3-fixtures

input="s3://${FOGBANK_BUCKET}/datasets/transform/partition-basic/input.parquet"
output="s3://${FOGBANK_BUCKET}/datasets/transform/plugin-normalize-output.parquet"

aws --endpoint-url "$FOGBANK_ENDPOINT" s3 cp \
  fixtures/transform/partition-basic/input.parquet "$input"

SPEC_TMP="$(mktemp "${TMPDIR:-/tmp}/plugin-s3-demo.XXXXXX.yaml")"
cat > "$SPEC_TMP" <<EOF
schema-version: 1
input: $input
output: $output
options:
  overwrite: true
steps:
  - operation:
      type: plugin
      plugin: normalize-strings
      config:
        columns:
          - email
EOF

cargo run -p parqonaut-cli --bin prqnt --features s3 -- transform --spec "$SPEC_TMP" --json > target/plugin-s3-demo-report.json

PY="$PWD/python/parqonaut_plugins/.venv/bin/python"
"$PY" - <<'PY'
import json
import subprocess
import sys

report = json.load(open("target/plugin-s3-demo-report.json"))
assert report.get("intermediate_files_created", 0) == 0, report
assert report.get("files_written", 0) >= 1, report

endpoint = __import__("os").environ["FOGBANK_ENDPOINT"]
bucket = __import__("os").environ["FOGBANK_BUCKET"]
out_key = "datasets/transform/plugin-normalize-output.parquet"
local = "target/plugin-s3-demo-out.parquet"
subprocess.check_call(
    ["aws", "--endpoint-url", endpoint, "s3", "cp", f"s3://{bucket}/{out_key}", local]
)

import pyarrow.parquet as pq

table = pq.read_table(local, columns=["email"])
before = pq.read_table("fixtures/transform/partition-basic/input.parquet", columns=["email"])
assert table.schema.equals(before.schema, check_metadata=False)
assert table.num_rows == before.num_rows
# normalize-strings trims whitespace; ensure no leading/trailing spaces in output emails
emails = table.column("email").to_pylist()
assert all(e == e.strip() for e in emails if e is not None), "normalization failed"
print("plugin-s3-demo assertions ok")
PY
