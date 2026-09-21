#!/usr/bin/env bash
set -euo pipefail
export PARQONAUT_PLUGIN_ROOTS="${PARQONAUT_PLUGIN_ROOTS:-$PWD/fixtures/plugins}"
export PARQONAUT_PLUGIN_SDK_PATH="${PARQONAUT_PLUGIN_SDK_PATH:-$PWD/python/parqonaut_plugins/src}"
just s3-up
source scripts/s3-test/env.sh
just s3-fixtures
input="s3://${FOGBANK_BUCKET}/datasets/transform/partition-basic/input.parquet"
output="s3://${FOGBANK_BUCKET}/datasets/transform/plugin-normalize-output.parquet"
aws --endpoint-url "$FOGBANK_ENDPOINT" s3 cp \
  fixtures/transform/partition-basic/input.parquet "$input" 2>/dev/null || \
  aws --endpoint-url "$FOGBANK_ENDPOINT" s3 cp \
  fixtures/transform/partition-basic/input.parquet "$input"
spec="$(mktemp)"
cat > "$spec" <<EOF
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
cargo run -p parqonaut-cli --bin prqnt --features s3 -- transform --spec "$spec"
rm -f "$spec"
just s3-down
