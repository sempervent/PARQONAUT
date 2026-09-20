#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

source scripts/s3-test/env.sh 2>/dev/null || {
  echo "pipeline-s3-demo: set up RustFS via 'just s3-up' and source env" >&2
  exit 1
}

just s3-up
just s3-fixtures

LOCAL="$ROOT/target/pipeline-s3-demo"
rm -rf "$LOCAL"
mkdir -p "$LOCAL"
cp fixtures/transform/partition-basic/input.parquet "$LOCAL/in.parquet"

S3_IN="s3://${FOGBANK_BUCKET}/pipeline-demo/in.parquet"
S3_OUT="s3://${FOGBANK_BUCKET}/pipeline-demo/out.parquet"

aws s3 cp "$LOCAL/in.parquet" "$S3_IN" --endpoint-url "$PARQONAUT_S3_ENDPOINT"

echo "=== remote rewrite (storage columnar) ==="
cargo run -q -p parqonaut-cli --bin prqnt --features s3 -- rewrite "$S3_IN" "$S3_OUT" --compression zstd

aws s3 ls "s3://${FOGBANK_BUCKET}/pipeline-demo/" --endpoint-url "$PARQONAUT_S3_ENDPOINT" | grep -q out.parquet
echo "pipeline-s3-demo: PASS"
