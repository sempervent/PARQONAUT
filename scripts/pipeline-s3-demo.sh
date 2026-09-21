#!/usr/bin/env bash
# v0.9.1 S3 pipeline: schema drift CSV → unify → fused transform → partition on RustFS.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

source scripts/s3-test/env.sh 2>/dev/null || {
  echo "pipeline-s3-demo: set up RustFS via 'just s3-up' and source env" >&2
  exit 1
}

PRQNT="${PRQNT:-$ROOT/target/debug/prqnt}"
DEMO_PREFIX="pipeline-s3-demo-$$"
LOCAL="$ROOT/target/pipeline-s3-demo"
RUSTFS_CID=""
RUSTFS_VOL=""

cleanup() {
  set +e
  if [[ -n "${RUSTFS_CID}" ]]; then
    docker rm -f "${RUSTFS_CID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${RUSTFS_VOL}" ]]; then
    docker volume rm "${RUSTFS_VOL}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${PARQONAUT_S3_ENDPOINT:-}" && -n "${FOGBANK_BUCKET:-}" ]]; then
    aws --endpoint-url "$PARQONAUT_S3_ENDPOINT" s3 rm "s3://${FOGBANK_BUCKET}/${DEMO_PREFIX}/" --recursive >/dev/null 2>&1 || true
  fi
  rm -rf "$LOCAL"
}
trap cleanup EXIT

if [[ -z "${PARQONAUT_S3_ENDPOINT:-}" ]]; then
  bash scripts/s3-test/up.sh
  source scripts/s3-test/env.sh
fi
bash scripts/s3-test/fixtures.sh

cargo build -q -p parqonaut-cli --bin prqnt --features s3
PRQNT="$ROOT/target/debug/prqnt"

rm -rf "$LOCAL"
mkdir -p "$LOCAL/src"

cat >"$LOCAL/src/a.csv" <<'CSV'
region,id,value
north,1,10
CSV
cat >"$LOCAL/src/b.csv" <<'CSV'
region,id,value
south,2,20.5
CSV

S3_BASE="s3://${FOGBANK_BUCKET}/${DEMO_PREFIX}"
S3_SRC="${S3_BASE}/src"
S3_UNIFIED="${S3_BASE}/unified.parquet"
S3_OUT="${S3_BASE}/partitioned"

inventory_remote() {
  aws --endpoint-url "$PARQONAUT_S3_ENDPOINT" s3 ls "s3://${FOGBANK_BUCKET}/${DEMO_PREFIX}/" --recursive \
    | awk '{print $4}' | grep -v '/\.parqonaut/' | grep -v '_staging' || true
}

before="$(inventory_remote)"
if [[ -n "$before" ]]; then
  echo "pipeline-s3-demo: unexpected pre-existing objects under ${DEMO_PREFIX}" >&2
  exit 1
fi

echo "=== stream convert (schema widen) local CSV → S3 Parquet ==="
"$PRQNT" convert "$LOCAL/src/"*.csv \
  -o "$S3_UNIFIED" --out-format parquet --schema-conflicts widen

echo "=== fused transform (rewrite → partition) on S3 ==="
cat >"$LOCAL/spec.yaml" <<YAML
schema-version: 1
input: $S3_UNIFIED
output: $S3_OUT
options:
  overwrite: true
steps:
  - operation:
      type: rewrite
      compression: zstd
  - operation:
      type: partition
      partition-by:
        - region
YAML

"$PRQNT" transform --spec "$LOCAL/spec.yaml"

after="$(inventory_remote)"
intermediate="$(echo "$after" | grep -E '\.parqonaut|staging|\.parqonaut-spec' || true)"
if [[ -n "$intermediate" ]]; then
  echo "pipeline-s3-demo: FAIL — intermediate objects: $intermediate" >&2
  exit 1
fi

partitions="$(aws --endpoint-url "$PARQONAUT_S3_ENDPOINT" s3 ls "${S3_OUT}/" --recursive | grep -c '\.parquet$' || true)"
if [[ "${partitions:-0}" -lt 2 ]]; then
  echo "pipeline-s3-demo: FAIL — expected partitioned outputs, got ${partitions}" >&2
  exit 1
fi

echo "=== scan partitioned output ==="
"$PRQNT" scan "${S3_OUT}/" --json | grep -q region

echo "pipeline-s3-demo: PASS (${partitions} partition files under ${S3_OUT})"
