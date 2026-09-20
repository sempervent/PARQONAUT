#!/usr/bin/env bash
# v0.9 local pipeline: CSV schema drift → unify → fused transform → partition (no .parqonaut-spec staging).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT="${PARQONAUT_PIPELINE_DEMO_OUT:-target/pipeline-demo}"
rm -rf "$OUT"
mkdir -p "$OUT/src"

cat >"$OUT/src/a.csv" <<'CSV'
region,id,value
north,1,10
CSV
cat >"$OUT/src/b.csv" <<'CSV'
region,id,value
south,2,20.5
CSV

echo "=== stream convert (schema widen) ==="
cargo run -q -p parqonaut-cli --bin prqnt -- convert "$OUT/src/"*.csv \
  -o "$OUT/unified.parquet" --out-format parquet --schema-conflicts widen

echo "=== fused transform spec (rewrite → partition) ==="
cat >"$OUT/spec.yaml" <<YAML
schema-version: 1
input: $OUT/unified.parquet
output: $OUT/partitioned
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

cargo run -q -p parqonaut-cli --bin prqnt -- transform --spec "$OUT/spec.yaml"

if find "$OUT" -name '.parqonaut-spec-*' 2>/dev/null | grep -q .; then
  echo "pipeline-demo: FAIL — unexpected .parqonaut-spec staging under $OUT" >&2
  exit 1
fi

test -d "$OUT/partitioned"
echo "pipeline-demo: PASS (output under $OUT)"
