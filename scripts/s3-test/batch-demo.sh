#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/s3-test/env.sh
source "$ROOT/scripts/s3-test/env.sh"

echo "=== Mixed-storage fleet batch demo (jobs=3) ==="
just -f "$ROOT/Justfile" s3-up
just -f "$ROOT/Justfile" s3-fixtures

rm -rf "$ROOT/target/mixed-storage-batch-journal" "$ROOT/target/mixed-storage-batch"
mkdir -p "$ROOT/target/mixed-storage-batch"

PLAN="$ROOT/target/mixed-storage-batch.plan.json"
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch plan \
  --config "$ROOT/fixtures/object-storage/batch-mixed.toml" \
  --output "$PLAN"

echo "=== batch repair (expect partial failure on bad-remote) ==="
set +e
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch repair --plan "$PLAN" --jobs 3
code=$?
set -e
test "$code" -eq 2 -o "$code" -eq 1

run_dir=$(find "$ROOT/target/mixed-storage-batch-journal" -name journal.sqlite 2>/dev/null | head -1 | xargs dirname)
test -n "$run_dir"

echo "=== batch status ==="
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch status --run-dir "$run_dir"

echo "=== batch verify (remote-aware) ==="
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch verify --run-dir "$run_dir" || true

test -f "$run_dir/report.json"
echo "mixed-storage-batch-demo PASS"
