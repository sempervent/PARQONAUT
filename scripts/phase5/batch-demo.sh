#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/phase5/env.sh
source "$ROOT/scripts/phase5/env.sh"

echo "=== Phase 5 mixed fleet batch demo (jobs=3) ==="
just -f "$ROOT/Justfile" phase5-up
just -f "$ROOT/Justfile" phase5-fixtures

rm -rf "$ROOT/target/phase5-batch-journal" "$ROOT/target/phase5-batch"
mkdir -p "$ROOT/target/phase5-batch"

PLAN="$ROOT/target/phase5-batch.plan.json"
cargo run -p parqonaut-cli --features s3 -- batch plan \
  --config "$ROOT/fixtures/phase5/batch-mixed.toml" \
  --output "$PLAN"

echo "=== batch repair (expect partial failure on bad-remote) ==="
set +e
cargo run -p parqonaut-cli --features s3 -- batch repair --plan "$PLAN" --jobs 3
code=$?
set -e
test "$code" -eq 2 -o "$code" -eq 1

run_dir=$(find "$ROOT/target/phase5-batch-journal" -name journal.sqlite 2>/dev/null | head -1 | xargs dirname)
test -n "$run_dir"

echo "=== batch status ==="
cargo run -p parqonaut-cli --features s3 -- batch status --run-dir "$run_dir"

echo "=== batch verify (remote-aware) ==="
cargo run -p parqonaut-cli --features s3 -- batch verify --run-dir "$run_dir" || true

test -f "$run_dir/report.json"
echo "phase5-batch-demo PASS"
