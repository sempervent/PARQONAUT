#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/s3-test/env.sh
source "$ROOT/scripts/s3-test/env.sh"

echo "=== S3 resume demo (RustFS + FOGBANK) ==="
just -f "$ROOT/Justfile" s3-up
just -f "$ROOT/Justfile" s3-fixtures

rm -rf "$ROOT/target/s3-resume-journal" "$ROOT/target/s3-resume-out"
mkdir -p "$ROOT/target/s3-resume-out"

PLAN="$ROOT/target/s3-resume.plan.json"
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch plan \
  --config "$ROOT/fixtures/object-storage/batch-resume.toml" \
  --output "$PLAN"

echo "=== repair (interrupt after 1 completed dataset) ==="
set +e
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch repair --plan "$PLAN" --jobs 1 --interrupt-after 1
code=$?
set -e
test "$code" -eq 130 -o "$code" -eq 2 -o "$code" -eq 0

run_dir=$(find "$ROOT/target/s3-resume-journal" -name journal.sqlite 2>/dev/null | head -1 | xargs dirname)
test -n "$run_dir"
test -f "$run_dir/journal.sqlite"

echo "=== journal present; resume ==="
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch resume --run-dir "$run_dir" --jobs 1 || test $? -eq 2

echo "=== remote verify ==="
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch verify --run-dir "$run_dir"

echo "=== second resume is no-op ==="
cargo run -p parqonaut-cli --bin prqnt --features s3 -- batch resume --run-dir "$run_dir" --jobs 1

echo "s3-resume-demo PASS"
