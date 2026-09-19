#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/phase5/env.sh
source "$ROOT/scripts/phase5/env.sh"

LOCAL_ROOT="${1:-$ROOT/fixtures/phase5/local}"
UPLOAD="${FOGBANK_UPLOAD:-1}"

echo "=== generating Phase 5 FOGBANK local fixtures under ${LOCAL_ROOT} ==="
cargo run -p parqonaut-storage --features "s3,fogbank-fixtures" --bin generate-phase5-fogbank-fixtures -- "$LOCAL_ROOT"

if [[ "$UPLOAD" == "0" || "$UPLOAD" == "false" ]]; then
  echo "FOGBANK_UPLOAD=0 — skipping S3 upload"
  exit 0
fi

if ! curl -sf "${PARQONAUT_S3_ENDPOINT}/health/ready" >/dev/null 2>&1 \
  && ! curl -sf "${PARQONAUT_S3_ENDPOINT}/health/live" >/dev/null 2>&1; then
  echo "S3 endpoint not reachable at ${PARQONAUT_S3_ENDPOINT}; local fixtures only." >&2
  echo "Run: scripts/phase5/up.sh && scripts/phase5/fixtures.sh" >&2
  exit 0
fi

echo "=== uploading fixtures to s3://${FOGBANK_BUCKET}/ ==="
cargo run -p parqonaut-storage --features "s3,fogbank-fixtures" --bin generate-phase5-fogbank-fixtures -- \
  "$LOCAL_ROOT" --upload

echo "FOGBANK fixtures ready."
