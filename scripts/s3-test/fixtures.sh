#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/s3-test/env.sh
source "$ROOT/scripts/s3-test/env.sh"

LOCAL_ROOT="${1:-$ROOT/fixtures/object-storage/local}"
UPLOAD="${FOGBANK_UPLOAD:-1}"

echo "=== generating FOGBANK local fixtures under ${LOCAL_ROOT} ==="
cargo xtask fixtures object-storage "$LOCAL_ROOT"

if [[ "$UPLOAD" == "0" || "$UPLOAD" == "false" ]]; then
  echo "FOGBANK_UPLOAD=0 — skipping S3 upload"
  exit 0
fi

if ! curl -sf "${PARQONAUT_S3_ENDPOINT}/health/ready" >/dev/null 2>&1 \
  && ! curl -sf "${PARQONAUT_S3_ENDPOINT}/health/live" >/dev/null 2>&1; then
  echo "S3 endpoint not reachable at ${PARQONAUT_S3_ENDPOINT}; local fixtures only." >&2
  echo "Run: scripts/s3-test/up.sh && scripts/s3-test/fixtures.sh" >&2
  exit 0
fi

echo "=== uploading fixtures to s3://${FOGBANK_BUCKET}/ ==="
PARQONAUT_XTASK_UPLOAD=1 cargo xtask fixtures object-storage "$LOCAL_ROOT"

echo "FOGBANK fixtures ready."
