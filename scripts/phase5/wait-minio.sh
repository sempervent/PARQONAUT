#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/phase5/env.sh
source "$ROOT/scripts/phase5/env.sh"

HOST="${MINIO_ENDPOINT#http://}"
HOST="${HOST#https://}"
HOST="${HOST%%/*}"

deadline=$((SECONDS + ${1:-60}))
while (( SECONDS < deadline )); do
  if curl -sf "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    echo "MinIO live at ${MINIO_ENDPOINT}"
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for MinIO at ${MINIO_ENDPOINT}" >&2
exit 1
