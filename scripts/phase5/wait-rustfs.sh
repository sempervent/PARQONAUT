#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${PARQONAUT_S3_ENDPOINT:-http://127.0.0.1:9000}"
READY_URL="${ENDPOINT%/}/health/ready"
LIVE_URL="${ENDPOINT%/}/health/live"

for i in $(seq 1 60); do
  if curl -fsS "$READY_URL" >/dev/null 2>&1 || curl -fsS "$LIVE_URL" >/dev/null 2>&1; then
    echo "RustFS ready at ${ENDPOINT}"
    exit 0
  fi
  if [[ "$i" == "60" ]]; then
    echo "RustFS failed to become ready at ${ENDPOINT}" >&2
    docker logs parqonaut-rustfs 2>&1 || true
    exit 1
  fi
  sleep 2
done
