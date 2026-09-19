#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.phase5.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found; install Docker to run FOGBANK MinIO locally." >&2
  exit 1
fi

docker compose -f "$COMPOSE_FILE" up -d

"$ROOT/scripts/phase5/wait-minio.sh" 90

echo
echo "FOGBANK MinIO is up."
echo "  S3 API:   http://127.0.0.1:9000"
echo "  Console:  http://127.0.0.1:9001  (minioadmin / minioadmin)"
echo "  Buckets:  fogbank, parqonaut-test"
echo
echo "Next: source scripts/phase5/env.sh && scripts/phase5/fixtures.sh"
