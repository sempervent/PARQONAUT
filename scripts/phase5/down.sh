#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.phase5.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found; nothing to stop." >&2
  exit 0
fi

docker compose -f "$COMPOSE_FILE" down "$@"

echo "FOGBANK MinIO stopped."
