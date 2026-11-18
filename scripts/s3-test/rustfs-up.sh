#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/s3-test/rustfs-image.env"

docker pull "${RUSTFS_IMAGE}@${RUSTFS_IMAGE_DIGEST}"

docker rm -f parqonaut-rustfs >/dev/null 2>&1 || true
docker volume rm parqonaut-rustfs-data >/dev/null 2>&1 || true
docker volume create parqonaut-rustfs-data >/dev/null

docker run -d \
  --name parqonaut-rustfs \
  -p 9000:9000 \
  -v parqonaut-rustfs-data:/data \
  -e RUSTFS_VOLUMES=/data \
  -e RUSTFS_ADDRESS=0.0.0.0:9000 \
  -e RUSTFS_CONSOLE_ENABLE=false \
  -e RUSTFS_ACCESS_KEY=rustfsadmin \
  -e RUSTFS_SECRET_KEY=rustfsadmin \
  -e RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true \
  "${RUSTFS_IMAGE}@${RUSTFS_IMAGE_DIGEST}"

export PARQONAUT_S3_ENDPOINT="${PARQONAUT_S3_ENDPOINT:-http://127.0.0.1:9000}"
"$ROOT/scripts/s3-test/wait-rustfs.sh"

# shellcheck source=/dev/null
source "$ROOT/scripts/s3-test/env.sh"

if command -v aws >/dev/null 2>&1; then
  aws --endpoint-url "$PARQONAUT_S3_ENDPOINT" s3 mb "s3://${FOGBANK_BUCKET}" 2>/dev/null || true
  aws --endpoint-url "$PARQONAUT_S3_ENDPOINT" s3 mb "s3://${PARQONAUT_S3_BUCKET}" 2>/dev/null || true
  aws --endpoint-url "$PARQONAUT_S3_ENDPOINT" s3 mb "s3://contract-test" 2>/dev/null || true
fi

echo "RustFS FOGBANK endpoint: ${PARQONAUT_S3_ENDPOINT}"
