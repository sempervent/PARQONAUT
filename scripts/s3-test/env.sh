#!/usr/bin/env bash
# Sourceable environment for FOGBANK / S3 integration tests.
# Usage: source scripts/s3-test/env.sh

_legacy_endpoint="${MINIO_ENDPOINT:-}"
export PARQONAUT_S3_ENDPOINT="${PARQONAUT_S3_ENDPOINT:-${_legacy_endpoint:-http://127.0.0.1:9000}}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export PARQONAUT_S3_PATH_STYLE="${PARQONAUT_S3_PATH_STYLE:-true}"
export PARQONAUT_S3_BUCKET="${PARQONAUT_S3_BUCKET:-parqonaut-test}"
export FOGBANK_BUCKET="${FOGBANK_BUCKET:-fogbank}"
# Avoid 5 MiB multipart splits on ~5–6 MiB Parquet objects (see write_s3_roundtrip regression).
export PARQONAUT_S3_PART_SIZE_BYTES="${PARQONAUT_S3_PART_SIZE_BYTES:-67108864}"
export FOGBANK_DATASET_PREFIX="${FOGBANK_DATASET_PREFIX:-datasets}"

# Back-compat for S3Config::from_env and older scripts
export MINIO_ENDPOINT="${PARQONAUT_S3_ENDPOINT}"
export MINIO_PATH_STYLE="${PARQONAUT_S3_PATH_STYLE}"
