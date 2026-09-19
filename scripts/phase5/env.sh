#!/usr/bin/env bash
# Sourceable environment for Phase 5 FOGBANK / MinIO tests.
# Usage: source scripts/phase5/env.sh

export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export MINIO_REGION="${MINIO_REGION:-us-east-1}"
export MINIO_PATH_STYLE="${MINIO_PATH_STYLE:-true}"
export MINIO_BUCKET="${MINIO_BUCKET:-parqonaut-test}"
export FOGBANK_BUCKET="${FOGBANK_BUCKET:-fogbank}"
export FOGBANK_DATASET_PREFIX="${FOGBANK_DATASET_PREFIX:-datasets}"
