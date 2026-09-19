#!/usr/bin/env bash
# Sourceable environment for Phase 5 FOGBANK / S3 integration tests.
# Usage: source scripts/phase5/env.sh

_legacy_endpoint="${MINIO_ENDPOINT:-}"
export PARQONAUT_S3_ENDPOINT="${PARQONAUT_S3_ENDPOINT:-${_legacy_endpoint:-http://127.0.0.1:9000}}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export PARQONAUT_S3_PATH_STYLE="${PARQONAUT_S3_PATH_STYLE:-true}"
export PARQONAUT_S3_BUCKET="${PARQONAUT_S3_BUCKET:-parqonaut-test}"
export FOGBANK_BUCKET="${FOGBANK_BUCKET:-fogbank}"
export FOGBANK_DATASET_PREFIX="${FOGBANK_DATASET_PREFIX:-datasets}"

# Back-compat for S3Config::from_env and older scripts
export MINIO_ENDPOINT="${PARQONAUT_S3_ENDPOINT}"
export MINIO_PATH_STYLE="${PARQONAUT_S3_PATH_STYLE}"
