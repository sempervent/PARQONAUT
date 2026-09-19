#!/usr/bin/env bash
# Prove S3 API semantics against the integration endpoint (RustFS in CI/local).
set -euo pipefail

source "$(dirname "$0")/env.sh"

BUCKET="parqonaut-smoke-$$"
KEY="smoke/object.txt"
BODY="parqonaut-smoke"

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION
ENDPOINT="${PARQONAUT_S3_ENDPOINT}"

if command -v aws >/dev/null 2>&1; then
  AWS=(aws --endpoint-url "$ENDPOINT")
  if [[ "${PARQONAUT_S3_PATH_STYLE:-true}" == "true" ]]; then
    AWS+=(--cli-read-timeout 30)
  fi
  "${AWS[@]}" s3 mb "s3://${BUCKET}" >/dev/null
  echo -n "$BODY" | "${AWS[@]}" s3 cp - "s3://${BUCKET}/${KEY}" >/dev/null
  "${AWS[@]}" s3api head-object --bucket "$BUCKET" --key "$KEY" >/dev/null
  out=$("${AWS[@]}" s3 cp "s3://${BUCKET}/${KEY}" -)
  test "$out" = "$BODY"
  "${AWS[@]}" s3 rm "s3://${BUCKET}/${KEY}" >/dev/null
  "${AWS[@]}" s3 rb "s3://${BUCKET}" >/dev/null
  echo "S3 smoke test PASS (aws cli)"
  exit 0
fi

if [[ "${PARQONAUT_S3_INTEGRATION:-}" == "1" ]]; then
  echo "PARQONAUT_S3_INTEGRATION=1 requires aws CLI for S3 smoke test" >&2
  exit 1
fi
echo "aws CLI not installed; skipping extended smoke (local dev only)" >&2
exit 0
