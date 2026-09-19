#!/usr/bin/env bash
# Print the bootstrap bearer token used for API integration (deterministic unless overridden).
set -euo pipefail
source "$(dirname "$0")/env.sh"
printf '%s\n' "${PRQNT_BOOTSTRAP_ADMIN_TOKEN}"
