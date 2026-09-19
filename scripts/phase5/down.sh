#!/usr/bin/env bash
set -euo pipefail
docker rm -f parqonaut-rustfs parqonaut-phase5-rustfs >/dev/null 2>&1 || true
