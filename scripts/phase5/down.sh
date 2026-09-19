#!/usr/bin/env bash
set -euo pipefail
docker rm -f parqonaut-rustfs >/dev/null 2>&1 || true
