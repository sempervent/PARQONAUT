#!/usr/bin/env bash
# Verify check-active-naming.sh fails on injected predecessor branding.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

trap 'rm -rf crates/_naming_selftest_stub' EXIT
mkdir -p crates/_naming_selftest_stub
printf '%s\n' "// $(printf 'para%s' 'clete') selftest injection" > crates/_naming_selftest_stub/lib.rs

if bash scripts/check-active-naming.sh; then
  echo "naming-check selftest: expected FAIL"
  exit 1
fi

echo "naming-check selftest: PASS"
