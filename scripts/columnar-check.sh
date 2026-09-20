#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if rg -n 'arrow2|parquet2' crates --glob '*.rs' --glob 'Cargo.toml' 2>/dev/null; then
  echo "columnar-check: FAIL — arrow2/parquet2 in product crates" >&2
  exit 1
fi

if rg -n '(^|\s)arrow\s*=\s*"[0-9]' crates --glob 'Cargo.toml' 2>/dev/null; then
  echo "columnar-check: FAIL — direct arrow version pin (use workspace)" >&2
  exit 1
fi

if rg -n 'parquet\s*=\s*(\{ version = "[0-9]|"[0-9])' crates --glob 'Cargo.toml' 2>/dev/null; then
  echo "columnar-check: FAIL — direct parquet version pin (use workspace)" >&2
  exit 1
fi

echo "columnar-check: PASS"
