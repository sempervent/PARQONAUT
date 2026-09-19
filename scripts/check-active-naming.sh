#!/usr/bin/env bash
# Fail if active architecture reintroduces numbered development-phase names or legacy product binaries.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0

phase_pattern='phase[0-9]+|Phase [0-9]+'
legacy_bin_pattern='name = "parqonaut"|name = "paraclete-http"|generate-phase[0-9]'

echo "=== active path names (numbered phase) ==="
while IFS= read -r path; do
  case "$path" in
    ./CHANGELOG.md|./docs/history/*|./docs/provenance.md|./docs/development/naming-migration-v0.6.md) continue ;;
  esac
  echo "FORBIDDEN PATH: $path"
  fail=1
done < <(
  find . \
    -not -path './.git/*' \
    -not -path './target/*' \
    \( -iname '*phase1*' -o -iname '*phase2*' -o -iname '*phase3*' \
       -o -iname '*phase4*' -o -iname '*phase5*' -o -iname '*phase6*' \) \
    -print 2>/dev/null | sort
)

echo "=== active file contents (numbered phase) ==="
if rg -n "$phase_pattern" \
  --glob '!target/**' \
  --glob '!.git/**' \
  --glob '!CHANGELOG.md' \
  --glob '!docs/history/**' \
  --glob '!docs/adr/**' \
  --glob '!docs/provenance.md' \
  --glob '!docs/migration-analysis.md' \
  --glob '!docs/release-notes-*.md' \
  --glob '!**/migrations/**' \
  --glob '!scripts/check-active-naming.sh' \
  --glob '!fixtures/parquet/**' \
  crates fixtures scripts .github Justfile README.md docs 2>/dev/null; then
  fail=1
fi

echo "=== public binary definitions ==="
if rg -n "$legacy_bin_pattern" crates Cargo.toml .github 2>/dev/null; then
  fail=1
fi

echo "=== product binary must be prqnt ==="
if ! rg -q 'name = "prqnt"' crates/parqonaut-cli/Cargo.toml; then
  echo "MISSING: [[bin]] name = \"prqnt\" in parqonaut-cli"
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "naming-check: FAIL"
  exit 1
fi

echo "naming-check: PASS"
