#!/usr/bin/env bash
# Fail if active architecture reintroduces numbered development-phase names or legacy product binaries.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0
phase_pattern='phase[0-9]+|Phase [0-9]+'
legacy_bin_pattern='name = "parqonaut"|name = "paraclete-http"|generate-phase[0-9]'

should_skip_content_file() {
  local f="$1"
  case "$f" in
    */target/*|*/.git/*) return 0 ;;
    CHANGELOG.md|*/CHANGELOG.md) return 0 ;;
    docs/history/*|docs/adr/*|docs/provenance.md) return 0 ;;
    docs/migration-analysis.md|docs/release-notes-*) return 0 ;;
    docs/development/*) return 0 ;;
    */migrations/*|fixtures/parquet/*) return 0 ;;
    *check-active-naming.sh) return 0 ;;
  esac
  return 1
}

echo "=== active path names (numbered phase) ==="
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  case "$path" in
    ./CHANGELOG.md|./docs/history/*|./docs/provenance.md|./docs/development/*) continue ;;
  esac
  echo "FORBIDDEN PATH: $path"
  fail=1
done < <(
  find . \
    -not -path './.git/*' \
    -not -path './target/*' \
    -not -path './docs/history/*' \
    \( -iname '*phase1*' -o -iname '*phase2*' -o -iname '*phase3*' \
       -o -iname '*phase4*' -o -iname '*phase5*' -o -iname '*phase6*' \) \
    -print 2>/dev/null | sort
)

echo "=== active file contents (numbered phase) ==="
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  should_skip_content_file "$file" && continue
  if grep -En "$phase_pattern" "$file" 2>/dev/null; then
    fail=1
  fi
done < <(
  find crates fixtures scripts .github -type f 2>/dev/null
  find docs -type f ! -path 'docs/history/*' 2>/dev/null
  [[ -f Justfile ]] && printf '%s\n' Justfile
  [[ -f README.md ]] && printf '%s\n' README.md
)

echo "=== public binary definitions ==="
while IFS= read -r file; do
  if grep -En "$legacy_bin_pattern" "$file" 2>/dev/null; then
    fail=1
  fi
done < <(find crates -name Cargo.toml; [[ -f Cargo.toml ]] && printf '%s\n' Cargo.toml)

echo "=== product binary must be prqnt ==="
if ! grep -q 'name = "prqnt"' crates/parqonaut-cli/Cargo.toml; then
  echo "MISSING: [[bin]] name = \"prqnt\" in parqonaut-cli"
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "naming-check: FAIL"
  exit 1
fi

echo "naming-check: PASS"
