#!/usr/bin/env bash
# Fail if active code reintroduces predecessor product names or legacy phase/bin identifiers.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0
phase_pattern='phase[0-9]+|Phase [0-9]+'
legacy_bin_pattern='name = "parqonaut"|name = "paraclete-http"|generate-phase[0-9]'
legacy_name_pattern='paraclete|parqknife|streaming-parquet|\bmaw\b'

is_allowlisted_content_file() {
  local f="$1"
  case "$f" in
    ./NOTICE|./THIRD_PARTY_LICENSES.md|./CHANGELOG.md) return 0 ;;
    ./docs/provenance.md|./docs/migration-analysis.md) return 0 ;;
    ./docs/history/*) return 0 ;;
    ./docs/development/legacy-name-audit-v0.10.md) return 0 ;;
    ./docs/development/naming-migration-v0.6.md) return 0 ;;
    ./docs/development/*recon*) return 0 ;;
    ./docs/development/application-server-agent-plan-v0.7.md) return 0 ;;
    ./docs/adr/ADR-0001-consolidation-strategy.md|./docs/adr/ADR-0002-coexisting-arrow-stacks.md|./docs/adr/ADR-0003-unified-cli-architecture.md) return 0 ;;
    ./docs/migration-analysis.md|./docs/release-notes-*) return 0 ;;
    ./scripts/check-active-naming.sh|./scripts/check-active-naming-selftest.sh) return 0 ;;
  esac
  return 1
}

echo "=== workspace package names (no paraclete-*) ==="
if command -v cargo >/dev/null 2>&1; then
  bad_pkgs="$(
    cargo metadata --no-deps --format-version 1 \
      | python3 -c "
import json, sys
data = json.load(sys.stdin)
bad = sorted({p['name'] for p in data['packages'] if p['name'].startswith('paraclete-')})
for n in bad:
    print(n)
" 2>/dev/null || true
  )"
  if [[ -n "${bad_pkgs:-}" ]]; then
    echo "$bad_pkgs"
    fail=1
  fi
else
  echo "WARN: cargo not found; skipping package name check"
fi

echo "=== Cargo.lock package names (no paraclete-*) ==="
if [[ -f Cargo.lock ]]; then
  if grep -En 'name = "paraclete-' Cargo.lock 2>/dev/null; then
    fail=1
  fi
fi

legacy_search_roots=(
  crates
  python
  scripts
  xtask
  benches
  .github
  fixtures
  docs
  Cargo.toml
  Cargo.lock
  Justfile
  README.md
)

echo "=== active predecessor identifiers ==="
for root in "${legacy_search_roots[@]}"; do
  [[ -e "$root" ]] || continue
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    rel="./${file#./}"
    is_allowlisted_content_file "$rel" && continue
    if grep -EinqI "$legacy_name_pattern" "$file" 2>/dev/null; then
      grep -EinI "$legacy_name_pattern" "$file" 2>/dev/null || true
      fail=1
    fi
  done < <(
    if [[ -f "$root" ]]; then
      printf '%s\n' "$root"
    else
      find "$root" -type f \
        ! -path '*/target/*' \
        ! -path '*/.git/*' \
        ! -path '*/__pycache__/*' \
        ! -path '*/.venv/*' \
        2>/dev/null
    fi
  )
done

echo "=== Rust identifiers (paraclete_/Paraclete/PARACLETE) ==="
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  if grep -En 'paraclete_|Paraclete|PARACLETE' "$file" 2>/dev/null; then
    fail=1
  fi
done < <(find crates xtask -name '*.rs' ! -path '*/target/*' 2>/dev/null)

echo "=== Python identifiers (paraclete_plugins / Paraclete) ==="
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  if grep -En 'paraclete_plugins|Paraclete' "$file" 2>/dev/null; then
    fail=1
  fi
done < <(find python -name '*.py' ! -path '*/__pycache__/*' 2>/dev/null)

echo "=== active path names (numbered phase) ==="
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  case "$path" in
    ./CHANGELOG.md|./docs/history/*|./docs/provenance.md|./docs/development/*recon*) continue ;;
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
  rel="./${file#./}"
  is_allowlisted_content_file "$rel" && continue
  case "$rel" in
    crates/*|fixtures/*|scripts/*|.github/*|Justfile|README.md|docs/*) ;;
    *) continue ;;
  esac
  case "$rel" in
    docs/history/*) continue ;;
  esac
  if grep -En "$phase_pattern" "$file" 2>/dev/null; then
    fail=1
  fi
done < <(find crates fixtures scripts .github docs -type f 2>/dev/null; [[ -f Justfile ]] && printf '%s\n' Justfile; [[ -f README.md ]] && printf '%s\n' README.md)

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
