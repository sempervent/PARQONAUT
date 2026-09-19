#!/usr/bin/env bash
# Shared documentation build configuration (pinned toolchain versions).
set -euo pipefail

_docs_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${_docs_dir}/../.." && pwd)"

# Authoritative mdBook version — keep in sync with CI and `scripts/docs/install-mdbook.sh`.
export MDBOOK_VERSION="${MDBOOK_VERSION:-0.5.4}"

export DOCS_OUTPUT="${DOCS_OUTPUT:-${REPO_ROOT}/target/pages}"
export PATH="${REPO_ROOT}/.tools/bin:${PATH}"
