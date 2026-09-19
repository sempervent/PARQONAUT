#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

export PATH="${REPO_ROOT}/.tools/bin:${PATH}"
bash "$(dirname "$0")/install-mdbook.sh"

cd "${REPO_ROOT}"
cargo build -q -p parqonaut-cli --bin prqnt
cargo xtask docs cli --check
cargo xtask docs openapi --check
python3 "$(dirname "$0")/check-links.py"
scripts/check-active-naming.sh

rm -rf "${DOCS_OUTPUT}"
"${REPO_ROOT}/.tools/bin/mdbook" build --dest-dir "${DOCS_OUTPUT}"

if find "${DOCS_OUTPUT}" -path '*history*' -name '*.html' | grep -q .; then
  echo "history pages leaked into site output" >&2
  find "${DOCS_OUTPUT}" -path '*history*' -name '*.html'
  exit 1
fi

echo "docs-check: OK"
