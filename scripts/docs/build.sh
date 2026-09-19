#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

export PATH="${REPO_ROOT}/.tools/bin:${PATH}"
bash "$(dirname "$0")/install-mdbook.sh"

cd "${REPO_ROOT}"
cargo build -q -p parqonaut-cli --bin prqnt
cargo xtask docs cli
cargo xtask docs openapi

rm -rf "${DOCS_OUTPUT}"
"${REPO_ROOT}/.tools/bin/mdbook" build --dest-dir "${DOCS_OUTPUT}"

RUSTDOC_OUT="${REPO_ROOT}/target/rustdoc-cargo"
rm -rf "${RUSTDOC_OUT}"
cargo doc --workspace --no-deps --target-dir "${RUSTDOC_OUT}"
rm -rf "${DOCS_OUTPUT}/rustdoc"
cp -a "${RUSTDOC_OUT}/doc" "${DOCS_OUTPUT}/rustdoc"
cat >"${DOCS_OUTPUT}/rustdoc/index.html" <<'EOF'
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=prqnt/index.html" />
    <title>PARQONAUT Rust API</title>
  </head>
  <body>
    <p><a href="prqnt/index.html">PARQONAUT Rust API (prqnt)</a></p>
  </body>
</html>
EOF

mkdir -p "${DOCS_OUTPUT}/openapi"
cp "${REPO_ROOT}/fixtures/api/openapi-v1.json" "${DOCS_OUTPUT}/openapi/openapi.json"

echo "documentation site: ${DOCS_OUTPUT}"
