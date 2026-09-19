#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

if command -v mdbook >/dev/null 2>&1; then
  installed="$(mdbook --version 2>/dev/null | awk '{print $2}' || true)"
  if [[ "${installed}" == "${MDBOOK_VERSION}" ]]; then
    exit 0
  fi
fi

case "$(uname -sm)" in
  "Linux x86_64")
    asset="mdbook-v${MDBOOK_VERSION}-x86_64-unknown-linux-gnu.tar.gz"
    ;;
  "Linux aarch64" | "Linux arm64")
    asset="mdbook-v${MDBOOK_VERSION}-aarch64-unknown-linux-musl.tar.gz"
    ;;
  "Darwin arm64")
    asset="mdbook-v${MDBOOK_VERSION}-aarch64-apple-darwin.tar.gz"
    ;;
  "Darwin x86_64")
    asset="mdbook-v${MDBOOK_VERSION}-x86_64-apple-darwin.tar.gz"
    ;;
  *)
    echo "unsupported platform for mdBook install: $(uname -sm)" >&2
    exit 1
    ;;
esac

url="https://github.com/rust-lang/mdBook/releases/download/v${MDBOOK_VERSION}/${asset}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

curl -fsSL "${url}" -o "${tmpdir}/mdbook.tgz"
tar -xzf "${tmpdir}/mdbook.tgz" -C "${tmpdir}"
mkdir -p "${REPO_ROOT}/.tools/bin"
install -m 0755 "${tmpdir}/mdbook" "${REPO_ROOT}/.tools/bin/mdbook"
export PATH="${REPO_ROOT}/.tools/bin:${PATH}"

mdbook --version
