#!/usr/bin/env bash
# TUI verification & interactive launch demo (non-TTY safe for CI).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

cargo test -p parqonaut-tui
cargo run -q -p parqonaut-cli --bin prqnt -- tui --help >/dev/null

if [[ -t 0 && -t 1 && -n "${PARQONAUT_TUI_INTERACTIVE:-}" ]]; then
  exec cargo run -p parqonaut-cli --bin prqnt -- tui --url "${PARQONAUT_SERVER_URL:-http://127.0.0.1:8080}"
fi

echo "tui-demo PASS (reducer, RBAC, 80x24 TestBackend render, panic-safe TerminalRestoreGuard, CLI surface)"
