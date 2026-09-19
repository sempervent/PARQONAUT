#!/usr/bin/env python3
"""Check internal Markdown links under docs/ (stdlib only)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
LINK_RE = re.compile(r"\]\(([^)]+)\)")
SKIP_PREFIXES = ("http://", "https://", "mailto:", "#")


def targets_from_summary() -> list[Path]:
    summary = DOCS / "SUMMARY.md"
    text = summary.read_text(encoding="utf-8")
    out: list[Path] = []
    for m in LINK_RE.finditer(text):
        href = m.group(1).strip()
        if href.startswith(SKIP_PREFIXES):
            continue
        if href.startswith("rustdoc/"):
            continue
        path = href.split("#", 1)[0]
        if not path:
            continue
        out.append((DOCS / path).resolve())
    return out


def scan_file(md: Path, errors: list[str]) -> None:
    base = md.parent
    for m in LINK_RE.finditer(md.read_text(encoding="utf-8")):
        href = m.group(1).strip()
        if href.startswith(SKIP_PREFIXES):
            continue
        if href.startswith("rustdoc/"):
            continue
        path_part, _, anchor = href.partition("#")
        if not path_part:
            continue
        target = (base / path_part).resolve()
        if not target.exists():
            errors.append(f"{md.relative_to(REPO)}: missing target {href}")
            continue
        if target.is_dir():
            errors.append(f"{md.relative_to(REPO)}: directory link {href}")


def main() -> int:
    errors: list[str] = []
    for p in targets_from_summary():
        if not p.exists():
            errors.append(f"SUMMARY.md: missing {p.relative_to(REPO)}")
    for md in DOCS.rglob("*.md"):
        if "history" in md.parts or "development" in md.parts:
            continue
        if md.name == "SUMMARY.md":
            continue
        scan_file(md, errors)
    if errors:
        print("link check failed:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1
    print("link check: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
