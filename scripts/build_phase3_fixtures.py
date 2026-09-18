#!/usr/bin/env python3
"""Generate Phase 3 fixtures (schema split under one anchor, nested paths)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    root = Path(__file__).resolve().parents[1] / "fixtures" / "phase3"
    root.mkdir(parents=True, exist_ok=True)

    split = root / "schema_split_flat"
    split.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"only_a": [1, 2]}), split / "alpha.parquet")
    pq.write_table(pa.table({"only_b": ["x", "y"]}), split / "beta.parquet")

    nested = root / "nested_mixed_anchor"
    nested.mkdir(parents=True, exist_ok=True)
    (nested / "area").mkdir(exist_ok=True)
    (nested / "area" / "deep").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"k": [1]}), nested / "area" / "one.parquet")
    pq.write_table(pa.table({"k": [2]}), nested / "area" / "deep" / "two.parquet")

    print("Wrote fixtures under", root)


if __name__ == "__main__":
    main()
