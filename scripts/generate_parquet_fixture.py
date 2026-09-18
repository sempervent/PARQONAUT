#!/usr/bin/env python3
"""Write a tiny valid Parquet file under fixtures/parquet/.

Run from repo root or anywhere, with dev deps installed:

  cd python/paraclete_plugins && uv sync --group dev && uv run python ../../scripts/generate_parquet_fixture.py
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out = root / "fixtures" / "parquet" / "tiny.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "cara"]})
    pq.write_table(table, out)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
