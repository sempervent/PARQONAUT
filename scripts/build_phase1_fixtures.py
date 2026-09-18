#!/usr/bin/env python3
"""Generate Phase 1 Parquet fixtures (requires pyarrow in the paraclete_plugins dev env)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    root = Path(__file__).resolve().parents[1] / "fixtures" / "phase1"
    root.mkdir(parents=True, exist_ok=True)

    # Single healthy Parquet (one row group, modest size).
    single = root / "single_parquet"
    single.mkdir(exist_ok=True)
    t = pa.table({"id": list(range(500)), "v": [float(i) for i in range(500)]})
    pq.write_table(t, single / "data.parquet", compression="snappy")

    # Dataset directory: two parquet files.
    ds = root / "parquet_dataset"
    ds.mkdir(exist_ok=True)
    pq.write_table(pa.table({"x": [1, 2], "y": ["a", "b"]}), ds / "part-000.parquet")
    pq.write_table(pa.table({"x": [3, 4], "y": ["c", "d"]}), ds / "part-001.parquet")

    # Mixed format directory.
    mixed = root / "mixed_dir"
    mixed.mkdir(exist_ok=True)
    pq.write_table(pa.table({"k": [1]}), mixed / "a.parquet")
    (mixed / "b.csv").write_text("c,d\n1,2\n")

    # Very small Parquet on disk (< 4KiB) for file_too_small.
    tiny = root / "tiny_parquet"
    tiny.mkdir(exist_ok=True)
    small = pa.table({"n": [1]})
    pq.write_table(small, tiny / "micro.parquet", compression=None)

    # Many tiny row groups for row_group_suspiciously_small heuristic.
    frag = root / "fragmented_rg"
    frag.mkdir(exist_ok=True)
    big = pa.table({"z": list(range(2000))})
    pq.write_table(
        big,
        frag / "fragmented.parquet",
        row_group_size=5,
        compression="snappy",
    )

    # Hive inconsistent keys under one scan root.
    hive = root / "hive_inconsistent"
    (hive / "dt=1").mkdir(parents=True, exist_ok=True)
    (hive / "region=eu").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"a": [1]}), hive / "dt=1" / "p.parquet")
    pq.write_table(pa.table({"a": [2]}), hive / "region=eu" / "p.parquet")

    print("Wrote fixtures under", root)


if __name__ == "__main__":
    main()
