#!/usr/bin/env python3
"""Generate Phase 2 fixtures (pyarrow for Parquet; plain files for corrupt CSV/JSON)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    root = Path(__file__).resolve().parents[1] / "fixtures" / "phase2"
    root.mkdir(parents=True, exist_ok=True)

    corrupt = root / "corrupt_parquet"
    corrupt.mkdir(parents=True, exist_ok=True)
    (corrupt / "bad.parquet").write_bytes(b"NOTPAR1" + b"\x00" * 120)

    multi = root / "multi_dataset"
    (multi / "sales").mkdir(parents=True, exist_ok=True)
    (multi / "logs").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": [1, 2], "v": [10, 20]}), multi / "sales" / "a.parquet")
    pq.write_table(pa.table({"id": [3], "msg": ["x"]}), multi / "logs" / "b.parquet")

    mixed = root / "mixed_siblings"
    mixed.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"k": [1]}), mixed / "table.parquet")
    (mixed / "side.csv").write_text("name,score\nada,99\nbob,12\n")
    (mixed / "meta.json").write_text('{"schema_version":1,"owner":"fixture","tags":["a","b"]}\n')

    flat = root / "unpartitioned_pair"
    flat.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"x": [1]}), flat / "one.parquet")
    pq.write_table(pa.table({"x": [2]}), flat / "two.parquet")

    shallow = root / "shallow_text"
    shallow.mkdir(parents=True, exist_ok=True)
    (shallow / "rows.ndjson").write_text('{"evt":"open","id":1}\n{"evt":"close","id":1}\n')

    print("Wrote fixtures under", root)


if __name__ == "__main__":
    main()
