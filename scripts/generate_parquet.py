#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "numpy>=2.0",
#     "pyarrow>=17.0",
# ]
# ///

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def generate_table(rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)

    customer_ids = np.arange(1, rows + 1, dtype=np.int64)

    regions = np.array(
        ["north", "south", "east", "west", "central"],
        dtype=object,
    )

    statuses = np.array(
        ["active", "inactive", "pending", "suspended"],
        dtype=object,
    )

    product_categories = np.array(
        ["software", "hardware", "services", "media", "other"],
        dtype=object,
    )

    now = np.datetime64(
        datetime.now(timezone.utc).replace(tzinfo=None),
        "us",
    )

    # Random dates within approximately the last five years.
    created_at = now - rng.integers(
        0,
        365 * 5 * 24 * 60 * 60,
        size=rows,
        dtype=np.int64,
    ).astype("timedelta64[s]")

    updated_at = created_at + rng.integers(
        0,
        180 * 24 * 60 * 60,
        size=rows,
        dtype=np.int64,
    ).astype("timedelta64[s]")

    quantity = rng.integers(1, 20, size=rows, dtype=np.int32)
    unit_price = np.round(rng.uniform(5.0, 500.0, size=rows), 2)
    discount = np.round(rng.uniform(0.0, 0.4, size=rows), 4)

    subtotal = quantity * unit_price
    total = np.round(subtotal * (1.0 - discount), 2)

    emails = np.array(
        [f"user-{i:08d}@example.com" for i in customer_ids],
        dtype=object,
    )

    # Introduce some nulls to make the dataset a little more realistic.
    notes = np.array(
        [f"Dummy record {i}" for i in customer_ids],
        dtype=object,
    )
    null_mask = rng.random(rows) < 0.10
    notes[null_mask] = None

    table = pa.table(
        {
            "id": customer_ids,
            "email": emails,
            "region": rng.choice(regions, size=rows),
            "status": rng.choice(statuses, size=rows),
            "product_category": rng.choice(product_categories, size=rows),
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "total": total,
            "is_priority": rng.random(rows) < 0.15,
            "score": np.round(rng.normal(75.0, 12.5, size=rows), 2),
            "created_at": created_at,
            "updated_at": updated_at,
            "notes": notes,
        }
    )

    return table


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Parquet file containing dummy data."
    )

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("dummy.parquet"),
        help="Output Parquet file. Default: dummy.parquet",
    )

    parser.add_argument(
        "-n",
        "--rows",
        type=int,
        default=100_000,
        help="Number of rows to generate. Default: 100000",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed. Default: 42",
    )

    parser.add_argument(
        "--compression",
        choices=["snappy", "gzip", "brotli", "lz4", "zstd", "none"],
        default="zstd",
        help="Parquet compression codec. Default: zstd",
    )

    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be greater than zero")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    table = generate_table(args.rows, args.seed)

    compression = None if args.compression == "none" else args.compression

    pq.write_table(
        table,
        args.output,
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
    )

    file_size_mb = args.output.stat().st_size / (1024 * 1024)

    print(f"Wrote:       {args.output}")
    print(f"Rows:        {table.num_rows:,}")
    print(f"Columns:     {table.num_columns}")
    print(f"Compression: {args.compression}")
    print(f"File size:   {file_size_mb:.2f} MiB")
    print()
    print(table.schema)


if __name__ == "__main__":
    main()
