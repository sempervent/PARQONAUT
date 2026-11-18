from __future__ import annotations

import sys
from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    print("not-arrow-ipc", file=sys.stdout)
    sys.stdout.flush()
    return batch
