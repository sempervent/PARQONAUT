from __future__ import annotations

from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    if batch.num_rows == 0:
        return batch
    indices = pa.array(list(range(batch.num_rows)) * 10)
    return batch.take(indices)
