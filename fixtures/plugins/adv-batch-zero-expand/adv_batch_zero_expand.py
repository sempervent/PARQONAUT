from __future__ import annotations

from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    if batch.num_rows == 0:
        arrays = []
        for field in batch.schema:
            if pa.types.is_integer(field.type):
                arrays.append(pa.array([0], type=field.type))
            else:
                arrays.append(pa.array([None], type=field.type))
        return pa.RecordBatch.from_arrays(arrays, schema=batch.schema)
    return batch
