from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    columns = config.get("columns") or []
    if not columns:
        return batch
    arrays = []
    for name in batch.schema.names:
        col = batch.column(name)
        if name in columns and pa.types.is_string(col.type):
            trimmed = pc.utf8_trim_whitespace(col)
            arrays.append(trimmed)
        else:
            arrays.append(col)
    return pa.RecordBatch.from_arrays(arrays, schema=batch.schema)
