from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    column = config.get("column")
    if not column or column not in batch.schema.names:
        return batch
    col = batch.column(column)
    if not pa.types.is_boolean(col.type):
        return batch
    mask = col
    return batch.filter(mask)
