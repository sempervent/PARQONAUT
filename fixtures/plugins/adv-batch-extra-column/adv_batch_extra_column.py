from __future__ import annotations

from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    extra = pa.array([1] * batch.num_rows, type=pa.int32())
    return pa.RecordBatch.from_arrays(list(batch.columns) + [extra], names=list(batch.schema.names) + ["evil"])
