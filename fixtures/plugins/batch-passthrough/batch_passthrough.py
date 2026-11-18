from __future__ import annotations

from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    return batch
