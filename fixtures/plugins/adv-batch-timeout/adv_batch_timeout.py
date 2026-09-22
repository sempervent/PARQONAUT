from __future__ import annotations

import time
from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    time.sleep(30)
    return batch
