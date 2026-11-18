from __future__ import annotations

import time
from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    delay_ms = float(config.get("delay_ms", 50))
    time.sleep(delay_ms / 1000.0)
    return batch
