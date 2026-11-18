from __future__ import annotations

import sys
from typing import Any

import pyarrow as pa

_seen = 0


def transform(batch: pa.RecordBatch, _config: dict[str, Any]) -> pa.RecordBatch:
    global _seen
    _seen += 1
    if _seen >= 1:
        print("adv-batch-crash intentional failure", file=sys.stderr)
        sys.exit(2)
    return batch
