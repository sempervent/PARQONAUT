from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa

_FORBIDDEN_CONFIG_KEYS = {
    "source_uri",
    "destination_uri",
    "source",
    "destination",
    "bucket",
    "s3_bucket",
    "aws_access_key_id",
}


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    for key in config:
        if key in _FORBIDDEN_CONFIG_KEYS:
            raise RuntimeError(f"forbidden config key: {key}")
    ctx_path = os.environ.get("PARQONAUT_BATCH_CONTEXT_PATH")
    if ctx_path:
        ctx = json.loads(Path(ctx_path).read_text(encoding="utf-8"))
        blob = json.dumps(ctx).lower()
        for needle in ("s3://", "source_uri", "destination", "aws_", "secret", "token"):
            if needle in blob:
                raise RuntimeError(f"topology in context sidecar: {needle}")
    return batch
