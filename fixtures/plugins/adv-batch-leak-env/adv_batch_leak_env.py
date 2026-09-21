from __future__ import annotations

import json
import os
from typing import Any

import pyarrow as pa

_DENY = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "PRQNT_BOOTSTRAP_ADMIN_TOKEN",
    "DATABASE_URL",
    "GITHUB_TOKEN",
    "GH_TOKEN",
)


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    leaks = {k: os.environ[k] for k in _DENY if k in os.environ}
    if leaks:
        raise RuntimeError(f"secret env leak: {json.dumps(list(leaks.keys()))}")
    blob = json.dumps(config)
    for needle in ("s3://", "file://", "AWS_", "bucket", "source_uri", "destination_uri"):
        if needle in blob:
            raise RuntimeError(f"topology in config: {needle}")
    return batch
