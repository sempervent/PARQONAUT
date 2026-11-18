from __future__ import annotations

import subprocess
import sys
import time
from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(3600)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(3600)
    return batch
