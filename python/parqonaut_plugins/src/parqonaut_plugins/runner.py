"""First-party scan analyzer subprocess runner (protocol v1 JSON stdin/stdout)."""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Callable
from typing import Any

from parqonaut_plugins.contracts import (
    PLUGIN_PROTOCOL_VERSION,
    PluginRequest,
    PluginResponse,
    PluginResult,
    PluginScanContext,
)

MAX_IO_BYTES = 4 * 1024 * 1024


def _read_stdin(limit: int = MAX_IO_BYTES) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        part = sys.stdin.buffer.read(min(65536, limit - total))
        if not part:
            break
        total += len(part)
        if total > limit:
            raise ValueError("request too large")
        chunks.append(part)
    return b"".join(chunks)


def _load_callable(entrypoint: str) -> Callable[[PluginScanContext], PluginResult]:
    module_name, _, func_name = entrypoint.partition(":")
    if not module_name or not func_name:
        raise ValueError("entrypoint must be module:callable")
    module = importlib.import_module(module_name)
    func = getattr(module, func_name, None)
    if func is None or not callable(func):
        raise ValueError(f"entrypoint not callable: {entrypoint}")
    return func


def main() -> None:
    raw = _read_stdin()
    payload: dict[str, Any] = json.loads(raw.decode("utf-8"))
    request = PluginRequest.model_validate(payload)
    if request.protocol_version != PLUGIN_PROTOCOL_VERSION:
        raise ValueError(f"unsupported protocol_version {request.protocol_version}")
    analyze = _load_callable(request.manifest.entrypoint)
    result = analyze(request.context)
    if not isinstance(result, PluginResult):
        result = PluginResult.model_validate(result)
    response = PluginResponse(
        protocol_version=PLUGIN_PROTOCOL_VERSION,
        plugin=request.manifest.name,
        result=result,
    )
    sys.stdout.write(response.model_dump_json())
    sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 — diagnostics on stderr only
        print(str(exc), file=sys.stderr)
        sys.exit(1)
