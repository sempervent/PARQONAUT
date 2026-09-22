"""First-party plugin subprocess runner: explicit `scan` and `batch` modes."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

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


def _load_scan_callable(entrypoint: str) -> Callable[[PluginScanContext], PluginResult]:
    module_name, _, func_name = entrypoint.partition(":")
    if not module_name or not func_name:
        raise ValueError("entrypoint must be module:callable")
    module = importlib.import_module(module_name)
    func = getattr(module, func_name, None)
    if func is None or not callable(func):
        raise ValueError(f"entrypoint not callable: {entrypoint}")
    return func


def _load_batch_callable(
    entrypoint: str,
) -> Callable[[pa.RecordBatch, dict[str, Any]], pa.RecordBatch]:
    module_name, _, func_name = entrypoint.partition(":")
    if not module_name or not func_name:
        raise ValueError("entrypoint must be module:callable")
    module = importlib.import_module(module_name)
    func = getattr(module, func_name, None)
    if func is None or not callable(func):
        raise ValueError(f"entrypoint not callable: {entrypoint}")
    return func


def run_scan() -> int:
    raw = _read_stdin()
    payload: dict[str, Any] = json.loads(raw.decode("utf-8"))
    request = PluginRequest.model_validate(payload)
    if request.protocol_version != PLUGIN_PROTOCOL_VERSION:
        raise ValueError(f"unsupported protocol_version {request.protocol_version}")
    analyze = _load_scan_callable(request.manifest.entrypoint)
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
    return 0


def _write_batch_frame(batch: pa.RecordBatch) -> None:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    sys.stdout.buffer.write(len(payload).to_bytes(4, "little"))
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()


def _read_batch_frame() -> pa.RecordBatch | None:
    header = sys.stdin.buffer.read(4)
    if not header:
        return None
    length = int.from_bytes(header, "little")
    if length == 0:
        return None
    payload = sys.stdin.buffer.read(length)
    if len(payload) != length:
        raise ValueError("truncated batch frame")
    reader = ipc.open_stream(payload)
    batch = reader.read_next_batch()
    if batch is None:
        raise ValueError("empty batch frame")
    return batch


def run_batch(context_path: Path) -> int:
    ctx = json.loads(context_path.read_text(encoding="utf-8"))
    if ctx.get("protocol_version") != PLUGIN_PROTOCOL_VERSION:
        raise ValueError("unsupported batch protocol_version")
    transform = _load_batch_callable(ctx["entrypoint"])
    config: dict[str, Any] = ctx.get("config") or {}
    while True:
        batch = _read_batch_frame()
        if batch is None:
            break
        out = transform(batch, config)
        if not isinstance(out, pa.RecordBatch):
            raise ValueError("transform must return pyarrow.RecordBatch")
        if not batch.schema.equals(out.schema, check_metadata=False):
            print("schema-preserving violation (host will reject)", file=sys.stderr)
        _write_batch_frame(out)
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="parqonaut_plugins.runner")
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("scan", help="scan analyzer JSON stdin/stdout")
    batch_p = sub.add_parser("batch", help="batch transform Arrow IPC framing")
    batch_p.add_argument("--context", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.mode == "scan":
        return run_scan()
    return run_batch(args.context)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
