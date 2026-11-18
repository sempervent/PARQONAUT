"""Cross-language: Rust-shaped manifest/request JSON loads in Python."""

import json
from pathlib import Path

from parqonaut_plugins.contracts import PluginManifest, PluginResponse, PluginResult

FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "plugins"
    / "example-rules"
    / "parqonaut-plugin.json"
)


def test_manifest_fixture_matches_pydantic() -> None:
    raw = json.loads(FIXTURE.read_text())
    manifest = PluginManifest.model_validate(raw)
    assert manifest.name == "example-rules"


def test_response_roundtrip() -> None:
    resp = PluginResponse(
        protocol_version=1,
        plugin="example-rules",
        result=PluginResult(findings=[]),
    )
    back = PluginResponse.model_validate_json(resp.model_dump_json())
    assert back.plugin == "example-rules"


def test_read_stdin_rejects_oversized_payload(monkeypatch) -> None:
    import io

    import pytest

    from parqonaut_plugins.runner import _read_stdin

    class FakeStdin:
        def __init__(self, data: bytes) -> None:
            self.buffer = io.BytesIO(data)

    monkeypatch.setattr("sys.stdin", FakeStdin(b"abcde"))
    assert _read_stdin(limit=5) == b"abcde"

    monkeypatch.setattr("sys.stdin", FakeStdin(b"abcdef"))
    with pytest.raises(ValueError, match="request too large"):
        _read_stdin(limit=5)
