from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from paraclete_plugins.contracts import (
    DataFormat,
    FindingSeverity,
    PluginExecutionPhase,
    PluginFinding,
    PluginManifest,
    PluginResult,
    PluginScanContext,
    ScanRequest,
)


def _fixture(name: str) -> Path:
    return Path(__file__).resolve().parents[3] / "fixtures" / "plugins" / name


def test_sample_manifest_loads() -> None:
    raw = json.loads(_fixture("sample_manifest.json").read_text())
    m = PluginManifest.model_validate(raw)
    assert m.name == "example_rules"
    assert DataFormat.PARQUET in m.capabilities.supported_formats
    assert PluginExecutionPhase.POST_RULES in m.capabilities.supported_phases


def test_manifest_rejects_blank_name() -> None:
    with pytest.raises(ValidationError):
        PluginManifest.model_validate(
            {
                "name": "  ",
                "version": "1",
                "entrypoint": "x:y",
                "capabilities": {
                    "supported_formats": ["parquet"],
                    "supported_phases": ["pre_scan"],
                },
            }
        )


def test_plugin_result_roundtrip() -> None:
    result = PluginResult(
        findings=[
            PluginFinding(
                code="CUSTOM_CODE",
                severity=FindingSeverity.LOW,
                summary="hello",
                detail="world",
                evidence_ids=["00000000-0000-4000-8000-000000000099"],
            )
        ]
    )
    data = result.model_dump(mode="json")
    again = PluginResult.model_validate(data)
    assert again == result


def test_scan_context_with_request_dict() -> None:
    ctx = PluginScanContext(
        request=ScanRequest(
            scan_id=UUID("00000000-0000-4000-8000-000000000001"),
            target={"type": "local_directory", "path": "fixtures/csv"},
            profile="quick",
            options={"mode": "full", "max_files": 100000},
        ),
        discovered_files=["fixtures/csv/tiny.csv"],
    )
    dumped = ctx.model_dump(mode="json")
    PluginScanContext.model_validate(dumped)
