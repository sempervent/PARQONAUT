"""Example scan analyzer plugin for PARQONAUT integration tests."""

from __future__ import annotations

from parqonaut_plugins.contracts import (
    FindingSeverity,
    PluginFindingContribution,
    PluginResult,
    PluginScanContext,
)


def analyze(context: PluginScanContext) -> PluginResult:
    asset_count = len(context.discovered_files)
    return PluginResult(
        findings=[
            PluginFindingContribution(
                code="EXAMPLE_SIGNAL",
                severity=FindingSeverity.INFO,
                summary="Example plugin observed scan inventory",
                detail=f"discovered_files={asset_count}",
                evidence_ids=[],
            )
        ],
        annotations={"example_rules": True},
    )
