from parqonaut_plugins.contracts import (
    FindingSeverity,
    PluginFindingContribution,
    PluginResult,
    PluginScanContext,
)


def analyze(_context: PluginScanContext) -> PluginResult:
    return PluginResult(
        findings=[
            PluginFindingContribution(
                code="MARKER_A",
                severity=FindingSeverity.INFO,
                summary="plugin-a",
                detail="plugin-a",
            )
        ]
    )
