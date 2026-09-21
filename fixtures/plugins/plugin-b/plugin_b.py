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
                code="MARKER_B",
                severity=FindingSeverity.INFO,
                summary="plugin-b",
                detail="plugin-b",
            )
        ]
    )
