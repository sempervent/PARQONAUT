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
                code="LONG_DETAIL",
                severity=FindingSeverity.INFO,
                summary="ok",
                detail="D" * 65536,
            )
        ]
    )
