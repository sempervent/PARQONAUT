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
                code="LONG_SUMMARY",
                severity=FindingSeverity.INFO,
                summary="S" * 8192,
                detail="ok",
            )
        ]
    )
