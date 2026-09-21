from parqonaut_plugins.contracts import (
    FindingSeverity,
    PluginFindingContribution,
    PluginResult,
    PluginScanContext,
)


def analyze(_context: PluginScanContext) -> PluginResult:
    findings = [
        PluginFindingContribution(
            code=f"F{i}",
            severity=FindingSeverity.INFO,
            summary="x",
            detail="y",
        )
        for i in range(512)
    ]
    return PluginResult(findings=findings)
