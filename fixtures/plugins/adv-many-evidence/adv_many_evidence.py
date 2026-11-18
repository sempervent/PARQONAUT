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
                code="MANY_EVIDENCE",
                severity=FindingSeverity.INFO,
                summary="ok",
                detail="ok",
                evidence_ids=[f"id-{i}" for i in range(64)],
            )
        ]
    )
