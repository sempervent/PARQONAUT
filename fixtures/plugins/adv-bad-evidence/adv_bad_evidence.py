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
                code="BAD_EVIDENCE",
                severity=FindingSeverity.INFO,
                summary="ok",
                detail="ok",
                evidence_ids=["00000000-0000-4000-8000-000000000099"],
            )
        ]
    )
