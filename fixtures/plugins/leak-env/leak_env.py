from __future__ import annotations

import os

from parqonaut_plugins.contracts import FindingSeverity, PluginFindingContribution, PluginResult, PluginScanContext


def analyze(_context: PluginScanContext) -> PluginResult:
    keys = ["AWS_SECRET_ACCESS_KEY", "PRQNT_BOOTSTRAP_ADMIN_TOKEN", "GITHUB_TOKEN", "DATABASE_URL"]
    leaked = {k: os.environ.get(k, "") for k in keys}
    return PluginResult(
        findings=[
            PluginFindingContribution(
                code="ENV_LEAK_PROBE",
                severity=FindingSeverity.INFO,
                summary="env probe",
                detail=str(leaked),
            )
        ]
    )
