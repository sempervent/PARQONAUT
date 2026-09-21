import time

from parqonaut_plugins.contracts import PluginResult, PluginScanContext


def analyze(_context: PluginScanContext) -> PluginResult:
    time.sleep(30)
    return PluginResult(findings=[])
