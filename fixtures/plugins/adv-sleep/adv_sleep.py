import os
import time

from parqonaut_plugins.contracts import PluginResult, PluginScanContext


def analyze(_context: PluginScanContext) -> PluginResult:
    delay = float(os.environ.get("PARQONAUT_TEST_SLOW_SCAN_SEC", "30"))
    time.sleep(delay)
    return PluginResult(findings=[])
