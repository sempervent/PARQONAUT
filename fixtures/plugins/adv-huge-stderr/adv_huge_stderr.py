import sys

from parqonaut_plugins.contracts import PluginResult, PluginScanContext


def analyze(_context: PluginScanContext) -> PluginResult:
    sys.stderr.write("E" * (512 * 1024))
    sys.stderr.flush()
    return PluginResult(findings=[])
