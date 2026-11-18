from parqonaut_plugins.contracts import PluginResult, PluginScanContext


def analyze(_context: PluginScanContext) -> PluginResult:
    return PluginResult(annotations={"blob": "A" * (512 * 1024)})
