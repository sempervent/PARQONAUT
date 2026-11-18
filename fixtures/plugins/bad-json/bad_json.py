from parqonaut_plugins.contracts import PluginResult, PluginScanContext


def analyze(_context: PluginScanContext) -> PluginResult:
    print("prose before json", flush=True)
    return PluginResult(findings=[])
