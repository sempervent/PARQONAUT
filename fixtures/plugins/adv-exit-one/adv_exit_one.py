import sys

from parqonaut_plugins.contracts import PluginScanContext


def analyze(_context: PluginScanContext):
    print("diagnostic-from-plugin", file=sys.stderr)
    sys.exit(1)
