import sys

from parqonaut_plugins.contracts import PluginScanContext


def analyze(_context: PluginScanContext):
    sys.stdout.write("{not-json")
    sys.stdout.flush()
    sys.exit(0)
