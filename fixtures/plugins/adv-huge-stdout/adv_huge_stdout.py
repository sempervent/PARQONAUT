import sys

from parqonaut_plugins.contracts import PluginScanContext


def analyze(_context: PluginScanContext):
    sys.stdout.write("X" * (64 * 1024))
    sys.stdout.flush()
    sys.exit(0)
