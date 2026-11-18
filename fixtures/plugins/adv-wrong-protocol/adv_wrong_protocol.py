import json
import sys

from parqonaut_plugins.contracts import PluginScanContext


def analyze(_context: PluginScanContext):
    payload = {
        "protocol_version": 2,
        "plugin": "adv-wrong-protocol",
        "result": {"findings": [], "annotations": {}},
    }
    sys.stdout.write(json.dumps(payload))
    sys.stdout.flush()
    sys.exit(0)
