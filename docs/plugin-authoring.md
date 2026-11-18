# Plugin authoring

Protocol **v1** uses manifest file `parqonaut-plugin.json` and the Python package `parqonaut_plugins` (see `python/parqonaut_plugins/`).

## Scan analyzer (minimal)

```python
from parqonaut_plugins.contracts import PluginScanContext, PluginResult


def analyze(context: PluginScanContext) -> PluginResult:
    return PluginResult(findings=[])
```

Register in `parqonaut-plugin.json` with `capabilities.scan` and supported phases. Finding codes must be namespaced (`plugin.<name>.…`).

## Batch transform (v1)

```python
from typing import Any

import pyarrow as pa


def transform(batch: pa.RecordBatch, config: dict[str, Any]) -> pa.RecordBatch:
    return batch
```

v1 batch plugins must be **schema-preserving** (no column add/drop/rename, no type or nullability changes). Row filter and in-column value changes are allowed.

## Trust model

> PARQONAUT plugins are trusted code. They run in subprocesses for fault containment, cancellation, and resource control. The plugin subprocess is not a security sandbox for malicious code.

See [Plugins](plugins.md) for server allowlists and digest pinning.
