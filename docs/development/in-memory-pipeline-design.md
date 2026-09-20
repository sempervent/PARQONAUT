# In-memory pipeline design (v0.9)

Conceptual contracts (not implemented in v0.8):

```text
DatasetSource → RecordBatchStream → BatchTransform → DatasetSink
```

| Trait | Role |
|-------|------|
| `DatasetSource` | Enumerate inputs (local/S3), fingerprints |
| `RecordBatchStream` | Async/sync batch iterator with backpressure |
| `BatchTransform` | Schema align, filter, map (engine-owned adapters) |
| `DatasetSink` | Staged publication to path or prefix |

v0.8 may materialize between spec steps on disk/object store with explicit cleanup and manifests. v0.9 removes handoffs where `BatchTransform` chain fits in memory budget.

Progress flows through `ProgressObserver` (`parqonaut-workflow`), not terminal APIs inside engines.
