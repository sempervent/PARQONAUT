# Remote publication

Batch and repair workflows can write outputs to **`s3://`** prefixes using the same staged-publication model as local directories: outputs land in a dedicated prefix; manifests and fingerprints record what was written.

Publication semantics and execution manifests are described in [Batch execution](./batch-execution.md) and ADR-0010 (staged repair publication).

For storage layout and backends, see [Storage architecture](./storage-architecture.md) and [Object storage](./object-storage.md).
