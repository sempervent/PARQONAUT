# v0.9.1 plugin-readiness (architecture only)

Remote topology is abstracted behind `ColumnarPipelineIo`, `BatchSource`, and `BatchSink`. A future plugin transform step can remain:

```text
RecordBatch → RecordBatch
```

without embedding S3 clients, credential handles, filesystem paths, or storage-leg routing in plugin code. Routing and publication stay in the transform/stream/storage layers executed by the host.

No plugin runtime is implemented in v0.9.1.
