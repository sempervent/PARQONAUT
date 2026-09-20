# S3 streaming publication recon (v0.9.1)

## Symptom

`write_parquet_batch_stream` / `StorageBackend::write_stream` returned success (non-zero
`bytes_written`) but `HEAD` on the destination key returned `NotFound` on RustFS.

`conditional_create` (single PUT) to the same bucket/key was visible immediately.

## Root cause

`MultipartAsyncWrite::poll_shutdown` called `poll_pending` before scheduling
`complete()`. When `pending` was empty, `poll_pending` returned `Ready(Ok(()))`, so
shutdown finished without running `MultipartState::complete()` (no `PutObject` and no
`CompleteMultipartUpload`).

## Secondary hazard (fixed)

`Drop` spawned an async `abort_multipart_upload` that could race with a successful
complete. Async abort on drop was removed; incomplete uploads must be aborted on
explicit error paths.

## Not the cause

- Wrong bucket/key (fixtures and `HEAD` use the same `ObjectLocation` parser)
- RustFS read-after-write delay (object absent after synchronous `finish().await`)
- Staging vs final key confusion for direct `write_stream` to final key

## Correct contract

```text
BatchStream → ArrowWriter → ObjectWriteStream → multipart → complete() → HEAD OK
```

`finish()` on `ObjectWriteStream` must not return until `complete()` succeeds.
