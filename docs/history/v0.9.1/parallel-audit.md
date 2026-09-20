# v0.9.1 parallel audit

Read-only review tracks A–G (+ optional H). P0/P1/P2 correctness items resolved in branch `fix/remote-pipeline-completeness`.

## A — Streaming S3 publication, multipart, memory

- **Finding (P0, fixed):** `MultipartAsyncWrite::poll_shutdown` could return `Ok(())` without calling `complete()`.
- **Finding (P1, fixed):** Convert path buffered full Parquet then `conditional_create`.
- **Resolution:** Shutdown schedules `complete()`; convert uses `write_parquet_batch_stream` with 8× batch queue and 8× byte chunk queue.
- **Bounds:** Peak memory ∝ multipart part size + channel depth, not object size.

## B — Convert four-leg semantics

- **Status:** PASS via `remote_convert_s3` and matrix convert leg; schema/rows validated through read-back.

## C — Split/partition routing and resource bounds

- **Status:** PASS matrix tests; split asserts ≥2 parts and row sum; partition asserts non-empty inventory and row sum.

## D — Fused remote spec / zero intermediates

- **Status:** PASS matrix fused leg + `pipeline-s3-demo` prefix inventory; `intermediate_files_created == 0`.

## E — Cancellation/progress/error/secret safety

- **Status:** No regression; existing redaction tests retained; nested-runtime bridge uses `block_in_place` on multi-thread runtime.

## F — Repair/batch remote regression

- **Status:** Publication semantics unchanged for committed objects; streaming affects direct `write_stream` destinations only.

## G — CI/demo/release completeness

- **Status:** `s3-integration` runs storage roundtrip, convert, transform matrix; demos documented in release notes.

## H — Plugin-boundary readiness

See [plugin-readiness.md](./plugin-readiness.md).
