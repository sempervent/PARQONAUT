# PARQONAUT v0.9.2 — S3 multipart correctness

Fixes S3 multipart output corruption/finalization around the default 5 MiB part boundary.

When an `AsyncWrite` caller retried a write after `Poll::Pending`, the multipart writer could append the same bytes twice, producing objects with duplicated or corrupted payload (including Parquet files crossing the 5 MiB threshold).

No intended feature changes.
