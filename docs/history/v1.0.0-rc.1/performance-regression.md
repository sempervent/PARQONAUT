# Performance Regression & Operating Envelopes (`v1.0.0-rc.1`)

## 1. Benchmark Comparison (`perf_baseline`, Release, `--locked`, Idle Host)

Host: Apple M2 (8 cores, ARM64), 24 GiB RAM, macOS Darwin 27.0.0, `rustc 1.99.1`, zero competing Docker/fuzz workloads.

| Workload | Rows / Items | v0.12.0 Baseline (ms) | v1.0.0-rc.1 (ms) | Throughput (RC1) | Peak RSS (MiB) | Delta |
|---|---:|---:|---:|---|---:|---|
| Parquet metadata inspection (100k rows) | 1 file | 0.58 | **0.59** | 1,708.4 files/s | 13.5 | ~parity |
| Large-file standard scan (100k rows, 0.65 MiB) | 100,000 rows | 0.11 | **0.08** | 1.29B rows/s (8,314 MiB/s) | 13.5 | faster |
| Small-file dataset scan (40 files) | 40 files | 3.20 | **2.17** | 18,431.4 files/s | 13.9 | faster |
| Deeply partitioned scan (3x2x4=24 partitions) | 24 files | 2.20 | **1.82** | 13,194.7 files/s | 14.0 | faster |
| Schema reconciliation (8 files, mixed Int32/Int64) | 8 files | 2.56 | **2.12** | 471.0 plans/s | 14.8 | faster |
| Streaming CSV -> Parquet (100k rows, 2.53 MiB) | 100,000 rows | 60.12 | **46.44** | 2,153,463 rows/s (54.4 MiB/s) | 39.1 | **+29% faster** |
| Rewrite ZSTD + stats rebuild (100k rows) | 100,000 rows | 18.63 | **11.63** | 8,598,853 rows/s | 40.5 | **+60% faster** |
| Fused multi-step transform (rewrite + partition) | 100,000 rows | 35.97 | **24.31** | 4,113,005 rows/s | 46.1 | **+48% faster** |
| Repair plan + execution (40 small files) | 40 files | 51.20 | **37.31** | 26.8 datasets/s | 46.6 | **+37% faster** |
| Concurrent batch scheduling (4 datasets, jobs=2) | 4 datasets | 26.97 | **22.15** | 180.6 datasets/s | 50.3 | **+22% faster** |
| SQLite job event ingest (2,000 events) | 2,000 events | 2,202.55 | **1,374.89** | 1,455 events/s | 53.1 | **+60% faster** |
| SQLite job event page list (limit=500) | 500 events | 4.45 | **2.27** | 441.1 queries/s | 53.1 | **+96% faster** |

## 2. Criterion Suite (`cargo bench --locked -p parqonaut-bench`)

Fixed a pre-existing bug in `benches/stream-throughput/throughput.rs` where `diagnose_plan_and_repair_small_files` passed the already-created `tempfile::tempdir()` root instead of a fresh child path (`out_dir.path().join("repaired")`), which had previously been masked by `justfile`'s `|| true`. All 9 Criterion benchmarks now execute and pass cleanly:

- `scan_and_inspect/scan_quick_metadata_50k`: `42.48 µs` (`[41.06 µs .. 43.63 µs]`)
- `scan_and_inspect/scan_standard_large_50k`: `48.44 µs` (`1.032 Gelem/s`)
- `scan_and_inspect/scan_small_files_40`: `1.772 ms` (`28.22 Melem/s`)
- `scan_and_inspect/scan_deeply_partitioned_24`: `1.695 ms` (`29.50 Melem/s`)
- `stream_and_transform/csv_to_parquet_25k`: `13.12 ms` (`1.905 Melem/s`)
- `stream_and_transform/rewrite_zstd_50k`: `5.600 ms` (`8.929 Melem/s`)
- `stream_and_transform/fused_transform_50k`: `11.21 ms` (`4.459 Melem/s`)
- `repair_and_store/diagnose_plan_and_repair_small_files`: `63.12 ms` (`[47.47 ms .. 85.88 ms]`)
- `repair_and_store/store_event_ingest_and_list_500`: `126.23 ms` (`~3,960 events/s`)

## 3. Bounded-Memory Verification (`bounded_memory`, Release, `--locked`)

All 3 bounded-memory tests passed in `0.14s`:
- `streaming_csv_to_parquet_250k_rows_stays_memory_bounded`: `< 256 MiB` RSS delta with `--mem-budget 64 --writer-buffer 8`.
- `streaming_rewrite_200k_rows_stays_memory_bounded`: `< 256 MiB` RSS delta on 200k-row ZSTD rewrite.
- `high_rate_event_burst_bounds_memory_and_retains_terminal`: 10,000 non-critical burst events coalesced without blocking, retaining `<= 505` SQLite rows and preserving terminal `ExecutionCompleted`.

## 4. Investigation of SQLite Event Ingest Latency

Comparing Criterion `store_event_ingest_and_list_500` (`126.23 ms` / `~0.25 ms` per event = `~3,960 events/s`) against `perf_baseline`'s 2,000-event loop (`1,374.89 ms` / `~0.69 ms` per event = `~1,455 events/s`) explains the bottleneck clearly:

1. **Per-event synchronous transaction boundary:** Each `store.append_job_event` commits an individual `BEGIN IMMEDIATE` transaction (`UPDATE application_jobs ... RETURNING`, `INSERT INTO application_job_events`, `COMMIT`) under `WAL` + `synchronous=NORMAL`.
2. **Retention prune overhead above 500 events:** Once a job exceeds `max_events_per_job=500`, each subsequent `append_job_event` executes `DELETE FROM application_job_events WHERE ... AND seq NOT IN (SELECT seq ... LIMIT 500)` inside the same transaction, adding ~0.4–0.5 ms per event.
3. **Why the streaming data plane is unaffected:** `JobProgressObserver::emit` routes non-critical progress updates through `JobEventRecorder::record_nonblocking` (bounded `progress_tx` channel of capacity `PARQONAUT_EVENT_CHANNEL_CAPACITY`, default `64`). When the DB writer lags behind high-frequency streaming batches, overflow updates are **coalesced** in memory (`parqonaut_job_events_coalesced_total`) without ever blocking Arrow `RecordBatch` streaming or S3 uploads, while critical lifecycle transitions (`job_queued`, `job_claimed`, `job_recovered`, `cancel_requested`, terminal states) travel on the lossless `critical_tx` channel.

## 5. Supported Operating Envelopes (`v1.0.0-rc.1`)

| Dimension | Recommended / Tested Envelope | Notes & Limits |
|---|---|---|
| **Server deployment model** | Single-node `prqnt serve` with in-process workers (`--workers 2`–`4`) | Distributed multi-node worker queue is out of scope for v1.0 |
| **State backend** | SQLite (WAL, single-instance local state) or PostgreSQL 16+ | Use PostgreSQL for higher multi-writer concurrency or shared external DB HA |
| **Streaming memory budget** | `--mem-budget 64` (default `1024`), `--writer-buffer 8`–`64` | Verified `< 256 MiB` RSS delta at 250k rows; container verified at `512 MiB` / `1 CPU` |
| **Event journal** | `PARQONAUT_MAX_EVENTS_PER_JOB=500`, `PARQONAUT_EVENT_CHANNEL_CAPACITY=64` | Bounded DB rows per job; non-critical events coalesce under burst |
| **HTTP request / SSE limits** | JSON body cap `2 MiB`; HTTP timeout `300s`; SSE frame max `2 MiB` | Reverse proxy must disable buffering/compression on `/events/stream` |
| **S3 multipart uploads** | Default `5 MiB` part size (`PARQONAUT_S3_PART_SIZE_BYTES`) | Crossing 5 MiB boundaries verified on RustFS (`multipart_boundary`, `write_s3_roundtrip`) |
