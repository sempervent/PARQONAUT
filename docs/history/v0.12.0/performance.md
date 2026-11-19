# Performance Baseline & Bounded-Memory Report (v0.12.0)

## 1. Environment & Methodology

- **Host:** Apple M2 (8 cores, ARM64), 24 GiB RAM, macOS Darwin 27.0.0
- **Toolchain:** `rustc 1.99.1`, `cargo 1.99.1` (`--release` optimized)
- **Benchmark harnesses:**
  - Criterion suite: `benches/stream-throughput/throughput.rs` (`cargo bench --locked -p parqonaut-bench`)
  - Single-shot RSS & throughput profiler: `cargo run --release --locked -p parqonaut-bench --bin perf_baseline`
  - Bounded-memory integration gate: `cargo test --locked -p parqonaut-bench --test bounded_memory`
- **Fixtures:** Deterministically generated via `parqonaut_bench::BenchFixtures::generate(100_000, 100_000)` (no production data).

## 2. Measured Baseline (`perf_baseline`, release build)

| Workload | Rows / Items | Elapsed (ms) | Throughput | Peak RSS (MiB) |
|---|---:|---:|---:|---:|
| Parquet metadata inspection (100k rows) | 1 file | 0.58 | 1,738.0 files/s | 13.0 |
| Large-file standard scan (100k rows, 0.65 MiB) | 100,000 rows | 0.11 | 884,955,752 rows/s (5,723.1 MiB/s) | 13.0 |
| Small-file dataset scan (40 files) | 40 files | 3.20 | 12,494.8 files/s | 13.5 |
| Deeply partitioned scan (3x2x4=24 partitions) | 24 files | 2.20 | 10,933.5 files/s | 13.6 |
| Schema reconciliation (8 files, mixed Int32/Int64) | 8 files | 2.56 | 390.9 plans/s | 14.3 |
| Streaming CSV -> Parquet (100k rows, 2.53 MiB CSV) | 100,000 rows | 60.12 | 1,663,268 rows/s (42.0 MiB/s) | 38.1 |
| Rewrite ZSTD + stats rebuild (100k rows) | 100,000 rows | 18.63 | 5,369,055 rows/s | 39.5 |
| Fused multi-step transform (rewrite + partition, 100k rows) | 100,000 rows (4 parts) | 35.97 | 2,780,394 rows/s | 42.1 |
| Repair plan + execution (40 small files) | 40 files | 51.20 | 19.5 datasets/s | 42.7 |
| Concurrent batch scheduling (4 datasets, jobs=2) | 4 datasets | 26.97 | 148.3 datasets/s | 46.2 |
| SQLite job event ingest (2,000 events) | 2,000 events | 2,202.55 | 908 events/s | 48.9 |
| SQLite job event page list (limit=500) | 500 events | 4.45 | 224.9 queries/s | 48.9 |

*Note on scan throughput:* `ScanEngine::scan` reads Parquet footers/statistics rather than decompressing full column pages on `Standard` scans, yielding sub-millisecond metadata inspection latency.

## 3. Bounded-Memory Verification

Executed via `cargo test --locked -p parqonaut-bench --test bounded_memory`:

1. **`streaming_csv_to_parquet_250k_rows_stays_memory_bounded`:** 250,000-row CSV -> Parquet conversion with `--mem-budget 64 --writer-buffer 8` keeps RSS delta well below the 256 MiB bound.
2. **`streaming_rewrite_200k_rows_stays_memory_bounded`:** 200,000-row ZSTD rewrite with `--row-group-size-mb 1` keeps RSS delta bounded (`< 256 MiB`).
3. **`high_rate_event_burst_bounds_memory_and_retains_terminal`:** Flooding 10,000 non-critical `BatchProcessed` events into `JobEventRecorder` coalesces overflow without blocking or unbounded channel growth, bounds SQLite retention (`<= 505` rows), and delivers the terminal `ExecutionCompleted` event.

## 4. Regression Budgets & Policy

- **No absolute universal hardware SLA:** Numbers above reflect an idle Apple M2 workstation. CI runners (2-core GitHub-hosted VMs) have higher variance.
- **Release-gate regression budgets (on comparable hardware):**
  - Streaming CSV->Parquet and ZSTD rewrite throughput should remain within **-25%** of baseline across releases unless intentional correctness checks are added.
  - Peak RSS on the 250k-row bounded-memory test must remain **< 256 MiB** growth (hard CI assertion in `bounded_memory.rs`).
  - SQLite event page query latency (`limit=500`) must stay **< 25 ms** (currently `4.45 ms`).
  - Correctness and publication atomicity always outrank throughput; no buffering shortcuts or unsafe skipping are permitted to meet a benchmark target.
