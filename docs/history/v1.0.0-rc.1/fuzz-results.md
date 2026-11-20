# Extended Fuzz Qualification Results (`v1.0.0-rc.1`)

## 1. Execution Environment & Harness

- **Platform:** Linux `x86_64` (`docker run --platform linux/amd64`, image `rustlang/rust:nightly-bookworm`)
- **Toolchain:** `rustc 1.102.0-nightly`, `cargo-fuzz 0.13.2` (`libFuzzer` + AddressSanitizer)
- **Runner Script:** `scripts/fuzz/run-extended.sh`
- **Reproducible CI Workflow:** `.github/workflows/fuzz-extended.yml` (`ubuntu-24.04`, `dtolnay/rust-toolchain@nightly`, `workflow_dispatch` + weekly `cron`)
- **Campaign Parameters:** Seed `20261119`, `-rss_limit_mb=2048`, `-timeout=10`, `-max_total_time` 60s for primary parsers/protocols and 30s for secondary JSON/SSE/config/schema parsers
- **Total Executions:** **8,936,647** across all 9 targets
- **Crashes / Panics / Hangs / OOMs:** **0**

## 2. Per-Target Campaign Metrics (`target/fuzz-extended/summary.json`)

| Target | Duration (s) | Total Executions | Rate (exec/s) | Final Corpus | Coverage (cov / ft) | Peak RSS (MB) | Crashes |
|---|---:|---:|---:|---:|---|---:|---:|
| `fuzz_parquet_metadata` | 65 | 180,875 | 2,782 | 431 | 745 / 1,416 | 215 | 0 |
| `fuzz_arrow_ipc` | 61 | 1,844,293 | 30,234 | 104 | 391 / 899 | 498 | 0 |
| `fuzz_plugin_protocol` | 61 | 1,447,528 | 23,729 | 2,306 | 2,179 / 7,027 | 561 | 0 |
| `fuzz_dataset_uri` | 61 | 1,081,066 | 17,722 | 1,865 | 2,953 / 9,097 | 514 | 0 |
| `fuzz_repair_plan` | 61 | 1,437,935 | 23,572 | 2,093 | 2,726 / 7,796 | 575 | 0 |
| `fuzz_http_json` | 31 | 496,399 | 16,012 | 1,673 | 2,202 / 5,952 | 520 | 0 |
| `fuzz_sse_stream` | 31 | 990,406 | 31,948 | 1,410 | 1,579 / 3,998 | 551 | 0 |
| `fuzz_batch_config` | 31 | 464,865 | 14,995 | 1,602 | 3,908 / 9,992 | 519 | 0 |
| `fuzz_schema_compat` | 31 | 993,280 | 32,041 | 1,512 | 1,455 / 4,181 | 520 | 0 |

## 3. Deterministic Regression Smoke (`fuzz_smoke`)

In addition to the sustained Linux `x86_64` libFuzzer campaign above, all 9 seed corpora and prior crash-regression fixtures (including the `fuzz_arrow_ipc` negative/oversized metadata length bounds regression) execute deterministically on every `cargo test --workspace --locked` run via `crates/parqonaut-bench/tests/fuzz_smoke.rs`.
