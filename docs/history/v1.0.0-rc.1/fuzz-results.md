# Extended Fuzz Qualification Results (`v1.0.0-rc.1`)

## 1. Execution Environment & Harness

- **Platform:** Linux `x86_64` (`docker run --platform linux/amd64`, image `rustlang/rust:nightly-bookworm`)
- **Toolchain:** `rustc 1.102.0-nightly` (`2026-11-18`), `cargo-fuzz 0.13.2` (`libFuzzer` + AddressSanitizer)
- **Runner Script:** `scripts/fuzz/run-extended.sh` (reports `initial_corpus_files`, `final_corpus_files`, `corpus_growth`, actual runtime, executions, `exec/s`, edge coverage, features, peak RSS, and artifacts into `target/fuzz-extended/summary.{json,md}`)
- **Reproducible CI Workflow:** `.github/workflows/fuzz-extended.yml` (`ubuntu-24.04`, `dtolnay/rust-toolchain@nightly`, `workflow_dispatch` + weekly `cron`; triggers on GitHub once added to the default branch on PR merge)

## 2. Extended Priority/Standard Campaign (`v1.0.0-rc.1` Remediated Codebase, Seed `20261122`)

- **Scope & Bounds:** Seed `20261122`, `-rss_limit_mb=2048`, `-timeout=10`, `-max_total_time=240` (4 min/target) for the 5 priority targets (`parquet_metadata`, `arrow_ipc`, `plugin_protocol`, `dataset_uri`, `repair_plan`) and `-max_total_time=90` (1.5 min/target) for the 4 standard targets (`http_json`, `sse_stream`, `batch_config`, `schema_compat`).
- **Actual Total Fuzzing Time:** **1,574 s** (~26.2 minutes active fuzzing; bounded pre-release qualification campaign, not a multi-hour run)
- **Total Executions:** **18,076,045** across all 9 targets
- **Corpus Growth:** `10` seed files -> `16,348` final corpus files (**`+16,338`** newly discovered inputs)
- **Crashes / Panics / Hangs / OOMs / Artifacts:** **0**

| Target | Tier | Budget (s) | Actual (s) | Executions | Exec/s | Initial Corpus | Final Corpus | Growth | Edge Cov | Features | Peak RSS (MB) | Artifacts | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `fuzz_parquet_metadata` | priority | 240 | 246 | 97,449 | 396 | 1 | 345 | +344 | 704 | 1,338 | 170 | 0 | **PASS** |
| `fuzz_arrow_ipc` | priority | 240 | 241 | 5,863,622 | 24,330 | 2 | 105 | +103 | 412 | 849 | 510 | 0 | **PASS** |
| `fuzz_plugin_protocol` | priority | 240 | 241 | 3,220,411 | 13,362 | 1 | 2,999 | +2,998 | 2,538 | 8,623 | 550 | 0 | **PASS** |
| `fuzz_dataset_uri` | priority | 240 | 241 | 1,525,192 | 6,328 | 1 | 2,418 | +2,417 | 3,586 | 11,173 | 518 | 0 | **PASS** |
| `fuzz_repair_plan` | priority | 240 | 241 | 3,112,461 | 12,914 | 1 | 3,165 | +3,164 | 3,464 | 9,923 | 542 | 0 | **PASS** |
| `fuzz_http_json` | standard | 90 | 91 | 787,293 | 8,651 | 1 | 1,968 | +1,967 | 2,300 | 6,526 | 522 | 0 | **PASS** |
| `fuzz_sse_stream` | standard | 90 | 91 | 1,356,567 | 14,907 | 1 | 1,761 | +1,760 | 1,712 | 4,687 | 539 | 0 | **PASS** |
| `fuzz_batch_config` | standard | 90 | 91 | 656,631 | 7,215 | 1 | 2,046 | +2,045 | 4,541 | 11,754 | 523 | 0 | **PASS** |
| `fuzz_schema_compat` | standard | 90 | 91 | 1,456,419 | 16,004 | 1 | 1,541 | +1,540 | 1,503 | 4,171 | 497 | 0 | **PASS** |

## 3. Preserved Earlier Baseline Campaign (`2026-11-19`, Seed `20261119`)

Earlier short qualification run retained for comparison (`60s` primary / `30s` secondary, **433 s** active fuzzing, **8,936,647** total executions, **0** crashes):

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

## 4. Deterministic Regression Smoke (`fuzz_smoke`)

All 9 seed corpora and prior crash-regression fixtures (including the `fuzz_arrow_ipc` negative/oversized metadata length bounds regression) execute deterministically on every `cargo test --workspace --locked` run via `crates/parqonaut-bench/tests/fuzz_smoke.rs`.
