# v0.9.0 performance notes

## Environment

Recorded on developer hardware during release verification (Apple Silicon, Rust 1.98+, workspace `0.9.0`).

## Architectural metric (primary)

Fully streamable transform specs (for example rewrite → partition) report:

```text
intermediate_files_created = 0
intermediate_bytes_written = 0
intermediate_bytes_read = 0
```

Evidence: `crates/parqonaut-transform/tests/zero_intermediate_io.rs`, `just pipeline-demo`.

v0.8.1 materialized `.parqonaut-spec-*` staging between streamable transform steps when multi-step specs were executed.

## Throughput

Formal before/after benchmarks (CSV→Parquet, merge, partition, multi-step spec) against tag `v0.8.1` were not re-run in this document revision. No severe regression was observed in local demo and integration tests during convergence.

Re-run locally:

```bash
git worktree add ../PARQONAUT-v0.8.1 v0.8.1
# identical fixture commands on both trees, compare wall time and intermediate bytes
```
