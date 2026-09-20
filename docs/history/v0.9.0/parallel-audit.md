# PARQONAUT v0.9.0 parallel audit (read-only)

Eight focused passes over the converged branch. Disposition: **fixed in branch** | **accepted (documented scope)** | **deferred**.

## Audit A — Cargo graph and dependency convergence

- **Finding:** Product crates resolve `arrow` / `parquet` **54.3.1**; `scripts/columnar-check.sh` fails on `arrow2` / `parquet2` in manifests and `crates/`.
- **Disposition:** fixed in branch (`just columnar-check`).

## Audit B — Stream migration semantic equivalence

- **Finding:** `parqonaut-stream` uses Arrow 54 `RecordBatch`, `parqonaut-columnar` aligners, resume checkpoints schema v1 with stable fingerprints.
- **Disposition:** fixed in branch (`resume_e2e`, schema vector tests).

## Audit C — Transform fusion and execution equivalence

- **Finding:** `compile_plan` fuses streamable segments; `zero_intermediate_io` asserts zero intermediate bytes; split remains explicit barrier.
- **Disposition:** fixed in branch.

## Audit D — Schema compatibility and complex types

- **Finding:** Canonical rules in `parqonaut-columnar::schema`; fixtures in `fixtures/schema/compatibility-vectors.json`.
- **Disposition:** fixed in branch; nested/complex Parquet fixtures continue to expand via scan/transform tests.

## Audit E — Backpressure, cancellation, progress, resume

- **Finding:** Relay capacity **4** with slow-consumer test; progress observers on stream pipeline; cancellation wired where async I/O exists.
- **Disposition:** fixed in branch; mid-pipeline cancel coverage strongest on storage/stream paths.

## Audit F — Repair execution reuse and RepairPlan invariants

- **Finding:** Repair execution uses shared batch/Parquet primitives; golden plans regenerated for Parquet 54; plan JSON semantics unchanged.
- **Disposition:** fixed in branch.

## Audit G — S3/range/multipart/full remote matrix

- **Finding:** Rewrite/merge on all four I/O legs covered by `remote_matrix_s3` (RustFS). Convert/split/partition/fused spec remote legs remain local or orchestrator-scoped in v0.9.
- **Disposition:** accepted (documented scope) — see `docs/transform.md` and acceptance matrix **PARTIAL** cells.

## Audit H — Zero-intermediate-I/O and performance evidence

- **Finding:** `zero_intermediate_io` + `just pipeline-demo`; `docs/history/v0.9.0/performance.md` records methodology (not marketing benchmarks).
- **Disposition:** fixed in branch.

## Audit I (optional) — Plugin/TUI/dashboard readiness

- **Finding:** `docs/development/plugin-execution-boundaries.md` documents v0.10 extension points only; no plugin loader.
- **Disposition:** accepted (v0.10 scope).
