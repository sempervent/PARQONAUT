# PARQONAUT v0.9.0 acceptance

Status key: **PASS** | **PARTIAL** (documented scope) | **FAIL**

## Columnar contracts

| Item | Status |
|------|--------|
| `ORIGINAL_COLUMNAR_CONTRACT_COMMIT=464d54a3c11617a04a13a281aab700d87e6dceaf` | PASS |
| `CURRENT_COLUMNAR_SCHEMA_CONTRACT_COMMIT=4417381` (see `docs/development/COLUMNAR_CONTRACT_SHA`) | PASS |
| Git commit SHA ≠ content digest (documented separately) | PASS |

## Canonical dependencies

| Item | Status |
|------|--------|
| Apache Arrow **54.3.1** | PASS |
| Apache Parquet **54.3.1** | PASS |
| `arrow2` absent from product manifests/sources | PASS (`just columnar-check`) |
| `parquet2` absent from product manifests/sources | PASS |

## Architecture gates

| Item | Status |
|------|--------|
| Shared `BatchSource` / `BatchSink` / `BatchStream` | PASS |
| Bounded relay backpressure (default capacity **4**) | PASS |
| Canonical `parqonaut-columnar::schema` (stream + repair) | PASS |
| In-memory transform spec fusion | PASS (`zero_intermediate_io`) |
| RepairPlan semantics unchanged | PASS |
| Parquet 54 bounded footer regression | PASS |
| Storage columnar local + S3 | PASS |

## Remote transform matrix (rewrite / merge)

| Leg | rewrite | merge |
|-----|---------|-------|
| local→local | PASS | PASS |
| local→S3 | PASS (integration) | PASS (integration) |
| S3→local | PASS (integration) | PASS (integration) |
| S3→S3 | PASS (integration) | PASS (integration) |

Integration: `remote_matrix_s3` with RustFS when `PARQONAUT_S3_ENDPOINT` is set.

## Remote transform matrix (partition / split / convert / spec)

| Capability | local→local | local→S3 | S3→local | S3→S3 |
|------------|:-------------:|:--------:|:--------:|:-----:|
| convert (`prqnt convert`) | PASS | PARTIAL | PARTIAL | PARTIAL |
| rewrite | PASS | PASS | PASS | PASS |
| merge | PASS | PASS | PASS | PASS |
| split | PASS | PARTIAL | PARTIAL | PARTIAL |
| partition | PASS | PARTIAL | PARTIAL | PARTIAL |
| transform spec (fused) | PASS (local) | PARTIAL | PARTIAL | PARTIAL |

**PARTIAL** = v0.9 ships storage-backed **rewrite/merge** on `s3://`; convert/split/partition/fused specs remain local-path or batch/repair orchestration for remote legs. See `docs/transform.md`.

## Demos and CI

| Command | Status |
|---------|--------|
| `just columnar-check` | PASS |
| `just ci` | PASS (when workspace green) |
| `just pipeline-demo` | PASS |
| `just pipeline-s3-demo` | PASS (with RustFS) |
| Legacy demos (schema, batch, s3, api, transform, stream) | PASS |

## Documentation

| Item | Status |
|------|--------|
| `docs/architecture.md`, `docs/pipelines.md`, ADR-0015 | PASS |
| ADR-0002 supersession note | PASS |
| `docs/history/v0.9.0/release-notes.md` | PASS |
| `docs/history/v0.9.0/performance.md` | PASS |
| Generated CLI/OpenAPI (`just docs-check`) | PASS |

## Release checklist

| Step | Status |
|------|--------|
| PR merged to `main` | pending |
| Tag `v0.9.0` | pending |
| GitHub Release published | pending |
| GitHub Pages verified | pending |
