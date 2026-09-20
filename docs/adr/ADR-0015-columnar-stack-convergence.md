# ADR-0015: Columnar stack convergence (Apache Arrow Rust 54)

## Status

Accepted (v0.9.0 implementation in progress)

## Context

PARQONAUT currently runs **three** columnar families: parquet 53 (scan), arrow/parquet 54 (transform/repair rewrite), and arrow2/parquet2 (stream). That split blocks in-memory composition and duplicates schema logic.

## Decision

Converge product engines on **Apache Arrow Rust 54.3.1** and **Apache Parquet Rust 54.3.1**, centralized in `[workspace.dependencies]`, with shared execution types in `parqonaut-columnar`.

Rationale: aligned with existing transform/repair rewrite path; mature Parquet reader/writer; S3 range reads remain in `parqonaut-storage`; migration cost lower than jumping to arrow 60 while scan catches up.

## Consequences

- Remove direct `arrow2` / `parquet2` dependencies.
- Rewrite `parqonaut-stream` on `RecordBatch`.
- Unify schema compatibility for stream + repair.
- Enable bounded `BatchStream` pipelines without temp Parquet between streamable stages.

## Alternatives considered

- **Arrow 60 / latest:** newer, but widens scan/transform gap and increases migration risk for v0.9 timeline.
- **Keep arrow2 in stream:** rejected; perpetuates dual stacks and blocks shared types.
