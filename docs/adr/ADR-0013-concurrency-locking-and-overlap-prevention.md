# ADR-0013: Concurrency, locking, and overlap prevention

## Status

Accepted

## Context

Concurrent batch repairs must not corrupt shared output trees or exceed operator-specified parallelism.

## Decision

- Bounded concurrency via Tokio semaphore (`max_concurrency` / `--jobs`).
- Deterministic output mapping: `batch.output_root` + per-dataset segment (default dataset id).
- Duplicate outputs and source/output or cross-dataset path overlap are rejected at check/plan time.
- Exclusive per-output lockfiles during execution (`{output}.parqonaut.lock`).
- Cooperative cancellation stops scheduling; in-flight repairs finish at the repair executor commit boundary.

## Alternatives considered

- Global batch mutex: rejected ( unnecessary throughput loss).
- Advisory locks only in journal: rejected ( insufficient cross-process safety).

## Consequences

- Output roots must be disjoint across datasets in a batch.
- Mid-rewrite interruption relies on repair staging semantics (ADR-0010), not Parquet transactions.
