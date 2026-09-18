# ADR-0007: Deterministic schema compatibility model

## Status

Accepted

## Context

Phase 2 detected schema drift as coarse signatures. Phase 3 requires structured per-field diffs and loss classification without AI inference.

## Decision

Implement a compatibility lattice classifying conversions as `Lossless`, `ConditionallyLossless`, `PotentiallyLossy`, or `Unsupported`. Numeric widening chains (int8→…→int64, float32→float64) and nullable widening are lossless only when explicitly allowed by schema policy. String↔numeric and similar ambiguous transitions are `Unsupported`.

## Consequences

- Repair proposals cite structured diffs, not string summaries.
- Potentially lossy conversions remain blocked unless future policy mechanisms explicitly allow them.
