# ADR-0004: Evidence-driven repair planning

## Status

Accepted (Phase 2)

## Context

PARQONAUT must prescribe repairs from forensic evidence, not heuristics disguised as certainty.

## Decision

Repair plans are generated only from **findings** (scan + diagnosis) via explicit **rules**:

```text
Finding (+ evidence) → RepairRule → 0..N RepairOperation proposals
```

Each `RepairOperation` records `finding_ids`, `evidence`, `preconditions`, `rationale`, and
`expected_outcomes`. Plans are versioned JSON (`schema_version: 1`) with embedded policy.

Operation and plan IDs are **content-derived hashes** for determinism; execution IDs are random.

## Consequences

- Rules live in `parqonaut-repair`; CLI remains thin.
- New repair types require a finding code + rule + (optional) executor support.
- Golden/contract tests guard plan JSON shape.
