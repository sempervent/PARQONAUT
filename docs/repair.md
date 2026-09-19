# Diagnosis and repair

PARQONAUT repair is **evidence-driven** and **plan-bound**:

1. **Scan** the source dataset (read-only).
2. **Diagnose** findings into repair opportunities.
3. **Plan** — produce a durable JSON plan with operation IDs, safety class, and fingerprints.
4. **Repair** — write to a **separate** output path or `s3://` prefix; never in-place by default.
5. **Verify** — compare before/after invariants and optional execution manifests.

CLI commands (`diagnose`, `plan`, `repair`, `verify`, `check`, `doctor`) call **`parqonaut-app`**. HTTP clients use `/api/v1/diagnose`, `/plans`, `/checks`, `/repairs`, `/verifications` as documented in [HTTP API](./api.md).

## Safety and authorization

- Operations are classified (safe vs **ReviewRequired**).
- Review-required steps need explicit **`--authorize <operation_id>`** on CLI or equivalent API authorization.
- Plans bind to dataset fingerprints; stale plans are rejected.

Deep dives:

- [Repair model](./repair-model.md)
- [Repair planning](./repair-planning.md)
- [Repair policy](./repair-policy.md)
- [Repair plan contract](./plan-contract.md)
