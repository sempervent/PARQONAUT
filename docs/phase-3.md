# PARQONAUT Phase 3

Phase 3 adds policy-governed schema reconciliation with explicit authorization, durable plan contracts, execution manifests, and CI-oriented checks.

## Workflow

```text
scan → diagnose → plan → safe ops (automatic)
                      → review-required ops (blocked until --authorize)
                      → blocked ops (never executed)
                      → verify → manifest
```

## FRANKENLAKE v2

Pathological laboratory at `fixtures/phase3/frankenlake-v2/` combining:

- tiny files, poor row groups, mixed compression
- missing statistics, oversized file
- compatible numeric + nullable drift
- blocked `sensor_id` utf8 vs int64 conflict

```bash
just phase3-demo
```

## Key modules

| Module | Role |
|--------|------|
| `schema/` | Diff, compatibility lattice, canonical resolution |
| `schema_policy.rs` | TOML policy + fingerprint |
| `authorization.rs` | Per-operation audit trail |
| `manifest.rs` | `.parqonaut-manifest.json` sidecar |
| `deps.rs` | Operation ordering |
| `check.rs` | CI exit codes |

See also: [schema-reconciliation.md](schema-reconciliation.md), [plan-contract.md](plan-contract.md), [ci-policy.md](ci-policy.md).
