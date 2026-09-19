# Phase 5 parallel agent plan

Baseline: **lead-owned** on `feat/object-storage` from `v0.4.1` / `main` @ `480b031`.

Mission: first-class S3-compatible object storage with the same deterministic planning, authorization, repair, verification, failure-isolation, and recovery guarantees as local filesystem operation.

See [phase-5-storage-recon.md](phase-5-storage-recon.md) for existing code assessment.

## Shared contracts (lead-frozen before parallel implementation)

| Type | Location (planned) | Notes |
|------|-------------------|-------|
| `DatasetLocation` | `parqonaut-storage/location.rs` | Local + S3 variants; credential-free serialization |
| `ObjectLocation` | `parqonaut-storage/location.rs` | Single object identity |
| `StorageBackend` | `parqonaut-storage/backend.rs` | list, head, range_read, write, capabilities |
| `StorageCapabilities` | `parqonaut-storage/backend.rs` | conditional create, multipart, etc. |
| `RemoteFingerprint` | `parqonaut-storage/fingerprint.rs` | stale-source detection for object stores |
| `PublicationVersion` | `parqonaut-storage/publication.rs` | immutable prefix + `_COMMITTED` marker |
| `StorageMetrics` | `parqonaut-storage/metrics.rs` | LIST/HEAD/range/byte counters |

Library boundaries unchanged in spirit:

```text
parqonaut-storage   → bytes, objects, publication
parqonaut-repair    → one dataset mechanics (via storage)
parqonaut-orchestrator → many datasets (via storage + repair)
parqonaut-cli       → command surface only
```

## Agent ownership

### LEAD (integration)

- Branch: `feat/object-storage`
- Owns: workspace, shared contracts, merges, `docs/phase-5-agent-plan.md`, release
- Must approve changes to `DatasetLocation`, `StorageBackend`, publication protocol

### SUBAGENT A — storage contracts + local backend

- Worktree/branch: `phase5-storage-local`
- Owns:
  - `crates/parqonaut-storage/` (except `s3.rs`, `publication.rs` until interfaces frozen)
  - `location.rs`, `backend.rs`, `local.rs`, `redact.rs`
  - backend contract tests (local)
- Must NOT edit: repair, orchestrator, CLI
- Depends on: lead contract commit

### SUBAGENT B — S3 backend

- Worktree/branch: `phase5-storage-s3`
- Owns:
  - `crates/parqonaut-storage/src/s3.rs`
  - S3 error mapping, retry hooks, multipart upload
  - MinIO compose file (with Agent F coordination)
- Depends on: frozen `StorageBackend` trait from lead
- Must NOT edit: local backend, publication protocol

### SUBAGENT C — fingerprints + remote metadata scan

- Worktree/branch: `phase5-fingerprint-scan`
- Owns:
  - `inventory.rs`, `fingerprint.rs`
  - Parquet footer range reader
  - storage-aware scan adapter in `paraclete-core` or thin wrapper crate
- Depends on: A/B backend read paths

### SUBAGENT D — publication + locking

- Worktree/branch: `phase5-publication`
- Owns:
  - `publication.rs`, `lock.rs`
  - immutable version layout, commit marker, CURRENT pointer
  - conditional publication semantics
- Depends on: frozen location + backend contracts

### SUBAGENT E — repair/orchestrator integration

- Worktree/branch: `phase5-integration`
- Owns:
  - `parqonaut-repair` storage hooks (minimal diffs)
  - `parqonaut-orchestrator` mixed-backend batch
  - cross-backend transfer paths
- Begins after A–D interfaces land

### SUBAGENT F — FOGBANK / chaos tests

- Worktree/branch: `phase5-fogbank`
- Owns:
  - `fixtures/phase5/fogbank/`
  - fixture generator binary
  - fault-injection test backend wrapper
  - `phase5_*` integration tests, Justfile recipes
- Can start fixture design in parallel; full tests after public API freeze

### SUBAGENT G — documentation

- Owns: `docs/phase-5.md`, `docs/storage-architecture.md`, `docs/s3.md`, `docs/remote-publication.md`, `docs/remote-recovery.md`, `docs/phase-5-security.md`, ADRs 0015–0019
- Begins after behavior stabilizes

## Parallelization graph

```text
LEAD contracts ──┬──► A local backend
                 └──► B S3 backend (after trait freeze)
A + B ──► C fingerprints / range scan
A + B ──► D publication / locks
C + D + E ──► repair + orchestrator integration
All ──► F FOGBANK tests
Stabilize ──► G docs
Final ──► parallel read-only audit (5+ agents)
```

## Serialized intentionally

- Publication protocol (D) before orchestrator resume semantics for remote outputs
- Local backend contract tests (A) before ripping local assumptions out of repair
- S3 backend (B) before FOGBANK integration tests assert real behavior

## Subagent deliverable format

Each agent returns:

```text
commit SHA
files changed
tests run
assumptions
known limitations
```

Lead reviews every diff before merge. No concurrent edits to lead-owned contract files.

## Acceptance reference

See Phase 5 specification acceptance criteria (82 items). Tracked in `docs/phase-5-acceptance.md` (to be created during implementation).
