# v0.6.0 parallel audit (summary)

## A — Public binary / install surface

- `parqonaut-cli` ships **`prqnt` only**; `cargo install --path crates/parqonaut-cli` installs `$root/bin/prqnt`.
- Legacy **`paraclete-http`** binary target removed; `paraclete-service` remains a library.
- Developer boundary: **`xtask`** via `cargo xtask` (`.cargo/config.toml` alias).

## B — Active phase naming

- `scripts/check-active-naming.sh` enforces zero numbered-phase paths/names in active trees (with documented history exclusions).
- Core scan modules renamed to `scan_rules`, `scan_findings`, `schema_findings`.

## C — Fixture / reference integrity

- Fixture roots migrated to capability paths; golden repair plans regenerated (`cargo xtask golden-plans`).
- SHIPWRECK batch config policies point at `fixtures/schema/policy.toml`.

## D — Docs / CLI migration

- README and CHANGELOG document **`parqonaut` → `prqnt`** breaking change.
- CLI help uses PARQONAUT capability descriptions (no Paraclete/parqknife/maw engine labels).

## E — Cargo binary audit

| Package | Binary | Classification |
|---------|--------|----------------|
| `parqonaut-cli` | `prqnt` | PUBLIC PRODUCT |
| `xtask` | `xtask` | DEVELOPER TOOL |

## F — Backwards contract

- Repair plan golden tests pass with updated fixture paths and `parqonaut_version` `0.6.0`; `schema_version` fields unchanged.
