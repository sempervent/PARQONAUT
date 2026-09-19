# PARQONAUT v0.6.0 — Unified `prqnt` CLI

## Breaking change

Install and invoke **`prqnt`** instead of **`parqonaut`**. There is no compatibility shim.

```bash
cargo install --path crates/parqonaut-cli
prqnt --help
prqnt scan ./data
```

## Removed entrypoints

- `paraclete-http`
- `generate-phase*` / `generate-golden-plans` product binaries (use `cargo xtask`)

## Developer tooling

```bash
cargo xtask fixtures repair
cargo xtask fixtures schema
cargo xtask fixtures orchestration
cargo xtask fixtures object-storage
cargo xtask golden-plans
```

## Naming cleanup

Active architecture, fixtures, scripts, Just targets, and CI jobs now use capability-oriented names. See `docs/development/naming-migration-v0.6.md`.
