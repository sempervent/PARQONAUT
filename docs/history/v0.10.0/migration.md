# PARQONAUT v0.10.0 migration

## Rust crate rename (pre-1.0, no compatibility shims)

| Former | v0.10 active |
|--------|----------------|
| `paraclete-types` | `parqonaut-types` |
| `paraclete-core` | `parqonaut-core` |
| `paraclete-report` | `parqonaut-report` |
| `paraclete-store` | `parqonaut-store` |
| `paraclete-service` | `parqonaut-service` |

Update `Cargo.toml` path dependencies, CI job names, and import paths. There are no `paraclete-*` re-export crates.

## Python packaging

| Former | v0.10 active |
|--------|----------------|
| PyPI / project name (conceptual) | `parqonaut-plugins` |
| Import module | `parqonaut_plugins` |

Install from `python/parqonaut_plugins` with `uv sync` as documented in [plugin authoring](../../plugin-authoring.md).

## Server plugin jobs

- Server plugin execution remains **disabled by default**.
- Enable with `PARQONAUT_SERVER_PLUGINS_ENABLED`, explicit `PARQONAUT_SERVER_PLUGIN_ROOTS`, and `PARQONAUT_SERVER_ALLOWED_PLUGINS`.
- Durable scan jobs store **digest-pinned** resolved plugin selections; recovery revalidates against on-disk catalog digests.

## CLI

- Use `prqnt plugin …` and `prqnt transform --spec` for local/batch plugin workflows.
- HTTP scan jobs accept **plugin names only** in JSON (no paths or executables).
