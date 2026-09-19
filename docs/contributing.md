# Contributing

## Toolchain

- Rust stable with `rustfmt` and `clippy` (`just setup`)
- Optional: [just](https://github.com/casey/just), Docker (RustFS S3 tests), Postgres (store integration)

## Common commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
just ci
just naming-check
```

Fixtures and goldens:

```bash
cargo xtask fixtures repair
cargo xtask golden-plans
```

## Documentation

```bash
just docs-check   # verify generated refs + links + mdBook
just docs-build   # full site under target/pages/
cargo xtask docs cli
cargo xtask docs openapi
```

Pinned **mdBook** version: see `scripts/docs/env.sh` (`MDBOOK_VERSION`).

## Integration tests

| Target | Command |
|--------|---------|
| API (SQLite) | `just api-test` |
| S3 / RustFS | `just s3-demo` (requires RustFS) |
| Postgres | `PARACLETE_TEST_PG_URL=… cargo test -p paraclete-store --test postgres_store_integration -- --ignored` |

## Naming

Active user-facing docs and code must not reintroduce numbered “phase” product naming. Run `just naming-check`.

## Pull requests

- Keep runtime changes out of documentation-only PRs unless fixing doc-related tests or generators.
- Ensure CI passes: `rust`, `api-integration`, `postgres-integration`, `s3-integration`, **documentation**.

Release flow: merge to `main`, verify GitHub Pages deploy, tag with release notes **in the tagged commit** (see v0.7.1 discipline).
