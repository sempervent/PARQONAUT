# Third-Party and Predecessor Licensing

PARQONAUT consolidates code from three predecessor repositories. This document records **what was imported**, **what each upstream declared**, and **what appears in this repository**. It is factual documentation, not legal advice.

## Summary

| Component | Location in PARQONAUT | Upstream declared license | Files in PARQONAUT |
|-----------|----------------------|---------------------------|------------------|
| Paraclete | `crates/paraclete-*`, `fixtures/`, `python/paraclete_plugins/`, `scripts/` | MIT | MIT (`LICENSE`) |
| streaming-parquet (maw) | `crates/parqonaut-stream/`, `benches/stream-throughput/` | MIT | MIT (`LICENSE`) |
| parqknife | `crates/parqonaut-transform/` | `MIT OR Apache-2.0` in `Cargo.toml` | See parqknife section below |

The repository root [`LICENSE`](LICENSE) is **MIT** and applies to the combined work unless a more specific notice applies to a subtree (notably `parqonaut-transform`).

## Paraclete (MIT)

- **Source:** https://github.com/sempervent/paraclete @ `4dd0316f76811370bdd38f7b3778beed7b71a1c6`
- **Upstream copyright:** Copyright (c) 2026 Joshua Grant
- **Upstream license file:** MIT
- **Imported:** workspace crates (`paraclete-types`, `paraclete-core`, `paraclete-report`, `paraclete-store`, `paraclete-service`, `paraclete-plugin-protocol`), fixtures, Python plugin contracts, scripts

## streaming-parquet / maw (MIT)

- **Source:** https://github.com/sempervent/streaming-parquet @ `4de2dbe2f97bfe5a43cf53c94f05eb11e1bfa4ad`
- **Upstream copyright:** Copyright (c) 2025 Joshua Grant
- **Upstream license file:** MIT
- **Imported:** `crates/parqonaut-stream/` (refactored from binary to library), stream tests, throughput bench stub

## parqknife (MIT OR Apache-2.0)

- **Source:** https://github.com/sempervent/parqknife @ `a179c9a8d5ddd0c2cbc055e631135470d63a20a7`
- **Upstream `Cargo.toml`:** `license = "MIT OR Apache-2.0"`, `authors = ["parqknife contributors"]`
- **Upstream `LICENSE` file at import:** contained **GNU Affero GPL v3** text, which **conflicted** with the crate metadata. PARQONAUT **did not copy** that AGPL file into this repository.
- **Imported:** `crates/parqonaut-transform/src/` and `crates/parqonaut-transform/tests/` (with Arrow 54 API fixes applied during import)
- **This repository:**
  - `crates/parqonaut-transform/Cargo.toml` retains `license = "MIT OR Apache-2.0"`
  - [`LICENSE-APACHE-2.0`](LICENSE-APACHE-2.0) provides the Apache-2.0 license text for recipients who elect that option under the dual-license terms declared in the crate manifest

If you distribute `parqonaut-transform` under Apache-2.0, include `LICENSE-APACHE-2.0` and applicable copyright notices. If you distribute under MIT, include the root `LICENSE`.

## Unified CLI and glue code

- **`crates/parqonaut-cli/`**, root `Cargo.toml`, `Justfile`, CI, and consolidation docs: MIT, Copyright (c) 2026 Joshua Grant (see root `LICENSE`).

## Rust dependency licenses

Runtime dependencies are listed in `Cargo.lock`. Their licenses are not reproduced here; use `cargo license` or your organization's compliance tooling for a full dependency audit.
