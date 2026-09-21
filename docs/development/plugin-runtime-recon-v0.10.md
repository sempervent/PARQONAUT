# Plugin runtime recon (v0.10.0)

Baseline: `main` @ `a786901` (v0.9.1). Branch `feat/plugin-execution`, workspace **0.10.0**.

## Existing assets

| Area | Location | State |
|------|----------|--------|
| Rust protocol | `crates/parqonaut-plugin-protocol/` (renamed from `paraclete-plugin-protocol`) | Types only; execution deferred |
| Plugin host (new) | `crates/parqonaut-plugin-host/` | Catalog stub; subprocess TBD |
| Core hook | `crates/paraclete-core/src/plugin_invoker.rs`, `Orchestrator.plugins` | Trait wired; **not invoked** in scan path |
| Python SDK | `python/parqonaut_plugins/` | Pydantic mirrors protocol v1 |
| Sample manifest | `fixtures/plugins/sample_manifest.json` | Updated to v1 shape |
| JSON schema test | `parqonaut-plugin-protocol/tests/json_schema.rs` | Generates `PluginManifest` schema |
| Docs | `docs/development/plugin-execution-boundaries.md`, `docs/history/v0.9.1/plugin-readiness.md` | v0.9 planning only |

## Protocol field disposition (v1)

| Field / type | Action |
|--------------|--------|
| `PluginManifest.name/version/entrypoint` | **keep** + stricter validation |
| Flat `PluginCapabilities.supported_formats/phases` | **extend** → `capabilities.scan` + `capabilities.batch_transform` |
| `metadata` | **keep** |
| — | **add** `protocol_version` (must be `1`) |
| — | **add** `requires_parqonaut` (optional semver range) |
| Manifest filename | **rename** canonical → `parqonaut-plugin.json` |
| `PluginScanContext.discovered_files` | **keep** |
| `hints` | **keep** (bounded at host) |
| — | **add** `inventory_summary`, `builtin_finding_summaries` (phase-gated population) |
| `PluginRequest` | **extend** `protocol_version`, `config` |
| `PluginResponse` | **extend** `protocol_version` |
| `PluginExecutor` trait (in-process) | **keep** as boundary; host implements via subprocess |
| `PluginExecutorError::NotImplemented` | **extend** with typed host errors in `parqonaut-plugin-host` |
| Python `PluginFinding` vs Rust `PluginFindingContribution` | **rename** Python to match or alias in runner |
| Entrypoint `paraclete_plugins.*` | **remove** from public fixtures; use `parqonaut_plugins.*` |

Unknown `protocol_version` on wire: **reject** (no forward-compatible deserialize).

## Integration points (to wire)

| Surface | Crate | Notes |
|---------|-------|-------|
| Scan pipeline | `paraclete-core` orchestrator, `parqonaut-app` scan use case | Phases PreScan / PostInventory / PostRules |
| Batch transform | `parqonaut-columnar` `BatchTransform`, `parqonaut-transform` spec compile | New `plugin` operation; schema-preserving |
| Progress | `parqonaut-workflow` | PluginStarted / BatchProgress / Failed |
| CLI | `parqonaut-cli` | `plugin list|inspect|validate`, `scan --plugin` |
| Server | `paraclete-service`, `paraclete-store` | Disabled by default; catalog GET; scan job plugin names |
| Cancellation | workflow + host | Kill/reap child |

## Phase context (target)

| Phase | Context populated |
|-------|-------------------|
| `pre_scan` | Normalized `ScanRequest` only |
| `post_inventory` | Request + bounded inventory summary + discovered asset identities |
| `post_rules` | Above + bounded built-in finding summaries |

No fake fields at earlier phases.

## Transport

| Class | stdin/stdout | stderr |
|-------|----------------|--------|
| Scan analyzer | JSON `PluginRequest` / `PluginResponse` | diagnostics (bounded) |
| Batch transform | Arrow IPC stream / stream | diagnostics (bounded) |

One subprocess per transform **execution** (not per batch).

## Out of scope v0.10

- TUI / web dashboard (v0.11)
- Marketplace / remote install
- Native `.so` plugins, PyO3 in-process
- Hostile-code sandbox claims
- Repair authorization via plugins
- Auto-run discovered plugins

## Trust model (public)

Plugins are **trusted admin-installed code** run in a **resource-constrained subprocess** for fault containment, **not** a security boundary against malicious code.

## Next implementation gates

1. **Protocol freeze commit** (`feat(plugin): freeze plugin protocol v1`) after golden JSON schema + Python parity fixtures.
2. Host: digest, path canonicalization, env scrubbing, runner invocation.
3. Scan + batch E2E, adversarial fixtures, CI `plugin-integration`.
