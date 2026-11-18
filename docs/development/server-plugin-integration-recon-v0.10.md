# Server plugin integration recon (v0.10)

## Request path (HTTP scan)

```text
HTTP POST /api/v1/scans | /api/v1/jobs/scans
  → handlers::post_async_scan | post_scan_sync
  → ParqonautService::submit_scan_job | execute_scan_and_persist
  → location::dataset_location_from_scan_target + StoragePolicy::validate_dataset
  → ParqonautApp::scan (local) or storage scan (S3)
  → ScanEngine (+ optional ScanPluginBridge → PluginHost)
```

## Durable scan jobs

```text
StartScanRequest (HTTP)
  → validate location + plugin policy
  → resolve & pin plugin identities
  → ScanJobPayload JSON → scan_jobs.request_json
  → worker claim_next_queued_scan_job
  → run_scan_job(payload, cancel flag)
  → revalidate pins vs current catalog
  → execute_scan_and_persist
  → complete_scan_job_success
```

## Configuration construction

| Layer | Location |
|-------|----------|
| `prqnt serve` TOML | `crates/parqonaut-cli/src/serve.rs` (`ServerFileConfig`, storage policy) |
| Server plugin policy | `PARQONAUT_SERVER_*` env + `[plugins]` TOML section (v0.10) |
| `ParqonautService` | `with_storage_policy` + optional `ServerPluginState` |
| Storage policy | `parqonaut-app::StoragePolicy` applied before enqueue and scan |

## Plugin catalog placement

Single validated catalog at server startup:

```text
ServerPluginPolicy
  → PluginCatalog::discover_from_roots (non-executing)
  → allowlist filter
  → ServerPluginState (held on ParqonautService)
```

HTTP handlers call `ParqonautService` only; they do not construct `PluginHost` directly.

## Recovery

`worker.rs` calls `recover_stale_scan_jobs` before claim. Reclaimed jobs reuse stored `request_json` including pinned digests; execution-time revalidation enforces stale-plugin rejection.
