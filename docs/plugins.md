# PARQONAUT plugins

PARQONAUT plugins extend forensic **scan analyzers** and **batch transforms** using protocol v1. Plugins are **trusted code**: they run in isolated subprocesses for fault containment, cancellation, and resource limits — not as a hostile-code sandbox.

## Capabilities

| Class | Surface | Description |
|-------|---------|-------------|
| Scan analyzer | `prqnt scan --plugin NAME` | Phased findings with namespaced codes and evidence |
| Batch transform | `prqnt transform --spec …` plugin steps | Schema-preserving Arrow IPC row transforms |

## Discovery (CLI)

CLI discovery uses explicit roots (`PARQONAUT_PLUGIN_ROOTS`) and optional `~/.config/parqonaut/plugins/`. Commands:

- `prqnt plugin list`
- `prqnt plugin inspect NAME`
- `prqnt plugin validate PATH`

## Server policy

`prqnt serve` keeps plugin execution **disabled by default**. Operators must set:

- `PARQONAUT_SERVER_PLUGINS_ENABLED=true`
- `PARQONAUT_SERVER_PLUGIN_ROOTS` (colon-separated directories)
- `PARQONAUT_SERVER_ALLOWED_PLUGINS` (comma-separated names; empty allowlist = deny all)

HTTP clients may pass **plugin names only** on scan jobs (`plugins: ["example-rules"]`). Paths, executables, and roots are never accepted from HTTP.

Catalog: `GET /api/v1/plugins`, `GET /api/v1/plugins/{name}` (allowlisted entries only).

Durable scan jobs store **digest-pinned** plugin identities. Recovery revalidates digests; changed plugin code fails with `plugin_stale` before spawn.

## Python runtime & digest boundary

Plugin packages require a Python interpreter (`>=3.12`) with `parqonaut_plugins` SDK dependencies (`pydantic`, `pyarrow`) installed from `python/parqonaut_plugins/uv.lock`.

- Set `PARQONAUT_PLUGIN_PYTHON` to an explicit interpreter path (recommended in production/CI, e.g. `python/parqonaut_plugins/.venv/bin/python`) or a `PATH`-resolvable executable name (`python3`).
- Set `PARQONAUT_PLUGIN_SDK_PATH` to `python/parqonaut_plugins/src` when running outside an installed wheel.
- **Digest boundary:** A plugin's SHA-256 digest pins `parqonaut-plugin.json` and the non-excluded files inside that plugin's package directory (rejecting unexpected symlinks). The Python interpreter binary and external site-packages (`pyarrow`, `pydantic`) are operator-provisioned runtime dependencies **outside** the plugin package digest.

See [Plugin authoring](plugin-authoring.md) and [Server](server.md).
