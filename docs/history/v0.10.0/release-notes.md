# PARQONAUT v0.10.0 - Plugin Execution Bridge

PARQONAUT v0.10.0 connects Rust scan/batch engines to a versioned Python plugin protocol, CLI management, and optional server-managed durable scan jobs.

## Highlights

- Plugin protocol v1 and PARQONAUT Python SDK (`parqonaut_plugins`)
- Scan analyzer plugins (subprocess host, provenance on findings)
- Schema-preserving Arrow IPC batch transform plugins
- Digest-pinned durable job payloads and recovery revalidation
- Subprocess lifecycle, cancellation, backpressure, and resource limits
- CLI plugin list/inspect/validate and transform spec plugins
- Server plugin catalog HTTP API and async scan jobs (opt-in)
- Local/S3 batch storage matrix independence
- Full active `parqonaut-*` namespace migration

## Trust model

> PARQONAUT plugins are trusted code. Subprocess execution provides fault containment, cancellation, and resource control. It is not a security sandbox for malicious plugins.

## Server scope (v0.10)

- Durable **scan** jobs may request allowlisted analyzer plugins by name.
- Batch transform plugins run via `prqnt transform --spec` and `parqonaut-app` batch execution.
- Generic HTTP transform jobs are **not** part of v0.10.

See [acceptance](acceptance.md) for verification categories and [migration](migration.md) for rename notes.
