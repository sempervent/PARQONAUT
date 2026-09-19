# Quick start

This guide walks through a **local** scan and a **non-destructive** repair plan using repository fixtures.

## Prerequisites

- Rust stable (`rustfmt`, `clippy`)
- Built `prqnt` binary (see [Installation](./installation.md))

## Scan a fixture dataset

```bash
prqnt scan fixtures/scan/single_parquet/data.parquet
```

Use `--json` for machine-readable output on supported commands.

## Diagnose and plan

```bash
prqnt doctor fixtures/schema/frankenlake-v2 \
  --policy fixtures/schema/policy.toml
```

`doctor` runs scan → diagnose → plan in one invocation. It does not write repaired data unless you pass `--repair` and `--output`.

Generate a plan file explicitly:

```bash
prqnt plan fixtures/schema/frankenlake-v2 \
  --policy fixtures/schema/policy.toml \
  --output /tmp/frankenlake-plan.json
```

## Execute a safe repair (separate output)

Never repair in place — always write to a new directory:

```bash
prqnt repair fixtures/schema/frankenlake-v2 \
  --plan /tmp/frankenlake-plan.json \
  --output /tmp/frankenlake-repaired
```

Operations classified **ReviewRequired** need explicit authorization:

```bash
prqnt repair … --authorize <operation_id>
```

## CI-style check

```bash
prqnt check fixtures/schema/frankenlake-v2 \
  --policy docs/ci-policy.toml
```

Exit codes are stable; see [CLI overview](./cli.md).

## Try the HTTP server

```bash
prqnt serve --state-dir /tmp/prqnt-demo --listen 127.0.0.1:8080
```

Bootstrap an admin token with `PRQNT_BOOTSTRAP_ADMIN_TOKEN` (never commit or log the value). See [Authentication](./authentication.md) and [HTTP API](./api.md).
