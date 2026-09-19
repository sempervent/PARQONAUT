# Command overview

All user-facing operations go through **`prqnt`**. Global flag:

- **`--json`** — JSON output where the command supports it (errors may still print JSON when `--json` is set).

## Exit semantics

| Code | Meaning (typical) |
|------|-------------------|
| `0` | Success |
| `1` | Operational / usage error |
| `2` | Findings or policy violations (`check`, some repair gates) |
| `3` | Blocked repair (safety / authorization) |
| `4` | Stale plan or fingerprint mismatch |
| `5` | Internal or I/O failure |

Exact behavior is covered by CLI integration tests; do not rely on undocumented codes.

## Capability map

| Command group | Purpose |
|---------------|---------|
| **`scan`** | Forensic dataset scan (local or `s3://`) |
| **`inspect`**, **`rewrite`**, **`convert`** | Direct Parquet/CSV utilities (not routed through `parqonaut-app`) |
| **`diagnose`**, **`plan`**, **`plan-diff`**, **`check`**, **`repair`**, **`verify`**, **`doctor`** | Repair and schema workflows via **`parqonaut-app`** |
| **`batch *`** | Multi-dataset orchestration via **`parqonaut-app`** |
| **`serve`** | HTTP application server and durable jobs |

## When to use which command

- **`scan`** — inventory, findings, dataset identity; input to planning.
- **`doctor`** — interactive CLI path: scan + diagnose + plan (optional repair). Not a separate HTTP job in v0.7.
- **`plan` / `repair` / `verify`** — durable, evidence-bound repair pipeline with separate output locations.
- **`check`** — CI gate; non-mutating.
- **`batch plan|repair|resume|verify`** — fleet operations with journal and resume semantics.
- **`serve`** — automation, remote operators, async jobs.

## Generated reference

Flag-level detail is generated from Clap to avoid drift:

- [CLI reference (generated)](./reference/cli.md) — refresh with `cargo xtask docs cli`
