# PARQONAUT v0.8.1 — Transform demo correction

Patch release correcting `just transform-demo` command-line flags and merge input glob so the recipe matches the released `prqnt` CLI.

No intended runtime behavior change from v0.8.0.

## Changes

- `Justfile`: use `--output`, `--by`, and an explicit Parquet glob for merge (was `-o`, `--partition-by`, and directory merge input)
- S3 batch demos: non-overlapping fixture paths; inventory lists committed publication data for verification; batch configs accept path-style local output overrides

## Upgrade

Safe to upgrade from v0.8.0 without migration steps.
