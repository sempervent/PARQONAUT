# Development environment notes

## Cargo parallel jobs (`jobs may not be 0`)

Some developer machines have `~/.cargo/config.toml` containing:

```toml
[build]
jobs = 0
```

Older Cargo treated `0` as “use all CPUs”. **Cargo 1.98+ rejects `jobs = 0`** with:

```text
error: jobs may not be 0
```

**Fix (recommended):** edit `~/.cargo/config.toml` and remove the `jobs = 0` line, or set a positive integer (e.g. `jobs = 8`).

**Workaround:** set the environment variable before building:

```bash
export CARGO_BUILD_JOBS=8
```

The project `Justfile` exports a CPU-based default when `CARGO_BUILD_JOBS` is unset, so `just ci` and demo recipes work even with a broken global config.

PARQONAUT does **not** commit a global `jobs = 1` override — that would unnecessarily slow CI on healthy machines.
