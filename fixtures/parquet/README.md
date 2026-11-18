# Parquet fixtures

This directory includes a **tiny valid Parquet** file for early integration tests and manual
smoke checks. Regenerate it any time with:

```bash
cd python/parqonaut_plugins
uv sync --group dev
uv run python ../../scripts/generate_parquet_fixture.py
```

The generator uses **PyArrow** (declared only as a Python dev dependency) so the Rust
workspace does not pick up a Parquet writer stack prematurely.
