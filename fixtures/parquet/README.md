# Parquet fixtures

Phase 0 includes a **tiny valid Parquet** file for early integration tests and manual
smoke checks. Regenerate it any time with:

```bash
cd python/paraclete_plugins
uv sync --group dev
uv run python ../../scripts/generate_parquet_fixture.py
```

The generator uses **PyArrow** (declared only as a Python dev dependency) so the Rust
workspace does not pick up a Parquet writer stack prematurely.
