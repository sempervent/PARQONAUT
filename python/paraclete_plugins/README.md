# paraclete-plugins (Python)

This package holds **Pydantic models** that mirror the Rust plugin protocol and future
user-authored plugins. The Rust engine remains authoritative; Python is an extension membrane.

## Development

From this directory:

```bash
uv sync --group dev
uv run ruff check .
uv run ruff format .
uv run pytest
```

Regenerate the tiny Parquet fixture used by the repo (requires dev dependencies):

```bash
uv run python ../../scripts/generate_parquet_fixture.py
```
