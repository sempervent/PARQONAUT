# SQLx migrations

## SQLite (`migrations/`)

Migrations run in **lexicographic filename order**. Prefix with a UTC-ish timestamp so new files sort
after existing ones (e.g. `20250420120000_description.sql`).

- **`20250415120000_init.sql`** — core `scan_*` tables; begins with `PRAGMA foreign_keys = ON` for
  `sqlx migrate run` tooling. Runtime connections also set **`foreign_keys=ON`** via SQLx (ADR 0025).
- Later files add columns and tables (`run_summary_json`, `scan_jobs`, auth tokens, etc.).

## Postgres (`migrations/postgres/`)

Separate folder for **`PostgresScanStore`**: one consolidated schema (Phase 17). Do not mix SQLite and Postgres
migration files in the same directory.

Do **not** reorder or rename applied migrations in production databases; add a new forward migration instead.
