# SQL in `paraclete-store`

Parameterized SQL lives in **`sqlite_store.rs`** and **`auth_store.rs`** as Rust string literals next to the
methods that execute them. This keeps the SQLite implementation in one place while HTTP and engine code stay
free of SQL.

A future **Postgres** backend would likely:

- lift shared query *intent* into small helpers or named modules per domain (runs, jobs, auth), and
- keep dialect-specific SQL in backend-specific modules (see `docs/postgres-sqlite-compatibility.md`).
