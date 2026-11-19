# Database Migration, Backup, and Restore Validation (v0.12.0)

## 1. Schema Evolution History

PARQONAUT embeds forward-only `sqlx` migrations for both **SQLite** (`crates/parqonaut-store/migrations/`) and **PostgreSQL** (`crates/parqonaut-store/migrations/postgres/`).

- **v0.8 / early v0.9 (SQLite migrations 1–7):** `scan_runs`, `scan_reports`, `scan_assets`, `scan_datasets`, `scan_findings`, `scan_jobs` (with recovery lease columns), and `auth_tokens` (with `note`, `token_prefix`, `last_used_at`, `replaced_by_token_id`).
- **v0.9.2 / v0.10.0 (SQLite migration 8; Postgres migrations 1–2):** `20250919140000_application_jobs.sql` renames `scan_jobs` -> `application_jobs` and adds `job_kind`, `payload_schema_version`, `cancel_requested`, `result_ref_json`. Note: v0.9.2 and v0.10.0 share the exact same database schema.
- **v0.11.0 / v0.12.0 (SQLite migration 9; Postgres migration 3):** `20261117120000_job_events_and_idempotency.sql` adds `last_event_seq` on `application_jobs`, `application_job_events`, and `idempotency_keys`. No new DDL was needed for v0.12.0.

Automatic schema downgrades are **not** supported; rollback uses pre-upgrade database backups.

## 2. Automated Upgrade & Recovery Test Suite

Implemented in `crates/parqonaut-store/tests/migration_upgrade.rs`:

1. **`sqlite_upgrade_from_pre_application_jobs_v0_8_schema_and_restart`:**
   - Initializes a pre-rename v0.8/early-v0.9 SQLite database using migrations 1–7.
   - Seeds historical `scan_runs`, `scan_reports`, `scan_jobs`, and `auth_tokens`.
   - Captures a WAL-safe backup via `VACUUM INTO` + `PRAGMA integrity_check = ok`.
   - Connects `SqliteScanStore::connect` (applying migrations 8–9), verifies all historical records and legacy token authentication, executes new idempotent v0.12 job+event writes, drops/reopens the store ("service restart"), and re-verifies full data integrity.
2. **`sqlite_upgrade_from_v0_9_v0_10_and_interrupted_migration_restore`:**
   - Initializes a v0.9.2/v0.10.0 SQLite database (migrations 1–8) with queued `repair` jobs and runs.
   - Captures a `VACUUM INTO` backup.
   - Injects a conflicting partial DDL (`CREATE TABLE application_job_events (broken INTEGER)`) simulating an interrupted migration; confirms `SqliteScanStore::connect` fails cleanly without silent corruption.
   - Removes `-wal`/`-shm` sidecars, restores the backup, confirms `SqliteScanStore::connect` upgrades cleanly and preserves all historical runs and queued jobs.
3. **`sqlite_wal_online_backup_and_restore_to_clean_instance`:**
   - Opens an active WAL-mode database, writes runs, jobs, events, and operator tokens while the pool remains open, executes `VACUUM INTO`, verifies `PRAGMA integrity_check = ok`, copies the backup to a brand-new directory, opens `SqliteScanStore`, and confirms all runs, events, and tokens work identically.
4. **`postgres_upgrade_from_v0_9_v0_10_schema` (`PARQONAUT_TEST_PG_URL`):**
   - Initializes Postgres with migrations 1–2 (`20250415120000_init.sql`, `20250919140000_application_jobs.sql`), seeds historical jobs, connects `PostgresScanStore::connect` (applying migration 3), and verifies historical rows survive intact.

## 3. PostgreSQL Backup & Restore Verification

Verified against PostgreSQL 16 (`pg_dump --format=custom` and `pg_restore --clean --if-exists`) in addition to SQLite `VACUUM INTO` (see [Operations Runbook](../../operations.md#3-backup-and-restoration)).
