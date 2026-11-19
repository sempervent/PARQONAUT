//! Historical schema migration, backup, interrupted-upgrade recovery, and restore tests.

use std::path::{Path, PathBuf};

use camino::Utf8PathBuf;
use chrono::Utc;
use parqonaut_core::ScanEngine;
use parqonaut_store::{
    AppendJobEvent, IdempotencyInsertResult, PostgresScanStore, RedactionPolicy, RunId,
    SqliteScanStore, StoreBackend,
};
use parqonaut_types::{
    validate_report, AuthRole, JobId, JobKind, JobStage, ScanProfile, ScanRequest, ScanTarget,
    TargetIdentity,
};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use sha2::{Digest, Sha256};
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use uuid::Uuid;

fn migrations_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

fn fixture(rel: &str) -> Utf8PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(rel);
    Utf8PathBuf::from_path_buf(path).expect("utf8 fixture path")
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

async fn apply_sqlite_migrations_through(db_path: &Path, through_prefix: &str) {
    let mig_tmp = tempfile::tempdir().unwrap();
    let mut entries: Vec<_> = std::fs::read_dir(migrations_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sql"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let name = entry.file_name().to_string_lossy().to_string();
        std::fs::copy(entry.path(), mig_tmp.path().join(&name)).unwrap();
        if name.starts_with(through_prefix) {
            break;
        }
    }

    std::fs::File::create(db_path).unwrap();
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new().max_connections(2).connect_with(opts).await.unwrap();
    let migrator = Migrator::new(mig_tmp.path()).await.unwrap();
    migrator.run(&pool).await.unwrap();
    pool.close().await;
}

async fn seed_historical_scan_run(db_path: &Path) -> (RunId, String) {
    let mut req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("scan/single_parquet/data.parquet") },
        ScanProfile::Standard,
    );
    req.scan_id = Uuid::parse_str("00000000-0000-4000-8000-000000000101").unwrap();
    let report = ScanEngine::scan(&req).unwrap();
    validate_report(&report).unwrap();
    let run_id = RunId::new();
    let identity = TargetIdentity::from_scan_target(&report.request.target);
    let report_json = serde_json::to_string(&report).unwrap();
    let summary_json = serde_json::to_string(&report.summary).unwrap();
    let target_json = serde_json::to_string(&report.request.target).unwrap();
    let now = Utc::now().to_rfc3339();

    let opts = SqliteConnectOptions::new().filename(db_path).foreign_keys(true);
    let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.unwrap();
    sqlx::query(
        r#"INSERT INTO scan_runs (
            run_id, request_scan_id, target_kind, normalized_target_key, target_json,
            started_at, completed_at, run_outcome, engine_revision,
            contract_schema_version, report_format_version, report_sha256, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
    )
    .bind(run_id.0.to_string())
    .bind(req.scan_id.to_string())
    .bind(&identity.target_kind)
    .bind(&identity.normalized_key)
    .bind(&target_json)
    .bind(&now)
    .bind(&now)
    .bind("completed")
    .bind("0.9.2")
    .bind(&report.metadata.contract_schema.0)
    .bind(&report.metadata.report_format.0)
    .bind("0".repeat(64))
    .bind(&summary_json)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query("INSERT INTO scan_reports (run_id, report_json) VALUES (?, ?)")
        .bind(run_id.0.to_string())
        .bind(&report_json)
        .execute(&pool)
        .await
        .unwrap();

    pool.close().await;
    (run_id, report_json)
}

async fn sqlite_vacuum_backup(src_db: &Path, backup_db: &Path) {
    let opts = SqliteConnectOptions::new().filename(src_db).foreign_keys(true);
    let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.unwrap();
    let escaped = backup_db.display().to_string().replace('\'', "''");
    sqlx::query(&format!("VACUUM INTO '{escaped}'")).execute(&pool).await.unwrap();
    pool.close().await;

    let check_opts = SqliteConnectOptions::new().filename(backup_db).read_only(true);
    let check_pool =
        SqlitePoolOptions::new().max_connections(1).connect_with(check_opts).await.unwrap();
    let integrity: String =
        sqlx::query_scalar("PRAGMA integrity_check").fetch_one(&check_pool).await.unwrap();
    assert_eq!(integrity, "ok");
    check_pool.close().await;
}

#[tokio::test]
async fn sqlite_upgrade_from_pre_application_jobs_v0_8_schema_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("v08.sqlite");
    let backup_path = dir.path().join("v08.backup.sqlite");

    // 1. Open historical v0.8/early-v0.9 schema (through 20250420120000_auth_tokens_last_used_rotation).
    apply_sqlite_migrations_through(&db_path, "20250420120000").await;
    let (hist_run_id, hist_report_json) = seed_historical_scan_run(&db_path).await;

    // Seed legacy scan_jobs + auth_tokens rows.
    let hist_job_id = JobId::new();
    let hist_tok_id = Uuid::new_v4().to_string();
    {
        let opts = SqliteConnectOptions::new().filename(&db_path).foreign_keys(true);
        let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.unwrap();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO scan_jobs (
                job_id, submitted_at, completed_at, status, target_kind, normalized_target_key,
                request_json, run_id, attempt_count
            ) VALUES (?, ?, ?, 'succeeded', 'local_file', '/legacy.parquet', '{}', ?, 1)"#,
        )
        .bind(hist_job_id.0.to_string())
        .bind(&now)
        .bind(&now)
        .bind(hist_run_id.0.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let tok_hash = hex::encode(Sha256::digest(b"legacy-secret-08"));
        sqlx::query(
            r#"INSERT INTO auth_tokens (
                token_id, token_hash, label, role, created_at, token_prefix
            ) VALUES (?, ?, 'legacy-admin', 'admin', ?, 'legacy')"#,
        )
        .bind(&hist_tok_id)
        .bind(&tok_hash)
        .bind(&now)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    // 2. Consistent WAL-safe backup before upgrade.
    sqlite_vacuum_backup(&db_path, &backup_path).await;

    // 3. Apply forward migrations via SqliteScanStore::connect.
    let store = SqliteScanStore::connect(&sqlite_url(&db_path)).await.unwrap();

    // 4. Validate historical run, report, job, and auth token.
    let loaded = store.load_report(hist_run_id).await.unwrap();
    assert_eq!(serde_json::to_string(&loaded).unwrap(), hist_report_json);
    let hist_job = store.get_scan_job(hist_job_id).await.unwrap();
    assert_eq!(hist_job.job_kind, "scan");
    assert_eq!(hist_job.status, "succeeded");
    let auth = store.verify_bearer_token("legacy-secret-08").await.unwrap().expect("legacy token");
    assert_eq!(auth.label, "legacy-admin");

    // 5. Execute new v0.12 operations.
    let new_jid = JobId::new();
    let tid =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/new.parquet".into() };
    let ev = AppendJobEvent {
        job_id: new_jid,
        occurred_at: Utc::now(),
        kind: ProgressEventKind::JobQueued,
        stage: JobStage::Queue,
        is_terminal: false,
        metrics: ProgressMetrics::default(),
        message: Some("post-upgrade job".into()),
        run_id: None,
        plugin_name: None,
        plugin_digest: None,
    };
    let (res, _) = store
        .insert_application_job_with_idempotency_and_event(
            new_jid,
            JobKind::Scan,
            &tid,
            "{}",
            Some(("scan", "post-upgrade-key", "sha1", Utc::now() + chrono::Duration::hours(1))),
            ev,
            100,
        )
        .await
        .unwrap();
    assert!(matches!(res, IdempotencyInsertResult::Created { .. }));

    // 6. Restart service / reopen store & verify integrity.
    drop(store);
    let reopened = SqliteScanStore::connect(&sqlite_url(&db_path)).await.unwrap();
    assert_eq!(
        serde_json::to_string(&reopened.load_report(hist_run_id).await.unwrap()).unwrap(),
        hist_report_json
    );
    assert_eq!(reopened.list_job_events(new_jid, 0, 10, None, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn sqlite_upgrade_from_v0_9_v0_10_and_interrupted_migration_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("v010.sqlite");
    let backup_path = dir.path().join("v010.backup.sqlite");

    // 1. Open v0.9.2 / v0.10.0 schema (through 20250919140000_application_jobs).
    apply_sqlite_migrations_through(&db_path, "20250919140000").await;
    let (hist_run_id, hist_report_json) = seed_historical_scan_run(&db_path).await;
    let hist_job_id = JobId::new();
    {
        let opts = SqliteConnectOptions::new().filename(&db_path).foreign_keys(true);
        let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.unwrap();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO application_jobs (
                job_id, submitted_at, status, target_kind, normalized_target_key,
                request_json, job_kind, payload_schema_version, cancel_requested
            ) VALUES (?, ?, 'queued', 'local_file', '/queued-v010.parquet', '{}', 'repair', 1, 0)"#,
        )
        .bind(hist_job_id.0.to_string())
        .bind(&now)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    // 2. Take consistent backup before upgrade.
    sqlite_vacuum_backup(&db_path, &backup_path).await;

    // 3. Simulate interrupted/conflicting partial DDL before v0.11 migration finishes.
    {
        let opts = SqliteConnectOptions::new().filename(&db_path).foreign_keys(true);
        let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.unwrap();
        sqlx::query("CREATE TABLE application_job_events (broken INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
    let failed_upgrade = SqliteScanStore::connect(&sqlite_url(&db_path)).await;
    assert!(failed_upgrade.is_err(), "partial DDL conflict should fail migration cleanly");

    // 4. Restore from pre-upgrade backup (removing WAL/SHM sidecars if any) and retry upgrade.
    for ext in ["", "-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", db_path.display(), ext));
        let _ = std::fs::remove_file(sidecar);
    }
    std::fs::copy(&backup_path, &db_path).unwrap();

    let store = SqliteScanStore::connect(&sqlite_url(&db_path)).await.unwrap();
    assert_eq!(
        serde_json::to_string(&store.load_report(hist_run_id).await.unwrap()).unwrap(),
        hist_report_json
    );
    let q_job = store.get_scan_job(hist_job_id).await.unwrap();
    assert_eq!(q_job.job_kind, "repair");
    assert_eq!(q_job.status, "queued");
}

#[tokio::test]
async fn sqlite_wal_online_backup_and_restore_to_clean_instance() {
    let dir = tempfile::tempdir().unwrap();
    let live_db = dir.path().join("live.sqlite");
    let backup_db = dir.path().join("backup.sqlite");
    let clean_dir = tempfile::tempdir().unwrap();
    let restored_db = clean_dir.path().join("restored.sqlite");

    std::fs::File::create(&live_db).unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&live_db)).await.unwrap();
    let (_, tok) =
        store.create_auth_token("ops", AuthRole::Operator, Some("backup-test")).await.unwrap();

    let mut req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("scan/tiny_parquet/micro.parquet") },
        ScanProfile::Quick,
    );
    req.scan_id = Uuid::new_v4();
    let report = ScanEngine::scan(&req).unwrap();
    let run_id = RunId::new();
    let now = Utc::now();
    store.persist_scan_run(run_id, now, now, &report, &RedactionPolicy::default()).await.unwrap();

    let jid = JobId::new();
    let tid = TargetIdentity::from_scan_target(&report.request.target);
    let ev = AppendJobEvent {
        job_id: jid,
        occurred_at: now,
        kind: ProgressEventKind::JobQueued,
        stage: JobStage::Queue,
        is_terminal: false,
        metrics: ProgressMetrics::default(),
        message: Some("queued before backup".into()),
        run_id: None,
        plugin_name: None,
        plugin_digest: None,
    };
    let _ = store
        .insert_application_job_with_idempotency_and_event(
            jid,
            JobKind::Scan,
            &tid,
            "{}",
            None,
            ev,
            100,
        )
        .await
        .unwrap();

    // Perform online WAL-safe backup while store pool remains open.
    sqlite_vacuum_backup(&live_db, &backup_db).await;
    drop(store);

    // Restore onto a brand-new clean directory.
    std::fs::copy(&backup_db, &restored_db).unwrap();
    let restored = SqliteScanStore::connect(&sqlite_url(&restored_db)).await.unwrap();
    assert_eq!(restored.load_report(run_id).await.unwrap().request.scan_id, report.request.scan_id);
    assert_eq!(restored.list_job_events(jid, 0, 20, None, None).await.unwrap().len(), 1);
    assert!(restored.verify_bearer_token(&tok).await.unwrap().is_some());
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL to a clean Postgres database to validate historical PG upgrade"]
async fn postgres_upgrade_from_v0_9_v0_10_schema() {
    let Ok(url) = std::env::var("PARQONAUT_TEST_PG_URL") else {
        return;
    };
    let mig_tmp = tempfile::tempdir().unwrap();
    let pg_migs = migrations_dir().join("postgres");
    for name in ["20250415120000_init.sql", "20250919140000_application_jobs.sql"] {
        std::fs::copy(pg_migs.join(name), mig_tmp.path().join(name)).unwrap();
    }
    let pool = sqlx::postgres::PgPoolOptions::new().max_connections(2).connect(&url).await.unwrap();
    let migrator = Migrator::new(mig_tmp.path()).await.unwrap();
    migrator.run(&pool).await.unwrap();
    let now = Utc::now().to_rfc3339();
    let hist_job_id = JobId::new();
    sqlx::query(
        r#"INSERT INTO application_jobs (
            job_id, submitted_at, status, target_kind, normalized_target_key,
            request_json, job_kind, payload_schema_version, cancel_requested
        ) VALUES ($1, $2, 'queued', 'local_file', '/pg-v010.parquet', '{}', 'scan', 1, 0)"#,
    )
    .bind(hist_job_id.0.to_string())
    .bind(&now)
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let store = StoreBackend::Postgres(PostgresScanStore::connect(&url).await.unwrap());
    let row = store.get_scan_job(hist_job_id).await.unwrap();
    assert_eq!(row.status, "queued");
}
