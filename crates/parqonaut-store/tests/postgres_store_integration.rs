//! Postgres-backed store tests (requires a running server).
//!
//! Set `PARQONAUT_TEST_PG_URL`, e.g.:
//! `postgres://postgres:postgres@127.0.0.1:5432/parqonaut_test`
//!
//! Create DB once: `createdb parqonaut_test` (or use a disposable URL).

use camino::Utf8PathBuf;
use chrono::Utc;
use parqonaut_store::{PostgresScanStore, RedactionPolicy, RunId, SqliteScanStore, TargetIdentity};
use parqonaut_types::{JobId, JobRecoveryPolicy, ScanRequest, ScanTarget};

fn pg_url() -> Option<String> {
    std::env::var("PARQONAUT_TEST_PG_URL").ok().filter(|s| !s.trim().is_empty())
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL to run (see module docs)"]
async fn postgres_persist_and_integrity() {
    let url = pg_url().expect("PARQONAUT_TEST_PG_URL");
    let store = PostgresScanStore::connect(&url).await.expect("connect pg");

    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/scan/single_parquet/data.parquet");
    let path = Utf8PathBuf::from_path_buf(fixture).unwrap();
    let target = ScanTarget::LocalFile { path };
    let scan_req = ScanRequest::new(target, parqonaut_types::ScanProfile::Standard);
    let report = parqonaut_core::ScanEngine::run(&scan_req).unwrap();
    let run_id = RunId::new();
    store
        .persist_scan_run(
            run_id,
            Utc::now(),
            Utc::now(),
            &report,
            &RedactionPolicy::transport_safe_persist(),
        )
        .await
        .expect("persist");

    store.verify_projection_integrity(run_id).await.expect("integrity");
    let loaded = store.load_report(run_id).await.expect("load");
    assert_eq!(loaded.summary.dataset_count, report.summary.dataset_count);
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL to run (see module docs)"]
async fn postgres_jobs_and_auth_lifecycle() {
    let url = pg_url().expect("PARQONAUT_TEST_PG_URL");
    let store = PostgresScanStore::connect(&url).await.expect("connect pg");

    let jid = JobId::new();
    let identity = TargetIdentity {
        target_kind: "local_file".into(),
        normalized_key: "/tmp/x.parquet".into(),
    };
    store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();
    let now = Utc::now();
    let row = store
        .claim_next_queued_scan_job("w1", now, now + chrono::Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "running");

    let (id, secret) =
        store.create_auth_token("t", parqonaut_types::AuthRole::Reader, None).await.unwrap();
    let p = store.verify_bearer_token(&secret).await.unwrap().expect("auth");
    assert_eq!(p.token_id.0, id.0);
    let summary = store.get_auth_token_summary(id).await.unwrap().expect("row");
    assert!(summary.last_used_at.is_some());

    let (new_id, new_secret) = store.rotate_auth_token(id).await.unwrap();
    assert_ne!(new_id.0, id.0);
    store.verify_bearer_token(&new_secret).await.unwrap().expect("new ok");
    assert!(store.verify_bearer_token(&secret).await.unwrap().is_none());

    store.complete_scan_job_failure(jid, "scan_failed", "x").await.unwrap();
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL to run (see module docs)"]
async fn postgres_recover_stale_scan_jobs() {
    let url = pg_url().expect("PARQONAUT_TEST_PG_URL");
    let store = PostgresScanStore::connect(&url).await.expect("connect pg");

    let jid = JobId::new();
    let identity = TargetIdentity {
        target_kind: "local_file".into(),
        normalized_key: "/tmp/y.parquet".into(),
    };
    store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();
    let now = Utc::now();
    store
        .claim_next_queued_scan_job("w", now, now + chrono::Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();

    sqlx::query("UPDATE application_jobs SET leased_until = $1 WHERE job_id = $2")
        .bind("1999-01-01T00:00:00Z")
        .bind(jid.0.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let stats =
        store.recover_stale_scan_jobs(Utc::now(), JobRecoveryPolicy::default()).await.unwrap();
    assert_eq!(stats.requeued, 1);
}

#[tokio::test]
async fn sqlite_and_postgres_summary_parity_when_pg_configured() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    let sqlite_url = format!("sqlite://{}", abs.display());
    let sqlite = SqliteScanStore::connect(&sqlite_url).await.unwrap();

    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/scan/single_parquet/data.parquet");
    let path = Utf8PathBuf::from_path_buf(fixture).unwrap();
    let target = ScanTarget::LocalFile { path };
    let scan_req = ScanRequest::new(target, parqonaut_types::ScanProfile::Standard);
    let report = parqonaut_core::ScanEngine::run(&scan_req).unwrap();
    let run_id = RunId::new();
    sqlite
        .persist_scan_run(
            run_id,
            Utc::now(),
            Utc::now(),
            &report,
            &RedactionPolicy::transport_safe_persist(),
        )
        .await
        .unwrap();
    let s_sum = sqlite.load_stored_scan_summary(run_id).await.unwrap().dataset_count;

    let Some(pg) = pg_url() else {
        return;
    };
    let pg_store = PostgresScanStore::connect(&pg).await.expect("pg");
    let run_id2 = RunId::new();
    pg_store
        .persist_scan_run(
            run_id2,
            Utc::now(),
            Utc::now(),
            &report,
            &RedactionPolicy::transport_safe_persist(),
        )
        .await
        .unwrap();
    let p_sum = pg_store.load_stored_scan_summary(run_id2).await.unwrap().dataset_count;
    assert_eq!(s_sum, p_sum);
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL to run (see module docs)"]
async fn postgres_job_events_retention_and_idempotency_parity() {
    use chrono::Duration;
    use parqonaut_store::{AppendJobEvent, IdempotencyInsertResult, StoreError};
    use parqonaut_types::{JobStage, TargetIdentity};
    use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
    use uuid::Uuid;

    let url = pg_url().expect("PARQONAUT_TEST_PG_URL");
    let store = PostgresScanStore::connect(&url).await.expect("connect pg");
    let jid = JobId::new();
    let id = TargetIdentity {
        target_kind: "local_file".into(),
        normalized_key: "/pg-ev.parquet".into(),
    };
    let key = format!("pg-idem-{}", Uuid::new_v4());
    let future = Utc::now() + Duration::hours(1);

    let r1 = store
        .insert_application_job_with_idempotency(
            jid,
            parqonaut_types::JobKind::Scan,
            &id,
            r#"{"pg":1}"#,
            Some(("scan", &key, "sha-pg-1", future)),
        )
        .await
        .unwrap();
    assert!(matches!(r1, IdempotencyInsertResult::Created { job_id, .. } if job_id == jid));

    let r2 = store
        .insert_application_job_with_idempotency(
            JobId::new(),
            parqonaut_types::JobKind::Scan,
            &id,
            r#"{"pg":1}"#,
            Some(("scan", &key, "sha-pg-1", future)),
        )
        .await
        .unwrap();
    assert!(
        matches!(r2, IdempotencyInsertResult::Replayed { existing } if existing.job_id == jid.0.to_string())
    );

    let err = store
        .insert_application_job_with_idempotency(
            JobId::new(),
            parqonaut_types::JobKind::Scan,
            &id,
            r#"{"pg":2}"#,
            Some(("scan", &key, "sha-pg-2", future)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::IdempotencyConflict(_)));

    store
        .append_job_event(
            AppendJobEvent {
                job_id: jid,
                occurred_at: Utc::now(),
                kind: ProgressEventKind::JobQueued,
                stage: JobStage::Queue,
                is_terminal: false,
                metrics: ProgressMetrics::default(),
                message: Some("queued".into()),
                run_id: None,
                plugin_name: None,
                plugin_digest: None,
            },
            3,
        )
        .await
        .unwrap();

    for i in 0..8u64 {
        store
            .append_job_event(
                AppendJobEvent {
                    job_id: jid,
                    occurred_at: Utc::now(),
                    kind: ProgressEventKind::BatchProcessed,
                    stage: JobStage::Scan,
                    is_terminal: false,
                    metrics: ProgressMetrics { rows: i, ..Default::default() },
                    message: Some(format!("pg batch {i}")),
                    run_id: None,
                    plugin_name: None,
                    plugin_digest: None,
                },
                3,
            )
            .await
            .unwrap();
    }

    store
        .append_job_event(
            AppendJobEvent {
                job_id: jid,
                occurred_at: Utc::now(),
                kind: ProgressEventKind::ExecutionCompleted,
                stage: JobStage::Scan,
                is_terminal: true,
                metrics: ProgressMetrics::default(),
                message: Some("done".into()),
                run_id: None,
                plugin_name: None,
                plugin_digest: None,
            },
            3,
        )
        .await
        .unwrap();

    let events = store.list_job_events(jid, 0, 50, None, None).await.unwrap();
    assert!(events.len() <= 5);
    assert_eq!(events.first().unwrap().kind, ProgressEventKind::JobQueued);
    assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
    assert!(events.windows(2).all(|w| w[0].seq < w[1].seq));

    // Clean up test job row so queued job tests in the same DB remain isolated.
    sqlx::query("DELETE FROM application_jobs WHERE job_id = $1")
        .bind(jid.0.to_string())
        .execute(store.pool())
        .await
        .unwrap();
}
