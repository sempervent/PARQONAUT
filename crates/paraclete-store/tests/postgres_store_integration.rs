//! Postgres-backed store tests (requires a running server).
//!
//! Set `PARACLETE_TEST_PG_URL`, e.g.:
//! `postgres://postgres:postgres@127.0.0.1:5432/paraclete_test`
//!
//! Create DB once: `createdb paraclete_test` (or use a disposable URL).

use camino::Utf8PathBuf;
use chrono::Utc;
use paraclete_store::{PostgresScanStore, RedactionPolicy, RunId, SqliteScanStore, TargetIdentity};
use paraclete_types::{JobId, JobRecoveryPolicy, ScanRequest, ScanTarget};

fn pg_url() -> Option<String> {
    std::env::var("PARACLETE_TEST_PG_URL").ok().filter(|s| !s.trim().is_empty())
}

#[tokio::test]
#[ignore = "set PARACLETE_TEST_PG_URL to run (see module docs)"]
async fn postgres_persist_and_integrity() {
    let url = pg_url().expect("PARACLETE_TEST_PG_URL");
    let store = PostgresScanStore::connect(&url).await.expect("connect pg");

    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/phase1/single_parquet/data.parquet");
    let path = Utf8PathBuf::from_path_buf(fixture).unwrap();
    let target = ScanTarget::LocalFile { path };
    let scan_req = ScanRequest::new(target, paraclete_types::ScanProfile::Standard);
    let report = paraclete_core::ScanEngine::run(&scan_req).unwrap();
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
#[ignore = "set PARACLETE_TEST_PG_URL to run (see module docs)"]
async fn postgres_jobs_and_auth_lifecycle() {
    let url = pg_url().expect("PARACLETE_TEST_PG_URL");
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
        store.create_auth_token("t", paraclete_types::AuthRole::Reader, None).await.unwrap();
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
#[ignore = "set PARACLETE_TEST_PG_URL to run (see module docs)"]
async fn postgres_recover_stale_scan_jobs() {
    let url = pg_url().expect("PARACLETE_TEST_PG_URL");
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

    sqlx::query("UPDATE scan_jobs SET leased_until = $1 WHERE job_id = $2")
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
        .join("../../fixtures/phase1/single_parquet/data.parquet");
    let path = Utf8PathBuf::from_path_buf(fixture).unwrap();
    let target = ScanTarget::LocalFile { path };
    let scan_req = ScanRequest::new(target, paraclete_types::ScanProfile::Standard);
    let report = paraclete_core::ScanEngine::run(&scan_req).unwrap();
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
