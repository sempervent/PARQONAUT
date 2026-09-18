use chrono::Utc;
use parqonaut_orchestrator::{
    BatchPlanId, DatasetId, DatasetRunRecord, DatasetState, RunId, RunIdentity, RunJournal,
    SqliteRunJournal, JOURNAL_SCHEMA_VERSION,
};
use parqonaut_repair::PARQONAUT_VERSION;
use tempfile::TempDir;

#[tokio::test]
async fn bootstrap_empty_database() {
    let tmp = TempDir::new().unwrap();
    let path = camino::Utf8PathBuf::from(tmp.path().join("journal.sqlite").to_str().unwrap());
    let identity = sample_identity();
    let journal = SqliteRunJournal::open(&path, &identity).await.unwrap();
    assert!(path.exists());
    assert_eq!(journal.run_identity().unwrap().unwrap().run_id, identity.run_id);
}

#[tokio::test]
async fn reopen_existing_database_applies_migrations() {
    let tmp = TempDir::new().unwrap();
    let path = camino::Utf8PathBuf::from(tmp.path().join("journal.sqlite").to_str().unwrap());
    let identity = sample_identity();
    {
        let journal = SqliteRunJournal::open(&path, &identity).await.unwrap();
        journal
            .upsert_dataset(&DatasetRunRecord {
                dataset_id: DatasetId("ds".into()),
                output_path: "/tmp/out".into(),
                repair_plan_id: "plan".into(),
                policy_fingerprint: "pol".into(),
                state: DatasetState::Pending,
                attempts: 0,
                source_fingerprint: None,
                output_fingerprint: None,
                error_class: None,
                error_message: None,
                started_at: None,
                updated_at: Utc::now(),
                completed_at: None,
            })
            .unwrap();
    }
    let reopened = SqliteRunJournal::open_existing(&path).await.unwrap();
    let rows = reopened.list_datasets().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].dataset_id.0, "ds");
}

#[tokio::test]
async fn schema_version_recorded() {
    let tmp = TempDir::new().unwrap();
    let path = camino::Utf8PathBuf::from(tmp.path().join("journal.sqlite").to_str().unwrap());
    let identity = sample_identity();
    let _journal = SqliteRunJournal::open(&path, &identity).await.unwrap();
    let pool = sqlx::sqlite::SqlitePoolOptions::new().connect(path.as_str()).await.unwrap();
    let version: String =
        sqlx::query_scalar("SELECT value FROM parqonaut_journal_meta WHERE key = 'schema_version'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(version, JOURNAL_SCHEMA_VERSION.to_string());
}

fn sample_identity() -> RunIdentity {
    RunIdentity {
        run_id: RunId::new(),
        batch_plan_id: BatchPlanId("batch-plan-test".into()),
        config_fingerprint: "cfg".into(),
        batch_name: "test".into(),
        output_root: "/tmp/out".into(),
        parqonaut_version: PARQONAUT_VERSION.to_string(),
        plan_digest: "digest".into(),
        started_at: Utc::now(),
        updated_at: Utc::now(),
        completed_at: None,
        cancelled_at: None,
    }
}
