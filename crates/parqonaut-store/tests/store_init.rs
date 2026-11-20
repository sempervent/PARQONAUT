//! SQLite startup: hardened pragmas and migration path.

#[tokio::test]
async fn hardened_sqlite_pragmas_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("init.sqlite");
    std::fs::File::create(&p).unwrap();
    let url = format!("sqlite://{}", p.canonicalize().unwrap().display());

    let store = parqonaut_store::SqliteScanStore::connect(&url).await.expect("connect");

    let journal: String = sqlx::query_scalar("SELECT journal_mode FROM pragma_journal_mode")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(journal.to_ascii_lowercase(), "wal");

    let sync: i64 = sqlx::query_scalar("PRAGMA synchronous").fetch_one(store.pool()).await.unwrap();
    // NORMAL = 1
    assert_eq!(sync, 1);

    let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys").fetch_one(store.pool()).await.unwrap();
    assert_eq!(fk, 1);

    let busy_ms: i64 =
        sqlx::query_scalar("PRAGMA busy_timeout").fetch_one(store.pool()).await.unwrap();
    assert_eq!(busy_ms, 5000);
}

#[tokio::test]
async fn connect_creates_new_sqlite_file_without_pre_touch() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("fresh").join("parqonaut.sqlite");
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let url = format!("sqlite://{}", db_path.display());

    let store = parqonaut_store::SqliteScanStore::connect(&url).await.expect("connect fresh file");
    store.ping().await.expect("ping fresh store");
    assert!(db_path.exists());
}

#[tokio::test]
async fn concurrent_event_burst_and_cancel_no_sqlite_busy() {
    use chrono::Utc;
    use parqonaut_store::AppendJobEvent;
    use parqonaut_types::{JobId, JobKind, JobStage, ScanTarget, TargetIdentity};
    use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("concurrent.sqlite");
    let url = format!("sqlite://{}", db_path.display());
    let store = parqonaut_store::SqliteScanStore::connect(&url).await.expect("connect");

    let jid = JobId::new();
    let identity = TargetIdentity::from_scan_target(&ScanTarget::LocalFile {
        path: "/tmp/demo.parquet".into(),
    });
    let initial = AppendJobEvent {
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
    };
    store
        .insert_application_job_with_idempotency_and_event(
            jid,
            JobKind::Scan,
            &identity,
            "{}",
            None,
            initial,
            500,
        )
        .await
        .expect("enqueue");
    store
        .claim_next_queued_scan_job_with_event(
            "worker-1",
            Utc::now(),
            Utc::now() + chrono::Duration::seconds(60),
            500,
        )
        .await
        .expect("claim")
        .expect("claimed");

    let store_writer = store.clone();
    let writer = tokio::spawn(async move {
        for i in 0..50 {
            store_writer
                .append_job_event(
                    AppendJobEvent {
                        job_id: jid,
                        occurred_at: Utc::now(),
                        kind: ProgressEventKind::PluginProgress,
                        stage: JobStage::Plugin,
                        is_terminal: false,
                        metrics: ProgressMetrics::default(),
                        message: Some(format!("tick {i}")),
                        run_id: None,
                        plugin_name: Some("adv-sleep".into()),
                        plugin_digest: None,
                    },
                    500,
                )
                .await
                .expect("append_job_event must not fail with SQLITE_BUSY");
        }
    });

    // Interleave cancel request while worker is actively appending events.
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    store
        .request_cancel_job_with_event(jid, 500)
        .await
        .expect("request_cancel_job_with_event must not fail with SQLITE_BUSY");

    writer.await.expect("writer join");
}
