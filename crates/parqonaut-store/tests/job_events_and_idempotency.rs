//! Store-level unit & integration tests for durable job events, retention, and idempotency.

use chrono::{Duration, Utc};
use parqonaut_store::{AppendJobEvent, IdempotencyInsertResult, SqliteScanStore, StoreError};
use parqonaut_types::{JobId, JobKind, JobRecoveryPolicy, JobStage, TargetIdentity};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};

async fn open_sqlite() -> (tempfile::TempDir, String, SqliteScanStore) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("events.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    let url = format!("sqlite://{}", abs.display());
    let store = SqliteScanStore::connect(&url).await.unwrap();
    (dir, url, store)
}

#[tokio::test]
async fn event_ordering_cursor_filtering_and_restart_persistence() {
    let (_dir, url, store) = open_sqlite().await;
    let j1 = JobId::new();
    let j2 = JobId::new();
    let id =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/a.parquet".into() };
    store.insert_application_job(j1, JobKind::Scan, &id, "{}").await.unwrap();
    store.insert_application_job(j2, JobKind::Repair, &id, "{}").await.unwrap();

    for i in 1..=5u64 {
        let ev = store
            .append_job_event(
                AppendJobEvent {
                    job_id: j1,
                    occurred_at: Utc::now(),
                    kind: ProgressEventKind::BatchProcessed,
                    stage: JobStage::Scan,
                    is_terminal: false,
                    metrics: ProgressMetrics { rows: i * 10, ..Default::default() },
                    message: Some(format!("batch {i}")),
                    run_id: None,
                    plugin_name: None,
                    plugin_digest: None,
                },
                500,
            )
            .await
            .unwrap();
        assert_eq!(ev.seq, i as i64);
    }

    // Events on job 2 have independent 1-based cursor and never leak into job 1.
    let ev2 = store
        .append_job_event(
            AppendJobEvent {
                job_id: j2,
                occurred_at: Utc::now(),
                kind: ProgressEventKind::ExecutionCompleted,
                stage: JobStage::Repair,
                is_terminal: true,
                metrics: ProgressMetrics::default(),
                message: Some("j2 done".into()),
                run_id: None,
                plugin_name: None,
                plugin_digest: None,
            },
            500,
        )
        .await
        .unwrap();
    assert_eq!(ev2.seq, 1);

    drop(store);
    let reopened = SqliteScanStore::connect(&url).await.unwrap();
    let page1 = reopened.list_job_events(j1, 0, 2, None, None).await.unwrap();
    assert_eq!(page1.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
    let page2 = reopened.list_job_events(j1, 2, 10, None, None).await.unwrap();
    assert_eq!(page2.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
    assert!(page2.iter().all(|e| e.job_id == j1));
}

#[tokio::test]
async fn retention_pruning_preserves_terminal_and_lifecycle_events() {
    let (_dir, _url, store) = open_sqlite().await;
    let jid = JobId::new();
    let id =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/r.parquet".into() };
    store.insert_application_job(jid, JobKind::Scan, &id, "{}").await.unwrap();

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
            5,
        )
        .await
        .unwrap();

    for i in 0..20 {
        store
            .append_job_event(
                AppendJobEvent {
                    job_id: jid,
                    occurred_at: Utc::now(),
                    kind: ProgressEventKind::BatchProcessed,
                    stage: JobStage::Scan,
                    is_terminal: false,
                    metrics: ProgressMetrics { rows: i, ..Default::default() },
                    message: Some(format!("progress {i}")),
                    run_id: None,
                    plugin_name: None,
                    plugin_digest: None,
                },
                5,
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
            5,
        )
        .await
        .unwrap();

    let events = store.list_job_events(jid, 0, 100, None, None).await.unwrap();
    assert!(events.len() <= 7, "expected bounded events, got {}", events.len());
    assert_eq!(events.first().unwrap().kind, ProgressEventKind::JobQueued);
    assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
    assert!(events.windows(2).all(|w| w[0].seq < w[1].seq));
}

#[tokio::test]
async fn idempotency_create_replay_conflict_and_expiry() {
    let (_dir, _url, store) = open_sqlite().await;
    let id =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/i.parquet".into() };
    let future = Utc::now() + Duration::hours(1);

    let j1 = JobId::new();
    let res1 = store
        .insert_application_job_with_idempotency(
            j1,
            JobKind::Repair,
            &id,
            r#"{"a":1}"#,
            Some(("repair", "key-1", "sha-a", future)),
        )
        .await
        .unwrap();
    assert!(matches!(res1, IdempotencyInsertResult::Created { job_id, .. } if job_id == j1));

    // Replay with same key & same payload hash returns existing job.
    let j2 = JobId::new();
    let res2 = store
        .insert_application_job_with_idempotency(
            j2,
            JobKind::Repair,
            &id,
            r#"{"a":1}"#,
            Some(("repair", "key-1", "sha-a", future)),
        )
        .await
        .unwrap();
    assert!(
        matches!(res2, IdempotencyInsertResult::Replayed { existing } if existing.job_id == j1.0.to_string())
    );

    // Same key with different payload hash returns IdempotencyConflict.
    let j3 = JobId::new();
    let err = store
        .insert_application_job_with_idempotency(
            j3,
            JobKind::Repair,
            &id,
            r#"{"a":2}"#,
            Some(("repair", "key-1", "sha-b", future)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::IdempotencyConflict(_)));

    // Expired key can be reused cleanly.
    let past = Utc::now() - Duration::seconds(5);
    let j4 = JobId::new();
    let _ = store
        .insert_application_job_with_idempotency(
            j4,
            JobKind::Repair,
            &id,
            r#"{"a":1}"#,
            Some(("repair", "key-expired", "sha-a", past)),
        )
        .await
        .unwrap();
    let j5 = JobId::new();
    let res5 = store
        .insert_application_job_with_idempotency(
            j5,
            JobKind::Repair,
            &id,
            r#"{"a":2}"#,
            Some(("repair", "key-expired", "sha-b", future)),
        )
        .await
        .unwrap();
    assert!(matches!(res5, IdempotencyInsertResult::Created { job_id, .. } if job_id == j5));
}

#[tokio::test]
async fn stale_recovery_records_durable_recovery_events() {
    let (_dir, _url, store) = open_sqlite().await;
    let jid = JobId::new();
    let id =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/rec.parquet".into() };
    store.insert_application_job(jid, JobKind::Scan, &id, "{}").await.unwrap();
    let now = Utc::now();
    store
        .claim_next_queued_scan_job(
            "w-dead",
            now - Duration::minutes(10),
            now - Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();

    let stats = store.recover_stale_scan_jobs(now, JobRecoveryPolicy::default()).await.unwrap();
    assert_eq!(stats.requeued, 1);
    assert_eq!(stats.recovered_events.len(), 1);
    assert_eq!(stats.recovered_events[0].kind, ProgressEventKind::JobRecovered);

    let events = store.list_job_events(jid, 0, 50, None, None).await.unwrap();
    assert!(events.iter().any(|e| e.kind == ProgressEventKind::JobRecovered));
}

#[tokio::test]
async fn concurrent_idempotent_inserts_never_duplicate_jobs_and_prune_expired() {
    let (_dir, _url, store) = open_sqlite().await;
    let id =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/conc.parquet".into() };
    let past = Utc::now() - Duration::seconds(30);

    // Seed expired idempotency keys that should be bounded-pruned on subsequent inserts.
    for i in 0..10 {
        let j = JobId::new();
        let _ = store
            .insert_application_job_with_idempotency(
                j,
                JobKind::Scan,
                &id,
                "{}",
                Some(("scan", &format!("expired-{i}"), "sha-exp", past)),
            )
            .await
            .unwrap();
    }

    let future = Utc::now() + Duration::hours(1);
    let mut handles = Vec::new();
    for _ in 0..16 {
        let st = store.clone();
        let tid = id.clone();
        handles.push(tokio::spawn(async move {
            let jid = JobId::new();
            let ev = AppendJobEvent {
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
            st.insert_application_job_with_idempotency_and_event(
                jid,
                JobKind::Scan,
                &tid,
                r#"{"a":1}"#,
                Some(("scan", "concurrent-race-key", "sha-same", future)),
                ev,
                500,
            )
            .await
        }));
    }

    let mut resolved_ids = std::collections::BTreeSet::new();
    let mut created_count = 0usize;
    for h in handles {
        let (res, ev_opt) = h.await.unwrap().unwrap();
        match res {
            IdempotencyInsertResult::Created { job_id, .. } => {
                created_count += 1;
                assert!(ev_opt.is_some());
                resolved_ids.insert(job_id.0.to_string());
            }
            IdempotencyInsertResult::Replayed { existing } => {
                assert!(ev_opt.is_none());
                resolved_ids.insert(existing.job_id);
            }
        }
    }
    assert_eq!(created_count, 1, "exactly one concurrent request must win creation");
    assert_eq!(resolved_ids.len(), 1, "all 16 callers must observe the same job_id");
    let winner_id: uuid::Uuid = resolved_ids.into_iter().next().unwrap().parse().unwrap();
    let events = store.list_job_events(JobId(winner_id), 0, 50, None, None).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, ProgressEventKind::JobQueued);
}

#[tokio::test]
async fn atomic_lifecycle_transitions_persist_paired_events() {
    let (_dir, _url, store) = open_sqlite().await;
    let id =
        TargetIdentity { target_kind: "local_file".into(), normalized_key: "/atom.parquet".into() };
    let jid = JobId::new();
    let ev_q = AppendJobEvent {
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
    let _ = store
        .insert_application_job_with_idempotency_and_event(
            jid,
            JobKind::Scan,
            &id,
            "{}",
            None,
            ev_q,
            100,
        )
        .await
        .unwrap();

    let now = Utc::now();
    let (_row, claimed_ev) = store
        .claim_next_queued_scan_job_with_event("w-atom", now, now + Duration::seconds(30), 100)
        .await
        .unwrap()
        .expect("claimed");
    assert_eq!(claimed_ev.kind, ProgressEventKind::JobClaimed);

    let done_ev = AppendJobEvent {
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
    };
    let committed_ev = store
        .complete_application_job_success_with_event(jid, None, None, done_ev, 100)
        .await
        .unwrap();
    assert!(committed_ev.is_terminal);
    let job = store.get_scan_job(jid).await.unwrap();
    assert_eq!(job.status, "succeeded");
    let events = store.list_job_events(jid, 0, 20, None, None).await.unwrap();
    assert_eq!(
        events.iter().map(|e| e.kind).collect::<Vec<_>>(),
        vec![
            ProgressEventKind::JobQueued,
            ProgressEventKind::JobClaimed,
            ProgressEventKind::ExecutionCompleted,
        ]
    );
}
