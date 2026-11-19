//! Fault-injection tests for abrupt worker loss, SQLite lock contention, and mid-stream SSE failure.

use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use parqonaut_app::StoragePolicy;
use parqonaut_service::events::lifecycle_event;
use parqonaut_service::{build_router_with_workers, ParqonautService};
use parqonaut_store::SqliteScanStore;
use parqonaut_types::{
    AuthRole, JobId, JobKind, JobRecoveryPolicy, JobStage, JobStatus, TargetIdentity,
};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

mod support;

#[tokio::test]
async fn sqlite_lock_contention_under_20_concurrent_writers_and_readers() {
    let (dir, store) = support::connect_store_with_token(AuthRole::Operator, "op").await;
    let svc = ParqonautService::with_storage_policy(store, StoragePolicy::cli_unrestricted_local());
    let app = build_router_with_workers(svc.clone(), 2);
    let fixture = support::fixture("scan/tiny_parquet/micro.parquet");

    let mut handles = Vec::new();
    for i in 0..20usize {
        let app_c = app.clone();
        let svc_c = svc.clone();
        let fix = fixture.clone();
        handles.push(tokio::spawn(async move {
            let body = json!({
                "target": { "type": "local_file", "path": fix.as_str() },
                "profile": "quick",
            });
            let res = app_c
                .clone()
                .oneshot(
                    Request::post("/api/v1/jobs/scans")
                        .header(AUTHORIZATION, support::bearer_operator())
                        .header("content-type", "application/json")
                        .header("Idempotency-Key", format!("contention-{i}"))
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::ACCEPTED);
            let v: serde_json::Value =
                serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            let jid = JobId(v["job_id"].as_str().unwrap().parse().unwrap());
            for _ in 0..10 {
                let _ = svc_c.store().list_job_events(jid, 0, 50, None, None).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            jid
        }));
    }

    let mut job_ids = Vec::new();
    for h in handles {
        job_ids.push(h.await.unwrap());
    }

    for jid in &job_ids {
        for _ in 0..400 {
            let view = svc.get_scan_job_view(*jid).await.unwrap();
            if view.status == JobStatus::Succeeded {
                break;
            }
            if view.status == JobStatus::Failed {
                panic!("contention job {} failed: {:?}", jid.0, view.failure);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let events = svc.store().list_job_events(*jid, 0, 100, None, None).await.unwrap();
        assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
    }

    svc.graceful_shutdown(Duration::from_secs(5)).await;
    drop(dir);
}

#[tokio::test]
async fn lost_worker_heartbeat_recovers_or_exhausts_without_corrupt_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = support::sqlite_url(&dir);
    let store = SqliteScanStore::connect(&db_url).await.unwrap();
    let jid = JobId::new();
    let tid = TargetIdentity {
        target_kind: "local_file".into(),
        normalized_key: support::fixture("scan/tiny_parquet/micro.parquet").to_string(),
    };
    let req_json = serde_json::json!({
        "target": { "type": "local_file", "path": support::fixture("scan/tiny_parquet/micro.parquet").as_str() },
        "profile": "quick",
        "options": {},
        "plugins": [],
        "resolved_plugins": []
    })
    .to_string();

    store.insert_application_job(jid, JobKind::Scan, &tid, &req_json).await.unwrap();
    let now = Utc::now();
    // Simulate a dead worker whose lease expired before committing any run.
    let _ = store
        .claim_next_queued_scan_job(
            "dead-worker-1",
            now - ChronoDuration::minutes(10),
            now - ChronoDuration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();

    // Exhaust retries via stale recovery with max_attempts = 1.
    let policy = JobRecoveryPolicy { max_attempts: 1, ..Default::default() };
    let stats = store.recover_stale_scan_jobs(now, policy).await.unwrap();
    assert_eq!(stats.failed_retries_exhausted, 1);
    let row = store.get_scan_job(jid).await.unwrap();
    assert_eq!(row.status, "failed");
    assert!(row.run_id.is_none(), "aborted job must never link an uncommitted run");

    let events = store.list_job_events(jid, 0, 50, None, None).await.unwrap();
    assert!(events.iter().any(|e| e.is_terminal));
}

#[tokio::test]
async fn event_coalescing_on_high_load_preserves_critical_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&support::sqlite_url(&dir)).await.unwrap();
    let svc = ParqonautService::with_storage_policy(store, StoragePolicy::cli_unrestricted_local());
    let jid = JobId(Uuid::new_v4());
    let tid = TargetIdentity { target_kind: "local_file".into(), normalized_key: "/c.pq".into() };
    svc.store().insert_application_job(jid, JobKind::Scan, &tid, "{}").await.unwrap();

    for i in 0..500u64 {
        svc.events().record_nonblocking(lifecycle_event(
            jid,
            ProgressEventKind::BatchProcessed,
            JobStage::Scan,
            ProgressMetrics { rows: i, ..Default::default() },
            format!("row {i}"),
        ));
    }
    svc.events().flush().await;
    svc.events()
        .record_lifecycle(lifecycle_event(
            jid,
            ProgressEventKind::ExecutionFailed,
            JobStage::Worker,
            ProgressMetrics::default(),
            "simulated failure",
        ))
        .await
        .unwrap();
    let events = svc.store().list_job_events(jid, 0, 1000, None, None).await.unwrap();
    assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionFailed);
}
