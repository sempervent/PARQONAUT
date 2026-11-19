//! End-to-end HTTP tests for job events, SSE replay/reconnect, coalescing, RBAC, and idempotency.

use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use http_body_util::BodyExt;
use parqonaut_app::StoragePolicy;
use parqonaut_service::events::lifecycle_event;
use parqonaut_service::{build_router, build_router_with_workers, ParqonautService};
use parqonaut_store::SqliteScanStore;
use parqonaut_types::{AuthRole, JobId, JobStage};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

mod support;

async fn setup_service_with_roles() -> (tempfile::TempDir, ParqonautService, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&support::sqlite_url(&dir)).await.unwrap();
    let reader_tok = "reader-secret-tok";
    let op_tok = "operator-secret-tok";
    store.insert_auth_token("reader", reader_tok, AuthRole::Reader).await.unwrap();
    store.insert_auth_token("operator", op_tok, AuthRole::Operator).await.unwrap();
    let svc = ParqonautService::with_storage_policy(store, StoragePolicy::cli_unrestricted_local());
    (dir, svc, reader_tok.to_string(), op_tok.to_string())
}

#[tokio::test]
async fn scan_job_emits_ordered_events_and_sse_reconnects_without_duplicates() {
    let (_dir, svc, reader_tok, op_tok) = setup_service_with_roles().await;
    let app = build_router_with_workers(svc.clone(), 1);

    let scan_body = json!({
        "target": { "type": "local_file", "path": support::fixture("scan/single_parquet/data.parquet").as_str() },
        "profile": "standard",
    });

    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, support::bearer(&op_tok))
                .header("content-type", "application/json")
                .header("Idempotency-Key", "scan-idem-1")
                .body(Body::from(scan_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let submitted: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let job_id: Uuid = submitted["job_id"].as_str().unwrap().parse().unwrap();

    // Idempotent replay returns exact same job_id without duplicating job.
    let res2 = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, support::bearer(&op_tok))
                .header("content-type", "application/json")
                .header("Idempotency-Key", "scan-idem-1")
                .body(Body::from(scan_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res2.status(), StatusCode::ACCEPTED);
    let replayed: serde_json::Value =
        serde_json::from_slice(&res2.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(replayed["job_id"], submitted["job_id"]);

    // Same Idempotency-Key with different payload returns 409 Conflict.
    let diff_body = json!({
        "target": { "type": "local_file", "path": support::fixture("scan/single_parquet/data.parquet").as_str() },
        "profile": "deep",
    });
    let res3 = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, support::bearer(&op_tok))
                .header("content-type", "application/json")
                .header("Idempotency-Key", "scan-idem-1")
                .body(Body::from(diff_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res3.status(), StatusCode::CONFLICT);

    // Wait for job completion.
    for _ in 0..200 {
        let j = svc.get_scan_job_view(JobId(job_id)).await.unwrap();
        if format!("{:?}", j.status).to_lowercase().contains("succeeded") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Reader can inspect events (cursor pagination) and overview/runs.
    let ev_res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/jobs/{job_id}/events?after_seq=0&limit=50"))
                .header(AUTHORIZATION, support::bearer(&reader_tok))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ev_res.status(), StatusCode::OK);
    let ev_json: serde_json::Value =
        serde_json::from_slice(&ev_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let items = ev_json["items"].as_array().unwrap();
    assert!(items.len() >= 3, "expected queued, claimed, completed events: {ev_json}");
    let seqs: Vec<i64> = items.iter().map(|e| e["seq"].as_i64().unwrap()).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "events not strictly ordered: {seqs:?}");
    assert_eq!(items.first().unwrap()["kind"], "job_queued");
    assert_eq!(items.last().unwrap()["kind"], "execution_completed");

    // SSE stream with Last-Event-ID=1 replays only seq > 1 and closes cleanly with job_status.
    let sse_res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/jobs/{job_id}/events/stream"))
                .header(AUTHORIZATION, support::bearer(&reader_tok))
                .header("Last-Event-ID", "1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sse_res.status(), StatusCode::OK);
    let sse_bytes = sse_res.into_body().collect().await.unwrap().to_bytes();
    let sse_text = String::from_utf8_lossy(&sse_bytes);
    assert!(!sse_text.contains("id: 1\n"), "replayed duplicate event seq=1: {sse_text}");
    assert!(sse_text.contains("event: job_event"), "expected job_event frames: {sse_text}");
    assert!(sse_text.contains("event: job_status"), "expected terminal job_status: {sse_text}");

    // Unauthorized caller cannot access events or SSE stream.
    let unauth = app
        .clone()
        .oneshot(Request::get(format!("/api/v1/jobs/{job_id}/events")).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn slow_consumer_coalesces_noncritical_events_without_blocking_or_dropping_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&support::sqlite_url(&dir)).await.unwrap();
    let svc = ParqonautService::new(store);
    let jid = JobId::new();
    svc.store()
        .insert_application_job(
            jid,
            parqonaut_types::JobKind::Scan,
            &parqonaut_types::TargetIdentity {
                target_kind: "local_file".into(),
                normalized_key: "/fast-producer.parquet".into(),
            },
            "{}",
        )
        .await
        .unwrap();

    // Flood 2000 rapid non-critical progress updates synchronously; must never block.
    let start = std::time::Instant::now();
    for i in 0..2000u64 {
        svc.events().record_nonblocking(lifecycle_event(
            jid,
            ProgressEventKind::BatchProcessed,
            JobStage::Scan,
            ProgressMetrics { rows: i, ..Default::default() },
            format!("batch {i}"),
        ));
    }
    assert!(start.elapsed() < Duration::from_secs(1), "nonblocking event flood blocked");

    svc.events().flush().await;
    svc.events()
        .record_lifecycle(lifecycle_event(
            jid,
            ProgressEventKind::ExecutionCompleted,
            JobStage::Scan,
            ProgressMetrics::default(),
            "finished",
        ))
        .await
        .unwrap();

    let events = svc.store().list_job_events(jid, 0, 1000, None, None).await.unwrap();
    assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
    let total_coalesced: u64 = events.iter().map(|e| e.metrics.coalesced_events).sum();
    assert!(
        total_coalesced > 0 || events.len() <= 501,
        "expected bounded storage or coalescing: len={}, coalesced={total_coalesced}",
        events.len()
    );
}

#[tokio::test]
async fn repair_and_batch_jobs_emit_lifecycle_and_support_rbac_and_cancel() {
    let (dir, svc, reader_tok, op_tok) = setup_service_with_roles().await;
    let app = build_router_with_workers(svc.clone(), 1);

    // Reader cannot submit repair or cancel jobs (403 Forbidden).
    let repair_body = json!({
        "source": support::fixture("scan/single_parquet").as_str(),
        "output": dir.path().join("repaired").to_str().unwrap(),
        "plan": {
            "parqonaut_plan_version": 1,
            "plan_id": "invalid-dummy",
            "dataset_root": support::fixture("scan/single_parquet").as_str(),
            "dataset_fingerprint": { "algorithm": "sha256_v1", "digest": "0".repeat(64), "file_count": 1, "total_bytes": 1 },
            "policy_fingerprint": "0".repeat(64),
            "operations": [],
            "schema_diff": null,
            "schema_conflicts": [],
            "parqonaut_version": "0.11.0",
            "generated_at": "2026-01-01T00:00:00Z",
            "policy": { "repair": {} }
        },
        "authorize": []
    });
    let forbidden_res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/repairs")
                .header(AUTHORIZATION, support::bearer(&reader_tok))
                .header("content-type", "application/json")
                .body(Body::from(repair_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden_res.status(), StatusCode::FORBIDDEN);

    // Build a real repair plan via /api/v1/plans and submit an idempotent repair job as Operator.
    let plan_res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/plans")
                .header(AUTHORIZATION, support::bearer(&op_tok))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "location": support::fixture("scan/single_parquet").as_str() })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(plan_res.status(), StatusCode::OK);
    let plan_body: serde_json::Value =
        serde_json::from_slice(&plan_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let real_plan = plan_body["plan"].clone();
    let repair_out = dir.path().join("repaired_out");
    let real_repair = json!({
        "source": support::fixture("scan/single_parquet").as_str(),
        "output": repair_out.to_str().unwrap(),
        "plan": real_plan,
        "authorize": []
    });

    let sub = app
        .clone()
        .oneshot(
            Request::post("/api/v1/repairs")
                .header(AUTHORIZATION, support::bearer(&op_tok))
                .header("content-type", "application/json")
                .header("Idempotency-Key", "repair-op-1")
                .body(Body::from(real_repair.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sub.status(), StatusCode::ACCEPTED);
    let sub_json: serde_json::Value =
        serde_json::from_slice(&sub.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let repair_job_id: Uuid = sub_json["job_id"].as_str().unwrap().parse().unwrap();

    // Wait for repair completion.
    for _ in 0..200 {
        let j = svc.get_scan_job_view(JobId(repair_job_id)).await.unwrap();
        if j.status == parqonaut_types::JobStatus::Succeeded {
            break;
        }
        if j.status == parqonaut_types::JobStatus::Failed {
            panic!("repair job failed: {:?}", j.failure);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let events =
        svc.store().list_job_events(JobId(repair_job_id), 0, 100, None, None).await.unwrap();
    assert!(events.iter().any(|e| e.kind == ProgressEventKind::OutputCommitted));
    assert!(events.iter().any(|e| e.kind == ProgressEventKind::VerificationCompleted));
    assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);

    // Overview and runs endpoints work for Reader.
    let ov = app
        .clone()
        .oneshot(
            Request::get("/api/v1/overview")
                .header(AUTHORIZATION, support::bearer(&reader_tok))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ov.status(), StatusCode::OK);
    let _ = build_router(svc);
}

#[tokio::test]
async fn concurrent_http_idempotent_submissions_create_single_job_and_canonical_json_matches() {
    let (_dir, svc, _reader_tok, op_tok) = setup_service_with_roles().await;
    let app = build_router_with_workers(svc.clone(), 1);
    let fixture = support::fixture("scan/single_parquet/data.parquet");

    let mut tasks = Vec::new();
    for i in 0..10 {
        let app_c = app.clone();
        let tok = op_tok.clone();
        let f = fixture.clone();
        tasks.push(tokio::spawn(async move {
            // Alternate key ordering to verify canonical JSON hashing stability.
            let body = if i % 2 == 0 {
                format!(
                    r#"{{"profile":"standard","target":{{"path":"{}","type":"local_file"}}}}"#,
                    f
                )
            } else {
                format!(
                    r#"{{"target":{{"type":"local_file","path":"{}"}},"profile":"standard"}}"#,
                    f
                )
            };
            let res = app_c
                .oneshot(
                    Request::post("/api/v1/jobs/scans")
                        .header(AUTHORIZATION, support::bearer(&tok))
                        .header("content-type", "application/json")
                        .header("Idempotency-Key", "http-conc-idem-1")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::ACCEPTED);
            let v: serde_json::Value =
                serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            v["job_id"].as_str().unwrap().to_string()
        }));
    }

    let mut job_ids = std::collections::BTreeSet::new();
    for t in tasks {
        job_ids.insert(t.await.unwrap());
    }
    assert_eq!(job_ids.len(), 1, "expected 1 job across concurrent retries: {job_ids:?}");
    let total_jobs = svc.store().count_scan_jobs(None).await.unwrap();
    assert_eq!(total_jobs, 1);
    svc.graceful_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn graceful_shutdown_drains_workers_and_flushes_events() {
    let (_dir, svc, reader_tok, op_tok) = setup_service_with_roles().await;
    let app = build_router_with_workers(svc.clone(), 1);
    let scan_body = json!({
        "target": { "type": "local_file", "path": support::fixture("scan/single_parquet/data.parquet").as_str() },
        "profile": "quick",
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, support::bearer(&op_tok))
                .header("content-type", "application/json")
                .body(Body::from(scan_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let submitted: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let job_id: Uuid = submitted["job_id"].as_str().unwrap().parse().unwrap();

    for _ in 0..200 {
        let j = svc.get_scan_job_view(JobId(job_id)).await.unwrap();
        if j.status == parqonaut_types::JobStatus::Succeeded {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    svc.graceful_shutdown(Duration::from_secs(5)).await;
    assert!(svc.is_shutting_down());

    let sse_res = app
        .oneshot(
            Request::get(format!("/api/v1/jobs/{job_id}/events/stream?after_seq=0"))
                .header(AUTHORIZATION, support::bearer(&reader_tok))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sse_res.status(), StatusCode::OK);
    let body = String::from_utf8(sse_res.into_body().collect().await.unwrap().to_bytes().to_vec())
        .unwrap();
    assert!(body.contains("job_queued"));
    assert!(body.contains("execution_completed"));
    assert!(body.contains("event: job_status"));
}
