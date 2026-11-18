//! S3 repair job + durable events integration against RustFS when `PARQONAUT_S3_ENDPOINT` is set.

#![cfg(feature = "s3")]

use std::path::PathBuf;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use http_body_util::BodyExt;
use parqonaut_app::StoragePolicy;
use parqonaut_service::{build_router_with_workers, ParqonautService};
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::conditional::ConditionalCreate;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
use parqonaut_types::{AuthRole, JobId, JobStatus};
use parqonaut_workflow::ProgressEventKind;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

mod support;

#[tokio::test]
async fn s3_repair_job_persists_progress_events_and_publishes_to_rustfs() {
    let Ok(endpoint) = std::env::var("PARQONAUT_S3_ENDPOINT") else {
        eprintln!("skipping s3_operator_e2e: PARQONAUT_S3_ENDPOINT not set");
        return;
    };
    let bucket = std::env::var("FOGBANK_BUCKET")
        .or_else(|_| std::env::var("PARQONAUT_S3_BUCKET"))
        .unwrap_or_else(|_| "parqonaut-test".into());

    let s3 = S3StorageBackend::new(S3Config::minio(endpoint)).await;
    let id = Uuid::new_v4();
    let src_key = format!("operator-e2e/{id}/in/data.parquet");
    let src_bytes = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/scan/single_parquet/data.parquet"),
    )
    .unwrap();
    s3.conditional_create(
        &ObjectLocation::S3 { bucket: bucket.clone(), key: src_key.clone() },
        ConditionalCreate::must_not_exist(),
        src_bytes.into(),
    )
    .await
    .unwrap();

    let src_uri = format!("s3://{bucket}/operator-e2e/{id}/in/");
    let out_uri = format!("s3://{bucket}/operator-e2e/{id}/out/");

    let (dir, store) = support::connect_store_with_token(AuthRole::Operator, "s3-op").await;
    let _keep_dir = dir;
    let policy = StoragePolicy {
        allow_unrestricted_local: false,
        allowed_local_roots: vec![],
        allowed_s3_buckets: vec![bucket.clone()],
        allowed_s3_prefixes: vec![],
    };
    let svc = ParqonautService::with_storage_policy(store, policy);
    let app = build_router_with_workers(svc.clone(), 1);

    let plan_res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/plans")
                .header(AUTHORIZATION, support::bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(json!({ "location": src_uri }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(plan_res.status(), StatusCode::OK);
    let plan_json: serde_json::Value =
        serde_json::from_slice(&plan_res.into_body().collect().await.unwrap().to_bytes()).unwrap();

    let rep_res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/repairs")
                .header(AUTHORIZATION, support::bearer_operator())
                .header("content-type", "application/json")
                .header("Idempotency-Key", format!("s3-rep-{id}"))
                .body(Body::from(
                    json!({
                        "source": src_uri,
                        "output": out_uri,
                        "plan": plan_json["plan"],
                        "authorize": []
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rep_res.status(), StatusCode::ACCEPTED);
    let sub: serde_json::Value =
        serde_json::from_slice(&rep_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let jid = JobId(sub["job_id"].as_str().unwrap().parse().unwrap());

    for _ in 0..400 {
        let v = svc.get_scan_job_view(jid).await.unwrap();
        if v.status == JobStatus::Succeeded {
            let events = svc.store().list_job_events(jid, 0, 100, None, None).await.unwrap();
            assert!(events.iter().any(|e| e.kind == ProgressEventKind::OutputCommitted));
            assert_eq!(events.last().unwrap().kind, ProgressEventKind::ExecutionCompleted);
            let out_obj = ObjectLocation::S3 {
                bucket: bucket.clone(),
                key: format!("operator-e2e/{id}/out/data.parquet"),
            };
            let head = s3.head(&out_obj).await.expect("S3 output visible");
            assert!(head.size > 0);
            return;
        }
        if v.status == JobStatus::Failed {
            panic!("S3 repair job failed: {:?}", v.failure);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("S3 repair job timed out");
}
