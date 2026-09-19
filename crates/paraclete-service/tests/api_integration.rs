//! Cross-cutting HTTP integration: auth, storage policy, sync vs async scan parity (skeleton).

use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use camino::Utf8PathBuf;
use http_body_util::BodyExt;
use paraclete_service::{build_router, build_router_with_workers, ParacleteService};
use paraclete_store::SqliteScanStore;
use paraclete_types::AuthRole;
use parqonaut_app::StoragePolicy;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

mod support;

use support::{bearer_operator, connect_store_with_token, fixture, TEST_BEARER_SECRET};

#[tokio::test]
async fn health_is_public_whoami_requires_auth() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&support::sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("x", TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();
    let app = build_router(ParacleteService::new(store));

    let res = app
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .clone()
        .oneshot(Request::get("/api/v1/whoami").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let res = app
        .oneshot(
            Request::get("/api/v1/whoami")
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn storage_policy_rejects_path_outside_allowed_roots() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator, "integration").await;
    let allowed_root = fixture("scan/single_parquet");
    let policy = StoragePolicy {
        allow_unrestricted_local: false,
        allowed_local_roots: vec![allowed_root.clone()],
        ..StoragePolicy::default()
    };
    let app = build_router(ParacleteService::with_storage_policy(store, policy));

    let outside_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside_file.path(), b"not-parquet").unwrap();
    let outside = Utf8PathBuf::from_path_buf(outside_file.path().to_path_buf()).unwrap();
    let body = json!({
        "target": { "type": "local_file", "path": outside.as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let res = app
        .oneshot(
            Request::post("/api/v1/scans/sync")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], "location_not_allowed");
}

#[tokio::test]
async fn sync_and_async_scan_run_summary_parity_skeleton() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator, "integration").await;
    let app = build_router_with_workers(ParacleteService::new(store), 2);

    let scan_body = json!({
        "target": { "type": "local_file", "path": fixture("scan/single_parquet/data.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });

    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/scans/sync")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(scan_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let sync_run: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(scan_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let submitted: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let job_id = submitted["job_id"].as_str().unwrap().parse::<Uuid>().unwrap();

    let mut async_run = None;
    for _ in 0..400u32 {
        let res = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/jobs/{job_id}"))
                    .header(AUTHORIZATION, bearer_operator())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let job: serde_json::Value =
            serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
        if job["status"] == "succeeded" {
            async_run = Some(job);
            break;
        }
        if job["status"] == "failed" {
            panic!("async job failed: {job}");
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let async_job = async_run.expect("async job did not succeed");
    let async_run_id = async_job["run_id"].as_str().unwrap();

    async fn fetch_run(app: &axum::Router, run_id: &str) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::get(format!("/api/v1/runs/{run_id}"))
                    .header(AUTHORIZATION, bearer_operator())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    let sync_id = sync_run["run_id"].as_str().unwrap();
    let res = fetch_run(&app, sync_id).await;
    assert_eq!(res.status(), StatusCode::OK);
    let sync_summary: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();

    let res = fetch_run(&app, async_run_id).await;
    assert_eq!(res.status(), StatusCode::OK);
    let async_summary: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();

    for key in ["target_kind", "normalized_target_key"] {
        assert_eq!(sync_run.get(key), async_job.get(key), "submission view mismatch on {key}");
        assert_eq!(sync_summary.get(key), async_summary.get(key), "run summary mismatch on {key}");
    }
    assert_eq!(sync_summary.get("run_outcome"), async_summary.get("run_outcome"));
    assert_eq!(sync_summary["summary"]["dataset_count"], async_summary["summary"]["dataset_count"]);
}
