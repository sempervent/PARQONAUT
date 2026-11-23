//! Readiness (`/api/v1/health/ready`) and liveness (`/api/v1/health/live`) contract tests.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use parqonaut_app::StoragePolicy;
use parqonaut_service::{build_router_with_workers, ParqonautService};
use parqonaut_store::SqliteScanStore;
use tower::ServiceExt;

mod support;

#[tokio::test]
async fn liveness_stays_200_and_readiness_returns_503_on_store_outage() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("health.sqlite");
    let store = SqliteScanStore::connect(&format!("sqlite://{}?mode=rwc", db_path.display()))
        .await
        .expect("connect sqlite");
    let pool = store.pool().clone();

    let svc = ParqonautService::with_storage_policy(store, StoragePolicy::cli_unrestricted_local());
    let app = build_router_with_workers(svc, 1);

    let live_ok = app
        .clone()
        .oneshot(Request::get("/api/v1/health/live").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(live_ok.status(), StatusCode::OK);

    let ready_ok = app
        .clone()
        .oneshot(Request::get("/api/v1/health/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready_ok.status(), StatusCode::OK);
    let ok_json: serde_json::Value =
        serde_json::from_slice(&ready_ok.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(ok_json["status"], "ready");

    // Simulate store outage by closing the backing pool.
    pool.close().await;

    let live_during_outage = app
        .clone()
        .oneshot(Request::get("/api/v1/health/live").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(live_during_outage.status(), StatusCode::OK);

    let ready_during_outage = app
        .clone()
        .oneshot(Request::get("/api/v1/health/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready_during_outage.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body_bytes = ready_during_outage.into_body().collect().await.unwrap().to_bytes();
    let body_text = String::from_utf8_lossy(&body_bytes);
    assert!(!body_text.contains(db_path.to_str().unwrap_or("")));
    let err_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(err_json["error"]["code"], "service_unavailable");
    assert_eq!(err_json["error"]["message"], "store dependency unavailable");
}
