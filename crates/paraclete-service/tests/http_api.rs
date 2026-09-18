//! HTTP integration tests (hermetic temp SQLite + Axum router).

use std::path::PathBuf;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use camino::Utf8PathBuf;
use http_body_util::BodyExt;
use paraclete_service::{build_router, openapi_spec, ParacleteService};
use paraclete_store::SqliteScanStore;
use paraclete_types::AuthRole;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

const TEST_BEARER_SECRET: &str = "paraclete-http-test-token";

fn bearer_operator() -> String {
    format!("Bearer {TEST_BEARER_SECRET}")
}

async fn connect_store_with_token(role: AuthRole) -> (tempfile::TempDir, SqliteScanStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("integration", TEST_BEARER_SECRET, role).await.unwrap();
    (dir, store)
}

fn sqlite_url(dir: &tempfile::TempDir) -> String {
    let p = dir.path().join("t.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    format!("sqlite://{}", abs.display())
}

fn fixture(rel: &str) -> Utf8PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(rel);
    Utf8PathBuf::from_path_buf(path).unwrap()
}

#[tokio::test]
async fn health_ok() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let res =
        app.oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["status"], "ok");
}

#[tokio::test]
async fn openapi_lists_core_paths() {
    let doc = openapi_spec();
    assert!(doc.paths.paths.contains_key("/metrics"));
    assert!(doc.paths.paths.contains_key("/api/v1/health"));
    assert!(doc.paths.paths.contains_key("/api/v1/whoami"));
    assert!(doc.paths.paths.contains_key("/api/v1/scans"));
    assert!(doc.paths.paths.contains_key("/api/v1/jobs"));
    assert!(doc.paths.paths.contains_key("/api/v1/diff"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens/{token_id}"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens/{token_id}/rotate"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens/{token_id}/disable"));
    assert!(doc.components.is_some());
}

#[tokio::test]
async fn scan_persist_summary_report_assets_findings_diff() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));

    let body = json!({
        "target": { "type": "local_file", "path": fixture("phase1/single_parquet/data.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/scans/sync")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let created: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let run_id = created["run_id"].as_str().unwrap().parse::<Uuid>().unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/runs/{run_id}"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/runs/{run_id}/report"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/runs/{run_id}/assets?limit=10&offset=0"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let page: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(page["total"], 1);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);

    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/runs/{run_id}/findings?limit=10&offset=0"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Second run for diff
    let body2 = json!({
        "target": { "type": "local_file", "path": fixture("phase1/tiny_parquet/micro.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/scans/sync")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body2.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let run2: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let run_b = run2["run_id"].as_str().unwrap();

    let res = app
        .oneshot(
            Request::get(format!("/api/v1/diff?left_run_id={run_id}&right_run_id={run_b}"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn run_not_found() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let id = Uuid::new_v4();
    let res = app
        .oneshot(
            Request::get(format!("/api/v1/runs/{id}"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], "run_not_found");
}

#[tokio::test]
async fn invalid_json_request_syntax() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let res = app
        .oneshot(
            Request::post("/api/v1/scans")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from("not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], "invalid_json_request");
    assert!(v["error"]["message"].is_string());
}

#[tokio::test]
async fn invalid_json_request_wrong_shape() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let res = app
        .oneshot(
            Request::post("/api/v1/scans")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], "invalid_json_request");
}

#[tokio::test]
async fn invalid_json_request_missing_content_type() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let res = app
        .oneshot(
            Request::post("/api/v1/scans")
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], "invalid_json_request");
}

#[tokio::test]
async fn unknown_target_kind_404() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let res = app
        .oneshot(
            Request::get("/api/v1/targets/not_a_kind/runs?normalized_key=x")
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_runs_for_target() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let path = fixture("phase1/single_parquet/data.parquet");
    let body = json!({
        "target": { "type": "local_file", "path": path.as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/scans/sync")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let key = path.as_str();
    let res = app
        .oneshot(
            Request::get(format!(
                "/api/v1/targets/local_file/runs?normalized_key={}",
                urlencoding::encode(key)
            ))
            .header(AUTHORIZATION, bearer_operator())
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn async_scan_job_poll_then_fetch_run() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));

    let body = json!({
        "target": { "type": "local_file", "path": fixture("phase1/single_parquet/data.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let submitted: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let job_id = submitted["job_id"].as_str().unwrap().parse::<Uuid>().unwrap();

    let mut run_id = None;
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
        assert_eq!(res.status(), StatusCode::OK);
        let j: serde_json::Value =
            serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
        match j["status"].as_str() {
            Some("succeeded") => {
                assert_eq!(j["attempt_count"], 1);
                assert!(j["worker_id"].is_null());
                run_id = j["run_id"].as_str().and_then(|s| s.parse::<Uuid>().ok());
                break;
            }
            Some("failed") => panic!("job failed: {j}"),
            _ => tokio::time::sleep(Duration::from_millis(15)).await,
        }
    }
    let run_id = run_id.expect("job did not succeed in time");

    let res = app
        .oneshot(
            Request::get(format!("/api/v1/runs/{run_id}"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn job_not_found_returns_envelope() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
    let id = Uuid::new_v4();
    let res = app
        .oneshot(
            Request::get(format!("/api/v1/jobs/{id}"))
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], "job_not_found");
}

#[tokio::test]
async fn async_scan_job_records_failure() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));

    let body = json!({
        "target": { "type": "local_file", "path": "/no/such/paraclete_missing_file.parquet" },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/scans")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let submitted: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let job_id = submitted["job_id"].as_str().unwrap().parse::<Uuid>().unwrap();

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
        let j: serde_json::Value =
            serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
        if j["status"] == "failed" {
            assert!(j["failure"].is_object());
            assert_eq!(j["run_id"], serde_json::Value::Null);
            assert!(j.get("attempt_count").is_some());
            return;
        }
        if j["status"] == "succeeded" {
            panic!("expected failure, got {j}");
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("job did not fail in time");
}

#[tokio::test]
async fn list_jobs_returns_total() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));

    let body = json!({
        "target": { "type": "local_file", "path": fixture("phase1/single_parquet/data.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let _ = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let res = app
        .oneshot(
            Request::get("/api/v1/jobs?limit=10&offset=0")
                .header(AUTHORIZATION, bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let page: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(page["total"], 1);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn missing_bearer_returns_401() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("x", TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let res = app.oneshot(Request::get("/api/v1/jobs").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert!(res.headers().get("www-authenticate").is_some());
}

#[tokio::test]
async fn invalid_bearer_returns_401() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("x", TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let res = app
        .oneshot(
            Request::get("/api/v1/jobs")
                .header(AUTHORIZATION, "Bearer wrong-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn reader_cannot_post_scan_returns_403() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("reader", TEST_BEARER_SECRET, AuthRole::Reader).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let body = json!({
        "target": { "type": "local_file", "path": fixture("phase1/single_parquet/data.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let res = app
        .oneshot(
            Request::post("/api/v1/scans")
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
    assert_eq!(v["error"]["code"], "forbidden");
}

#[tokio::test]
async fn whoami_returns_identity() {
    let (_dir, store) = connect_store_with_token(AuthRole::Operator).await;
    let app = build_router(ParacleteService::new(store));
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
    let v: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(v["label"], "integration");
    assert_eq!(v["role"], "operator");
}
