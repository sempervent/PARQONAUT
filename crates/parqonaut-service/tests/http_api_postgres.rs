//! HTTP smoke against Postgres when `PARQONAUT_TEST_PG_URL` is set.
//!
//! Default CI runs SQLite-only tests; run manually:
//! `PARQONAUT_TEST_PG_URL=postgres://... cargo test -p parqonaut-service --test http_api_postgres -- --ignored`

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use http_body_util::BodyExt;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use camino::Utf8PathBuf;
use parqonaut_app::{ServerPluginPolicy, ServerPluginState, StoragePolicy};
use parqonaut_service::scan_job::parse_scan_job_payload;
use parqonaut_service::{build_router, build_router_with_workers, ParqonautService};
use parqonaut_store::StoreBackend;
use parqonaut_types::{AuthRole, JobId};
use serde_json::json;
use tower::ServiceExt;

const TEST_BEARER: &str = "parqonaut-pg-http-test";

fn pg_url() -> Option<String> {
    std::env::var("PARQONAUT_TEST_PG_URL").ok().filter(|s| !s.trim().is_empty())
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL (see module docs)"]
async fn health_and_whoami_on_postgres() {
    let url = pg_url().expect("PARQONAUT_TEST_PG_URL");
    let store = StoreBackend::connect(&url).await.expect("connect");
    store.insert_auth_token("pg-http", TEST_BEARER, AuthRole::Operator).await.unwrap();
    let app = build_router(ParqonautService::new(store));

    let res = app
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .oneshot(
            Request::get("/api/v1/whoami")
                .header(AUTHORIZATION, format!("Bearer {TEST_BEARER}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["role"], "operator");
}

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn scan_fixture() -> Utf8PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/scan/tiny_parquet/micro.parquet");
    Utf8PathBuf::from_path_buf(path).unwrap()
}

fn set_plugin_env() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins");
    std::env::set_var("PARQONAUT_PLUGIN_SDK_PATH", root.join("src"));
    let venv_py = root.join(".venv/bin/python");
    if venv_py.is_file() {
        std::env::set_var("PARQONAUT_PLUGIN_PYTHON", venv_py);
    } else {
        std::env::set_var("PARQONAUT_PLUGIN_PYTHON", "python3");
    }
}

#[tokio::test]
#[ignore = "set PARQONAUT_TEST_PG_URL (see module docs)"]
async fn plugin_scan_job_on_postgres() {
    set_plugin_env();
    let url = pg_url().expect("PARQONAUT_TEST_PG_URL");
    let store = StoreBackend::connect(&url).await.expect("connect");
    store.insert_auth_token("pg-plugin", TEST_BEARER, AuthRole::Operator).await.unwrap();

    let fixture_root = scan_fixture().parent().unwrap().parent().unwrap().to_path_buf();
    let policy = StoragePolicy {
        allow_unrestricted_local: false,
        allowed_local_roots: vec![fixture_root],
        allowed_s3_buckets: Vec::new(),
        allowed_s3_prefixes: Vec::new(),
    };
    let plugins = ServerPluginState::bootstrap(ServerPluginPolicy {
        enabled: true,
        roots: vec![plugins_root()],
        allowed: BTreeSet::from(["example-rules".to_string()]),
        runtime: Default::default(),
    })
    .unwrap();
    let svc = ParqonautService::with_storage_policy_and_plugins(store, policy, plugins);
    let app = build_router_with_workers(svc.clone(), 1);

    let body = json!({
        "target": { "type": "local_file", "path": scan_fixture().as_str() },
        "profile": "quick",
        "plugins": ["example-rules"],
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, format!("Bearer {TEST_BEARER}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let v: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let job_id = v["job_id"].as_str().unwrap().parse::<uuid::Uuid>().unwrap();
    let row = svc.store().get_scan_job(JobId(job_id)).await.unwrap();
    let payload = parse_scan_job_payload(&row.request_json).unwrap();
    assert_eq!(payload.resolved_plugins[0].name, "example-rules");
    assert!(!payload.resolved_plugins[0].digest.is_empty());

    for _ in 0..400 {
        let j = svc.store().get_scan_job(JobId(job_id)).await.unwrap();
        if j.status == "succeeded" {
            assert!(j.run_id.is_some());
            return;
        }
        if j.status == "failed" {
            panic!("postgres plugin job failed: {:?}", j.failure_message);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("postgres plugin job timed out");
}
