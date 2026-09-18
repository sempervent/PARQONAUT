//! Prometheus metrics integration tests. Uses [`serial_test`] because metrics are process-global.

use std::path::PathBuf;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use camino::Utf8PathBuf;
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use paraclete_service::{build_router, ParacleteService};
use paraclete_store::SqliteScanStore;
use paraclete_types::{AuthRole, JobId, TargetIdentity};
use serde_json::json;
use serial_test::serial;
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

fn sum_samples_for_metric(body: &str, name: &str) -> f64 {
    let mut sum = 0.0;
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if !line.starts_with(name) {
            continue;
        }
        let rest = &line[name.len()..];
        if !(rest.starts_with(' ') || rest.starts_with('{')) {
            continue;
        }
        if let Some(n) = line.rsplit(' ').next() {
            sum += n.parse::<f64>().unwrap_or(0.0);
        }
    }
    sum
}

async fn scrape_metrics(
    app: impl tower::Service<
            Request<Body>,
            Response = axum::response::Response,
            Error = std::convert::Infallible,
        > + Clone,
) -> String {
    let res = app.oneshot(Request::get("/metrics").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let _ = app
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = scrape_metrics(app).await;
    assert!(
        body.contains("paraclete_http_requests_total"),
        "expected paraclete metrics after one request; body:\n{body}"
    );
}

#[tokio::test]
#[serial]
async fn http_request_counter_increments_for_health() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let before =
        sum_samples_for_metric(&scrape_metrics(app.clone()).await, "paraclete_http_requests_total");
    let _ = app
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let after = sum_samples_for_metric(&scrape_metrics(app).await, "paraclete_http_requests_total");
    assert!(
        after > before,
        "expected http request counter to increase: before={before} after={after}"
    );
}

#[tokio::test]
#[serial]
async fn authorization_denied_increments_when_reader_posts_scan() {
    let (_dir, store) = connect_store_with_token(AuthRole::Reader).await;
    let app = build_router(ParacleteService::new(store));
    let before = sum_samples_for_metric(
        &scrape_metrics(app.clone()).await,
        "paraclete_authorization_denied_total",
    );
    let body = json!({
        "target": { "type": "local_file", "path": fixture("phase1/single_parquet/data.parquet").as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    });
    let _ = app
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
    let after =
        sum_samples_for_metric(&scrape_metrics(app).await, "paraclete_authorization_denied_total");
    assert!(after > before, "authorization denied counter: before={before} after={after}");
}

#[tokio::test]
#[serial]
async fn auth_failure_increments_on_missing_bearer() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("x", TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let before =
        sum_samples_for_metric(&scrape_metrics(app.clone()).await, "paraclete_auth_failure_total");
    let _ = app
        .clone()
        .oneshot(Request::get("/api/v1/jobs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let after = sum_samples_for_metric(&scrape_metrics(app).await, "paraclete_auth_failure_total");
    assert!(after > before, "auth failure counter: before={before} after={after}");
}

#[tokio::test]
#[serial]
async fn async_job_success_exposes_job_counters() {
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

    for _ in 0..500u32 {
        let body = scrape_metrics(app.clone()).await;
        if sum_samples_for_metric(&body, "paraclete_jobs_succeeded_total") >= 1.0 {
            assert!(body.contains("paraclete_jobs_submitted_total"));
            assert!(body.contains("paraclete_runs_persisted_total"));
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("job did not complete in time");
}

#[tokio::test]
#[serial]
async fn worker_recovery_increments_jobs_requeued_total() {
    let dir = tempfile::tempdir().unwrap();
    let url = sqlite_url(&dir);
    let store = SqliteScanStore::connect(&url).await.unwrap();
    store.insert_auth_token("integration", TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();

    let path = fixture("phase1/single_parquet/data.parquet");
    let jid = JobId::new();
    let identity = TargetIdentity {
        target_kind: "local_file".into(),
        normalized_key: path.as_str().to_string(),
    };
    let request_json = json!({
        "target": { "type": "local_file", "path": path.as_str() },
        "profile": "standard",
        "options": { "mode": "full", "max_files": 100000, "format_hints": [] }
    })
    .to_string();
    store.insert_scan_job_queued(jid, &identity, &request_json).await.unwrap();
    let now = Utc::now();
    store
        .claim_next_queued_scan_job("fixture-worker", now, now + ChronoDuration::seconds(60))
        .await
        .unwrap()
        .unwrap();

    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    sqlx::query("UPDATE scan_jobs SET leased_until = ? WHERE job_id = ?")
        .bind("1999-01-01T00:00:00Z")
        .bind(jid.0.to_string())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let service = ParacleteService::new(store);
    let app = build_router(service);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let body = scrape_metrics(app).await;
    assert!(body.contains("paraclete_jobs_requeued_total"), "metrics:\n{body}");
    let v = sum_samples_for_metric(&body, "paraclete_jobs_requeued_total");
    assert!(v >= 1.0, "expected requeued counter >= 1, got {v}");
}

#[tokio::test]
#[serial]
async fn request_id_header_on_protected_route() {
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
    let id = res.headers().get("x-request-id");
    assert!(id.is_some(), "expected X-Request-Id header");
    let s = id.unwrap().to_str().unwrap();
    assert_eq!(s.len(), 36);
    Uuid::parse_str(s).unwrap();
}
