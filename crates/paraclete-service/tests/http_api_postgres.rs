//! HTTP smoke against Postgres when `PARACLETE_TEST_PG_URL` is set.
//!
//! Default CI runs SQLite-only tests; run manually:
//! `PARACLETE_TEST_PG_URL=postgres://... cargo test -p paraclete-service --test http_api_postgres -- --ignored`

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use http_body_util::BodyExt;
use paraclete_service::{build_router, ParacleteService};
use paraclete_store::StoreBackend;
use paraclete_types::AuthRole;
use tower::ServiceExt;

const TEST_BEARER: &str = "paraclete-pg-http-test";

fn pg_url() -> Option<String> {
    std::env::var("PARACLETE_TEST_PG_URL").ok().filter(|s| !s.trim().is_empty())
}

#[tokio::test]
#[ignore = "set PARACLETE_TEST_PG_URL (see module docs)"]
async fn health_and_whoami_on_postgres() {
    let url = pg_url().expect("PARACLETE_TEST_PG_URL");
    let store = StoreBackend::connect(&url).await.expect("connect");
    store.insert_auth_token("pg-http", TEST_BEARER, AuthRole::Operator).await.unwrap();
    let app = build_router(ParacleteService::new(store));

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
