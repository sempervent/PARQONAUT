//! Token administration API (admin role).

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use http_body_util::BodyExt;
use paraclete_service::{build_router, openapi_spec, ParacleteService};
use paraclete_store::SqliteScanStore;
use paraclete_types::AuthRole;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

const ADMIN_SECRET: &str = "paraclete-admin-test-token";
const OP_SECRET: &str = "paraclete-http-test-token";

fn bearer(secret: &str) -> String {
    format!("Bearer {secret}")
}

async fn store_with_tokens() -> (tempfile::TempDir, SqliteScanStore) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    let url = format!("sqlite://{}", abs.display());
    let store = SqliteScanStore::connect(&url).await.unwrap();
    store.insert_auth_token("admin", ADMIN_SECRET, AuthRole::Admin).await.unwrap();
    store.insert_auth_token("op", OP_SECRET, AuthRole::Operator).await.unwrap();
    (dir, store)
}

#[tokio::test]
async fn openapi_lists_admin_token_paths() {
    let doc = openapi_spec();
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens/{token_id}"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens/{token_id}/rotate"));
    assert!(doc.paths.paths.contains_key("/api/v1/admin/tokens/{token_id}/disable"));
}

#[tokio::test]
async fn admin_can_create_list_get_disable() {
    let (_dir, store) = store_with_tokens().await;
    let app = build_router(ParacleteService::new(store));

    let body = json!({
        "label": "api-ci",
        "role": "operator",
        "note": "integration"
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/admin/tokens")
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let created: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let tid = created["token_id"].as_str().unwrap();
    let secret = created["token_secret"].as_str().unwrap();
    assert!(secret.starts_with("plc_"));
    assert_eq!(created["token_prefix"].as_str().unwrap().len(), 12);
    assert!(created["token_prefix"].as_str().unwrap().starts_with("plc_"));

    let res = app
        .clone()
        .oneshot(
            Request::get("/api/v1/admin/tokens")
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let list: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(list["items"].as_array().unwrap().len() >= 3);

    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/admin/tokens/{tid}"))
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/admin/tokens/{tid}/disable"))
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let row: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(row["status"], "disabled");

    let res = app
        .oneshot(
            Request::get("/api/v1/whoami")
                .header(AUTHORIZATION, bearer(secret))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn operator_cannot_create_token() {
    let (_dir, store) = store_with_tokens().await;
    let app = build_router(ParacleteService::new(store));
    let body = json!({ "label": "x", "role": "reader" });
    let res = app
        .oneshot(
            Request::post("/api/v1/admin/tokens")
                .header(AUTHORIZATION, bearer(OP_SECRET))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn reader_cannot_list_tokens() {
    let (_dir, store) = store_with_tokens().await;
    store.insert_auth_token("r", "reader-only-secret", AuthRole::Reader).await.unwrap();
    let app = build_router(ParacleteService::new(store));
    let res = app
        .oneshot(
            Request::get("/api/v1/admin/tokens")
                .header(AUTHORIZATION, bearer("reader-only-secret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn disable_unknown_token_404() {
    let (_dir, store) = store_with_tokens().await;
    let app = build_router(ParacleteService::new(store));
    let id = Uuid::new_v4();
    let res = app
        .oneshot(
            Request::post(format!("/api/v1/admin/tokens/{id}/disable"))
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_rotate_invalidates_old_secret_and_sets_last_used() {
    let (_dir, store) = store_with_tokens().await;
    let app = build_router(ParacleteService::new(store));

    let body = json!({ "label": "rot-target", "role": "reader" });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/admin/tokens")
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let created: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let tid = created["token_id"].as_str().unwrap();
    let secret = created["token_secret"].as_str().unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::get("/api/v1/whoami")
                .header(AUTHORIZATION, bearer(secret))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/admin/tokens/{tid}"))
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let meta: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(meta["last_used_at"].is_string());

    let res = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/admin/tokens/{tid}/rotate"))
                .header(AUTHORIZATION, bearer(ADMIN_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let rot: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let new_secret = rot["token_secret"].as_str().unwrap();
    assert_eq!(rot["previous_token_id"].as_str().unwrap(), tid);
    assert!(new_secret.starts_with("plc_"));

    let res = app
        .clone()
        .oneshot(
            Request::get("/api/v1/whoami")
                .header(AUTHORIZATION, bearer(secret))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let res = app
        .oneshot(
            Request::get("/api/v1/whoami")
                .header(AUTHORIZATION, bearer(new_secret))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn operator_cannot_rotate_token() {
    let (_dir, store) = store_with_tokens().await;
    let app = build_router(ParacleteService::new(store));
    let id = Uuid::new_v4();
    let res = app
        .oneshot(
            Request::post(format!("/api/v1/admin/tokens/{id}/rotate"))
                .header(AUTHORIZATION, bearer(OP_SECRET))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}
