//! Server plugin policy, catalog, and durable job pinning.

use std::collections::BTreeSet;
use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_app::{ServerPluginPolicy, ServerPluginState, StoragePolicy};
use parqonaut_service::scan_job::parse_scan_job_payload;
use parqonaut_service::{build_router, ParqonautService};
use parqonaut_store::SqliteScanStore;
use parqonaut_types::AuthRole;

mod support;

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn example_rules_policy() -> ServerPluginPolicy {
    ServerPluginPolicy {
        enabled: true,
        roots: vec![plugins_root()],
        allowed: BTreeSet::from(["example-rules".to_string()]),
        runtime: Default::default(),
    }
}

fn scan_fixture() -> Utf8PathBuf {
    support::fixture("scan/tiny_parquet/micro.parquet")
}

fn storage_policy_for_scan() -> StoragePolicy {
    let root = scan_fixture().parent().unwrap().parent().unwrap().to_path_buf();
    StoragePolicy {
        allow_unrestricted_local: false,
        allowed_local_roots: vec![root],
        allowed_s3_buckets: Vec::new(),
        allowed_s3_prefixes: Vec::new(),
    }
}

fn set_plugin_sdk_env() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins");
    let sdk = root.join("src");
    std::env::set_var("PARQONAUT_PLUGIN_SDK_PATH", sdk);
    let venv_py = root.join(".venv/bin/python");
    if venv_py.is_file() {
        std::env::set_var("PARQONAUT_PLUGIN_PYTHON", venv_py);
    } else {
        std::env::set_var("PARQONAUT_PLUGIN_PYTHON", "python3");
    }
}

async fn service_with_plugins() -> (tempfile::TempDir, ParqonautService) {
    set_plugin_sdk_env();
    let (dir, store) = support::connect_store_with_token(AuthRole::Operator, "op").await;
    let plugins = ServerPluginState::bootstrap(example_rules_policy()).expect("bootstrap plugins");
    let svc = ParqonautService::with_storage_policy_and_plugins(
        store,
        storage_policy_for_scan(),
        plugins,
    );
    (dir, svc)
}

#[test]
fn plugins_disabled_rejects_plugin_request() {
    let state = ServerPluginState::disabled();
    let err = state.validate_request(&["example-rules".to_string()]).unwrap_err();
    assert!(matches!(err, parqonaut_app::ApplicationError::PluginExecutionDisabled));
}

#[test]
fn stale_pinned_digest_rejected() {
    set_plugin_sdk_env();
    let state = ServerPluginState::bootstrap(example_rules_policy()).expect("bootstrap");
    let mut pinned = state.resolve_for_enqueue(&["example-rules".into()]).expect("resolve");
    pinned[0].digest = "0".repeat(64);
    let err = state.revalidate_pinned(&pinned).unwrap_err();
    assert!(matches!(err, parqonaut_app::ApplicationError::PluginStale { .. }));
}

#[test]
fn empty_allowlist_rejects_plugin_names() {
    let policy = ServerPluginPolicy {
        enabled: true,
        roots: vec![plugins_root()],
        allowed: BTreeSet::new(),
        runtime: Default::default(),
    };
    let state = ServerPluginState::bootstrap(policy).expect("bootstrap");
    let err = state.validate_request(&["example-rules".to_string()]).unwrap_err();
    assert!(matches!(err, parqonaut_app::ApplicationError::PluginNotAllowed(_)));
}

#[test]
fn legacy_job_payload_deserializes_without_plugins() {
    let legacy = r#"{"target":{"type":"local_file","path":"/tmp/x.parquet"},"profile":"quick"}"#;
    let p = parse_scan_job_payload(legacy).expect("parse");
    assert!(p.plugins.is_empty());
    assert!(p.resolved_plugins.is_empty());
}

#[tokio::test]
async fn catalog_lists_allowed_plugin_only() {
    let (_dir, svc) = service_with_plugins().await;
    let cat = svc.list_plugin_catalog();
    assert!(cat.plugins_enabled);
    assert_eq!(cat.plugins.len(), 1);
    assert_eq!(cat.plugins[0].name, "example-rules");
    assert!(!cat.plugins[0].digest.is_empty());
}

#[tokio::test]
async fn plugin_disabled_server_rejects_scan_with_plugin() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&support::sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token("op", support::TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();
    let svc = ParqonautService::with_storage_policy(store, storage_policy_for_scan());
    let err = svc
        .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
            target: parqonaut_types::ScanTarget::LocalFile { path: scan_fixture() },
            profile: parqonaut_types::ScanProfile::Quick,
            options: Default::default(),
            scan_id: None,
            redaction: None,
            plugins: vec!["example-rules".into()],
        })
        .await
        .unwrap_err();
    assert!(matches!(err, parqonaut_service::error::AppError::PluginExecutionDisabled));
}

#[tokio::test]
async fn enqueue_pins_resolved_plugin_identities() {
    let (_dir, svc) = service_with_plugins().await;
    let path = scan_fixture();
    let resp = svc
        .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
            target: parqonaut_types::ScanTarget::LocalFile { path },
            profile: parqonaut_types::ScanProfile::Quick,
            options: Default::default(),
            scan_id: None,
            redaction: None,
            plugins: vec!["example-rules".into()],
        })
        .await
        .expect("enqueue");
    let row = svc.store().get_scan_job(parqonaut_types::JobId(resp.job_id)).await.unwrap();
    let payload = parse_scan_job_payload(&row.request_json).expect("payload");
    assert_eq!(payload.plugins, vec!["example-rules".to_string()]);
    assert_eq!(payload.resolved_plugins.len(), 1);
    assert_eq!(payload.resolved_plugins[0].name, "example-rules");
    assert!(!payload.resolved_plugins[0].digest.is_empty());
}

#[tokio::test]
async fn http_lists_plugins_when_enabled() {
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let (_dir, svc) = service_with_plugins().await;
    let app = build_router(svc);
    let res = app
        .oneshot(
            Request::get("/api/v1/plugins")
                .header(AUTHORIZATION, support::bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["plugins_enabled"], true);
    assert_eq!(v["plugins"][0]["name"], "example-rules");
}
