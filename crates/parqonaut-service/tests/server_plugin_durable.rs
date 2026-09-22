//! Durable server plugin jobs: cancellation, recovery, stale digest, policy, secrets.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use camino::Utf8PathBuf;
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use parqonaut_app::{ServerPluginPolicy, ServerPluginState, StoragePolicy};
use parqonaut_service::scan_job::parse_scan_job_payload;
use parqonaut_service::{build_router_with_workers, ParqonautService};
use parqonaut_store::SqliteScanStore;
use parqonaut_types::{AuthRole, JobId, JobRecoveryPolicy};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

mod support;

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn scan_fixture() -> Utf8PathBuf {
    support::fixture("scan/tiny_parquet/micro.parquet")
}

fn policy_allowed(names: &[&str]) -> ServerPluginPolicy {
    ServerPluginPolicy {
        enabled: true,
        roots: vec![plugins_root()],
        allowed: names.iter().map(|s| (*s).to_string()).collect::<BTreeSet<_>>(),
        runtime: Default::default(),
    }
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

async fn service_with_policy(names: &[&str]) -> (tempfile::TempDir, ParqonautService) {
    set_plugin_env();
    if std::env::var("PARQONAUT_TEST_SLOW_SCAN_SEC").is_err() {
        std::env::set_var("PARQONAUT_TEST_SLOW_SCAN_SEC", "3");
    }
    let (dir, store) = support::connect_store_with_token(AuthRole::Operator, "op").await;
    let plugins = ServerPluginState::bootstrap(policy_allowed(names)).expect("bootstrap");
    let svc = ParqonautService::with_storage_policy_and_plugins(
        store,
        storage_policy_for_scan(),
        plugins,
    );
    (dir, svc)
}

async fn submit_scan(app: &axum::Router, plugins: &[&str]) -> Uuid {
    let body = json!({
        "target": { "type": "local_file", "path": scan_fixture().as_str() },
        "profile": "quick",
        "plugins": plugins,
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, support::bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let v: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    v["job_id"].as_str().unwrap().parse().unwrap()
}

async fn poll_job_status(app: &axum::Router, job_id: Uuid) -> serde_json::Value {
    let res = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/jobs/{job_id}"))
                .header(AUTHORIZATION, support::bearer_operator())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn cancel_slow_scan_plugin_job() {
    std::env::set_var("PARQONAUT_TEST_SLOW_SCAN_SEC", "8");
    let (_dir, svc) = service_with_policy(&["adv-sleep"]).await;
    let app = build_router_with_workers(svc.clone(), 1);
    let job_id = submit_scan(&app, &["adv-sleep"]).await;
    let jid = JobId(job_id);

    let mut saw_running = false;
    for _ in 0..1200 {
        let row = svc.store().get_scan_job(jid).await.unwrap();
        if row.status == "running" {
            saw_running = true;
            let _ = svc.cancel_job(jid).await;
        }
        if row.status == "canceled" {
            assert!(saw_running, "job canceled before worker started");
            return;
        }
        if row.status == "succeeded" {
            panic!("job succeeded instead of cancel (running={saw_running})");
        }
        if row.status == "failed" {
            panic!("job failed before cancel: {:?}", row.failure_message);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("job not canceled in time (running={saw_running})");
}

#[tokio::test]
async fn recovery_runs_pinned_plugin_after_stale_lease() {
    set_plugin_env();
    let dir = tempfile::tempdir().unwrap();
    let db_url = support::sqlite_url(&dir);
    let store = SqliteScanStore::connect(&db_url).await.unwrap();
    store.insert_auth_token("op", support::TEST_BEARER_SECRET, AuthRole::Operator).await.unwrap();
    let plugins = ServerPluginState::bootstrap(policy_allowed(&["adv-sleep"])).unwrap();
    let svc = ParqonautService::with_storage_policy_and_plugins(
        store,
        storage_policy_for_scan(),
        plugins,
    );

    let payload = svc
        .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
            target: parqonaut_types::ScanTarget::LocalFile { path: scan_fixture() },
            profile: parqonaut_types::ScanProfile::Quick,
            options: Default::default(),
            scan_id: None,
            redaction: None,
            plugins: vec!["adv-sleep".into()],
        })
        .await
        .expect("enqueue");
    let jid = JobId(payload.job_id);

    let now = Utc::now();
    let leased = now + ChronoDuration::seconds(60);
    svc.store()
        .claim_next_queued_scan_job("worker-a", now, leased)
        .await
        .unwrap()
        .expect("claimed");

    let pool = sqlx::SqlitePool::connect(&db_url).await.unwrap();
    sqlx::query("UPDATE application_jobs SET leased_until = ? WHERE job_id = ?")
        .bind("1999-01-01T00:00:00Z")
        .bind(jid.0.to_string())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let _app = build_router_with_workers(svc.clone(), 1);
    svc.store().recover_stale_scan_jobs(Utc::now(), JobRecoveryPolicy::default()).await.unwrap();

    for _ in 0..400 {
        let row = svc.store().get_scan_job(jid).await.unwrap();
        if row.status == "succeeded" {
            let parsed = parse_scan_job_payload(&row.request_json).unwrap();
            assert_eq!(parsed.resolved_plugins[0].name, "adv-sleep");
            return;
        }
        if row.status == "failed" {
            panic!("job failed: {:?}", row.failure_message);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("job did not succeed after recovery");
}

#[tokio::test]
#[serial_test::serial]
async fn stale_plugin_rejected_at_worker_without_spawn() {
    set_plugin_env();
    let (_dir, svc) = service_with_policy(&["example-rules"]).await;
    let submit = svc
        .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
            target: parqonaut_types::ScanTarget::LocalFile { path: scan_fixture() },
            profile: parqonaut_types::ScanProfile::Quick,
            options: Default::default(),
            scan_id: None,
            redaction: None,
            plugins: vec!["example-rules".into()],
        })
        .await
        .unwrap();
    let row = svc.store().get_scan_job(JobId(submit.job_id)).await.unwrap();
    let payload = parse_scan_job_payload(&row.request_json).unwrap();

    let plugin_py = plugins_root().join("example-rules/example_rules.py");
    let original = std::fs::read_to_string(&plugin_py).unwrap();
    std::fs::write(&plugin_py, format!("{original}\n# stale marker\n")).unwrap();

    let err = svc.execute_scan_job_payload(payload.clone(), None).await.unwrap_err();
    assert!(matches!(err, parqonaut_service::error::AppError::PluginStale { .. }));

    std::fs::write(&plugin_py, original).unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn removed_plugin_fails_at_execution() {
    set_plugin_env();
    let (_dir, svc) = service_with_policy(&["example-rules"]).await;
    let payload = {
        let submit = svc
            .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
                target: parqonaut_types::ScanTarget::LocalFile { path: scan_fixture() },
                profile: parqonaut_types::ScanProfile::Quick,
                options: Default::default(),
                scan_id: None,
                redaction: None,
                plugins: vec!["example-rules".into()],
            })
            .await
            .unwrap();
        parse_scan_job_payload(
            &svc.store().get_scan_job(JobId(submit.job_id)).await.unwrap().request_json,
        )
        .unwrap()
    };

    let plugin_dir = plugins_root().join("example-rules");
    let hidden =
        std::env::temp_dir().join(format!("parqonaut-removed-plugin-{}", uuid::Uuid::new_v4()));
    std::fs::rename(&plugin_dir, &hidden).unwrap();
    let err = svc.execute_scan_job_payload(payload, None).await.unwrap_err();
    std::fs::rename(&hidden, &plugin_dir).unwrap();
    assert!(
        matches!(err, parqonaut_service::error::AppError::PluginNotFound(_)),
        "expected plugin_not_found, got {err:?}"
    );
}

#[tokio::test]
async fn policy_change_after_enqueue_rejects_execution() {
    set_plugin_env();
    let (_dir, svc) = service_with_policy(&["example-rules"]).await;
    let payload = {
        let submit = svc
            .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
                target: parqonaut_types::ScanTarget::LocalFile { path: scan_fixture() },
                profile: parqonaut_types::ScanProfile::Quick,
                options: Default::default(),
                scan_id: None,
                redaction: None,
                plugins: vec!["example-rules".into()],
            })
            .await
            .unwrap();
        parse_scan_job_payload(
            &svc.store().get_scan_job(JobId(submit.job_id)).await.unwrap().request_json,
        )
        .unwrap()
    };

    let disabled_policy = ServerPluginPolicy {
        enabled: false,
        roots: vec![plugins_root()],
        allowed: BTreeSet::from(["example-rules".to_string()]),
        runtime: Default::default(),
    };
    let store = svc.store().clone();
    let svc2 = ParqonautService::with_storage_policy_and_plugins(
        store,
        storage_policy_for_scan(),
        ServerPluginState::bootstrap(disabled_policy).unwrap(),
    );
    let err = svc2.execute_scan_job_payload(payload, None).await.unwrap_err();
    assert!(matches!(err, parqonaut_service::error::AppError::PluginExecutionDisabled));
}

#[tokio::test]
async fn scan_request_ignores_execution_override_fields() {
    use axum::http::{header::AUTHORIZATION, Request};
    use http_body_util::BodyExt;

    let (_dir, svc) = service_with_policy(&["example-rules"]).await;
    let app = build_router_with_workers(svc.clone(), 1);
    let body = json!({
        "target": { "type": "local_file", "path": scan_fixture().as_str() },
        "profile": "quick",
        "plugins": ["example-rules"],
        "plugin_root": "/etc",
        "manifest_path": "/etc/passwd",
        "python": "/bin/sh",
        "entrypoint": "malicious",
        "environment": { "EVIL": "1" },
    });
    let res = app
        .clone()
        .oneshot(
            Request::post("/api/v1/jobs/scans")
                .header(AUTHORIZATION, support::bearer_operator())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let v: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let job_id = v["job_id"].as_str().unwrap();
    let row =
        svc.store().get_scan_job(JobId(uuid::Uuid::parse_str(job_id).unwrap())).await.unwrap();
    let payload = parse_scan_job_payload(&row.request_json).unwrap();
    assert_eq!(payload.resolved_plugins[0].name, "example-rules");
    assert!(!payload.resolved_plugins[0].digest.is_empty());
}

#[tokio::test]
async fn storage_policy_rejects_before_plugin_spawn() {
    let (_dir, svc) = service_with_policy(&["example-rules"]).await;
    let outside = "/no/such/parqonaut/forbidden/data.parquet";
    let err = svc
        .submit_scan_job(parqonaut_service::api_types::StartScanRequest {
            target: parqonaut_types::ScanTarget::LocalFile { path: Utf8PathBuf::from(outside) },
            profile: parqonaut_types::ScanProfile::Quick,
            options: Default::default(),
            scan_id: None,
            redaction: None,
            plugins: vec!["example-rules".into()],
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            parqonaut_service::error::AppError::LocationNotAllowed(_)
                | parqonaut_service::error::AppError::InvalidRequest(_)
        ),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn server_job_leak_env_plugin_does_not_see_secrets() {
    std::env::set_var("DATABASE_URL", "postgres://CANARY_DB_PASSWORD@127.0.0.1/db");
    std::env::set_var("PRQNT_BOOTSTRAP_ADMIN_TOKEN", "CANARY_BOOTSTRAP");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "CANARY_AWS_SECRET");
    std::env::set_var("GITHUB_TOKEN", "CANARY_GITHUB");

    let (_dir, svc) = service_with_policy(&["leak-env"]).await;
    let app = build_router_with_workers(svc, 1);
    let job_id = submit_scan(&app, &["leak-env"]).await;
    for _ in 0..400 {
        let j = poll_job_status(&app, job_id).await;
        if j["status"] == "succeeded" {
            let run_id = j["run_id"].as_str().expect("run_id");
            let res = app
                .clone()
                .oneshot(
                    Request::get(format!("/api/v1/runs/{run_id}/report"))
                        .header(AUTHORIZATION, support::bearer_operator())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let report: serde_json::Value =
                serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            let blob = report.to_string();
            for needle in
                ["CANARY_DB_PASSWORD", "CANARY_BOOTSTRAP", "CANARY_AWS_SECRET", "CANARY_GITHUB"]
            {
                assert!(!blob.contains(needle), "secret canary leaked in report: {needle}");
            }
            return;
        }
        if j["status"] == "failed" {
            panic!("leak-env job failed: {j}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("leak-env job timed out");
}

#[tokio::test]
async fn catalog_response_has_no_filesystem_paths() {
    let (_dir, svc) = service_with_policy(&["example-rules"]).await;
    let cat = svc.list_plugin_catalog();
    let s = serde_json::to_string(&cat).unwrap();
    assert!(!s.contains("/fixtures/"));
    assert!(!s.contains("python"));
    assert!(!s.contains("entrypoint"));
}
