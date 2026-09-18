//! OpenAPI document structure and schema presence (Phase 12).

use paraclete_service::openapi_spec;
use paraclete_service::{ErrorBody, ErrorCode, ErrorEnvelope};
use serde_json::Value;

fn openapi_json() -> Value {
    serde_json::to_value(openapi_spec()).expect("openapi serializes to JSON")
}

#[test]
fn openapi_version_and_security() {
    let v = openapi_json();
    assert_eq!(v["info"]["version"], "0.16.0");
    assert!(v["components"]["securitySchemes"]["bearerAuth"].is_object());
}

#[test]
fn openapi_required_paths_present() {
    let paths = &openapi_json()["paths"];
    for p in [
        "/metrics",
        "/api/v1/health",
        "/api/v1/openapi.json",
        "/api/v1/whoami",
        "/api/v1/scans",
        "/api/v1/scans/sync",
        "/api/v1/jobs/scans",
        "/api/v1/jobs/{job_id}",
        "/api/v1/jobs",
        "/api/v1/runs/{run_id}",
        "/api/v1/runs/{run_id}/report",
        "/api/v1/runs/{run_id}/assets",
        "/api/v1/runs/{run_id}/findings",
        "/api/v1/targets/{target_kind}/runs",
        "/api/v1/diff",
        "/api/v1/admin/tokens",
        "/api/v1/admin/tokens/{token_id}",
        "/api/v1/admin/tokens/{token_id}/rotate",
        "/api/v1/admin/tokens/{token_id}/disable",
    ] {
        assert!(paths.get(p).is_some(), "missing path {p}");
    }
}

#[test]
fn openapi_error_envelope_schemas() {
    let schemas = &openapi_json()["components"]["schemas"];
    assert!(schemas.get("ErrorBody").is_some());
    assert!(schemas.get("ErrorEnvelope").is_some());
    assert!(schemas.get("ErrorCode").is_some());
    let code_enum = &schemas["ErrorCode"]["enum"];
    assert!(
        code_enum.as_array().expect("ErrorCode enum").iter().any(|v| v == "invalid_json_request"),
        "OpenAPI ErrorCode should list invalid_json_request"
    );
}

#[test]
fn openapi_key_domain_schemas() {
    let schemas = &openapi_json()["components"]["schemas"];
    for name in [
        "ScanReport",
        "RunDiff",
        "ScanJobView",
        "RunSummaryView",
        "AuthTokenCreateResponse",
        "PagedAssetsResponse",
        "PagedFindingsResponse",
        "JobStatus",
        "AuthRole",
        "AuthTokenStatus",
    ] {
        assert!(schemas.get(name).is_some(), "missing schema {name}");
    }
}

#[test]
fn openapi_examples_on_load_bearing_schemas() {
    let schemas = &openapi_json()["components"]["schemas"];
    assert!(schemas["ScanJobSubmissionResponse"]["example"].is_object());
    assert!(schemas["ErrorBody"]["example"].is_object());
    assert!(schemas["PagedFindingsResponse"]["example"].is_object());
}

#[test]
fn openapi_enum_strings() {
    let s = openapi_json().to_string();
    assert!(s.contains("\"reader\"") && s.contains("\"operator\"") && s.contains("\"admin\""));
    assert!(s.contains("\"queued\"") || s.contains("queued"));
}

#[test]
fn error_body_json_matches_openapi_envelope() {
    let body = ErrorBody {
        error: ErrorEnvelope {
            code: ErrorCode::RunNotFound,
            message: "run not found".into(),
            details: serde_json::json!({}),
        },
    };
    let v = serde_json::to_value(&body).unwrap();
    assert_eq!(v["error"]["code"], "run_not_found");
    assert_eq!(v["error"]["message"], "run not found");
    assert!(v["error"]["details"].is_object());
}
