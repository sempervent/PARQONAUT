//! OpenAPI document for the HTTP surface (`utoipa` + path stubs in [`paths`]).

mod paths;

use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::Modify;
use utoipa::OpenApi;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .description(Some(
                            "Opaque API token; set `Authorization: Bearer <token>`. Tokens are stored hashed in SQLite."
                                .to_string(),
                        ))
                        .build(),
                ),
            );
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "PARQONAUT API",
        version = env!("CARGO_PKG_VERSION"),
        description = "PARQONAUT HTTP API for scan jobs, persisted runs, diffing, and bearer-token administration. Successful JSON responses and application-level errors use the `{ \"error\": { \"code\", \"message\", \"details\" } }` envelope for failures. JSON request bodies use a shared extractor so malformed JSON, wrong JSON shape, missing `Content-Type: application/json`, and related body-buffer failures map to `invalid_json_request` with appropriate HTTP status codes (`400`, `415`, `422`, `413` where applicable)."
    ),
    paths(
        paths::get_metrics,
        paths::get_health,
        paths::get_openapi_json,
        paths::get_whoami,
        paths::list_plugins,
        paths::get_plugin_by_name,
        paths::post_scans_async,
        paths::post_jobs_scans,
        paths::post_scans_sync,
        paths::get_job_by_id,
        paths::list_jobs,
        paths::get_run_by_id,
        paths::get_run_report,
        paths::get_run_assets,
        paths::get_run_findings,
        paths::list_target_runs,
        paths::get_diff,
        paths::get_admin_tokens,
        paths::post_admin_tokens,
        paths::get_admin_token_by_id,
        paths::post_admin_token_rotate,
        paths::post_admin_token_disable,
        paths::post_diagnose,
        paths::post_plans,
        paths::post_checks,
        paths::post_repairs,
        paths::post_verifications,
        paths::post_batch_check,
        paths::post_batch_plans,
        paths::post_batch_repairs,
        paths::get_batch_status,
        paths::post_batch_resume,
        paths::post_batch_verify,
        paths::post_job_cancel,
        paths::get_api_metrics,
    ),
    components(
        schemas(
            crate::error::ErrorCode,
            crate::error::ErrorEnvelope,
            crate::error::ErrorBody,
        )
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "health", description = "Liveness and identity"),
        (name = "jobs", description = "Async and synchronous scans"),
        (name = "runs", description = "Persisted runs, reports, projections, diff"),
        (name = "admin", description = "Bearer token administration (admin role)"),
        (name = "meta", description = "Metrics and API contract"),
        (name = "plugins", description = "Server allowlisted plugin catalog"),
        (name = "repair", description = "Diagnose, plan, check, repair, verify"),
        (name = "batch", description = "Batch orchestration control plane"),
    ),
)]
pub struct ApiDoc;

/// OpenAPI 3.1 document for the HTTP surface (schemas derived from Rust types).
pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
