//! Bearer authentication and role checks for `/api/v1` (except `GET /api/v1/health`).

use axum::extract::{Request, State};
use axum::http::header::{self, AUTHORIZATION};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::Json;
use paraclete_types::AuthRole;

use crate::error::{ErrorBody, ErrorCode, ErrorEnvelope};
use crate::http::AppState;
use crate::observability::audit;

/// Validates `Authorization: Bearer`, role policy, and attaches [`paraclete_types::AuthPrincipal`] to request extensions.
#[allow(clippy::result_large_err)] // inherited Paraclete axum middleware shape
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, Response> {
    // Nested `/api/v1/*` routes see paths without the `/api/v1` prefix (e.g. `/scans`).
    let path = req.uri().path();

    let Some(token) = parse_bearer(req.headers()) else {
        metrics::counter!("parqonaut_auth_failure_total", "reason" => "missing").increment(1);
        audit::auth_rejected("missing_bearer");
        return Err(unauthorized_response("missing or invalid Authorization header"));
    };

    let principal = match state.service.store().verify_bearer_token(token).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            metrics::counter!("parqonaut_auth_failure_total", "reason" => "invalid_token")
                .increment(1);
            audit::auth_rejected("invalid_or_disabled_token");
            return Err(unauthorized_response("invalid or disabled bearer token"));
        }
        Err(e) => {
            metrics::counter!("parqonaut_auth_failure_total", "reason" => "store").increment(1);
            tracing::error!(error = %e, "verify_bearer_token");
            audit::auth_rejected("store_error");
            return Err(internal_error_response());
        }
    };

    let required = required_role(path, req.method());
    if !principal.role.satisfies(required) {
        metrics::counter!("parqonaut_authorization_denied_total").increment(1);
        audit::auth_forbidden(principal.token_id.0, &principal.label, principal.role);
        return Err(forbidden_response("insufficient role for this operation"));
    }

    metrics::counter!("parqonaut_auth_success_total").increment(1);
    audit::auth_accepted(principal.token_id.0, &principal.label, principal.role);

    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

fn parse_bearer(headers: &HeaderMap) -> Option<&str> {
    let h = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let rest = h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer "))?;
    let t = rest.trim();
    if t.is_empty() {
        return None;
    }
    Some(t)
}

fn required_role(path: &str, method: &Method) -> AuthRole {
    if path.starts_with("/admin/tokens") {
        return AuthRole::Admin;
    }
    if method == Method::POST && matches!(path, "/scans" | "/scans/sync" | "/jobs/scans") {
        return AuthRole::Operator;
    }
    AuthRole::Reader
}

fn unauthorized_response(message: &str) -> Response {
    let body = ErrorBody {
        error: ErrorEnvelope {
            code: ErrorCode::Unauthorized,
            message: message.to_string(),
            details: serde_json::Value::Object(Default::default()),
        },
    };
    let mut r = (StatusCode::UNAUTHORIZED, Json(body)).into_response();
    r.headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer realm=\"paraclete\""));
    r
}

fn forbidden_response(message: &str) -> Response {
    let body = ErrorBody {
        error: ErrorEnvelope {
            code: ErrorCode::Forbidden,
            message: message.to_string(),
            details: serde_json::Value::Object(Default::default()),
        },
    };
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

fn internal_error_response() -> Response {
    let body = ErrorBody {
        error: ErrorEnvelope {
            code: ErrorCode::InternalError,
            message: "internal error".to_string(),
            details: serde_json::Value::Object(Default::default()),
        },
    };
    (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
}
