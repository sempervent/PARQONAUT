//! Bundled `/ui/` web dashboard static assets with strict CSP and no-store headers.

use axum::http::header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, REFERRER_POLICY};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};

const INDEX_HTML: &str = include_str!("ui/index.html");
const STYLES_CSS: &str = include_str!("ui/styles.css");
const APP_JS: &str = include_str!("ui/app.js");
const API_JS: &str = include_str!("ui/api.js");
const SSE_JS: &str = include_str!("ui/sse.js");

const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'";

fn asset_response(content_type: &'static str, body: &'static str) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    let headers = resp.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("x-content-type-options", HeaderValue::from_static("nosniff"));
    resp
}

pub async fn redirect_to_ui() -> Redirect {
    Redirect::temporary("/ui/")
}

pub async fn index_html() -> Response {
    asset_response("text/html; charset=utf-8", INDEX_HTML)
}

pub async fn styles_css() -> Response {
    asset_response("text/css; charset=utf-8", STYLES_CSS)
}

pub async fn app_js() -> Response {
    asset_response("application/javascript; charset=utf-8", APP_JS)
}

pub async fn api_js() -> Response {
    asset_response("application/javascript; charset=utf-8", API_JS)
}

pub async fn sse_js() -> Response {
    asset_response("application/javascript; charset=utf-8", SSE_JS)
}
