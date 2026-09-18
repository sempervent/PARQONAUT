//! Request ID and HTTP request metrics (low-cardinality route labels).

use std::time::Instant;

use axum::extract::Request;
use axum::http::header::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;
use uuid::Uuid;

/// Extension and response header for correlation.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

fn normalize_metric_path(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            if seg.len() == 36 && seg.contains('-') && Uuid::parse_str(seg).is_ok() {
                "{id}".to_string()
            } else {
                seg.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    let id = Uuid::new_v4().to_string();
    let span = tracing::info_span!("request", request_id = %id);
    req.extensions_mut().insert(RequestId(id.clone()));

    let mut response = async move { next.run(req).await }.instrument(span).await;
    if let Ok(val) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(HeaderName::from_static("x-request-id"), val);
    }
    response
}

pub async fn http_metrics_middleware(req: Request, next: Next) -> Response {
    metrics::gauge!("paraclete_http_requests_in_flight").increment(1.0);
    let start = Instant::now();
    let method = req.method().as_str().to_string();
    let route = normalize_metric_path(req.uri().path());
    let response = next.run(req).await;
    metrics::gauge!("paraclete_http_requests_in_flight").decrement(1.0);
    let status = response.status().as_u16().to_string();
    let elapsed = start.elapsed().as_secs_f64();

    metrics::histogram!(
        "paraclete_http_request_duration_seconds",
        "method" => method.clone(),
        "route" => route.clone(),
    )
    .record(elapsed);
    metrics::counter!(
        "paraclete_http_requests_total",
        "method" => method,
        "route" => route,
        "status" => status,
    )
    .increment(1);
    response
}
