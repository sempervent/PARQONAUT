//! Browser dashboard static asset, CSP, XSS-hygiene, and auth-boundary tests.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use parqonaut_service::{build_router, ParqonautService};
use parqonaut_store::SqliteScanStore;
use tower::ServiceExt;

mod support;

#[tokio::test]
async fn ui_assets_serve_with_strict_csp_and_no_operational_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&support::sqlite_url(&dir)).await.unwrap();
    let app = build_router(ParqonautService::new(store));

    let redir =
        app.clone().oneshot(Request::get("/ui").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(redir.status(), StatusCode::TEMPORARY_REDIRECT);

    for (path, expected_ct) in [
        ("/ui/", "text/html"),
        ("/ui/styles.css", "text/css"),
        ("/ui/app.js", "application/javascript"),
        ("/ui/api.js", "application/javascript"),
        ("/ui/sse.js", "application/javascript"),
    ] {
        let res =
            app.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "failed on {path}");
        let ct = res.headers().get("content-type").unwrap().to_str().unwrap().to_string();
        assert!(ct.contains(expected_ct), "{path} content-type={ct}");
        let csp =
            res.headers().get("content-security-policy").unwrap().to_str().unwrap().to_string();
        assert!(csp.contains("script-src 'self'"), "missing strict CSP on {path}: {csp}");
        assert!(!csp.contains("unsafe-inline"), "unsafe-inline forbidden in CSP: {csp}");
        assert_eq!(res.headers().get("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(res.headers().get("referrer-policy").unwrap(), "no-referrer");

        let body = String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec())
            .unwrap();
        assert!(!body.contains("localStorage"), "{path} must never use localStorage");
        assert!(!body.contains("sessionStorage"), "{path} must never use sessionStorage");
        assert!(!body.contains("innerHTML"), "{path} must never use innerHTML");
        assert!(!body.contains("https://cdn"), "{path} must never load external CDNs");
    }

    // Operational APIs remain strictly authenticated even when /ui/ is served.
    for protected in ["/api/v1/overview", "/api/v1/jobs", "/api/v1/runs", "/api/v1/plugins"] {
        let res = app
            .clone()
            .oneshot(Request::get(protected).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{protected} leaked without auth");
    }
}
