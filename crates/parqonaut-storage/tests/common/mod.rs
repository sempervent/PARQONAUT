//! Shared S3 integration test endpoint resolution (RustFS / any S3-compatible backend).

pub fn require_s3_endpoint() -> Option<String> {
    let endpoint = std::env::var("PARQONAUT_S3_ENDPOINT")
        .ok()
        .or_else(|| std::env::var("MINIO_ENDPOINT").ok());
    match endpoint {
        Some(e) => Some(e),
        None if std::env::var("PARQONAUT_S3_INTEGRATION").as_deref() == Ok("1") => {
            panic!("PARQONAUT_S3_INTEGRATION=1 requires PARQONAUT_S3_ENDPOINT");
        }
        None => None,
    }
}

#[allow(dead_code)]
pub fn fogbank_bucket() -> String {
    std::env::var("FOGBANK_BUCKET").unwrap_or_else(|_| "fogbank".into())
}
