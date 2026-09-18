use crate::error::{ParqknifeError, Result};
use url::Url;

pub fn parse_url(path: &str) -> Result<Url> {
    if path.starts_with("s3://") {
        Url::parse(path).map_err(|e| ParqknifeError::InvalidInput(format!("Invalid S3 URL: {}", e)))
    } else {
        // Treat as file path
        Url::from_file_path(path)
            .map_err(|_| ParqknifeError::InvalidInput(format!("Invalid file path: {}", path)))
    }
}

pub fn is_s3_url(path: &str) -> bool {
    path.starts_with("s3://")
}

pub fn normalize_path(path: &str) -> String {
    // Normalize for deterministic ordering
    if is_s3_url(path) {
        path.to_string()
    } else {
        std::fs::canonicalize(path)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| path.to_string())
    }
}
