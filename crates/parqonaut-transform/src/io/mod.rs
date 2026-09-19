mod local;
mod resolve;
mod s3;

pub use local::*;
pub use resolve::*;
pub use s3::*;

use crate::error::{ParqknifeError, Result};
use async_trait::async_trait;
use std::path::PathBuf;

#[async_trait]
pub trait InputSource: Send + Sync {
    async fn read_parquet(&self, path: &str) -> Result<Box<dyn std::io::Read + Send>>;
    fn is_s3(&self) -> bool;
}

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn write_parquet(&self, path: &str) -> Result<Box<dyn std::io::Write + Send>>;
    fn is_s3(&self) -> bool;
}

pub fn resolve_inputs(pattern: &str) -> Result<Vec<String>> {
    if pattern.starts_with("s3://") {
        Err(ParqknifeError::InvalidInput(
            "s3:// inputs are not supported in parqonaut-transform; use parqonaut scan/repair"
                .into(),
        ))
    } else {
        let paths: Result<Vec<_>> = glob::glob(pattern)
            .map_err(|e| ParqknifeError::InvalidInput(format!("Invalid glob pattern: {}", e)))?
            .map(|entry| {
                entry
                    .map(|p| p.to_string_lossy().to_string())
                    .map_err(|e| ParqknifeError::InvalidInput(e.to_string()))
            })
            .collect();
        let mut paths = paths?;
        paths.sort(); // Deterministic ordering
        Ok(paths)
    }
}
