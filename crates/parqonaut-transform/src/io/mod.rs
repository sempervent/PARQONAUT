mod local;
mod resolve;

pub use local::*;
pub use resolve::*;

use crate::error::{Result, TransformError};
use async_trait::async_trait;
use parqonaut_storage::location::ObjectLocation;

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
        ObjectLocation::parse(pattern).map_err(|e| TransformError::InvalidInput(e.to_string()))?;
        return Ok(vec![pattern.to_string()]);
    }
    let paths: Result<Vec<_>> = glob::glob(pattern)
        .map_err(|e| TransformError::InvalidInput(format!("Invalid glob pattern: {}", e)))?
        .map(|entry| {
            entry
                .map(|p| p.to_string_lossy().to_string())
                .map_err(|e| TransformError::InvalidInput(e.to_string()))
        })
        .collect();
    let mut paths = paths?;
    paths.sort();
    Ok(paths)
}
