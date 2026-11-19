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
    let root = std::path::Path::new(pattern);
    if root.is_dir() {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let entries = std::fs::read_dir(&dir).map_err(|e| {
                TransformError::InvalidInput(format!(
                    "unable to read directory {}: {e}",
                    dir.display()
                ))
            })?;
            for entry in entries {
                let entry = entry.map_err(|e| TransformError::InvalidInput(e.to_string()))?;
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    out.push(p.to_string_lossy().to_string());
                }
            }
        }
        out.sort();
        if out.is_empty() {
            return Err(TransformError::InvalidInput(format!(
                "no .parquet files found in directory {}",
                root.display()
            )));
        }
        return Ok(out);
    }
    let paths: Result<Vec<_>> = glob::glob(pattern)
        .map_err(|e| TransformError::InvalidInput(format!("Invalid glob pattern: {}", e)))?
        .filter_map(|entry| match entry {
            Ok(p) if p.is_dir() => None,
            Ok(p) => Some(Ok(p.to_string_lossy().to_string())),
            Err(e) => Some(Err(TransformError::InvalidInput(e.to_string()))),
        })
        .collect();
    let mut paths = paths?;
    paths.sort();
    Ok(paths)
}
