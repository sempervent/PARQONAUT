//! Legacy S3 I/O stub — quarantined in Phase 5.
//!
//! Object storage is implemented in `parqonaut-storage`. Transform engines operate on
//! local seekable files; remote datasets are staged via the repair/orchestrator path.

use crate::error::{ParqknifeError, Result};
use async_trait::async_trait;

const REMOVED: &str =
    "S3 I/O was removed from parqonaut-transform; use parqonaut-storage via parqonaut repair/scan";

pub struct S3InputSource;

impl S3InputSource {
    pub async fn new() -> Result<Self> {
        Err(ParqknifeError::InvalidInput(REMOVED.into()))
    }
}

#[async_trait]
impl crate::io::InputSource for S3InputSource {
    async fn read_parquet(&self, _path: &str) -> Result<Box<dyn std::io::Read + Send>> {
        Err(ParqknifeError::InvalidInput(REMOVED.into()))
    }

    fn is_s3(&self) -> bool {
        true
    }
}

pub struct S3OutputSink;

impl S3OutputSink {
    pub async fn new() -> Result<Self> {
        Err(ParqknifeError::InvalidInput(REMOVED.into()))
    }
}

#[async_trait]
impl crate::io::OutputSink for S3OutputSink {
    async fn write_parquet(&self, _path: &str) -> Result<Box<dyn std::io::Write + Send>> {
        Err(ParqknifeError::InvalidInput(REMOVED.into()))
    }

    fn is_s3(&self) -> bool {
        true
    }
}
