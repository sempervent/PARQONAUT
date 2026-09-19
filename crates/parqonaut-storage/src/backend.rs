use async_trait::async_trait;
use bytes::Bytes;

use crate::capabilities::StorageCapabilities;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};
use crate::metadata::ObjectMetadata;
use crate::metrics::StorageMetrics;
use crate::stream::{ObjectReadStream, ObjectWriteStream};

/// Inclusive byte range `[start, end]` for ranged reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn new(start: u64, end: u64) -> Result<Self, StorageError> {
        if end < start {
            return Err(StorageError::InvalidLocation {
                message: format!("invalid byte range: {start}..={end}"),
            });
        }
        Ok(Self { start, end })
    }

    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start) + 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Prefix listing options with deterministic ordering guarantees.
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub recursive: bool,
    pub max_keys: Option<usize>,
}

/// Result page from listing. Callers merge pages; storage layer sorts lexically.
#[derive(Debug, Clone)]
pub struct ListPage {
    pub objects: Vec<ObjectMetadata>,
    pub prefixes: Vec<String>,
    pub truncated: bool,
    pub continuation_token: Option<String>,
}

#[async_trait]
pub trait StorageBackend: Send + Sync {
    fn capabilities(&self) -> StorageCapabilities;

    fn metrics(&self) -> StorageMetrics;

    async fn list(
        &self,
        dataset: &DatasetLocation,
        options: ListOptions,
        continuation_token: Option<&str>,
    ) -> Result<ListPage, StorageError>;

    async fn head(&self, object: &ObjectLocation) -> Result<ObjectMetadata, StorageError>;

    /// Read a bounded byte range. Returns bytes directly (suitable for Parquet footer reads).
    async fn read_range(
        &self,
        object: &ObjectLocation,
        range: ByteRange,
    ) -> Result<Bytes, StorageError>;

    /// Stream an object or range without loading entirely into memory.
    async fn read_stream(
        &self,
        object: &ObjectLocation,
        range: Option<ByteRange>,
    ) -> Result<ObjectReadStream, StorageError>;

    async fn write_stream(
        &self,
        object: &ObjectLocation,
        content_length: Option<u64>,
    ) -> Result<ObjectWriteStream, StorageError>;

    async fn conditional_create(
        &self,
        object: &ObjectLocation,
        condition: ConditionalCreate,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError>;

    async fn conditional_replace(
        &self,
        object: &ObjectLocation,
        condition: ConditionalReplace,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError>;

    /// Delete an object owned by PARQONAUT (staging/temp/version cleanup only).
    async fn delete_owned_object(&self, object: &ObjectLocation) -> Result<(), StorageError>;
}
