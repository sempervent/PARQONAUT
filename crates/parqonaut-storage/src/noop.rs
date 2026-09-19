use async_trait::async_trait;
use bytes::Bytes;

use crate::backend::{ByteRange, ListOptions, ListPage, StorageBackend};
use crate::capabilities::StorageCapabilities;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};
use crate::metadata::ObjectMetadata;
use crate::metrics::{StorageMetrics, StorageMetricsCollector};
use crate::stream::{ObjectReadStream, ObjectWriteStream};

/// Placeholder backend used before real implementations land. All operations unsupported.
#[derive(Default)]
pub struct NoopStorageBackend {
    metrics: StorageMetricsCollector,
}

impl NoopStorageBackend {
    fn unsupported(op: &str) -> StorageError {
        StorageError::UnsupportedCapability { capability: op.into() }
    }
}

#[async_trait]
impl StorageBackend for NoopStorageBackend {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::NONE
    }

    fn metrics(&self) -> StorageMetrics {
        self.metrics.snapshot()
    }

    async fn list(
        &self,
        _: &DatasetLocation,
        _: ListOptions,
        _: Option<&str>,
    ) -> Result<ListPage, StorageError> {
        Err(Self::unsupported("list"))
    }

    async fn head(&self, _: &ObjectLocation) -> Result<ObjectMetadata, StorageError> {
        Err(Self::unsupported("head"))
    }

    async fn read_range(&self, _: &ObjectLocation, _: ByteRange) -> Result<Bytes, StorageError> {
        Err(Self::unsupported("read_range"))
    }

    async fn read_stream(
        &self,
        _: &ObjectLocation,
        _: Option<ByteRange>,
    ) -> Result<ObjectReadStream, StorageError> {
        Err(Self::unsupported("read_stream"))
    }

    async fn write_stream(
        &self,
        _: &ObjectLocation,
        _: Option<u64>,
    ) -> Result<ObjectWriteStream, StorageError> {
        Err(Self::unsupported("write_stream"))
    }

    async fn conditional_create(
        &self,
        _: &ObjectLocation,
        _: ConditionalCreate,
        _: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        Err(Self::unsupported("conditional_create"))
    }

    async fn conditional_replace(
        &self,
        _: &ObjectLocation,
        _: ConditionalReplace,
        _: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        Err(Self::unsupported("conditional_replace"))
    }

    async fn delete_owned_object(&self, _: &ObjectLocation) -> Result<(), StorageError> {
        Err(Self::unsupported("delete_owned_object"))
    }
}
