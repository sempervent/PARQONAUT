//! Storage abstraction for local filesystem and S3-compatible object stores.
//!
//! `parqonaut-repair` and `parqonaut-orchestrator` consume this crate; AWS SDK types
//! must not leak above the S3 backend module.

#![forbid(unsafe_code)]

pub mod backend;
pub mod capabilities;
pub mod conditional;
pub mod contract;
pub mod error;
pub mod fingerprint;
pub mod inventory;
pub mod local;
pub mod location;
pub mod locking;
pub mod memory;
pub mod metadata;
pub mod metrics;
pub mod noop;
pub mod parquet_range;
pub mod publication;

#[cfg(feature = "columnar")]
pub mod columnar;
pub mod redact;
pub mod stream;

#[cfg(feature = "s3")]
pub mod s3;

#[cfg(any(test, feature = "test-util"))]
pub mod chaos;

pub use backend::{ByteRange, ListOptions, ListPage, StorageBackend};
pub use capabilities::StorageCapabilities;
pub use conditional::{ConditionalCreate, ConditionalReplace};
pub use error::{RetryClass, StorageError};
pub use local::LocalStorageBackend;
pub use location::{DatasetLocation, LocalLocation, ObjectLocation, S3Location};
pub use memory::MemoryStorageBackend;
pub use metadata::ObjectMetadata;
pub use metrics::{StorageMetrics, StorageMetricsCollector};
pub use noop::NoopStorageBackend;
pub use redact::{RedactUri, Redacted};
pub use stream::{ObjectReadStream, ObjectWriteStream};

#[cfg(feature = "s3")]
pub use s3::{S3Config, S3StorageBackend};

#[cfg(feature = "columnar")]
pub use columnar::{
    StorageParquetBatchSink, StorageParquetBatchSource, StorageParquetReadOptions,
    StorageParquetWriteOptions,
};

#[cfg(any(test, feature = "test-util"))]
pub use chaos::FaultInjectingBackend;

pub const STORAGE_CONTRACT_VERSION: u32 = 1;
