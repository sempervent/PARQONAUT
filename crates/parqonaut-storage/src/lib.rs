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
pub mod location;
pub mod memory;
pub mod metadata;
pub mod metrics;
pub mod noop;
pub mod redact;
pub mod stream;

pub use backend::{ByteRange, ListOptions, ListPage, StorageBackend};
pub use capabilities::StorageCapabilities;
pub use conditional::{ConditionalCreate, ConditionalReplace};
pub use error::{RetryClass, StorageError};
pub use location::{DatasetLocation, LocalLocation, ObjectLocation, S3Location};
pub use memory::MemoryStorageBackend;
pub use metadata::ObjectMetadata;
pub use metrics::{StorageMetrics, StorageMetricsCollector};
pub use noop::NoopStorageBackend;
pub use redact::{RedactUri, Redacted};
pub use stream::{ObjectReadStream, ObjectWriteStream};

pub const STORAGE_CONTRACT_VERSION: u32 = 1;
