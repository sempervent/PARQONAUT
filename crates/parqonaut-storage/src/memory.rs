//! In-memory backend for contract tests and development.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::backend::{ByteRange, ListOptions, ListPage, StorageBackend};
use crate::capabilities::StorageCapabilities;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};
use crate::metadata::ObjectMetadata;
use crate::metrics::{StorageMetrics, StorageMetricsCollector};
use crate::stream::{ObjectReadStream, ObjectWriteStream};

#[derive(Default)]
struct MemoryState {
    objects: BTreeMap<String, StoredObject>,
}

#[derive(Clone)]
struct StoredObject {
    data: Bytes,
    etag: String,
    version_id: u64,
}

pub struct MemoryStorageBackend {
    state: Arc<RwLock<MemoryState>>,
    metrics: Arc<StorageMetricsCollector>,
    capabilities: StorageCapabilities,
}

impl Clone for MemoryStorageBackend {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            metrics: Arc::clone(&self.metrics),
            capabilities: self.capabilities,
        }
    }
}

impl MemoryStorageBackend {
    pub fn new(capabilities: StorageCapabilities) -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::default())),
            metrics: Arc::new(StorageMetricsCollector::default()),
            capabilities,
        }
    }

    fn key(object: &ObjectLocation) -> Result<String, StorageError> {
        match object {
            ObjectLocation::Local { path } => Ok(format!("local:{}", path.as_str())),
            ObjectLocation::S3 { bucket, key } => Ok(format!("s3:{bucket}/{key}")),
        }
    }
}

#[async_trait]
impl StorageBackend for MemoryStorageBackend {
    fn capabilities(&self) -> StorageCapabilities {
        self.capabilities
    }

    fn metrics(&self) -> StorageMetrics {
        self.metrics.snapshot()
    }

    async fn list(
        &self,
        dataset: &DatasetLocation,
        options: ListOptions,
        _: Option<&str>,
    ) -> Result<ListPage, StorageError> {
        self.metrics.record_list();
        let prefix = match dataset {
            DatasetLocation::Local(l) => format!("local:{}", l.path.as_str()),
            DatasetLocation::S3(s) => {
                if s.prefix.is_empty() {
                    format!("s3:{}/", s.bucket)
                } else {
                    format!("s3:{}/{}", s.bucket, s.prefix)
                }
            }
        };
        let state = self.state.read().await;
        let mut objects = Vec::new();
        for (k, v) in &state.objects {
            if !k.starts_with(&prefix) {
                continue;
            }
            if !options.recursive && k[prefix.len()..].contains('/') {
                continue;
            }
            objects.push(object_metadata_from_key(k, v));
        }
        objects.sort_by(|a, b| a.location.display_uri().cmp(&b.location.display_uri()));
        Ok(ListPage { objects, prefixes: vec![], truncated: false, continuation_token: None })
    }

    async fn head(&self, object: &ObjectLocation) -> Result<ObjectMetadata, StorageError> {
        self.metrics.record_head();
        let key = Self::key(object)?;
        let state = self.state.read().await;
        let stored = state
            .objects
            .get(&key)
            .ok_or_else(|| StorageError::NotFound { location: object.display_uri() })?;
        Ok(object_metadata_from_key(&key, stored))
    }

    async fn read_range(
        &self,
        object: &ObjectLocation,
        range: ByteRange,
    ) -> Result<Bytes, StorageError> {
        let _ = self.head(object).await?;
        let key = Self::key(object)?;
        let state = self.state.read().await;
        let stored = state.objects.get(&key).unwrap();
        let start = range.start as usize;
        let end = (range.end as usize).min(stored.data.len().saturating_sub(1));
        if start >= stored.data.len() {
            return Err(StorageError::InvalidLocation {
                message: "range start beyond object size".into(),
            });
        }
        let slice = stored.data.slice(start..=end);
        self.metrics.record_range_get(slice.len() as u64);
        Ok(slice)
    }

    async fn read_stream(
        &self,
        object: &ObjectLocation,
        range: Option<ByteRange>,
    ) -> Result<ObjectReadStream, StorageError> {
        let bytes = if let Some(r) = range {
            self.read_range(object, r).await?
        } else {
            let key = Self::key(object)?;
            let state = self.state.read().await;
            let stored = state
                .objects
                .get(&key)
                .ok_or_else(|| StorageError::NotFound { location: object.display_uri() })?;
            self.metrics.record_full_get(stored.data.len() as u64);
            stored.data.clone()
        };
        Ok(ObjectReadStream::from_bytes(bytes))
    }

    async fn write_stream(
        &self,
        _object: &ObjectLocation,
        _: Option<u64>,
    ) -> Result<ObjectWriteStream, StorageError> {
        if !self.capabilities.stream_writes {
            return Err(StorageError::UnsupportedCapability { capability: "stream_writes".into() });
        }
        Err(StorageError::UnsupportedCapability {
            capability: "memory write_stream use conditional_create in tests".into(),
        })
    }

    async fn conditional_create(
        &self,
        object: &ObjectLocation,
        condition: ConditionalCreate,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        if !self.capabilities.conditional_create {
            return Err(StorageError::UnsupportedCapability {
                capability: "conditional_create".into(),
            });
        }
        let key = Self::key(object)?;
        let mut state = self.state.write().await;
        if condition.if_absent && state.objects.contains_key(&key) {
            return Err(StorageError::Conflict {
                message: format!("object already exists: {key}"),
            });
        }
        let stored = StoredObject {
            data: data.clone(),
            etag: format!("mem-{}", data.len()),
            version_id: state.objects.len() as u64 + 1,
        };
        state.objects.insert(key.clone(), stored.clone());
        self.metrics.record_put(data.len() as u64);
        Ok(object_metadata_from_key(&key, &stored))
    }

    async fn conditional_replace(
        &self,
        object: &ObjectLocation,
        condition: ConditionalReplace,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        if !self.capabilities.conditional_replace {
            return Err(StorageError::UnsupportedCapability {
                capability: "conditional_replace".into(),
            });
        }
        let key = Self::key(object)?;
        let mut state = self.state.write().await;
        let existing = state
            .objects
            .get(&key)
            .ok_or_else(|| StorageError::NotFound { location: object.display_uri() })?;
        if condition.expected_etag.as_deref() != Some(existing.etag.as_str()) {
            return Err(StorageError::PreconditionFailed { message: "etag mismatch".into() });
        }
        let stored = StoredObject {
            data: data.clone(),
            etag: format!("mem-{}", data.len()),
            version_id: existing.version_id + 1,
        };
        state.objects.insert(key.clone(), stored.clone());
        self.metrics.record_put(data.len() as u64);
        Ok(object_metadata_from_key(&key, &stored))
    }

    async fn delete_owned_object(&self, object: &ObjectLocation) -> Result<(), StorageError> {
        let key = Self::key(object)?;
        self.state.write().await.objects.remove(&key);
        Ok(())
    }
}

fn object_metadata_from_key(key: &str, stored: &StoredObject) -> ObjectMetadata {
    let location = if let Some(rest) = key.strip_prefix("local:") {
        ObjectLocation::Local { path: camino::Utf8PathBuf::from(rest) }
    } else if let Some(rest) = key.strip_prefix("s3:") {
        let (bucket, object_key) = rest.split_once('/').unwrap_or((rest, ""));
        ObjectLocation::S3 { bucket: bucket.to_string(), key: object_key.to_string() }
    } else {
        ObjectLocation::Local { path: camino::Utf8PathBuf::from(key) }
    };
    ObjectMetadata {
        location,
        size: stored.data.len() as u64,
        etag: Some(stored.etag.clone()),
        version_id: Some(stored.version_id.to_string()),
        last_modified: None,
    }
}
