//! Remote dataset identity from object metadata (no content reads).
//!
//! The fingerprint binds a plan to a remote snapshot using object keys, sizes, opaque
//! ETags, and optional version IDs. ETags are never interpreted as content hashes.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::StorageError;
use crate::inventory::RemoteInventory;
use crate::metadata::ObjectMetadata;

pub const REMOTE_FINGERPRINT_VERSION: u32 = 1;

/// One inventory object identity included in a [`RemoteFingerprint`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFingerprintEntry {
    pub relative_key: String,
    pub size: u64,
    /// Opaque backend ETag; empty string when absent.
    pub etag: String,
    pub version_id: Option<String>,
}

/// Stable digest of remote object identities under a dataset prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFingerprint {
    pub version: u32,
    pub dataset_uri: String,
    pub digest: String,
    pub entries: Vec<RemoteFingerprintEntry>,
}

impl RemoteFingerprint {
    pub fn verify_against(&self, current: &RemoteFingerprint) -> Result<(), StorageError> {
        if self.digest != current.digest {
            return Err(StorageError::Conflict {
                message: format!(
                    "remote fingerprint mismatch: expected {}, got {}",
                    self.digest, current.digest
                ),
            });
        }
        Ok(())
    }
}

/// Build a fingerprint from a sorted [`RemoteInventory`].
pub fn compute_remote_fingerprint(inventory: &RemoteInventory) -> RemoteFingerprint {
    let mut entries: Vec<RemoteFingerprintEntry> = inventory
        .objects
        .iter()
        .filter_map(|object| entry_from_object(inventory, object))
        .collect();
    entries.sort_by(|a, b| a.relative_key.cmp(&b.relative_key));
    let digest = digest_entries(&entries);
    RemoteFingerprint {
        version: REMOTE_FINGERPRINT_VERSION,
        dataset_uri: inventory.dataset.display_uri(),
        entries,
        digest,
    }
}

fn entry_from_object(
    inventory: &RemoteInventory,
    object: &ObjectMetadata,
) -> Option<RemoteFingerprintEntry> {
    let relative_key = inventory.relative_key(object)?;
    Some(RemoteFingerprintEntry {
        relative_key,
        size: object.size,
        etag: object.etag.clone().unwrap_or_default(),
        version_id: object.version_id.clone(),
    })
}

fn digest_entries(entries: &[RemoteFingerprintEntry]) -> String {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.relative_key.cmp(&b.relative_key));
    let payload = serde_json::to_string(&sorted).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(b"parqonaut-remote-fp-v1\0");
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::backend::StorageBackend;
    use crate::capabilities::StorageCapabilities;
    use crate::conditional::ConditionalCreate;
    use crate::inventory::list_remote_inventory;
    use crate::location::{DatasetLocation, ObjectLocation};
    use crate::memory::MemoryStorageBackend;

    fn memory_backend() -> MemoryStorageBackend {
        MemoryStorageBackend::new(StorageCapabilities {
            conditional_create: true,
            version_ids: true,
            ..StorageCapabilities::LOCAL
        })
    }

    async fn put_s3(
        backend: &MemoryStorageBackend,
        bucket: &str,
        key: &str,
        data: &[u8],
    ) -> ObjectMetadata {
        backend
            .conditional_create(
                &ObjectLocation::S3 { bucket: bucket.into(), key: key.into() },
                ConditionalCreate::must_not_exist(),
                Bytes::copy_from_slice(data),
            )
            .await
            .expect("conditional create")
    }

    #[test]
    fn fingerprint_order_independent() {
        let e1 = RemoteFingerprintEntry {
            relative_key: "a.parquet".into(),
            size: 100,
            etag: "opaque-a".into(),
            version_id: Some("1".into()),
        };
        let e2 = RemoteFingerprintEntry {
            relative_key: "b.parquet".into(),
            size: 200,
            etag: "opaque-b".into(),
            version_id: None,
        };
        let d1 = digest_entries(&[e1.clone(), e2.clone()]);
        let d2 = digest_entries(&[e2, e1]);
        assert_eq!(d1, d2);
    }

    #[test]
    fn fingerprint_changes_when_identity_changes() {
        let base = RemoteFingerprintEntry {
            relative_key: "a.parquet".into(),
            size: 100,
            etag: "opaque".into(),
            version_id: Some("v1".into()),
        };
        let d0 = digest_entries(&[base.clone()]);

        let mut size_changed = base.clone();
        size_changed.size = 101;
        assert_ne!(d0, digest_entries(&[size_changed]));

        let mut etag_changed = base.clone();
        etag_changed.etag = "other".into();
        assert_ne!(d0, digest_entries(&[etag_changed]));

        let mut version_changed = base.clone();
        version_changed.version_id = Some("v2".into());
        assert_ne!(d0, digest_entries(&[version_changed]));
    }

    #[tokio::test]
    async fn compute_fingerprint_from_inventory() {
        let backend = Arc::new(memory_backend());
        let dataset = DatasetLocation::parse("s3://fp-bucket/data/").unwrap();

        put_s3(&backend, "fp-bucket", "data/a.parquet", b"aaa").await;
        put_s3(&backend, "fp-bucket", "data/b.parquet", b"bbbb").await;

        let inventory = list_remote_inventory(backend.as_ref(), &dataset).await.unwrap();
        let fp = compute_remote_fingerprint(&inventory);
        assert_eq!(fp.version, REMOTE_FINGERPRINT_VERSION);
        assert_eq!(fp.entries.len(), 2);
        assert_eq!(fp.entries[0].relative_key, "a.parquet");
        assert_eq!(fp.entries[1].relative_key, "b.parquet");
        assert!(!fp.digest.is_empty());

        let fp2 = compute_remote_fingerprint(&inventory);
        assert_eq!(fp.digest, fp2.digest);
    }

    #[tokio::test]
    async fn verify_detects_drift() {
        let backend = Arc::new(memory_backend());
        let dataset = DatasetLocation::parse("s3://fp-verify/data/").unwrap();
        put_s3(&backend, "fp-verify", "data/a.parquet", b"aaa").await;

        let inventory = list_remote_inventory(backend.as_ref(), &dataset).await.unwrap();
        let expected = compute_remote_fingerprint(&inventory);

        put_s3(&backend, "fp-verify", "data/b.parquet", b"b").await;
        let current_inventory = list_remote_inventory(backend.as_ref(), &dataset).await.unwrap();
        let current = compute_remote_fingerprint(&current_inventory);

        assert!(expected.verify_against(&current).is_err());
        assert!(expected.verify_against(&expected).is_ok());
    }
}
