//! Exclusive writer locks for remote dataset publication.
//!
//! Lock objects live at `.parqonaut/writer.lock` under the dataset prefix and record the
//! owning run id. Only one active writer may hold the lock; the same run may reclaim a
//! stale lock after crash recovery.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::backend::StorageBackend;
use crate::conditional::ConditionalCreate;
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};

use crate::publication::{
    object_at, read_json_object, PublicationError, PublicationVersionId, WRITER_LOCK_KEY,
};

/// JSON payload stored in the writer lock object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLockRecord {
    pub run_id: String,
    pub version_id: String,
}

/// Exclusive remote writer lock for a dataset publication run.
#[derive(Debug)]
pub struct RemotePublicationLock {
    pub(crate) lock_object: ObjectLocation,
    pub(crate) record: WriterLockRecord,
    released: bool,
}

impl RemotePublicationLock {
    /// Acquire the dataset writer lock for `run_id`.
    ///
    /// Fails with [`PublicationError::LockHeld`] when another run owns the lock. The same
    /// `run_id` may reclaim a stale lock (mirrors local `{output}.parqonaut.lock` recovery).
    pub async fn acquire<B: StorageBackend>(
        backend: &B,
        dataset: &DatasetLocation,
        run_id: &str,
    ) -> Result<Self, PublicationError> {
        if !backend.capabilities().conditional_create {
            return Err(PublicationError::Unsupported {
                message: "backend lacks conditional_create for writer locks".into(),
            });
        }

        let version_id = PublicationVersionId::from_run_id(run_id);
        let record = WriterLockRecord {
            run_id: run_id.to_string(),
            version_id: version_id.as_str().to_string(),
        };
        let lock_object = object_at(dataset, WRITER_LOCK_KEY)?;
        let payload = serde_json::to_vec(&record)
            .map_err(|e| StorageError::Other { message: e.to_string() })?;

        match backend
            .conditional_create(
                &lock_object,
                ConditionalCreate::must_not_exist(),
                Bytes::from(payload.clone()),
            )
            .await
        {
            Ok(_) => Ok(Self { lock_object, record, released: false }),
            Err(StorageError::Conflict { .. }) => {
                Self::reclaim_or_reject(backend, &lock_object, &record).await
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn reclaim_or_reject<B: StorageBackend>(
        backend: &B,
        lock_object: &ObjectLocation,
        record: &WriterLockRecord,
    ) -> Result<Self, PublicationError> {
        let existing = read_json_object::<WriterLockRecord, B>(backend, lock_object).await?;
        if existing.run_id != record.run_id {
            return Err(PublicationError::LockHeld { run_id: existing.run_id });
        }

        backend.delete_owned_object(lock_object).await?;
        let payload = serde_json::to_vec(record)
            .map_err(|e| StorageError::Other { message: e.to_string() })?;
        backend
            .conditional_create(
                lock_object,
                ConditionalCreate::must_not_exist(),
                Bytes::from(payload),
            )
            .await?;
        Ok(Self { lock_object: lock_object.clone(), record: record.clone(), released: false })
    }

    pub fn run_id(&self) -> &str {
        &self.record.run_id
    }

    pub fn version_id(&self) -> PublicationVersionId {
        PublicationVersionId::from_run_id(&self.record.run_id)
    }

    pub fn lock_object(&self) -> &ObjectLocation {
        &self.lock_object
    }

    /// Release the writer lock. Idempotent after the first successful release.
    pub async fn release<B: StorageBackend>(mut self, backend: &B) -> Result<(), PublicationError> {
        if self.released {
            return Ok(());
        }
        backend.delete_owned_object(&self.lock_object).await?;
        self.released = true;
        Ok(())
    }
}

// Lock release is explicit (`release`) because `StorageBackend` is async and cannot run in `Drop`.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::StorageCapabilities;
    use crate::memory::MemoryStorageBackend;

    fn memory_backend() -> MemoryStorageBackend {
        MemoryStorageBackend::new(StorageCapabilities {
            range_reads: true,
            stream_reads: true,
            conditional_create: true,
            conditional_replace: true,
            ..StorageCapabilities::S3
        })
    }

    #[tokio::test]
    async fn lock_is_exclusive_per_dataset() {
        let backend = memory_backend();
        let dataset = DatasetLocation::parse("s3://bucket/dataset/").unwrap();
        let first = RemotePublicationLock::acquire(&backend, &dataset, "run-a").await.unwrap();
        let err = match RemotePublicationLock::acquire(&backend, &dataset, "run-b").await {
            Err(err) => err,
            Ok(_) => panic!("expected lock conflict"),
        };
        assert!(matches!(err, PublicationError::LockHeld { .. }));
        first.release(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn same_run_can_reclaim_stale_lock() {
        let backend = memory_backend();
        let dataset = DatasetLocation::parse("s3://bucket/dataset/").unwrap();
        let lock = RemotePublicationLock::acquire(&backend, &dataset, "run-a").await.unwrap();
        let lock_object = lock.lock_object().clone();
        lock.release(&backend).await.unwrap();

        backend
            .conditional_create(
                &lock_object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"{\"run_id\":\"run-a\",\"version_id\":\"run-a\"}"),
            )
            .await
            .unwrap();

        let reclaimed = RemotePublicationLock::acquire(&backend, &dataset, "run-a").await.unwrap();
        reclaimed.release(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn lock_release_allows_new_run() {
        let backend = memory_backend();
        let dataset = DatasetLocation::parse("s3://bucket/dataset/").unwrap();
        let lock = RemotePublicationLock::acquire(&backend, &dataset, "run-a").await.unwrap();
        lock.release(&backend).await.unwrap();
        let _next = RemotePublicationLock::acquire(&backend, &dataset, "run-b").await.unwrap();
    }
}
