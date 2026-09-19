//! Storage runtime, existence checks, and remote publication state for mixed fleets.

use std::future::Future;
use std::sync::Arc;

use parqonaut_repair::RepairBackend;
use parqonaut_storage::backend::{ListOptions, StorageBackend};
use parqonaut_storage::capabilities::StorageCapabilities;
use parqonaut_storage::location::DatasetLocation;
use parqonaut_storage::memory::MemoryStorageBackend;
use parqonaut_storage::publication::{
    is_version_committed, observed_state, read_current_version, PublicationError, PublicationState,
    PublicationVersionId,
};
use parqonaut_storage::LocalStorageBackend;
use tokio::sync::Semaphore;

use crate::error::OrchestratorError;

/// Bounded concurrent storage requests across a batch run (`--jobs` is separate).
pub struct BatchStorageRuntime {
    local: LocalStorageBackend,
    remote: MemoryStorageBackend,
    requests: Arc<Semaphore>,
}

impl BatchStorageRuntime {
    pub fn new(max_storage_requests: u32) -> Self {
        Self {
            local: LocalStorageBackend::direct(),
            remote: MemoryStorageBackend::new(StorageCapabilities {
                range_reads: true,
                stream_reads: true,
                conditional_create: true,
                conditional_replace: true,
                ..StorageCapabilities::S3
            }),
            requests: Arc::new(Semaphore::new(max_storage_requests.max(1) as usize)),
        }
    }

    pub fn with_remote_backend(max_storage_requests: u32, remote: MemoryStorageBackend) -> Self {
        Self {
            local: LocalStorageBackend::direct(),
            remote,
            requests: Arc::new(Semaphore::new(max_storage_requests.max(1) as usize)),
        }
    }

    pub fn repair_backend_for(&self, location: &DatasetLocation) -> RepairBackend {
        match location {
            DatasetLocation::Local(_) => RepairBackend::Local(LocalStorageBackend::direct()),
            DatasetLocation::S3(_) => RepairBackend::Memory(self.remote.clone()),
        }
    }

    pub fn backend_for(&self, location: &DatasetLocation) -> &dyn StorageBackend {
        match location {
            DatasetLocation::Local(_) => &self.local,
            DatasetLocation::S3(_) => &self.remote,
        }
    }

    pub async fn with_request_permit<T, F, Fut>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let _permit = self.requests.acquire().await.expect("storage request semaphore closed");
        f().await
    }
}

impl Default for BatchStorageRuntime {
    fn default() -> Self {
        Self::new(default_max_storage_requests())
    }
}

pub fn default_max_storage_requests() -> u32 {
    8
}

/// Run an async future from sync orchestrator entry points.
pub fn block_on_async<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // `block_in_place` is invalid on current-thread runtimes (common in `#[tokio::test]`).
        if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
            return tokio::task::block_in_place(|| handle.block_on(future));
        }
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

pub async fn dataset_exists(
    runtime: &BatchStorageRuntime,
    location: &DatasetLocation,
) -> Result<bool, OrchestratorError> {
    runtime
        .with_request_permit(|| async {
            match location {
                DatasetLocation::Local(local) => Ok(local.path.as_std_path().exists()),
                DatasetLocation::S3(_) => {
                    let backend = runtime.backend_for(location);
                    let page = backend
                        .list(location, ListOptions { recursive: true, max_keys: Some(1) }, None)
                        .await
                        .map_err(storage_err)?;
                    Ok(!page.objects.is_empty() || !page.prefixes.is_empty())
                }
            }
        })
        .await
}

pub async fn dataset_is_directory_like(
    runtime: &BatchStorageRuntime,
    location: &DatasetLocation,
) -> Result<bool, OrchestratorError> {
    match location {
        DatasetLocation::Local(local) => Ok(local.path.as_std_path().is_dir()),
        DatasetLocation::S3(_) => {
            let _ = runtime.backend_for(location);
            Ok(true)
        }
    }
}

/// Remote output already committed for `run_id` (resume/idempotency).
pub async fn remote_output_committed(
    runtime: &BatchStorageRuntime,
    output: &DatasetLocation,
    run_id: &str,
) -> Result<bool, OrchestratorError> {
    let DatasetLocation::S3(_) = output else {
        return Ok(false);
    };
    let version = PublicationVersionId::from_run_id(run_id);
    runtime
        .with_request_permit(|| async {
            is_version_committed(runtime.backend_for(output), output, &version)
                .await
                .map_err(publication_err)
        })
        .await
}

/// Observed remote publication state for recovery display and resume decisions.
pub async fn remote_publication_state(
    runtime: &BatchStorageRuntime,
    output: &DatasetLocation,
    run_id: &str,
) -> Result<Option<PublicationState>, OrchestratorError> {
    let DatasetLocation::S3(_) = output else {
        return Ok(None);
    };
    let version = PublicationVersionId::from_run_id(run_id);
    runtime
        .with_request_permit(|| async {
            observed_state(runtime.backend_for(output), output, &version)
                .await
                .map_err(publication_err)
        })
        .await
}

pub async fn remote_current_version(
    runtime: &BatchStorageRuntime,
    output: &DatasetLocation,
) -> Result<Option<String>, OrchestratorError> {
    let DatasetLocation::S3(_) = output else {
        return Ok(None);
    };
    runtime
        .with_request_permit(|| async {
            read_current_version(runtime.backend_for(output), output).await.map_err(publication_err)
        })
        .await
}

fn storage_err(err: parqonaut_storage::StorageError) -> OrchestratorError {
    OrchestratorError::Storage(err.to_string())
}

fn publication_err(err: PublicationError) -> OrchestratorError {
    OrchestratorError::Storage(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parqonaut_storage::backend::StorageBackend;
    use parqonaut_storage::conditional::ConditionalCreate;
    use parqonaut_storage::location::ObjectLocation;

    async fn seed_prefix(backend: &MemoryStorageBackend, uri: &str) {
        let dataset = DatasetLocation::parse(uri).unwrap();
        let object = ObjectLocation::S3 {
            bucket: match &dataset {
                DatasetLocation::S3(s) => s.bucket.clone(),
                _ => panic!("expected s3"),
            },
            key: match &dataset {
                DatasetLocation::S3(s) if s.prefix.is_empty() => "marker".into(),
                DatasetLocation::S3(s) => format!("{}/marker", s.prefix.trim_end_matches('/')),
                _ => panic!("expected s3"),
            },
        };
        backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn s3_existence_uses_list_not_fs() {
        let backend = MemoryStorageBackend::new(StorageCapabilities {
            range_reads: true,
            stream_reads: true,
            conditional_create: true,
            conditional_replace: true,
            ..StorageCapabilities::S3
        });
        seed_prefix(&backend, "s3://fleet-bucket/datasets/a/").await;
        let runtime = BatchStorageRuntime::with_remote_backend(4, backend);
        let loc = DatasetLocation::parse("s3://fleet-bucket/datasets/a/").unwrap();
        assert!(dataset_exists(&runtime, &loc).await.unwrap());
        let missing = DatasetLocation::parse("s3://fleet-bucket/datasets/missing/").unwrap();
        assert!(!dataset_exists(&runtime, &missing).await.unwrap());
    }

    #[tokio::test]
    async fn request_permit_bounds_concurrency() {
        let runtime = Arc::new(BatchStorageRuntime::new(1));
        let r1 = Arc::clone(&runtime);
        let r2 = Arc::clone(&runtime);
        let first = tokio::spawn(async move {
            r1.with_request_permit(|| async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                1usize
            })
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let second = tokio::spawn(async move { r2.with_request_permit(|| async { 2usize }).await });
        let (a, b) = tokio::join!(first, second);
        assert_eq!(a.unwrap(), 1);
        assert_eq!(b.unwrap(), 2);
    }
}
