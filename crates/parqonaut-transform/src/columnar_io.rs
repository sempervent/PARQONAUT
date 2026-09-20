//! Dual-backend columnar I/O: independent source and sink storage resolution.

use std::sync::Arc;

use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::LocalStorageBackend;

use crate::error::{ParqknifeError, Result};

#[cfg(feature = "s3")]
use parqonaut_storage::{S3Config, S3StorageBackend};

/// Production columnar routing context (local filesystem + S3 when enabled).
#[derive(Clone)]
pub struct ColumnarPipelineIo {
    local: Arc<LocalStorageBackend>,
    #[cfg(feature = "s3")]
    s3: Arc<S3StorageBackend>,
}

impl ColumnarPipelineIo {
    #[cfg(not(feature = "s3"))]
    pub fn local_only() -> Self {
        Self { local: Arc::new(LocalStorageBackend::direct()) }
    }

    #[cfg(feature = "s3")]
    pub async fn from_env_async() -> Result<Self> {
        Ok(Self {
            local: Arc::new(LocalStorageBackend::direct()),
            s3: Arc::new(S3StorageBackend::new(S3Config::from_env()).await),
        })
    }

    /// Local-path tests when `s3` is enabled (S3 client is never contacted unless a test uses `s3://`).
    #[cfg(feature = "s3")]
    pub fn for_test() -> Self {
        crate::remote::run_io_runtime(|handle| {
            handle.block_on(async {
                let endpoint = std::env::var("PARQONAUT_S3_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9".into());
                Self {
                    local: Arc::new(LocalStorageBackend::direct()),
                    s3: Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await),
                }
            })
        })
    }

    #[cfg(feature = "s3")]
    pub fn for_test_s3(s3: Arc<S3StorageBackend>) -> Self {
        Self { local: Arc::new(LocalStorageBackend::direct()), s3 }
    }

    #[cfg(not(feature = "s3"))]
    pub fn for_test() -> Self {
        Self { local: Arc::new(LocalStorageBackend::direct()) }
    }

    pub fn backend_for(&self, loc: &ObjectLocation) -> Arc<dyn StorageBackend> {
        match loc {
            ObjectLocation::Local { .. } => self.local.clone() as Arc<dyn StorageBackend>,
            #[cfg(feature = "s3")]
            ObjectLocation::S3 { .. } => self.s3.clone() as Arc<dyn StorageBackend>,
            #[cfg(not(feature = "s3"))]
            ObjectLocation::S3 { .. } => {
                panic!("s3:// location requires parqonaut-transform `s3` feature")
            }
        }
    }

    pub fn local_backend(&self) -> Arc<LocalStorageBackend> {
        Arc::clone(&self.local)
    }
}

/// Blocking wrapper for CLI / sync transform entrypoints.
#[cfg(feature = "s3")]
pub fn columnar_io_from_env() -> Result<ColumnarPipelineIo> {
    crate::remote::run_io_runtime(|handle| {
        handle.block_on(async { ColumnarPipelineIo::from_env_async().await })
    })
}

#[cfg(not(feature = "s3"))]
pub fn columnar_io_from_env() -> Result<ColumnarPipelineIo> {
    Ok(ColumnarPipelineIo::for_test())
}

pub fn location_is_remote(uri: &str) -> bool {
    ObjectLocation::parse(uri).map(|o| o.is_remote()).unwrap_or(false)
}

pub fn needs_storage_routing(input: &str, output: &str) -> bool {
    location_is_remote(input) || location_is_remote(output)
}

pub fn ensure_s3_for_remote(input: &str, output: &str) -> Result<()> {
    if needs_storage_routing(input, output) {
        #[cfg(not(feature = "s3"))]
        {
            return Err(ParqknifeError::InvalidInput(
                "remote s3:// paths require building prqnt with the `s3` feature".into(),
            ));
        }
    }
    Ok(())
}
