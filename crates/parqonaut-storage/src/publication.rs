//! Transactional object-store publication with immutable version prefixes.
//!
//! Publication layout under a dataset prefix:
//!
//! ```text
//! .parqonaut/writer.lock
//! .parqonaut/CURRENT
//! .parqonaut/versions/{version_id}/state.json
//! .parqonaut/versions/{version_id}/manifest.json
//! .parqonaut/versions/{version_id}/data/{object_key}
//! .parqonaut/versions/{version_id}/COMMITTED   ← written last; sole commit signal
//! ```
//!
//! A version is **committed** only when the `COMMITTED` marker exists. Intermediate
//! `state.json` progress is used for same-run recovery but never implies commit.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::backend::StorageBackend;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};
use crate::locking::RemotePublicationLock;
use crate::metadata::ObjectMetadata;

pub const PUBLICATION_CONTRACT_VERSION: u32 = 1;

pub const WRITER_LOCK_KEY: &str = ".parqonaut/writer.lock";
pub const CURRENT_KEY: &str = ".parqonaut/CURRENT";
pub const VERSIONS_PREFIX: &str = ".parqonaut/versions";

const STATE_FILE: &str = "state.json";
const MANIFEST_FILE: &str = "manifest.json";
const COMMITTED_MARKER: &str = "COMMITTED";
const DATA_PREFIX: &str = "data";

/// Deterministic version identity for a publication run (currently identical to `run_id`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PublicationVersionId(pub String);

impl PublicationVersionId {
    pub fn from_run_id(run_id: &str) -> Self {
        Self(run_id.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Publication lifecycle states. `Committed` and `Current` are persisted via markers, not
/// only `state.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationState {
    Preparing,
    Uploading,
    Uploaded,
    Verified,
    ManifestWritten,
    Committed,
    Current,
}

impl PublicationState {
    fn allows_transition(from: Self, to: Self) -> bool {
        use PublicationState::*;
        matches!(
            (from, to),
            (Preparing, Preparing)
                | (Preparing, Uploading)
                | (Uploading, Uploading)
                | (Uploading, Uploaded)
                | (Uploaded, Verified)
                | (Verified, ManifestWritten)
                | (ManifestWritten, Committed)
                | (Committed, Current)
        )
    }

    fn ordinal(self) -> u8 {
        use PublicationState::*;
        match self {
            Preparing => 0,
            Uploading => 1,
            Uploaded => 2,
            Verified => 3,
            ManifestWritten => 4,
            Committed => 5,
            Current => 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationStateRecord {
    pub parqonaut_publication_version: u32,
    pub version_id: String,
    pub run_id: String,
    pub state: PublicationState,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationObjectRecord {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationManifest {
    pub parqonaut_publication_version: u32,
    pub version_id: String,
    pub run_id: String,
    pub objects: Vec<PublicationObjectRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitMarker {
    pub version_id: String,
    pub run_id: String,
    pub committed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentPointer {
    pub version_id: String,
}

#[derive(Debug, Error)]
pub enum PublicationError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("writer lock held by run `{run_id}`")]
    LockHeld { run_id: String },

    #[error("invalid state transition: {from:?} -> {to:?}")]
    InvalidTransition { from: PublicationState, to: PublicationState },

    #[error("publication incomplete: {message}")]
    Incomplete { message: String },

    #[error("unsupported: {message}")]
    Unsupported { message: String },
}

/// Active remote publication session for one run/version.
pub struct RemotePublicationSession<'a, B: StorageBackend + ?Sized> {
    backend: &'a B,
    dataset: DatasetLocation,
    version_id: PublicationVersionId,
    run_id: String,
    state: PublicationState,
    lock: Option<RemotePublicationLock>,
}

impl<'a, B: StorageBackend + ?Sized> RemotePublicationSession<'a, B> {
    /// Begin or resume a publication for `run_id`, acquiring the writer lock.
    pub async fn begin(
        backend: &'a B,
        dataset: DatasetLocation,
        run_id: &str,
    ) -> Result<Self, PublicationError> {
        let lock = RemotePublicationLock::acquire(backend, &dataset, run_id).await?;
        let version_id = lock.version_id();
        let state = match load_recovered_state(backend, &dataset, &version_id).await? {
            Some(record) => record.state,
            None => PublicationState::Preparing,
        };

        let mut session = Self {
            backend,
            dataset,
            version_id,
            run_id: run_id.to_string(),
            state,
            lock: Some(lock),
        };

        if session.state == PublicationState::Preparing {
            session.persist_state(PublicationState::Preparing).await?;
        }
        Ok(session)
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn version_id(&self) -> &PublicationVersionId {
        &self.version_id
    }

    pub fn state(&self) -> PublicationState {
        self.state
    }

    pub fn dataset(&self) -> &DatasetLocation {
        &self.dataset
    }

    /// Relative key under the immutable version prefix (`data/{relative}`).
    pub fn version_object_key(&self, relative: &str) -> String {
        version_data_key(self.version_id.as_str(), relative)
    }

    pub fn version_object(&self, relative: &str) -> Result<ObjectLocation, StorageError> {
        object_at(&self.dataset, &self.version_object_key(relative))
    }

    /// Advance to `next` after validating the state machine.
    pub async fn advance_to(&mut self, next: PublicationState) -> Result<(), PublicationError> {
        if self.state == next {
            return Ok(());
        }
        if !PublicationState::allows_transition(self.state, next) {
            return Err(PublicationError::InvalidTransition { from: self.state, to: next });
        }
        self.persist_state(next).await?;
        Ok(())
    }

    /// Create an object under the version prefix (transitions to `Uploading` when needed).
    pub async fn put_version_object(
        &mut self,
        relative: &str,
        data: Bytes,
    ) -> Result<ObjectMetadata, PublicationError> {
        if self.state.ordinal() < PublicationState::Preparing.ordinal() {
            return Err(PublicationError::Incomplete { message: "session not initialized".into() });
        }
        if self.state == PublicationState::Preparing {
            self.advance_to(PublicationState::Uploading).await?;
        } else if self.state.ordinal() > PublicationState::Uploading.ordinal() {
            return Err(PublicationError::InvalidTransition {
                from: self.state,
                to: PublicationState::Uploading,
            });
        }

        let object = self.version_object(relative)?;
        let meta = self
            .backend
            .conditional_create(&object, ConditionalCreate::must_not_exist(), data)
            .await?;
        Ok(meta)
    }

    pub async fn mark_uploaded(&mut self) -> Result<(), PublicationError> {
        self.advance_to(PublicationState::Uploaded).await
    }

    pub async fn mark_verified(&mut self) -> Result<(), PublicationError> {
        self.advance_to(PublicationState::Verified).await
    }

    /// Write manifest JSON (requires `Verified`).
    pub async fn write_manifest(
        &mut self,
        manifest: &PublicationManifest,
    ) -> Result<(), PublicationError> {
        if self.state != PublicationState::Verified {
            return Err(PublicationError::InvalidTransition {
                from: self.state,
                to: PublicationState::ManifestWritten,
            });
        }
        let object = object_at(&self.dataset, &version_manifest_key(self.version_id.as_str()))?;
        let payload = serde_json::to_vec(manifest)
            .map_err(|e| StorageError::Other { message: e.to_string() })?;
        self.backend
            .conditional_create(&object, ConditionalCreate::must_not_exist(), Bytes::from(payload))
            .await?;
        self.persist_state(PublicationState::ManifestWritten).await
    }

    /// Write the commit marker last. Returns `false` if already committed (same-run idempotent).
    pub async fn commit(&mut self) -> Result<bool, PublicationError> {
        if self.state != PublicationState::ManifestWritten {
            return Err(PublicationError::InvalidTransition {
                from: self.state,
                to: PublicationState::Committed,
            });
        }
        if is_version_committed(self.backend, &self.dataset, &self.version_id).await? {
            self.state = PublicationState::Committed;
            return Ok(false);
        }

        let marker = CommitMarker {
            version_id: self.version_id.as_str().to_string(),
            run_id: self.run_id.clone(),
            committed_at: Utc::now(),
        };
        let object = object_at(&self.dataset, &version_commit_key(self.version_id.as_str()))?;
        let payload = serde_json::to_vec(&marker)
            .map_err(|e| StorageError::Other { message: e.to_string() })?;
        self.backend
            .conditional_create(&object, ConditionalCreate::must_not_exist(), Bytes::from(payload))
            .await?;
        self.persist_state(PublicationState::Committed).await?;
        Ok(true)
    }

    /// Optionally update `.parqonaut/CURRENT` after commit (conditional create/replace).
    pub async fn promote_current(&mut self) -> Result<(), PublicationError> {
        if !is_version_committed(self.backend, &self.dataset, &self.version_id).await? {
            return Err(PublicationError::Incomplete {
                message: "cannot promote uncommitted version to CURRENT".into(),
            });
        }
        let pointer = CurrentPointer { version_id: self.version_id.as_str().to_string() };
        let object = object_at(&self.dataset, CURRENT_KEY)?;
        let payload = serde_json::to_vec(&pointer)
            .map_err(|e| StorageError::Other { message: e.to_string() })?;

        match self.backend.head(&object).await {
            Ok(head) => {
                self.backend
                    .conditional_replace(
                        &object,
                        ConditionalReplace::matching(&head),
                        Bytes::from(payload),
                    )
                    .await?;
            }
            Err(StorageError::NotFound { .. }) => {
                self.backend
                    .conditional_create(
                        &object,
                        ConditionalCreate::must_not_exist(),
                        Bytes::from(payload),
                    )
                    .await?;
            }
            Err(e) => return Err(e.into()),
        }

        self.persist_state(PublicationState::Current).await
    }

    /// Release the writer lock without committing.
    pub async fn abandon(mut self) -> Result<(), PublicationError> {
        if let Some(lock) = self.lock.take() {
            lock.release(self.backend).await?;
        }
        Ok(())
    }

    /// Commit workflow helper: commit, optionally promote CURRENT, release lock.
    pub async fn finish(mut self, promote_current: bool) -> Result<(), PublicationError> {
        if self.state == PublicationState::ManifestWritten {
            self.commit().await?;
        }
        if promote_current {
            self.promote_current().await?;
        }
        if let Some(lock) = self.lock.take() {
            lock.release(self.backend).await?;
        }
        Ok(())
    }

    async fn persist_state(&mut self, state: PublicationState) -> Result<(), PublicationError> {
        let record = PublicationStateRecord {
            parqonaut_publication_version: PUBLICATION_CONTRACT_VERSION,
            version_id: self.version_id.as_str().to_string(),
            run_id: self.run_id.clone(),
            state,
            updated_at: Utc::now(),
        };
        let object = object_at(&self.dataset, &version_state_key(self.version_id.as_str()))?;
        let payload = serde_json::to_vec(&record)
            .map_err(|e| StorageError::Other { message: e.to_string() })?;

        match self.backend.head(&object).await {
            Ok(head) => {
                self.backend
                    .conditional_replace(
                        &object,
                        ConditionalReplace::matching(&head),
                        Bytes::from(payload),
                    )
                    .await?;
            }
            Err(StorageError::NotFound { .. }) => {
                self.backend
                    .conditional_create(
                        &object,
                        ConditionalCreate::must_not_exist(),
                        Bytes::from(payload),
                    )
                    .await?;
            }
            Err(e) => return Err(e.into()),
        }

        self.state = state;
        Ok(())
    }
}

/// Returns `true` only when the commit marker object exists.
pub async fn is_version_committed<B: StorageBackend + ?Sized>(
    backend: &B,
    dataset: &DatasetLocation,
    version_id: &PublicationVersionId,
) -> Result<bool, PublicationError> {
    let object = object_at(dataset, &version_commit_key(version_id.as_str()))?;
    match backend.head(&object).await {
        Ok(_) => Ok(true),
        Err(StorageError::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Observed publication state for recovery/display. Never reports `Committed` without marker.
pub async fn observed_state<B: StorageBackend + ?Sized>(
    backend: &B,
    dataset: &DatasetLocation,
    version_id: &PublicationVersionId,
) -> Result<Option<PublicationState>, PublicationError> {
    if is_version_committed(backend, dataset, version_id).await? {
        let current = read_current_version(backend, dataset).await?;
        if current.as_deref() == Some(version_id.as_str()) {
            return Ok(Some(PublicationState::Current));
        }
        return Ok(Some(PublicationState::Committed));
    }

    let state_object = object_at(dataset, &version_state_key(version_id.as_str()))?;
    match read_json_object::<PublicationStateRecord, B>(backend, &state_object).await {
        Ok(record) => {
            if record.state.ordinal() >= PublicationState::Committed.ordinal() {
                Ok(Some(PublicationState::ManifestWritten))
            } else {
                Ok(Some(record.state))
            }
        }
        Err(PublicationError::Storage(StorageError::NotFound { .. })) => Ok(None),
        Err(e) => Err(e),
    }
}

pub async fn read_current_version<B: StorageBackend + ?Sized>(
    backend: &B,
    dataset: &DatasetLocation,
) -> Result<Option<String>, PublicationError> {
    let object = object_at(dataset, CURRENT_KEY)?;
    match read_json_object::<CurrentPointer, B>(backend, &object).await {
        Ok(pointer) => Ok(Some(pointer.version_id)),
        Err(PublicationError::Storage(StorageError::NotFound { .. })) => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn object_at(
    dataset: &DatasetLocation,
    relative: &str,
) -> Result<ObjectLocation, StorageError> {
    match dataset {
        DatasetLocation::Local(l) => Ok(ObjectLocation::Local { path: l.path.join(relative) }),
        DatasetLocation::S3(s) => {
            Ok(ObjectLocation::S3 { bucket: s.bucket.clone(), key: s.object_key(relative)? })
        }
    }
}

pub(crate) async fn read_json_object<T: for<'de> Deserialize<'de>, B: StorageBackend + ?Sized>(
    backend: &B,
    object: &ObjectLocation,
) -> Result<T, PublicationError> {
    use tokio::io::AsyncReadExt;

    let mut stream = backend.read_stream(object, None).await?;
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| StorageError::Other { message: e.to_string() })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        PublicationError::Storage(StorageError::Other {
            message: format!("invalid JSON at {}: {e}", object.display_uri()),
        })
    })
}

async fn load_recovered_state<B: StorageBackend + ?Sized>(
    backend: &B,
    dataset: &DatasetLocation,
    version_id: &PublicationVersionId,
) -> Result<Option<PublicationStateRecord>, PublicationError> {
    if is_version_committed(backend, dataset, version_id).await? {
        let state = if read_current_version(backend, dataset).await?.as_deref()
            == Some(version_id.as_str())
        {
            PublicationState::Current
        } else {
            PublicationState::Committed
        };
        return Ok(Some(PublicationStateRecord {
            parqonaut_publication_version: PUBLICATION_CONTRACT_VERSION,
            version_id: version_id.as_str().to_string(),
            run_id: version_id.as_str().to_string(),
            state,
            updated_at: Utc::now(),
        }));
    }

    let state_object = object_at(dataset, &version_state_key(version_id.as_str()))?;
    match read_json_object::<PublicationStateRecord, B>(backend, &state_object).await {
        Ok(record) => Ok(Some(record)),
        Err(PublicationError::Storage(StorageError::NotFound { .. })) => Ok(None),
        Err(e) => Err(e),
    }
}

fn version_prefix(version_id: &str) -> String {
    format!("{VERSIONS_PREFIX}/{version_id}")
}

fn version_state_key(version_id: &str) -> String {
    format!("{}/{}", version_prefix(version_id), STATE_FILE)
}

fn version_manifest_key(version_id: &str) -> String {
    format!("{}/{}", version_prefix(version_id), MANIFEST_FILE)
}

pub(crate) fn version_commit_key(version_id: &str) -> String {
    format!("{}/{}", version_prefix(version_id), COMMITTED_MARKER)
}

fn version_data_key(version_id: &str, relative: &str) -> String {
    format!("{}/{}/{}", version_prefix(version_id), DATA_PREFIX, relative.trim_start_matches('/'))
}

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

    fn dataset() -> DatasetLocation {
        DatasetLocation::parse("s3://pub-bucket/datasets/demo/").unwrap()
    }

    #[tokio::test]
    async fn happy_path_publication_through_current() {
        let backend = memory_backend();
        let ds = dataset();
        let run_id = "run-001";

        let mut session =
            RemotePublicationSession::begin(&backend, ds.clone(), run_id).await.unwrap();
        assert_eq!(session.state(), PublicationState::Preparing);

        session
            .put_version_object("part-000.parquet", Bytes::from_static(b"parquet-bytes"))
            .await
            .unwrap();
        session.mark_uploaded().await.unwrap();
        session.mark_verified().await.unwrap();

        let manifest = PublicationManifest {
            parqonaut_publication_version: PUBLICATION_CONTRACT_VERSION,
            version_id: run_id.to_string(),
            run_id: run_id.to_string(),
            objects: vec![PublicationObjectRecord {
                key: "part-000.parquet".into(),
                size: 13,
                etag: Some("mem-13".into()),
                version_id: Some("1".into()),
            }],
        };
        session.write_manifest(&manifest).await.unwrap();
        assert!(session.commit().await.unwrap());
        session.promote_current().await.unwrap();
        session.abandon().await.unwrap();

        assert!(is_version_committed(&backend, &ds, &PublicationVersionId::from_run_id(run_id))
            .await
            .unwrap());
        let current = read_current_version(&backend, &ds).await.unwrap();
        assert_eq!(current.as_deref(), Some(run_id));
    }

    #[tokio::test]
    async fn concurrent_writers_rejected() {
        let backend = memory_backend();
        let ds = dataset();
        let mut first =
            RemotePublicationSession::begin(&backend, ds.clone(), "run-a").await.unwrap();
        let err = match RemotePublicationSession::begin(&backend, ds, "run-b").await {
            Err(err) => err,
            Ok(_) => panic!("expected lock conflict"),
        };
        assert!(matches!(err, PublicationError::LockHeld { .. }));
        first.abandon().await.unwrap();
    }

    #[tokio::test]
    async fn same_run_recovery_resumes_state() {
        let backend = memory_backend();
        let ds = dataset();
        let run_id = "run-resume";

        {
            let mut session =
                RemotePublicationSession::begin(&backend, ds.clone(), run_id).await.unwrap();
            session.put_version_object("a.parquet", Bytes::from_static(b"abc")).await.unwrap();
            session.mark_uploaded().await.unwrap();
            session.abandon().await.unwrap();
        }

        let resumed = RemotePublicationSession::begin(&backend, ds.clone(), run_id).await.unwrap();
        assert_eq!(resumed.state(), PublicationState::Uploaded);
        assert!(!is_version_committed(&backend, &ds, &PublicationVersionId::from_run_id(run_id))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn incomplete_version_not_committed() {
        let backend = memory_backend();
        let ds = dataset();
        let run_id = "run-partial";
        let version = PublicationVersionId::from_run_id(run_id);

        let mut session =
            RemotePublicationSession::begin(&backend, ds.clone(), run_id).await.unwrap();
        session.put_version_object("x.parquet", Bytes::from_static(b"x")).await.unwrap();
        session.mark_uploaded().await.unwrap();
        session.abandon().await.unwrap();

        assert!(!is_version_committed(&backend, &ds, &version).await.unwrap());
        let observed = observed_state(&backend, &ds, &version).await.unwrap();
        assert_eq!(observed, Some(PublicationState::Uploaded));
    }

    #[tokio::test]
    async fn commit_marker_written_after_manifest() {
        let backend = memory_backend();
        let ds = dataset();
        let run_id = "run-order";
        let version = PublicationVersionId::from_run_id(run_id);

        let mut session =
            RemotePublicationSession::begin(&backend, ds.clone(), run_id).await.unwrap();
        session.put_version_object("f.parquet", Bytes::from_static(b"data")).await.unwrap();
        session.mark_uploaded().await.unwrap();
        session.mark_verified().await.unwrap();

        let manifest = PublicationManifest {
            parqonaut_publication_version: PUBLICATION_CONTRACT_VERSION,
            version_id: run_id.to_string(),
            run_id: run_id.to_string(),
            objects: vec![],
        };
        session.write_manifest(&manifest).await.unwrap();

        let manifest_obj = object_at(&ds, &version_manifest_key(run_id)).unwrap();
        assert!(backend.head(&manifest_obj).await.is_ok());
        assert!(!is_version_committed(&backend, &ds, &version).await.unwrap());

        session.commit().await.unwrap();
        assert!(is_version_committed(&backend, &ds, &version).await.unwrap());
        session.abandon().await.unwrap();
    }

    #[tokio::test]
    async fn finish_helper_commits_and_releases_lock() {
        let backend = memory_backend();
        let ds = dataset();
        let run_id = "run-finish";

        let mut session =
            RemotePublicationSession::begin(&backend, ds.clone(), run_id).await.unwrap();
        session.put_version_object("f.parquet", Bytes::from_static(b"z")).await.unwrap();
        session.mark_uploaded().await.unwrap();
        session.mark_verified().await.unwrap();
        session
            .write_manifest(&PublicationManifest {
                parqonaut_publication_version: PUBLICATION_CONTRACT_VERSION,
                version_id: run_id.to_string(),
                run_id: run_id.to_string(),
                objects: vec![],
            })
            .await
            .unwrap();
        session.finish(true).await.unwrap();

        let lock_obj = object_at(&ds, WRITER_LOCK_KEY).unwrap();
        assert!(matches!(backend.head(&lock_obj).await, Err(StorageError::NotFound { .. })));
        assert_eq!(read_current_version(&backend, &ds).await.unwrap().as_deref(), Some(run_id));
    }
}
