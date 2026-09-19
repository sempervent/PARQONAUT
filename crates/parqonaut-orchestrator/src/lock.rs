use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};

use camino::{Utf8Path, Utf8PathBuf};
use parqonaut_storage::location::DatasetLocation;
use parqonaut_storage::locking::RemotePublicationLock;
use parqonaut_storage::publication::PublicationError;

use crate::error::OrchestratorError;
use crate::ids::RunId;
use crate::storage::BatchStorageRuntime;

/// Exclusive filesystem lock for a local dataset output directory.
///
/// Lock files live beside the output path as `{output}.parqonaut.lock` and record the
/// owning batch `run_id`. Only one active lock may exist per output path.
pub struct DatasetLock {
    path: Utf8PathBuf,
    _file: File,
}

impl DatasetLock {
    pub fn lock_path_for(output: &Utf8Path) -> Utf8PathBuf {
        Utf8PathBuf::from(format!("{}.parqonaut.lock", output))
    }

    pub fn acquire(output: &Utf8Path, run_id: &RunId) -> Result<Self, OrchestratorError> {
        Self::acquire_inner(output, run_id, false)
    }

    fn acquire_inner(
        output: &Utf8Path,
        run_id: &RunId,
        retried: bool,
    ) -> Result<Self, OrchestratorError> {
        let path = Self::lock_path_for(output);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent.as_std_path())?;
        }

        match OpenOptions::new().write(true).create_new(true).open(path.as_std_path()) {
            Ok(file) => {
                let mut file = file;
                writeln!(file, "{}", run_id.0).map_err(OrchestratorError::Io)?;
                Ok(Self { path, _file: file })
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if !retried {
                    if let Ok(owner) = read_lock_owner(&path) {
                        if owner == run_id.0 {
                            let _ = fs::remove_file(path.as_std_path());
                            return Self::acquire_inner(output, run_id, true);
                        }
                    }
                }
                Err(OrchestratorError::LockHeld(format!(
                    "output `{}` is locked (existing lockfile `{}`)",
                    output, path
                )))
            }
            Err(e) => Err(OrchestratorError::Io(e)),
        }
    }

    pub fn path(&self) -> &Utf8Path {
        &self.path
    }
}

/// Unified output lock for local paths and remote publication prefixes.
pub enum OutputLock {
    Local(DatasetLock),
    Remote(RemotePublicationLock),
}

impl OutputLock {
    pub async fn acquire(
        output: &DatasetLocation,
        run_id: &RunId,
        runtime: &BatchStorageRuntime,
    ) -> Result<Self, OrchestratorError> {
        match output {
            DatasetLocation::Local(local) => {
                Ok(Self::Local(DatasetLock::acquire(Utf8Path::new(local.path.as_str()), run_id)?))
            }
            DatasetLocation::S3(_) => {
                let lock = runtime
                    .with_request_permit(|| async {
                        RemotePublicationLock::acquire(
                            runtime.backend_for(output),
                            output,
                            &run_id.0,
                        )
                        .await
                        .map_err(map_publication_lock_err)
                    })
                    .await?;
                Ok(Self::Remote(lock))
            }
        }
    }

    pub async fn release_for(
        self,
        runtime: &BatchStorageRuntime,
        output: &DatasetLocation,
    ) -> Result<(), OrchestratorError> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Remote(lock) => {
                runtime
                    .with_request_permit(|| async {
                        lock.release(runtime.backend_for(output))
                            .await
                            .map_err(map_publication_lock_err)
                    })
                    .await
            }
        }
    }
}

fn read_lock_owner(path: &Utf8Path) -> Result<String, std::io::Error> {
    let mut file = File::open(path.as_std_path())?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    Ok(buf.trim().to_string())
}

fn map_publication_lock_err(err: PublicationError) -> OrchestratorError {
    match err {
        PublicationError::LockHeld { run_id } => {
            OrchestratorError::LockHeld(format!("remote writer lock held by run `{run_id}`"))
        }
        other => OrchestratorError::Storage(other.to_string()),
    }
}

impl Drop for DatasetLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.path.as_std_path());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::RunId;
    use crate::storage::BatchStorageRuntime;
    use parqonaut_storage::capabilities::StorageCapabilities;
    use parqonaut_storage::memory::MemoryStorageBackend;
    use tempfile::TempDir;

    #[test]
    fn lock_is_exclusive_per_output() {
        let tmp = TempDir::new().unwrap();
        let out_path = tmp.path().join("out");
        let output = Utf8Path::from_path(&out_path).unwrap();
        let run = RunId::new();
        let _lock = DatasetLock::acquire(output, &run).unwrap();
        let err = DatasetLock::acquire(output, &RunId::new());
        assert!(matches!(err, Err(OrchestratorError::LockHeld(_))));
    }

    #[test]
    fn lock_released_on_drop() {
        let tmp = TempDir::new().unwrap();
        let out_path = tmp.path().join("out");
        let output = Utf8Path::from_path(&out_path).unwrap();
        let lock_path = DatasetLock::lock_path_for(output);
        {
            let _lock = DatasetLock::acquire(output, &RunId::new()).unwrap();
            assert!(lock_path.exists());
        }
        assert!(!lock_path.exists());
    }

    #[test]
    fn same_run_can_reacquire_stale_lock() {
        let tmp = TempDir::new().unwrap();
        let out_path = tmp.path().join("out");
        let output = Utf8Path::from_path(&out_path).unwrap();
        let run = RunId::new();
        let lock_path = DatasetLock::lock_path_for(output);
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lock_path.as_std_path())
                .unwrap();
            writeln!(file, "{}", run.0).unwrap();
        }
        let _lock = DatasetLock::acquire(output, &run).unwrap();
    }

    #[tokio::test]
    async fn remote_output_lock_is_exclusive() {
        let backend = MemoryStorageBackend::new(StorageCapabilities {
            conditional_create: true,
            ..StorageCapabilities::S3
        });
        let runtime = BatchStorageRuntime::with_remote_backend(4, backend);
        let output = DatasetLocation::parse("s3://bucket/out/a/").unwrap();
        let run_a = RunId::new();
        let lock = OutputLock::acquire(&output, &run_a, &runtime).await.unwrap();
        let err = OutputLock::acquire(&output, &RunId::new(), &runtime).await;
        assert!(matches!(err, Err(OrchestratorError::LockHeld(_))));
        lock.release_for(&runtime, &output).await.unwrap();
    }
}
