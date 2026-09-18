use std::fs::{self, File, OpenOptions};
use std::io::Write;

use camino::{Utf8Path, Utf8PathBuf};

use crate::error::OrchestratorError;
use crate::ids::RunId;

/// Exclusive filesystem lock for a dataset output directory.
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
        let path = Self::lock_path_for(output);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent.as_std_path())?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.as_std_path())
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    OrchestratorError::LockHeld(format!(
                        "output `{}` is locked (existing lockfile `{}`)",
                        output, path
                    ))
                } else {
                    OrchestratorError::Io(e)
                }
            })?;

        let mut file = file;
        writeln!(file, "{}", run_id.0).map_err(OrchestratorError::Io)?;

        Ok(Self { path, _file: file })
    }

    pub fn path(&self) -> &Utf8Path {
        &self.path
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
}
