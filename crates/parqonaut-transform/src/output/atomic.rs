use crate::error::{ParqknifeError, Result};
use std::path::{Path, PathBuf};

pub struct AtomicWriter {
    temp_dir: PathBuf,
    final_dir: PathBuf,
}

impl AtomicWriter {
    pub fn new(final_path: &str) -> Result<Self> {
        let final_path = Path::new(final_path);
        let parent = final_path
            .parent()
            .ok_or_else(|| ParqknifeError::InvalidInput("Invalid output path".to_string()))?;

        let temp_name = format!(".parqknife.tmp.{}", uuid::Uuid::new_v4());
        let temp_dir = parent.join(temp_name);

        std::fs::create_dir_all(&temp_dir)?;

        Ok(Self { temp_dir, final_dir: final_path.to_path_buf() })
    }

    pub fn temp_path(&self, relative: &str) -> PathBuf {
        self.temp_dir.join(relative)
    }

    pub fn commit(self) -> Result<()> {
        if self.final_dir.exists() {
            std::fs::remove_dir_all(&self.final_dir)?;
        }
        std::fs::rename(&self.temp_dir, &self.final_dir)?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        if self.temp_dir.exists() {
            std::fs::remove_dir_all(&self.temp_dir)?;
        }
        Ok(())
    }
}
