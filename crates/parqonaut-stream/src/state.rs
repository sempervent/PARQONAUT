use crate::error::{MawError, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
    time::SystemTime,
};

pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamExecutionIdentity {
    pub schema_version: u32,
    pub inputs_fingerprint: String,
    pub output_path: String,
    pub output_format: String,
    pub schema_policy: String,
    pub compression: String,
    pub batch_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub path: String,
    pub format: String,
    pub processed: bool,
    pub last_offset: Option<u64>,
    pub last_row_group: Option<usize>,
    pub bytes_processed: u64,
    pub rows_processed: u64,
    pub source_size: u64,
    pub source_modified: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingState {
    pub schema_version: u32,
    pub identity: StreamExecutionIdentity,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub files: HashMap<String, FileState>,
    pub total_files: usize,
    pub processed_files: usize,
    pub total_bytes: u64,
    pub processed_bytes: u64,
    pub committed: bool,
}

impl ProcessingState {
    pub fn new(identity: StreamExecutionIdentity) -> Self {
        Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            identity,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            files: HashMap::new(),
            total_files: 0,
            processed_files: 0,
            total_bytes: 0,
            processed_bytes: 0,
            committed: false,
        }
    }

    pub fn add_file(&mut self, path: String, format: String, size: u64, modified: SystemTime) {
        let file_state = FileState {
            path: path.clone(),
            format,
            processed: false,
            last_offset: None,
            last_row_group: None,
            bytes_processed: 0,
            rows_processed: 0,
            source_size: size,
            source_modified: modified,
        };

        self.files.insert(path, file_state);
        self.total_files += 1;
        self.total_bytes += size;
    }

    pub fn mark_file_processed(&mut self, path: &str, bytes_processed: u64, rows_processed: u64) {
        if let Some(file_state) = self.files.get_mut(path) {
            file_state.processed = true;
            file_state.bytes_processed = bytes_processed;
            file_state.rows_processed = rows_processed;
            self.processed_files += 1;
            self.processed_bytes += bytes_processed;
        }
        self.updated_at = SystemTime::now();
    }

    pub fn update_file_progress(&mut self, path: &str, offset: u64, row_group: Option<usize>) {
        if let Some(file_state) = self.files.get_mut(path) {
            file_state.last_offset = Some(offset);
            file_state.last_row_group = row_group;
            file_state.bytes_processed = offset;
        }
        self.updated_at = SystemTime::now();
    }

    pub fn is_file_processed(&self, path: &str) -> bool {
        self.files.get(path).map(|f| f.processed).unwrap_or(false)
    }

    pub fn get_file_state(&self, path: &str) -> Option<&FileState> {
        self.files.get(path)
    }

    pub fn get_resume_point(&self, path: &str) -> Option<(u64, Option<usize>)> {
        self.files.get(path).map(|f| (f.last_offset.unwrap_or(0), f.last_row_group))
    }

    pub fn is_complete(&self) -> bool {
        self.processed_files == self.total_files && self.total_files > 0
    }

    pub fn get_progress_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.processed_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
}

pub fn fingerprint_inputs(paths: &[String]) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut sorted = paths.to_vec();
    sorted.sort();
    for path in sorted {
        hasher.update(path.as_bytes());
        let meta = fs::metadata(&path)?;
        hasher.update(meta.len().to_le_bytes());
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        hasher.update(
            modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_le_bytes(),
        );
    }
    Ok(hex::encode(hasher.finalize()))
}

pub fn verify_sources_unchanged(state: &ProcessingState) -> Result<()> {
    for file in state.files.values() {
        let meta = fs::metadata(&file.path).map_err(|e| {
            MawError::State(format!("STALE CHECKPOINT: source missing {} ({e})", file.path))
        })?;
        if meta.len() != file.source_size {
            return Err(MawError::State(format!(
                "STALE CHECKPOINT: source size changed for {}",
                file.path
            )));
        }
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if modified != file.source_modified {
            return Err(MawError::State(format!(
                "STALE CHECKPOINT: source modified for {}",
                file.path
            )));
        }
    }
    Ok(())
}

pub struct StateManager {
    state_path: Option<String>,
    state: Option<ProcessingState>,
}

impl StateManager {
    pub fn new(state_path: Option<String>) -> Self {
        Self { state_path, state: None }
    }

    pub fn load_state(&mut self) -> Result<Option<ProcessingState>> {
        if let Some(path) = &self.state_path {
            if Path::new(path).exists() {
                let content = fs::read_to_string(path)?;
                let state: ProcessingState = serde_json::from_str(&content)?;
                if state.schema_version != CHECKPOINT_SCHEMA_VERSION {
                    return Err(MawError::State(format!(
                        "unsupported checkpoint schema version {}",
                        state.schema_version
                    )));
                }
                self.state = Some(state);
                return Ok(Some(self.state.as_ref().unwrap().clone()));
            }
        }
        Ok(None)
    }

    pub fn save_state(&mut self, state: &ProcessingState) -> Result<()> {
        if let Some(path) = &self.state_path {
            let content = serde_json::to_string_pretty(state)?;
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let tmp = path.with_extension("json.tmp");
            {
                let mut file = File::create(&tmp)?;
                file.write_all(content.as_bytes())?;
                file.sync_all()?;
            }
            fs::rename(&tmp, path)?;
            if let Some(parent) = path.parent() {
                if let Ok(dir) = OpenOptions::new().read(true).open(parent) {
                    let _ = dir.sync_all();
                }
            }
            self.state = Some(state.clone());
        }
        Ok(())
    }

    pub fn create_state(&mut self, identity: StreamExecutionIdentity) -> ProcessingState {
        let state = ProcessingState::new(identity);
        self.state = Some(state.clone());
        state
    }

    pub fn get_state(&self) -> Option<&ProcessingState> {
        self.state.as_ref()
    }

    pub fn cleanup(&self) -> Result<()> {
        if let Some(path) = &self.state_path {
            if Path::new(path).exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_processing_state() {
        let identity = StreamExecutionIdentity {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            inputs_fingerprint: "abc".into(),
            output_path: "out.parquet".into(),
            output_format: "parquet".into(),
            schema_policy: "strict".into(),
            compression: "none".into(),
            batch_size: 1000,
        };
        let mut state = ProcessingState::new(identity);

        state.add_file("file1.csv".to_string(), "csv".to_string(), 1000, SystemTime::now());
        state.add_file("file2.csv".to_string(), "csv".to_string(), 2000, SystemTime::now());

        assert_eq!(state.total_files, 2);
        assert_eq!(state.total_bytes, 3000);
        assert!(!state.is_complete());

        state.mark_file_processed("file1.csv", 1000, 100);
        assert_eq!(state.processed_files, 1);

        state.mark_file_processed("file2.csv", 2000, 200);
        assert!(state.is_complete());
    }

    #[test]
    fn atomic_checkpoint_replace() {
        let temp_dir = tempdir().unwrap();
        let state_file = temp_dir.path().join("state.json");
        let mut manager = StateManager::new(Some(state_file.to_string_lossy().to_string()));
        let identity = StreamExecutionIdentity {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            inputs_fingerprint: "x".into(),
            output_path: "o".into(),
            output_format: "csv".into(),
            schema_policy: "strict".into(),
            compression: "none".into(),
            batch_size: 1,
        };
        let state = manager.create_state(identity);
        manager.save_state(&state).unwrap();
        let loaded = manager.load_state().unwrap().unwrap();
        assert_eq!(loaded.schema_version, CHECKPOINT_SCHEMA_VERSION);
    }
}
