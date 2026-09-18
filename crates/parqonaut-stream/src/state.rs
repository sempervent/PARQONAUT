use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::Path, time::SystemTime};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub path: String,
    pub format: String,
    pub processed: bool,
    pub last_offset: Option<u64>,
    pub last_row_group: Option<usize>,
    pub bytes_processed: u64,
    pub rows_processed: u64,
    pub last_modified: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingState {
    pub version: String,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub files: HashMap<String, FileState>,
    pub output_path: String,
    pub output_format: String,
    pub total_files: usize,
    pub processed_files: usize,
    pub total_bytes: u64,
    pub processed_bytes: u64,
}

impl ProcessingState {
    pub fn new(output_path: String, output_format: String) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            files: HashMap::new(),
            output_path,
            output_format,
            total_files: 0,
            processed_files: 0,
            total_bytes: 0,
            processed_bytes: 0,
        }
    }

    pub fn add_file(&mut self, path: String, format: String, size: u64) {
        let file_state = FileState {
            path: path.clone(),
            format,
            processed: false,
            last_offset: None,
            last_row_group: None,
            bytes_processed: 0,
            rows_processed: 0,
            last_modified: SystemTime::now(),
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
        self.processed_files == self.total_files
    }

    pub fn get_progress_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.processed_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
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
                self.state = Some(state);
                return Ok(Some(self.state.as_ref().unwrap().clone()));
            }
        }
        Ok(None)
    }

    pub fn save_state(&mut self, state: &ProcessingState) -> Result<()> {
        if let Some(path) = &self.state_path {
            let content = serde_json::to_string_pretty(state)?;
            fs::write(path, content)?;
            self.state = Some(state.clone());
        }
        Ok(())
    }

    pub fn create_state(&mut self, output_path: String, output_format: String) -> ProcessingState {
        let state = ProcessingState::new(output_path, output_format);
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
        let mut state = ProcessingState::new("output.csv".to_string(), "csv".to_string());

        state.add_file("file1.csv".to_string(), "csv".to_string(), 1000);
        state.add_file("file2.csv".to_string(), "csv".to_string(), 2000);

        assert_eq!(state.total_files, 2);
        assert_eq!(state.total_bytes, 3000);
        assert!(!state.is_complete());

        state.mark_file_processed("file1.csv", 1000, 100);
        assert_eq!(state.processed_files, 1);
        assert_eq!(state.processed_bytes, 1000);

        state.mark_file_processed("file2.csv", 2000, 200);
        assert!(state.is_complete());
        assert_eq!(state.get_progress_percentage(), 100.0);
    }

    #[test]
    fn test_state_manager() {
        let temp_dir = tempdir().unwrap();
        let state_file = temp_dir.path().join("state.json");

        let mut manager = StateManager::new(Some(state_file.to_string_lossy().to_string()));
        let state = manager.create_state("output.csv".to_string(), "csv".to_string());

        manager.save_state(&state).unwrap();
        let loaded = manager.load_state().unwrap().unwrap();

        assert_eq!(loaded.output_path, "output.csv");
        assert_eq!(loaded.output_format, "csv");
    }
}
