use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub files_processed: usize,
    pub files_succeeded: usize,
    pub files_failed: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub row_groups_processed: usize,
    pub rows_processed: u64,
    pub errors: Vec<FileError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileError {
    pub file: String,
    pub error: String,
}

impl Report {
    pub fn new() -> Self {
        Self {
            files_processed: 0,
            files_succeeded: 0,
            files_failed: 0,
            bytes_read: 0,
            bytes_written: 0,
            row_groups_processed: 0,
            rows_processed: 0,
            errors: Vec::new(),
        }
    }

    pub fn add_error(&mut self, file: String, error: String) {
        self.files_failed += 1;
        self.errors.push(FileError { file, error });
    }

    pub fn add_success(&mut self) {
        self.files_succeeded += 1;
    }
}

impl Default for Report {
    fn default() -> Self {
        Self::new()
    }
}
