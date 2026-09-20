use serde::{Deserialize, Serialize};

/// I/O attributed to spec intermediate staging directories only (not source/final datasets).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntermediateIoCounters {
    pub files_read: u64,
    pub files_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl IntermediateIoCounters {
    pub fn intermediate_files_created(&self) -> u64 {
        self.files_written
    }

    pub fn record_read(&mut self, files: u64, bytes: u64) {
        self.files_read += files;
        self.bytes_read += bytes;
    }

    pub fn record_write(&mut self, files: u64, bytes: u64) {
        self.files_written += files;
        self.bytes_written += bytes;
    }

    pub fn is_zero(&self) -> bool {
        self.files_read == 0
            && self.files_written == 0
            && self.bytes_read == 0
            && self.bytes_written == 0
    }
}
