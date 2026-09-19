use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Request and byte counters for storage operations.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub head_requests: u64,
    pub list_requests: u64,
    pub range_get_requests: u64,
    pub full_get_requests: u64,
    pub put_requests: u64,
    pub multipart_parts: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub range_bytes_read: u64,
    pub retries: u64,
}

#[derive(Debug, Default)]
pub struct StorageMetricsCollector {
    head: AtomicU64,
    list: AtomicU64,
    range_get: AtomicU64,
    full_get: AtomicU64,
    put: AtomicU64,
    multipart: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    range_bytes: AtomicU64,
    retries: AtomicU64,
}

impl StorageMetricsCollector {
    pub fn record_head(&self) {
        self.head.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_list(&self) {
        self.list.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_range_get(&self, bytes: u64) {
        self.range_get.fetch_add(1, Ordering::Relaxed);
        self.range_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_full_get(&self, bytes: u64) {
        self.full_get.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_put(&self, bytes: u64) {
        self.put.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_multipart_part(&self, bytes: u64) {
        self.multipart.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StorageMetrics {
        StorageMetrics {
            head_requests: self.head.load(Ordering::Relaxed),
            list_requests: self.list.load(Ordering::Relaxed),
            range_get_requests: self.range_get.load(Ordering::Relaxed),
            full_get_requests: self.full_get.load(Ordering::Relaxed),
            put_requests: self.put.load(Ordering::Relaxed),
            multipart_parts: self.multipart.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            range_bytes_read: self.range_bytes.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
        }
    }
}
