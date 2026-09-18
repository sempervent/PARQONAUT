use crate::error::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ProgressTracker {
    pub global_progress: Arc<RwLock<GlobalProgress>>,
    pub progress_bar: Option<ProgressBar>,
}

#[derive(Debug, Clone)]
pub struct GlobalProgress {
    pub total_files: usize,
    pub processed_files: usize,
    pub total_bytes: u64,
    pub processed_bytes: u64,
    pub total_rows: u64,
    pub processed_rows: u64,
    pub start_time: std::time::Instant,
}

impl GlobalProgress {
    pub fn new(total_files: usize, total_bytes: u64) -> Self {
        Self {
            total_files,
            processed_files: 0,
            total_bytes,
            processed_bytes: 0,
            total_rows: 0,
            processed_rows: 0,
            start_time: std::time::Instant::now(),
        }
    }

    pub fn get_throughput_mbps(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (self.processed_bytes as f64 / 1_000_000.0) / elapsed
        } else {
            0.0
        }
    }

    pub fn get_eta_seconds(&self) -> Option<u64> {
        if self.processed_bytes > 0 && self.processed_bytes < self.total_bytes {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            let rate = self.processed_bytes as f64 / elapsed;
            let remaining_bytes = self.total_bytes - self.processed_bytes;
            Some((remaining_bytes as f64 / rate) as u64)
        } else {
            None
        }
    }

    pub fn get_progress_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.processed_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
}

impl ProgressTracker {
    pub fn new(show_progress: bool, total_files: usize, total_bytes: u64) -> Self {
        let global_progress = Arc::new(RwLock::new(GlobalProgress::new(total_files, total_bytes)));

        let progress_bar = if show_progress {
            let pb = ProgressBar::new(total_bytes);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message("Processing files...");
            Some(pb)
        } else {
            None
        };

        Self { global_progress, progress_bar }
    }

    pub async fn update_file_progress(
        &self,
        bytes_processed: u64,
        rows_processed: u64,
    ) -> Result<()> {
        let mut progress = self.global_progress.write().await;
        progress.processed_bytes += bytes_processed;
        progress.processed_rows += rows_processed;

        if let Some(pb) = &self.progress_bar {
            pb.set_position(progress.processed_bytes);
            pb.set_message(format!(
                "Throughput: {:.1} MB/s, ETA: {}",
                progress.get_throughput_mbps(),
                format_eta(progress.get_eta_seconds())
            ));
        }

        Ok(())
    }

    pub async fn mark_file_complete(&self) -> Result<()> {
        let mut progress = self.global_progress.write().await;
        progress.processed_files += 1;

        if let Some(pb) = &self.progress_bar {
            pb.set_message(format!(
                "Completed {}/{} files, Throughput: {:.1} MB/s",
                progress.processed_files,
                progress.total_files,
                progress.get_throughput_mbps()
            ));
        }

        Ok(())
    }

    pub async fn finish(&self) -> Result<()> {
        if let Some(pb) = &self.progress_bar {
            let progress = self.global_progress.read().await;
            pb.finish_with_message(format!(
                "Completed! Processed {} files, {:.1} MB/s average throughput",
                progress.processed_files,
                progress.get_throughput_mbps()
            ));
        }
        Ok(())
    }

    pub async fn get_stats(&self) -> GlobalProgress {
        self.global_progress.read().await.clone()
    }
}

fn format_eta(eta_seconds: Option<u64>) -> String {
    match eta_seconds {
        Some(seconds) => {
            let hours = seconds / 3600;
            let minutes = (seconds % 3600) / 60;
            let secs = seconds % 60;

            if hours > 0 {
                format!("{}h {}m {}s", hours, minutes, secs)
            } else if minutes > 0 {
                format!("{}m {}s", minutes, secs)
            } else {
                format!("{}s", secs)
            }
        }
        None => "Unknown".to_string(),
    }
}

pub struct FileProgressTracker {
    file_name: String,
    #[allow(dead_code)]
    file_size: u64,
    progress_bar: Option<ProgressBar>,
}

impl FileProgressTracker {
    pub fn new(file_name: String, file_size: u64, show_progress: bool) -> Self {
        let progress_bar = if show_progress {
            let pb = ProgressBar::new(file_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%)")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message(file_name.clone());
            Some(pb)
        } else {
            None
        };

        Self { file_name, file_size, progress_bar }
    }

    pub fn update(&self, bytes_processed: u64) {
        if let Some(pb) = &self.progress_bar {
            pb.set_position(bytes_processed);
        }
    }

    pub fn finish(&self) {
        if let Some(pb) = &self.progress_bar {
            pb.finish_with_message(format!("{} completed", self.file_name));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_progress_tracker() {
        let tracker = ProgressTracker::new(true, 10, 1000);

        tracker.update_file_progress(100, 10).await.unwrap();
        tracker.update_file_progress(200, 20).await.unwrap();

        let stats = tracker.get_stats().await;
        assert_eq!(stats.processed_bytes, 300);
        assert_eq!(stats.processed_rows, 30);
    }

    #[test]
    fn test_eta_formatting() {
        assert_eq!(format_eta(Some(0)), "0s");
        assert_eq!(format_eta(Some(59)), "59s");
        assert_eq!(format_eta(Some(60)), "1m 0s");
        assert_eq!(format_eta(Some(3661)), "1h 1m 1s");
        assert_eq!(format_eta(None), "Unknown");
    }
}
