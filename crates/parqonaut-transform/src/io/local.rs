use crate::error::{ParqknifeError, Result};
use async_trait::async_trait;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

pub struct LocalInputSource;

#[async_trait]
impl crate::io::InputSource for LocalInputSource {
    async fn read_parquet(&self, path: &str) -> Result<Box<dyn std::io::Read + Send>> {
        let file = File::open(path)?;
        Ok(Box::new(BufReader::new(file)))
    }

    fn is_s3(&self) -> bool {
        false
    }
}

pub struct LocalOutputSink {
    base_path: String,
}

impl LocalOutputSink {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl crate::io::OutputSink for LocalOutputSink {
    async fn write_parquet(&self, path: &str) -> Result<Box<dyn std::io::Write + Send>> {
        let full_path = if path.starts_with("/") || path.contains("://") {
            path.to_string()
        } else {
            format!("{}/{}", self.base_path, path)
        };

        // Create parent directories
        if let Some(parent) = Path::new(&full_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(&full_path)?;
        Ok(Box::new(BufWriter::new(file)))
    }

    fn is_s3(&self) -> bool {
        false
    }
}
