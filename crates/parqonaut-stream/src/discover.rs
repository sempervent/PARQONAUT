use crate::error::Result;
use globwalk::GlobWalkerBuilder;
use std::path::{Path, PathBuf};
use tracing::{debug, info};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct InputFile {
    pub path: PathBuf,
    pub format: FileFormat,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileFormat {
    Csv,
    Parquet,
}

impl FileFormat {
    pub fn from_extension(path: &Path) -> Option<Self> {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("csv") | Some("tsv") => Some(FileFormat::Csv),
            Some("parquet") => Some(FileFormat::Parquet),
            _ => None,
        }
    }
}

pub struct DiscoveryConfig {
    pub recursive: bool,
    pub follow_symlinks: bool,
    pub max_depth: Option<usize>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self { recursive: true, follow_symlinks: false, max_depth: None }
    }
}

pub fn discover_inputs(inputs: &[String], config: &DiscoveryConfig) -> Result<Vec<InputFile>> {
    let mut discovered = Vec::new();

    for input in inputs {
        if input == "-" {
            // Handle stdin
            discovered.push(InputFile {
                path: PathBuf::from("-"),
                format: FileFormat::Csv, // Assume CSV for stdin
                size: 0,                 // Unknown size for stdin
            });
            continue;
        }

        let path = PathBuf::from(input);

        if path.is_file() {
            // Single file
            if let Some(format) = FileFormat::from_extension(&path) {
                let size = std::fs::metadata(&path)?.len();
                discovered.push(InputFile { path, format, size });
            } else {
                debug!("Skipping file with unsupported extension: {}", path.display());
            }
        } else if path.is_dir() {
            // Directory - discover files recursively
            let files = discover_directory(&path, config)?;
            discovered.extend(files);
        } else {
            // Try as glob pattern
            let files = discover_glob(input, config)?;
            discovered.extend(files);
        }
    }

    // Remove duplicates and sort
    discovered.sort_by(|a, b| a.path.cmp(&b.path));
    discovered.dedup_by(|a, b| a.path == b.path);

    info!("Discovered {} input files", discovered.len());
    for file in &discovered {
        debug!("  {} ({}, {} bytes)", file.path.display(), format_name(&file.format), file.size);
    }

    Ok(discovered)
}

fn discover_directory(dir: &Path, config: &DiscoveryConfig) -> Result<Vec<InputFile>> {
    let mut files = Vec::new();

    let walker = WalkDir::new(dir)
        .follow_links(config.follow_symlinks)
        .max_depth(config.max_depth.unwrap_or(usize::MAX));

    for entry in walker {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(format) = FileFormat::from_extension(path) {
                let size = entry.metadata()?.len();
                files.push(InputFile { path: path.to_path_buf(), format, size });
            }
        }
    }

    Ok(files)
}

fn discover_glob(pattern: &str, config: &DiscoveryConfig) -> Result<Vec<InputFile>> {
    let mut files = Vec::new();

    let walker = GlobWalkerBuilder::from_patterns(".", &[pattern])
        .follow_links(config.follow_symlinks)
        .build()?;

    for entry in walker {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(format) = FileFormat::from_extension(path) {
                let size = entry.metadata()?.len();
                files.push(InputFile { path: path.to_path_buf(), format, size });
            }
        }
    }

    Ok(files)
}

fn format_name(format: &FileFormat) -> &'static str {
    match format {
        FileFormat::Csv => "CSV",
        FileFormat::Parquet => "Parquet",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_discover_single_file() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("test.csv");
        fs::write(&csv_file, "a,b,c\n1,2,3\n").unwrap();

        let inputs = vec![csv_file.to_string_lossy().to_string()];
        let config = DiscoveryConfig::default();
        let discovered = discover_inputs(&inputs, &config).unwrap();

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].format, FileFormat::Csv);
    }

    #[test]
    fn test_discover_directory() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("test.csv");
        let parquet_file = temp_dir.path().join("test.parquet");

        fs::write(&csv_file, "a,b,c\n1,2,3\n").unwrap();
        fs::write(&parquet_file, "fake parquet data").unwrap();

        let inputs = vec![temp_dir.path().to_string_lossy().to_string()];
        let config = DiscoveryConfig::default();
        let discovered = discover_inputs(&inputs, &config).unwrap();

        assert_eq!(discovered.len(), 2);
        assert!(discovered.iter().any(|f| f.format == FileFormat::Csv));
        assert!(discovered.iter().any(|f| f.format == FileFormat::Parquet));
    }
}
