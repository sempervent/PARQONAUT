//! Scan plan: resolved physical assets after target resolution.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use crate::DataFormat;

/// One file discovered during resolution (before deep inspection).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedAsset {
    pub path: Utf8PathBuf,
    pub format: DataFormat,
    pub size_bytes: u64,
}

/// Output of the resolution pipeline: physical assets ready for classification and rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPlan {
    /// Canonical root used for relative paths and dataset identity (directory or file parent).
    pub root: Utf8PathBuf,
    pub assets: Vec<ResolvedAsset>,
    /// True when enumeration stopped early because `ScanOptions.max_files` was reached.
    pub truncated: bool,
}
