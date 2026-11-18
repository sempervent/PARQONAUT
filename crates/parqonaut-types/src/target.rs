//! Scan targets: local paths, logical datasets, and reserved object-store placeholders.

use camino::Utf8PathBuf;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

/// High-level classification of what is being scanned.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    LocalFile,
    LocalDirectory,
    LogicalDataset,
    ObjectStore,
}

/// Normalized reference to a target independent of its kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum TargetReference {
    LocalPath(Utf8PathBuf),
    LogicalId(String),
    Uri(Url),
}

/// Logical dataset identifier used when files are resolved indirectly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LogicalDatasetTarget {
    pub dataset_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver_hint: Option<String>,
}

/// Placeholder for future object-store-backed scans (S3-compatible, GCS, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ObjectStoreTargetPlaceholder {
    pub uri: Url,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Concrete scan target with mutually consistent fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScanTarget {
    LocalFile {
        #[schema(value_type = String)]
        path: Utf8PathBuf,
    },
    LocalDirectory {
        #[schema(value_type = String)]
        path: Utf8PathBuf,
    },
    LogicalDataset {
        #[serde(flatten)]
        inner: LogicalDatasetTarget,
    },
    ObjectStorePlaceholder {
        #[serde(flatten)]
        inner: ObjectStoreTargetPlaceholder,
    },
}

impl ScanTarget {
    /// Returns the [`TargetKind`] for this target.
    pub fn target_kind(&self) -> TargetKind {
        match self {
            ScanTarget::LocalFile { .. } => TargetKind::LocalFile,
            ScanTarget::LocalDirectory { .. } => TargetKind::LocalDirectory,
            ScanTarget::LogicalDataset { .. } => TargetKind::LogicalDataset,
            ScanTarget::ObjectStorePlaceholder { .. } => TargetKind::ObjectStore,
        }
    }

    /// Returns a normalized [`TargetReference`] for fingerprinting and logging.
    pub fn reference(&self) -> TargetReference {
        match self {
            ScanTarget::LocalFile { path } | ScanTarget::LocalDirectory { path } => {
                TargetReference::LocalPath(path.clone())
            }
            ScanTarget::LogicalDataset { inner } => {
                TargetReference::LogicalId(inner.dataset_id.clone())
            }
            ScanTarget::ObjectStorePlaceholder { inner } => TargetReference::Uri(inner.uri.clone()),
        }
    }
}
