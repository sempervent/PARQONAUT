//! Dataset inventory models: files, partitions, and schema snapshots.

use camino::Utf8PathBuf;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::DataFormat;

/// How the engine grouped files into a logical Parquet dataset (co-location / inference signal).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GroupingKind {
    /// A single readable Parquet file (or lone member after splits).
    SingleFile,
    /// Grouped by path anchor heuristic only (see docs).
    PathAnchor,
    /// Same path anchor produced multiple schema signatures; split into separate datasets.
    PathAnchorSchemaSplit,
    /// Mixed schemas under one anchor were kept together because all files look like `part-*.parquet` shards.
    PathAnchorPartFilePattern,
}

/// Hive-style directory partitioning vs co-located files without partition columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PartitionLayout {
    /// No Hive `key=value` directory segments observed on member paths.
    Unpartitioned,
    /// At least one member path carries Hive-style partition directory segments.
    HiveDirectoryKeys,
}

/// Legacy partition scheme label (superseded by [`PartitionLayout`] + [`GroupingKind`] on [`Dataset`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PartitionScheme {
    None,
    Hive,
    InferredPathGrouping,
    Directory,
    Custom { name: String },
}

/// A single partition key/value pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct PartitionSegment {
    pub key: String,
    pub value: String,
}

/// One file discovered as part of a dataset inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DatasetFile {
    #[schema(value_type = String)]
    pub path: Utf8PathBuf,
    pub format: DataFormat,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<PartitionSegment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<u64>,
}

/// Placeholder for future column-level statistics and histograms.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ColumnProfile {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_fraction: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_estimate: Option<u64>,
}

/// Field definition within a schema snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FieldDefinition {
    pub name: String,
    pub logical_type: String,
    pub nullable: bool,
}

/// Captured schema view at scan time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SchemaSnapshot {
    pub fields: Vec<FieldDefinition>,
}

/// A logical dataset grouping one or more files under a single identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Dataset {
    pub dataset_id: String,
    /// Why these files were grouped together (path anchor, schema split, etc.).
    pub grouping_kind: GroupingKind,
    /// Whether Hive directory partition keys appear on paths (distinct from grouping).
    pub partition_layout: PartitionLayout,
    pub files: Vec<DatasetFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_profiles: Vec<ColumnProfile>,
}
