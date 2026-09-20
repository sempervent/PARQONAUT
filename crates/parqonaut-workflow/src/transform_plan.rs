use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const TRANSFORM_SPEC_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformSpecVersion(pub u32);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransformOperation {
    Rewrite { compression: Option<String>, row_group_size_mb: Option<u64> },
    Partition { columns: Vec<String> },
    Merge { row_group_size_mb: Option<u64> },
    Split { target_size_mb: Option<u64>, target_row_groups: Option<usize> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformPlan {
    pub schema_version: u32,
    pub input: String,
    pub output: String,
    pub steps: Vec<TransformOperation>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformReport {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    pub operations_attempted: u64,
    pub operations_completed: u64,
    #[serde(default)]
    pub source_locations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_destination: Option<String>,
    #[serde(default)]
    pub intermediate_locations: Vec<String>,
    #[serde(default)]
    pub input_files: Vec<String>,
    #[serde(default)]
    pub output_files: Vec<String>,
    #[serde(default)]
    pub rows_read: u64,
    #[serde(default)]
    pub rows_written: u64,
    #[serde(default)]
    pub files_read: u64,
    #[serde(default)]
    pub files_written: u64,
    #[serde(default)]
    pub bytes_read: u64,
    #[serde(default)]
    pub bytes_written: u64,
    /// Parquet files created under spec intermediate staging (v0.9).
    #[serde(default)]
    pub intermediate_files_created: u64,
    #[serde(default)]
    pub intermediate_bytes_written: u64,
    #[serde(default)]
    pub intermediate_files_read: u64,
    #[serde(default)]
    pub intermediate_bytes_read: u64,
    /// Legacy totals retained for older `--json` consumers.
    #[serde(default)]
    pub rows: u64,
    #[serde(default)]
    pub bytes: u64,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub failures: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<Value>,
}
