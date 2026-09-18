use serde::{Deserialize, Serialize};

/// Effective repair policy recorded in every generated plan for reproducibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairPolicy {
    pub default_compression: String,
    pub target_row_group_mb: u64,
    pub small_file_threshold_mb: u64,
    pub merge_target_mb: u64,
    /// Minimum parquet files before small-file consolidation is considered.
    pub min_files_for_merge: usize,
    /// Minimum distinct codecs before inconsistent-compression is diagnosed.
    pub min_codecs_for_inconsistency: usize,
}

impl Default for RepairPolicy {
    fn default() -> Self {
        Self {
            default_compression: "zstd".into(),
            target_row_group_mb: 128,
            small_file_threshold_mb: 16,
            merge_target_mb: 256,
            min_files_for_merge: 4,
            min_codecs_for_inconsistency: 2,
        }
    }
}

impl RepairPolicy {
    pub fn small_file_threshold_bytes(&self) -> u64 {
        self.small_file_threshold_mb * 1024 * 1024
    }

    pub fn merge_target_bytes(&self) -> u64 {
        self.merge_target_mb * 1024 * 1024
    }

    pub fn target_row_group_bytes(&self) -> u64 {
        self.target_row_group_mb * 1024 * 1024
    }

    /// Load policy from optional TOML string; falls back to defaults on missing sections.
    pub fn from_toml_optional(toml: Option<&str>) -> Result<Self, toml::de::Error> {
        let Some(raw) = toml else {
            return Ok(Self::default());
        };
        let table: toml::Table = toml::from_str(raw)?;
        let repair = table.get("repair");
        let mut policy = Self::default();
        if let Some(section) = repair.and_then(|v| v.as_table()) {
            if let Some(v) = section.get("default_compression").and_then(|v| v.as_str()) {
                policy.default_compression = v.to_string();
            }
            if let Some(v) = section.get("target_row_group_mb").and_then(|v| v.as_integer()) {
                policy.target_row_group_mb = v as u64;
            }
            if let Some(v) = section.get("small_file_threshold_mb").and_then(|v| v.as_integer()) {
                policy.small_file_threshold_mb = v as u64;
            }
            if let Some(v) = section.get("merge_target_mb").and_then(|v| v.as_integer()) {
                policy.merge_target_mb = v as u64;
            }
        }
        Ok(policy)
    }
}
