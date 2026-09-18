//! Namespaced finding codes (`system.*`, `plugin.<id>.*`, `user.*`).

use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Well-known system finding codes used by built-in rules.
pub mod system {
    pub const FORMAT_UNKNOWN: &str = "system.format.unknown";
    pub const FORMAT_MIXED_DATASET: &str = "system.format.mixed_dataset";
    pub const PARTITION_INCONSISTENT_KEYS: &str = "system.partition.inconsistent_keys";
    pub const METADATA_FILE_TOO_SMALL: &str = "system.metadata.file_too_small";
    pub const METADATA_ROW_GROUP_SUSPICIOUSLY_SMALL: &str =
        "system.metadata.row_group_suspiciously_small";
    pub const TARGET_SCAN_TRUNCATED: &str = "system.target.scan_truncated";
    pub const FORMAT_PARQUET_READ_FAILED: &str = "system.format.parquet_read_failed";
    pub const DATASET_MULTIPLE_DETECTED: &str = "system.dataset.multiple_datasets_detected";
    pub const PARTITION_UNPARTITIONED_COLLECTION: &str =
        "system.partition.unpartitioned_collection";
    pub const FORMAT_CSV_SHALLOW_INSPECTION: &str = "system.format.csv_shallow_inspection";
    pub const FORMAT_JSON_SHALLOW_INSPECTION: &str = "system.format.json_shallow_inspection";
    pub const FORMAT_TEXT_PROBE_BOUNDED: &str = "system.format.text_probe_bounded";
    pub const DATASET_GROUPING_AMBIGUOUS: &str = "system.dataset.grouping_ambiguous";
    pub const ASSET_INSPECTION_SKIPPED: &str = "system.asset.inspection_skipped";
    pub const ASSET_INSPECTION_PARTIAL: &str = "system.asset.inspection_partial";
}

/// Errors from [`FindingCode::try_new`].
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum FindingCodeError {
    #[error("finding code must not be empty")]
    Empty,
    #[error("finding code exceeds maximum length")]
    TooLong,
    #[error("invalid finding code namespace or shape: {0}")]
    InvalidShape(&'static str),
    #[error("invalid segment `{0}`: use lowercase letters, digits, and underscores only")]
    InvalidSegment(String),
}

/// Machine-facing finding code with enforced namespace policy.
///
/// Allowed forms:
/// - `system.<domain>.<name>[.<sub>...]` — at least three dot segments after `system`.
/// - `plugin.<plugin_id>.<name>[.<sub>...]` — `plugin_id` must be `[a-z0-9_-]+`.
/// - `user.<name>[.<sub>...]` — at least two segments total.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct FindingCode(pub String);

impl FindingCode {
    pub const MAX_LEN: usize = 256;

    /// Validates and wraps a code string.
    pub fn try_new(code: impl Into<String>) -> Result<Self, FindingCodeError> {
        let s = code.into();
        if s.is_empty() {
            return Err(FindingCodeError::Empty);
        }
        if s.len() > Self::MAX_LEN {
            return Err(FindingCodeError::TooLong);
        }
        validate_code_str(&s)?;
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for FindingCode {
    type Err = FindingCodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s)
    }
}

impl fmt::Display for FindingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn validate_segment(seg: &str) -> Result<(), FindingCodeError> {
    if seg.is_empty() {
        return Err(FindingCodeError::InvalidShape("empty segment"));
    }
    if !seg.chars().all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_')) {
        return Err(FindingCodeError::InvalidSegment(seg.into()));
    }
    Ok(())
}

fn validate_code_str(s: &str) -> Result<(), FindingCodeError> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() < 2 {
        return Err(FindingCodeError::InvalidShape("need at least two dot-separated segments"));
    }
    for p in &parts {
        validate_segment(p)?;
    }
    match parts[0] {
        "system" => {
            if parts.len() < 3 {
                return Err(FindingCodeError::InvalidShape(
                    "system codes require system.<domain>.<name>",
                ));
            }
        }
        "plugin" => {
            if parts.len() < 3 {
                return Err(FindingCodeError::InvalidShape(
                    "plugin codes require plugin.<plugin_id>.<name>",
                ));
            }
            if !parts[1].chars().all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_' | '-')) {
                return Err(FindingCodeError::InvalidSegment(parts[1].into()));
            }
        }
        "user" => {}
        _ => {
            return Err(FindingCodeError::InvalidShape(
                "code must start with system., plugin., or user.",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_system_and_plugin_and_user() {
        FindingCode::try_new(system::FORMAT_UNKNOWN).unwrap();
        FindingCode::try_new("plugin.demo.rule_x").unwrap();
        FindingCode::try_new("user.custom.check").unwrap();
    }

    #[test]
    fn rejects_uppercase_and_bad_plugin_id() {
        assert!(FindingCode::try_new("System.format.x").is_err());
        assert!(FindingCode::try_new("plugin.Demo.x").is_err());
        assert!(FindingCode::try_new("plugin..x").is_err());
    }
}
