use serde::{Deserialize, Serialize};

use crate::safety::RepairSafety;

/// Reference to supporting evidence from a finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRef {
    pub finding_id: String,
    pub evidence_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

/// Precondition that must hold before an operation executes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Precondition {
    pub kind: String,
    pub description: String,
}

/// Expected measurable outcome after a repair operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpectedOutcome {
    pub metric: String,
    pub before: serde_json::Value,
    pub after_expected: serde_json::Value,
}

/// Concrete repair action (serializable, stable JSON shape).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RepairAction {
    Recompress {
        codec: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        target_paths: Vec<String>,
    },
    RebuildStatistics {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        target_paths: Vec<String>,
    },
    ResizeRowGroups {
        target_bytes: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        target_paths: Vec<String>,
    },
    MergeSmallFiles {
        target_bytes: u64,
        input_paths: Vec<String>,
    },
    SplitLargeFile {
        target_bytes: u64,
        input_path: String,
    },
    AlignSchema {
        field: String,
        from_type: String,
        to_type: String,
    },
    CastColumn {
        column: String,
        from_type: String,
        to_type: String,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    Repartition {
        strategy: String,
    },
}

impl RepairAction {
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::Recompress { .. } => "recompress",
            Self::RebuildStatistics { .. } => "rebuild_statistics",
            Self::ResizeRowGroups { .. } => "resize_row_groups",
            Self::MergeSmallFiles { .. } => "merge_small_files",
            Self::SplitLargeFile { .. } => "split_large_file",
            Self::AlignSchema { .. } => "align_schema",
            Self::CastColumn { .. } => "cast_column",
            Self::RenameColumn { .. } => "rename_column",
            Self::Repartition { .. } => "repartition",
        }
    }

    pub fn default_safety(&self) -> RepairSafety {
        match self {
            Self::Recompress { .. }
            | Self::RebuildStatistics { .. }
            | Self::ResizeRowGroups { .. }
            | Self::MergeSmallFiles { .. }
            | Self::SplitLargeFile { .. } => RepairSafety::Safe,
            Self::AlignSchema { .. }
            | Self::CastColumn { .. }
            | Self::RenameColumn { .. }
            | Self::Repartition { .. } => RepairSafety::ReviewRequired,
        }
    }
}
