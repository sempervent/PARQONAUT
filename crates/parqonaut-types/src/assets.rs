//! Per-asset accounting and inspection outcomes (execution truth, separate from findings).

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use crate::DataFormat;
use crate::FailureKind;

/// Terminal state of format-specific inspection for one discovered asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InspectionStatus {
    /// Footer or shallow probe completed without error.
    Inspected,
    /// Inspection was attempted and failed (I/O, parse, Parquet deserialize, etc.).
    Failed,
    /// No format-specific probe ran (for example unknown extension).
    Skipped,
}

/// Bounded shallow-text probe limits and honesty signals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ProbeMetadata {
    /// Bytes actually read from disk for this probe (may be less than file size).
    pub bytes_sampled: u64,
    /// Total file size when known from resolution metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size_bytes: Option<u64>,
    /// Character decoding policy applied to sampled bytes.
    pub encoding_assumption: String,
    /// Whether the probe covered the whole file within the cap or only a prefix.
    #[serde(rename = "probe_depth")]
    pub probe_depth: ProbeDepth,
    /// Parser / structural confidence for what was inferred.
    pub parse_confidence: ParseConfidence,
    /// Human notes (for example NDJSON first-record-only semantics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProbeDepth {
    /// Entire file was within the configured read cap.
    FullWithinCap,
    /// Only a leading slice of the file was read.
    PartialHead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParseConfidence {
    High,
    Partial,
    Low,
}

/// One resolved file with durable inspection outcome (distinct from forensic [`crate::Finding`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AssetRecord {
    #[schema(value_type = String, example = "fixtures/data.parquet")]
    pub path: Utf8PathBuf,
    pub format: DataFormat,
    pub size_bytes: u64,
    pub inspection_status: InspectionStatus,
    /// When status is `Failed`, machine-readable failure class.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<FailureKind>,
    /// When status is `Failed`, human-readable detail (may be truncated at persistence).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    /// When this asset is a member of an inferred Parquet dataset inventory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
    /// Populated for successful shallow text probes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<ProbeMetadata>,
    /// Shallow text hints JSON (delimiter, keys, etc.); Parquet remains in `Dataset` schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspection_hints: Option<serde_json::Value>,
}
