//! Structured findings: namespaced codes, evidence, locations, and fingerprints.

use std::collections::BTreeMap;

use camino::Utf8PathBuf;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::finding_code::FindingCode;
use crate::fingerprint::{finding_fingerprint_payload, sort_json_value};
use crate::PartitionSegment;

/// Severity guides triage ordering; it is not a substitute for human judgment.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

/// Taxonomy bucket for analytics, dashboards, and policy routing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingCategory {
    Format,
    Target,
    Schema,
    Partitioning,
    Metadata,
    Integrity,
    Quality,
    Performance,
    Coverage,
    Plugin,
}

/// Where a finding applies within a dataset surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FindingLocation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub file: Option<Utf8PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition: Vec<PartitionSegment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group: Option<u32>,
}

/// Stable evidence classification for archival and diffing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    FileSlice,
    ColumnStats,
    SchemaFragment,
    MetadataTable,
    ParquetFooter,
    PartitionLayout,
    Custom,
}

/// Stable location reference for evidence (paths as UTF-8 strings for JSON portability).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct EvidenceLocationRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition: Vec<PartitionSegment>,
}

/// Supplementary pointer-style references (labels, URIs, row-group hints).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvidenceReference {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// Evidence record: kind + summary + optional stable location + optional structured payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Evidence {
    pub id: Uuid,
    pub kind: EvidenceKind,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location_ref: Option<EvidenceLocationRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<EvidenceReference>,
}

/// Optional remediation guidance separate from neutral detail text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Recommendation {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Algorithm identifier for fingerprint digests.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FingerprintAlgorithm {
    /// Legacy fingerprint (unordered JSON); retained for decoding old reports only.
    V1CanonicalJsonSha256,
    /// Key-sorted canonical JSON over a defined subset of finding fields.
    V2CanonicalSortedJsonSha256,
}

/// Dedupe-friendly fingerprint derived from stable finding fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FindingFingerprint {
    pub algorithm: FingerprintAlgorithm,
    pub digest: String,
}

/// A single structured issue discovered during a scan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Finding {
    pub id: Uuid,
    pub code: FindingCode,
    pub severity: FindingSeverity,
    pub category: FindingCategory,
    pub summary: String,
    /// Long-form human-readable text (product term: “detail”).
    pub detail: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<Evidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<FindingLocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<Recommendation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<FindingFingerprint>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, serde_json::Value>,
}

impl Finding {
    /// Computes a stable fingerprint using V2 canonical sorted JSON over defined fields.
    pub fn compute_fingerprint(&self) -> FindingFingerprint {
        let payload = finding_fingerprint_payload(self);
        let sorted = sort_json_value(payload);
        let json =
            serde_json::to_string(&sorted).expect("finding fingerprint payload must serialize");
        let digest = hex::encode(Sha256::digest(json.as_bytes()));
        FindingFingerprint { algorithm: FingerprintAlgorithm::V2CanonicalSortedJsonSha256, digest }
    }
}
