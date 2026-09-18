//! Scan requests, profiles, and execution options.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DataFormat, ScanTarget};

/// Coarse scan depth presets.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ScanProfile {
    Quick,
    Standard,
    Deep,
    Baseline,
}

/// How a scan should treat incremental state (placeholder for later phases).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    Default,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    #[default]
    Full,
    Incremental,
}

/// Tunables that influence engine behavior without changing the target identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ScanOptions {
    #[serde(default)]
    pub mode: ScanMode,
    /// Maximum files to enumerate in this scan (safety valve).
    #[serde(default = "default_max_files")]
    pub max_files: u64,
    /// Optional hint to restrict detection to a subset of formats.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub format_hints: Vec<DataFormat>,
}

fn default_max_files() -> u64 {
    100_000
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self { mode: ScanMode::Full, max_files: default_max_files(), format_hints: Vec::new() }
    }
}

/// A complete request to execute a scan against a target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ScanRequest {
    pub scan_id: Uuid,
    pub target: ScanTarget,
    pub profile: ScanProfile,
    #[serde(default)]
    pub options: ScanOptions,
}

impl ScanRequest {
    /// Builds a new [`ScanRequest`] with a random scan id.
    pub fn new(target: ScanTarget, profile: ScanProfile) -> Self {
        Self { scan_id: Uuid::new_v4(), target, profile, options: ScanOptions::default() }
    }
}

/// Stable string identifier for a scan profile (for logs and manifests).
pub fn scan_profile_id(profile: ScanProfile) -> &'static str {
    match profile {
        ScanProfile::Quick => "quick",
        ScanProfile::Standard => "standard",
        ScanProfile::Deep => "deep",
        ScanProfile::Baseline => "baseline",
    }
}
