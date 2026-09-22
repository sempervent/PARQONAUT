//! Durable scan job payload (plugin-free v0.9 jobs remain deserializable).

use parqonaut_types::{
    RedactionPolicy, ResolvedPluginSelection, ScanOptions, ScanProfile, ScanTarget,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const SCAN_JOB_PAYLOAD_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanJobPayload {
    #[serde(default = "default_payload_version")]
    pub schema_version: u32,
    pub target: ScanTarget,
    pub profile: ScanProfile,
    #[serde(default)]
    pub options: ScanOptions,
    #[serde(default)]
    pub scan_id: Option<Uuid>,
    #[serde(default)]
    pub redaction: Option<RedactionPolicy>,
    /// Requested plugin names (order preserved).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    /// Digest-pinned identities for durable execution (v0.10+).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_plugins: Vec<ResolvedPluginSelection>,
}

fn default_payload_version() -> u32 {
    1
}

impl ScanJobPayload {
    pub fn from_http(
        target: ScanTarget,
        profile: ScanProfile,
        options: ScanOptions,
        scan_id: Option<Uuid>,
        redaction: Option<RedactionPolicy>,
        plugins: Vec<String>,
        resolved_plugins: Vec<ResolvedPluginSelection>,
    ) -> Self {
        let schema_version = if resolved_plugins.is_empty() && plugins.is_empty() {
            1
        } else {
            SCAN_JOB_PAYLOAD_VERSION
        };
        Self {
            schema_version,
            target,
            profile,
            options,
            scan_id,
            redaction,
            plugins,
            resolved_plugins,
        }
    }
}

/// Parse stored job JSON (legacy `StartScanRequest` or v2 payload).
pub fn parse_scan_job_payload(json: &str) -> Result<ScanJobPayload, serde_json::Error> {
    if let Ok(p) = serde_json::from_str::<ScanJobPayload>(json) {
        return Ok(p);
    }
    #[derive(Deserialize)]
    struct Legacy {
        target: ScanTarget,
        profile: ScanProfile,
        #[serde(default)]
        options: ScanOptions,
        #[serde(default)]
        scan_id: Option<Uuid>,
        #[serde(default)]
        redaction: Option<RedactionPolicy>,
    }
    let leg: Legacy = serde_json::from_str(json)?;
    Ok(ScanJobPayload::from_http(
        leg.target,
        leg.profile,
        leg.options,
        leg.scan_id,
        leg.redaction,
        Vec::new(),
        Vec::new(),
    ))
}
