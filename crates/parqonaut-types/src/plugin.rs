//! Scan plugin execution metadata persisted on reports.

use serde::{Deserialize, Serialize};

/// One plugin invocation during a scan phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PluginExecutionRecord {
    pub name: String,
    pub version: String,
    pub digest: String,
    pub phase: String,
    pub duration_ms: u64,
    pub status: String,
    pub findings_contributed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Digest-pinned plugin identity stored on durable scan jobs (no filesystem paths).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ResolvedPluginSelection {
    pub name: String,
    pub version: String,
    pub protocol_version: u32,
    pub digest: String,
}

/// Ordered plugin selection and per-invocation outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ScanPluginMetadata {
    pub requested: Vec<String>,
    pub resolved_order: Vec<String>,
    pub executions: Vec<PluginExecutionRecord>,
}
