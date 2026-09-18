//! Plugin manifests, capabilities, and request/response payloads.

use std::collections::BTreeMap;

use paraclete_types::{DataFormat, FindingSeverity, ScanRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Phases where a plugin may participate in later engine versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginExecutionPhase {
    PreScan,
    PostInventory,
    PostRules,
}

/// Declares what a plugin can consume and emit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PluginCapabilities {
    pub supported_formats: Vec<DataFormat>,
    pub supported_phases: Vec<PluginExecutionPhase>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Authoring metadata for a plugin package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub entrypoint: String,
    pub capabilities: PluginCapabilities,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl PluginManifest {
    /// Validates basic manifest invariants.
    pub fn validate(&self) -> Result<(), super::PluginProtocolError> {
        if self.name.trim().is_empty() {
            return Err(super::PluginProtocolError::EmptyPluginName);
        }
        if self.version.trim().is_empty() {
            return Err(super::PluginProtocolError::EmptyPluginVersion);
        }
        Ok(())
    }
}

/// Normalized slice of scan state passed to plugins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginScanContext {
    pub request: ScanRequest,
    /// UTF-8 filesystem paths as strings for portable JSON and future schema tooling.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub discovered_files: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hints: BTreeMap<String, serde_json::Value>,
}

/// A finding emitted by a plugin, using string codes for forward compatibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginFindingContribution {
    pub code: String,
    pub severity: FindingSeverity,
    pub summary: String,
    pub detail: String,
    /// Evidence identifiers as canonical UUID strings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<String>,
}

/// Typed outcome envelope for plugin execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginResult {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<PluginFindingContribution>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, serde_json::Value>,
}

/// Request passed from engine to plugin host in a future phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginRequest {
    pub manifest: PluginManifest,
    pub phase: PluginExecutionPhase,
    pub context: PluginScanContext,
}

/// Response returned from plugin execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginResponse {
    pub plugin: String,
    pub result: PluginResult,
}
