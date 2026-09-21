//! Plugin manifests, capabilities, and scan request/response payloads (protocol v1).

use std::collections::BTreeMap;

use paraclete_types::{DataFormat, FindingSeverity, ScanRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::version::PluginProtocolVersion;

/// Phases where a scan analyzer plugin may run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginExecutionPhase {
    PreScan,
    PostInventory,
    PostRules,
}

/// Scan analyzer capability block (distinct from batch transform).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScanAnalyzerCapabilities {
    pub supported_formats: Vec<DataFormat>,
    pub supported_phases: Vec<PluginExecutionPhase>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Batch transform capability (schema-preserving RecordBatch → RecordBatch in v1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub struct BatchTransformCapabilities {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Declares which extension classes a plugin implements (at least one required).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PluginCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<ScanAnalyzerCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_transform: Option<BatchTransformCapabilities>,
}

impl PluginCapabilities {
    pub fn has_any(&self) -> bool {
        self.scan.is_some() || self.batch_transform.is_some()
    }
}

/// Authoring metadata for a plugin package (`parqonaut-plugin.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PluginManifest {
    pub protocol_version: u32,
    pub name: String,
    pub version: String,
    pub entrypoint: String,
    pub capabilities: PluginCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_parqonaut: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl PluginManifest {
    pub fn validate(&self) -> Result<(), super::PluginProtocolError> {
        PluginProtocolVersion::validate(self.protocol_version)?;
        validate_plugin_name(&self.name)?;
        validate_plugin_version(&self.version)?;
        validate_entrypoint(&self.entrypoint)?;
        if let Some(req) = &self.requires_parqonaut {
            validate_requires_parqonaut(req)?;
        }
        if !self.capabilities.has_any() {
            return Err(super::PluginProtocolError::NoCapabilities);
        }
        if let Some(scan) = &self.capabilities.scan {
            validate_scan_phases(&scan.supported_phases)?;
        }
        Ok(())
    }
}

fn validate_plugin_version(version: &str) -> Result<(), super::PluginProtocolError> {
    let version = version.trim();
    if version.is_empty() {
        return Err(super::PluginProtocolError::EmptyPluginVersion);
    }
    semver::Version::parse(version)
        .map_err(|_| super::PluginProtocolError::InvalidPluginVersion(version.to_string()))?;
    Ok(())
}

fn validate_requires_parqonaut(req: &str) -> Result<(), super::PluginProtocolError> {
    let req = req.trim();
    if req.is_empty() {
        return Err(super::PluginProtocolError::InvalidRequiresParqonaut(
            "must not be empty".into(),
        ));
    }
    semver::VersionReq::parse(req)
        .map_err(|e| super::PluginProtocolError::InvalidRequiresParqonaut(e.to_string()))?;
    Ok(())
}

fn validate_scan_phases(phases: &[PluginExecutionPhase]) -> Result<(), super::PluginProtocolError> {
    let mut seen = std::collections::BTreeSet::new();
    for phase in phases {
        let key = format!("{phase:?}");
        if !seen.insert(key.clone()) {
            return Err(super::PluginProtocolError::DuplicateScanPhase(key));
        }
    }
    Ok(())
}

fn validate_plugin_name(name: &str) -> Result<(), super::PluginProtocolError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(super::PluginProtocolError::EmptyPluginName);
    }
    if name.contains(['/', '\\']) {
        return Err(super::PluginProtocolError::InvalidPluginName(
            "name must not contain path separators".into(),
        ));
    }
    let ok = name.chars().enumerate().all(|(i, c)| {
        c.is_ascii_lowercase() || c.is_ascii_digit() || (c == '-' && i > 0) || (c == '_' && i > 0)
    }) && name.chars().next().is_some_and(|c| c.is_ascii_lowercase());
    if ok {
        Ok(())
    } else {
        Err(super::PluginProtocolError::InvalidPluginName(name.to_string()))
    }
}

fn validate_entrypoint(entrypoint: &str) -> Result<(), super::PluginProtocolError> {
    let entrypoint = entrypoint.trim();
    if entrypoint.is_empty() {
        return Err(super::PluginProtocolError::InvalidEntrypoint(
            "entrypoint must not be empty".into(),
        ));
    }
    if entrypoint.contains([' ', '\t', '\n', ';', '|', '&']) {
        return Err(super::PluginProtocolError::InvalidEntrypoint(
            "entrypoint must be module:call, not a shell command".into(),
        ));
    }
    let Some((module, func)) = entrypoint.split_once(':') else {
        return Err(super::PluginProtocolError::InvalidEntrypoint(
            "entrypoint must use module:call syntax".into(),
        ));
    };
    if module.is_empty() || func.is_empty() {
        return Err(super::PluginProtocolError::InvalidEntrypoint(
            "entrypoint module and callable must be non-empty".into(),
        ));
    }
    Ok(())
}

/// Bounded scan state for analyzer plugins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginScanContext {
    pub request: ScanRequest,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub discovered_files: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inventory_summary: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builtin_finding_summaries: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hints: BTreeMap<String, serde_json::Value>,
}

/// A finding emitted by a plugin (host namespaces effective codes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PluginFindingContribution {
    pub code: String,
    pub severity: FindingSeverity,
    pub summary: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PluginResult {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<PluginFindingContribution>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, serde_json::Value>,
}

/// Scan analyzer request (JSON stdin to Python runner).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginRequest {
    pub protocol_version: u32,
    pub manifest: PluginManifest,
    pub phase: PluginExecutionPhase,
    pub context: PluginScanContext,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub config: BTreeMap<String, serde_json::Value>,
}

impl PluginRequest {
    pub fn validate(&self) -> Result<(), super::PluginProtocolError> {
        PluginProtocolVersion::validate(self.protocol_version)?;
        self.manifest.validate()
    }
}

/// Scan analyzer response (JSON stdout from Python runner).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PluginResponse {
    pub protocol_version: u32,
    pub plugin: String,
    pub result: PluginResult,
}

impl PluginResponse {
    pub fn validate(&self) -> Result<(), super::PluginProtocolError> {
        PluginProtocolVersion::validate(self.protocol_version)?;
        if self.plugin.trim().is_empty() {
            return Err(super::PluginProtocolError::EmptyPluginName);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PLUGIN_PROTOCOL_VERSION;

    #[test]
    fn rejects_unknown_protocol_version() {
        let err = PluginProtocolVersion::validate(2).unwrap_err();
        assert!(matches!(err, super::super::PluginProtocolError::ProtocolVersionMismatch { .. }));
    }

    #[test]
    fn rejects_manifest_without_capabilities() {
        let m = PluginManifest {
            protocol_version: PLUGIN_PROTOCOL_VERSION,
            name: "demo".into(),
            version: "0.1.0".into(),
            entrypoint: "demo:run".into(),
            capabilities: PluginCapabilities { scan: None, batch_transform: None },
            requires_parqonaut: None,
            metadata: BTreeMap::new(),
        };
        assert!(matches!(
            m.validate().unwrap_err(),
            super::super::PluginProtocolError::NoCapabilities
        ));
    }
}
