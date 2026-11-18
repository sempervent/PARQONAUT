//! Map plugin contributions into host findings with namespaced codes and provenance.

use std::collections::{BTreeMap, BTreeSet};

use parqonaut_plugin_protocol::{PluginExecutionPhase, PluginFindingContribution, PluginManifest};
use parqonaut_types::{Finding, FindingCategory, FindingCode, FindingCodeError};
use uuid::Uuid;

use crate::error::PluginHostError;

pub fn namespaced_code(plugin_name: &str, raw: &str) -> Result<String, PluginHostError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(PluginHostError::InvalidResponse("finding code must not be empty".into()));
    }
    if raw.starts_with("system.") || raw.starts_with("plugin.") || raw.starts_with("user.") {
        return Err(PluginHostError::InvalidResponse(
            "plugin may not emit built-in namespace codes".into(),
        ));
    }
    let tail = raw
        .split('.')
        .map(|seg| seg.to_ascii_lowercase().replace('-', "_"))
        .collect::<Vec<_>>()
        .join(".");
    let plugin_id = plugin_name.replace('-', "_");
    Ok(format!("plugin.{plugin_id}.{tail}"))
}

pub fn plugin_finding_to_host(
    manifest: &PluginManifest,
    digest: &str,
    phase: PluginExecutionPhase,
    contribution: &PluginFindingContribution,
    known_evidence: &BTreeSet<String>,
) -> Result<Finding, PluginHostError> {
    for eid in &contribution.evidence_ids {
        if !known_evidence.contains(eid) {
            return Err(PluginHostError::InvalidEvidence(eid.clone()));
        }
    }
    let original_code = contribution.code.clone();
    let effective = namespaced_code(&manifest.name, &original_code)?;
    let code = FindingCode::try_new(&effective).map_err(|e: FindingCodeError| {
        PluginHostError::InvalidResponse(format!("namespaced code invalid: {e}"))
    })?;
    let mut attributes = BTreeMap::new();
    attributes.insert(
        "plugin".into(),
        serde_json::json!({
            "name": manifest.name,
            "version": manifest.version,
            "digest": digest,
            "phase": phase,
            "original_code": original_code,
        }),
    );
    Ok(Finding {
        id: Uuid::new_v4(),
        code,
        severity: contribution.severity,
        category: FindingCategory::Plugin,
        summary: contribution.summary.clone(),
        detail: contribution.detail.clone(),
        evidence: Vec::new(),
        locations: Vec::new(),
        recommendations: Vec::new(),
        fingerprint: None,
        attributes,
    })
}
