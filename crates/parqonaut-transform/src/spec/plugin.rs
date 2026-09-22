//! Batch plugin resolution for transform specs (digest-pinned at compile time).

use std::path::PathBuf;

use parqonaut_plugin_host::{
    BatchResourcePolicy, CatalogEntry, PluginCatalog, PluginCompatibility, PluginHostError,
    PluginRuntimeConfig,
};
use parqonaut_plugin_protocol::PLUGIN_PROTOCOL_VERSION;
use serde::{Deserialize, Serialize};

use crate::error::{Result, TransformError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinnedBatchPlugin {
    pub name: String,
    pub version: String,
    pub digest: String,
    pub protocol_version: u32,
    pub config: serde_json::Value,
    pub plugin_root: String,
}

pub fn plugin_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(extra) = std::env::var("PARQONAUT_PLUGIN_ROOTS") {
        for part in extra.split(':') {
            if !part.is_empty() {
                roots.push(PathBuf::from(part));
            }
        }
    }
    if roots.is_empty() {
        roots.push(PathBuf::from("fixtures/plugins"));
    }
    roots
}

pub fn resolve_pinned_batch_plugin(
    name: &str,
    config: serde_json::Value,
) -> Result<PinnedBatchPlugin> {
    let policy = BatchResourcePolicy::default();
    let serialized =
        serde_json::to_vec(&config).map_err(|e| TransformError::SpecError(e.to_string()))?;
    if serialized.len() > policy.max_config_bytes {
        return Err(TransformError::SpecError(format!(
            "plugin config exceeds max size {} bytes",
            policy.max_config_bytes
        )));
    }
    let catalog = PluginCatalog::discover_from_roots(&plugin_roots()).map_err(map_plugin_err)?;
    let entry = catalog
        .get(name)
        .ok_or_else(|| TransformError::SpecError(format!("unknown plugin: {name}")))?;
    if entry.compatibility != PluginCompatibility::Compatible {
        return Err(TransformError::SpecError(format!(
            "plugin {name} is not compatible with this PARQONAUT version"
        )));
    }
    if entry.manifest.protocol_version != PLUGIN_PROTOCOL_VERSION {
        return Err(TransformError::SpecError(format!("plugin {name} protocol version mismatch")));
    }
    entry.manifest.capabilities.batch_transform.as_ref().ok_or_else(|| {
        TransformError::SpecError(format!(
            "plugin {name} does not declare batch_transform capability"
        ))
    })?;
    Ok(PinnedBatchPlugin {
        name: entry.manifest.name.clone(),
        version: entry.manifest.version.clone(),
        digest: entry.digest.clone(),
        protocol_version: entry.manifest.protocol_version,
        config,
        plugin_root: entry.root.to_string_lossy().into_owned(),
    })
}

pub fn verify_pinned_plugin(pinned: &PinnedBatchPlugin) -> Result<CatalogEntry> {
    let catalog = PluginCatalog::discover_from_roots(&plugin_roots()).map_err(map_plugin_err)?;
    let entry = catalog
        .get(&pinned.name)
        .ok_or_else(|| TransformError::SpecError(format!("unknown plugin: {}", pinned.name)))?;
    if entry.digest != pinned.digest {
        return Err(TransformError::SpecError(format!(
            "STALE PLUGIN: plan digest {} != current {}",
            pinned.digest, entry.digest
        )));
    }
    Ok(entry.clone())
}

pub fn plugin_runtime_config() -> PluginRuntimeConfig {
    PluginRuntimeConfig::default()
}

pub fn map_plugin_err(err: PluginHostError) -> TransformError {
    TransformError::SpecError(err.to_string())
}
