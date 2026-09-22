//! Scan analyzer plugin bridge into `paraclete-core`.

use std::collections::BTreeSet;
use std::path::PathBuf;

use paraclete_core::ScanPluginHost;
use paraclete_types::{Finding, PluginExecutionRecord};
use parqonaut_plugin_host::{
    CancelToken, PluginCatalog, PluginHost, PluginRuntimeConfig, ResolvedPlugin,
};
use parqonaut_plugin_protocol::{PluginExecutionPhase, PluginScanContext};

use crate::error::ApplicationError;

pub fn default_plugin_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(extra) = std::env::var("PARQONAUT_PLUGIN_ROOTS") {
        for part in extra.split(':').filter(|p| !p.is_empty()) {
            roots.push(PathBuf::from(part));
        }
    }
    if let Some(config) = dirs::config_dir() {
        roots.push(config.join("parqonaut").join("plugins"));
    }
    roots
}

pub struct ScanPluginBridge {
    host: PluginHost,
    resolved: Vec<ResolvedPlugin>,
    _requested: Vec<String>,
    resolved_order: Vec<String>,
    cancel: CancelToken,
}

impl ScanPluginBridge {
    pub fn new(requested: Vec<String>) -> Result<Self, ApplicationError> {
        let roots = default_plugin_roots();
        let catalog = PluginCatalog::discover_from_roots(&roots)
            .map_err(|e| ApplicationError::PluginHost(e.to_string()))?;
        let runtime = PluginRuntimeConfig::default();
        let host = PluginHost::new(catalog, runtime);
        let resolved = host
            .resolve_plugins(&requested)
            .map_err(|e| ApplicationError::PluginHost(e.to_string()))?;
        let resolved_order: Vec<String> =
            resolved.iter().map(|p| p.entry.manifest.name.clone()).collect();
        Ok(Self { host, resolved, _requested: requested, resolved_order, cancel: CancelToken::new() })
    }

    pub fn from_catalog(
        catalog: PluginCatalog,
        requested: Vec<String>,
        runtime: PluginRuntimeConfig,
    ) -> Result<Self, ApplicationError> {
        let host = PluginHost::new(catalog, runtime);
        let resolved = host
            .resolve_plugins(&requested)
            .map_err(|e| ApplicationError::PluginHost(e.to_string()))?;
        let resolved_order: Vec<String> =
            resolved.iter().map(|p| p.entry.manifest.name.clone()).collect();
        Ok(Self { host, resolved, _requested: requested, resolved_order, cancel: CancelToken::new() })
    }
}

impl ScanPluginHost for ScanPluginBridge {
    fn run_phase(
        &self,
        phase: PluginExecutionPhase,
        context: PluginScanContext,
        known_evidence: &BTreeSet<String>,
    ) -> Result<(Vec<Finding>, Vec<PluginExecutionRecord>), paraclete_core::CoreError> {
        self.host
            .run_phase(&self.resolved, phase, context, known_evidence, &self.cancel)
            .map_err(|e| paraclete_core::CoreError::PluginHost(e.to_string()))
    }

    fn resolved_plugin_order(&self) -> &[String] {
        &self.resolved_order
    }
}
