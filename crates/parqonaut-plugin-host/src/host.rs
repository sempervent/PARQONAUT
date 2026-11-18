//! Application-facing plugin host: catalog resolution + scan phase execution.

use std::time::Instant;

use parqonaut_plugin_protocol::PluginExecutionPhase;
use parqonaut_types::{Finding, PluginExecutionRecord, ScanPluginMetadata};

use crate::catalog::{CatalogEntry, PluginCatalog, PluginCompatibility};
use crate::error::PluginHostError;
use crate::normalize::plugin_finding_to_host;
use crate::scan_execute::{CancelToken, PluginRuntimeConfig, ScanPluginExecutor};

#[derive(Debug, Clone)]
pub struct ResolvedPlugin {
    pub entry: CatalogEntry,
}

pub struct PluginHost {
    catalog: PluginCatalog,
    executor: ScanPluginExecutor,
}

impl PluginHost {
    pub fn new(catalog: PluginCatalog, runtime: PluginRuntimeConfig) -> Self {
        Self { catalog, executor: ScanPluginExecutor::new(runtime) }
    }

    pub fn catalog(&self) -> &PluginCatalog {
        &self.catalog
    }

    pub fn resolve_plugins(
        &self,
        names: &[String],
    ) -> Result<Vec<ResolvedPlugin>, PluginHostError> {
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let entry = self
                .catalog
                .get(name)
                .ok_or_else(|| PluginHostError::NotFound(name.clone()))?
                .clone();
            if entry.compatibility != PluginCompatibility::Compatible {
                let req = entry.manifest.requires_parqonaut.clone().unwrap_or_default();
                return Err(PluginHostError::HostVersionMismatch {
                    name: name.clone(),
                    requirement: req,
                });
            }
            out.push(ResolvedPlugin { entry });
        }
        Ok(out)
    }

    pub fn run_phase(
        &self,
        plugins: &[ResolvedPlugin],
        phase: PluginExecutionPhase,
        context: parqonaut_plugin_protocol::PluginScanContext,
        known_evidence: &std::collections::BTreeSet<String>,
        cancel: &CancelToken,
    ) -> Result<(Vec<Finding>, Vec<PluginExecutionRecord>), PluginHostError> {
        let mut findings = Vec::new();
        let mut executions = Vec::new();
        for plugin in plugins {
            let scan = match plugin.entry.manifest.capabilities.scan.as_ref() {
                Some(s) => s,
                None => continue,
            };
            if !scan.supported_phases.contains(&phase) {
                continue;
            }
            let start = Instant::now();
            let name = plugin.entry.manifest.name.clone();
            match self.executor.execute_scan(&plugin.entry, phase, context.clone(), cancel, None) {
                Ok((result, _stderr)) => {
                    let mut contributed = 0u64;
                    for c in &result.findings {
                        findings.push(plugin_finding_to_host(
                            &plugin.entry.manifest,
                            &plugin.entry.digest,
                            phase,
                            c,
                            known_evidence,
                        )?);
                        contributed += 1;
                    }
                    executions.push(PluginExecutionRecord {
                        name: name.clone(),
                        version: plugin.entry.manifest.version.clone(),
                        digest: plugin.entry.digest.clone(),
                        phase: phase_label(phase).into(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        status: "succeeded".into(),
                        findings_contributed: contributed,
                        error: None,
                    });
                }
                Err(e) => {
                    executions.push(PluginExecutionRecord {
                        name: name.clone(),
                        version: plugin.entry.manifest.version.clone(),
                        digest: plugin.entry.digest.clone(),
                        phase: phase_label(phase).into(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        status: "failed".into(),
                        findings_contributed: 0,
                        error: Some(e.to_string()),
                    });
                    return Err(e);
                }
            }
        }
        Ok((findings, executions))
    }
}

pub fn phase_label(phase: PluginExecutionPhase) -> &'static str {
    match phase {
        PluginExecutionPhase::PreScan => "pre_scan",
        PluginExecutionPhase::PostInventory => "post_inventory",
        PluginExecutionPhase::PostRules => "post_rules",
    }
}

pub fn merge_plugin_metadata(
    requested: &[String],
    resolved: &[String],
    executions: Vec<PluginExecutionRecord>,
) -> ScanPluginMetadata {
    ScanPluginMetadata {
        requested: requested.to_vec(),
        resolved_order: resolved.to_vec(),
        executions,
    }
}
