//! Operator-controlled server plugin policy and catalog (no CLI home discovery).

use std::collections::BTreeSet;
use std::path::PathBuf;

use parqonaut_plugin_host::{
    PluginCatalog, PluginCompatibility, PluginHost, PluginHostError, PluginRuntimeConfig,
    ResolvedPlugin, HOST_PARQONAUT_VERSION,
};
use parqonaut_plugin_protocol::PLUGIN_PROTOCOL_VERSION;
use parqonaut_types::ResolvedPluginSelection;

use crate::error::ApplicationError;

/// Server plugin execution gate and allowlist (disabled by default).
#[derive(Debug, Clone, Default)]
pub struct ServerPluginPolicy {
    pub enabled: bool,
    pub roots: Vec<PathBuf>,
    pub allowed: BTreeSet<String>,
    pub runtime: PluginRuntimeConfig,
}

impl ServerPluginPolicy {
    pub fn from_env() -> Self {
        let enabled = std::env::var("PARQONAUT_SERVER_PLUGINS_ENABLED")
            .ok()
            .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"));
        let roots = std::env::var("PARQONAUT_SERVER_PLUGIN_ROOTS")
            .ok()
            .map(|s| s.split(':').filter(|p| !p.is_empty()).map(PathBuf::from).collect::<Vec<_>>())
            .unwrap_or_default();
        let allowed = std::env::var("PARQONAUT_SERVER_ALLOWED_PLUGINS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        Self { enabled, roots, allowed, runtime: PluginRuntimeConfig::default() }
    }
}

/// Validated server catalog (discovery only at bootstrap).
#[derive(Debug, Clone)]
pub struct ServerPluginState {
    policy: ServerPluginPolicy,
    catalog: PluginCatalog,
}

impl ServerPluginState {
    pub fn bootstrap(policy: ServerPluginPolicy) -> Result<Self, ApplicationError> {
        if !policy.enabled {
            return Ok(Self { policy, catalog: PluginCatalog::new() });
        }
        if policy.roots.is_empty() {
            return Err(ApplicationError::InvalidRequest(
                "server plugins enabled but no plugin roots configured".into(),
            ));
        }
        let catalog = PluginCatalog::discover_from_roots(&policy.roots)
            .map_err(|e| ApplicationError::PluginHost(e.to_string()))?;
        for name in &policy.allowed {
            let Some(entry) = catalog.get(name) else {
                return Err(ApplicationError::InvalidRequest(format!(
                    "allowlisted plugin not found: {name}"
                )));
            };
            if entry.compatibility != PluginCompatibility::Compatible {
                return Err(ApplicationError::PluginIncompatible(format!(
                    "plugin {name} requires {}",
                    entry.manifest.requires_parqonaut.as_deref().unwrap_or("unknown")
                )));
            }
            if entry.manifest.capabilities.scan.is_none() {
                return Err(ApplicationError::PluginIncompatible(format!(
                    "plugin {name} does not support scan analyzers"
                )));
            }
        }
        Ok(Self { policy, catalog })
    }

    pub fn disabled() -> Self {
        Self { policy: ServerPluginPolicy::default(), catalog: PluginCatalog::new() }
    }

    pub fn policy(&self) -> &ServerPluginPolicy {
        &self.policy
    }

    pub fn plugins_enabled(&self) -> bool {
        self.policy.enabled
    }

    pub fn allowed_catalog_entries(&self) -> Vec<&parqonaut_plugin_host::CatalogEntry> {
        if !self.policy.enabled {
            return Vec::new();
        }
        self.policy.allowed.iter().filter_map(|n| self.catalog.get(n)).collect()
    }

    pub fn get_allowed_entry(&self, name: &str) -> Option<&parqonaut_plugin_host::CatalogEntry> {
        if !self.policy.enabled || !self.policy.allowed.contains(name) {
            return None;
        }
        self.catalog.get(name)
    }

    pub fn validate_request(&self, names: &[String]) -> Result<(), ApplicationError> {
        if names.is_empty() {
            return Ok(());
        }
        if !self.policy.enabled {
            return Err(ApplicationError::PluginExecutionDisabled);
        }
        for name in names {
            if !self.policy.allowed.contains(name) {
                return Err(ApplicationError::PluginNotAllowed(name.clone()));
            }
            let Some(entry) = self.catalog.get(name) else {
                return Err(ApplicationError::PluginNotFound(name.clone()));
            };
            if entry.compatibility != PluginCompatibility::Compatible {
                return Err(ApplicationError::PluginIncompatible(name.clone()));
            }
            if entry.manifest.capabilities.scan.is_none() {
                return Err(ApplicationError::PluginIncompatible(name.clone()));
            }
        }
        Ok(())
    }

    pub fn resolve_for_enqueue(
        &self,
        names: &[String],
    ) -> Result<Vec<ResolvedPluginSelection>, ApplicationError> {
        self.validate_request(names)?;
        let host = PluginHost::new(self.catalog.clone(), self.policy.runtime.clone());
        let resolved = host.resolve_plugins(names).map_err(map_host_err)?;
        Ok(resolved.iter().map(selection_from_resolved).collect())
    }

    pub fn revalidate_pinned(
        &self,
        pinned: &[ResolvedPluginSelection],
    ) -> Result<Vec<ResolvedPlugin>, ApplicationError> {
        if pinned.is_empty() {
            return Ok(Vec::new());
        }
        if !self.policy.enabled {
            return Err(ApplicationError::PluginExecutionDisabled);
        }
        let fresh = PluginCatalog::discover_from_roots(&self.policy.roots)
            .map_err(|e| ApplicationError::PluginHost(e.to_string()))?;
        let mut out = Vec::with_capacity(pinned.len());
        for pin in pinned {
            if !self.policy.allowed.contains(&pin.name) {
                return Err(ApplicationError::PluginNotAllowed(pin.name.clone()));
            }
            let Some(entry) = fresh.get(&pin.name) else {
                return Err(ApplicationError::PluginNotFound(pin.name.clone()));
            };
            if entry.manifest.version != pin.version
                || entry.manifest.protocol_version != pin.protocol_version
                || entry.digest != pin.digest
            {
                return Err(ApplicationError::PluginStale {
                    name: pin.name.clone(),
                    expected: pin.digest.clone(),
                    actual: entry.digest.clone(),
                });
            }
            if entry.compatibility != PluginCompatibility::Compatible {
                return Err(ApplicationError::PluginIncompatible(pin.name.clone()));
            }
            out.push(ResolvedPlugin { entry: entry.clone() });
        }
        Ok(out)
    }

    pub fn catalog(&self) -> &PluginCatalog {
        &self.catalog
    }

    pub fn runtime(&self) -> &PluginRuntimeConfig {
        &self.policy.runtime
    }
}

pub fn selection_from_resolved(p: &ResolvedPlugin) -> ResolvedPluginSelection {
    ResolvedPluginSelection {
        name: p.entry.manifest.name.clone(),
        version: p.entry.manifest.version.clone(),
        protocol_version: p.entry.manifest.protocol_version,
        digest: p.entry.digest.clone(),
    }
}

fn map_host_err(e: PluginHostError) -> ApplicationError {
    match e {
        PluginHostError::NotFound(n) => ApplicationError::PluginNotFound(n),
        PluginHostError::HostVersionMismatch { name, .. } => {
            ApplicationError::PluginIncompatible(name)
        }
        PluginHostError::StalePlugin { expected, actual, .. } => {
            ApplicationError::PluginStale { name: String::new(), expected, actual }
        }
        PluginHostError::Cancelled => ApplicationError::PluginCancelled,
        other => ApplicationError::PluginHost(other.to_string()),
    }
}

pub fn public_capabilities_summary(
    entry: &parqonaut_plugin_host::CatalogEntry,
) -> serde_json::Value {
    serde_json::json!({
        "scan": entry.manifest.capabilities.scan.is_some(),
        "batch_transform": entry.manifest.capabilities.batch_transform.is_some(),
        "host_parqonaut": HOST_PARQONAUT_VERSION,
        "protocol_version": PLUGIN_PROTOCOL_VERSION,
    })
}
