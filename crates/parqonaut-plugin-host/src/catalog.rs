use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use parqonaut_plugin_protocol::{PluginManifest, MANIFEST_FILENAME};
use semver::{Version, VersionReq};

use crate::digest::compute_plugin_digest;
use crate::error::PluginHostError;
use crate::path_safety::{canonical_plugin_root, ensure_within_root};

/// Host PARQONAUT semver (workspace release).
pub const HOST_PARQONAUT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Compatibility evaluation for catalog listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginCompatibility {
    Compatible,
    HostVersionMismatch,
}

/// Resolved plugin entry in the host catalog (discovery only; does not execute).
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub manifest: PluginManifest,
    pub root: PathBuf,
    pub digest: String,
    pub compatibility: PluginCompatibility,
}

/// Deterministic plugin discovery from configured roots.
#[derive(Debug, Default)]
pub struct PluginCatalog {
    entries: BTreeMap<String, CatalogEntry>,
}

impl PluginCatalog {
    pub fn new() -> Self {
        Self { entries: BTreeMap::new() }
    }

    pub fn get(&self, name: &str) -> Option<&CatalogEntry> {
        self.entries.get(name)
    }

    pub fn list(&self) -> impl Iterator<Item = (&str, &CatalogEntry)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &CatalogEntry)> {
        self.list()
    }

    /// Load manifests from immediate child directories of each configured root.
    pub fn discover_from_roots(roots: &[PathBuf]) -> Result<Self, PluginHostError> {
        let mut sorted_roots = roots.to_vec();
        sorted_roots.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));

        let host_version =
            Version::parse(HOST_PARQONAUT_VERSION).expect("host version must be semver");

        let mut catalog = Self::new();
        for root in sorted_roots {
            if !root.is_dir() {
                continue;
            }
            let canonical_root = canonical_plugin_root(&root)?;
            let mut children: Vec<PathBuf> = fs::read_dir(&canonical_root)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect();
            children.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

            for plugin_dir in children {
                let manifest_path = plugin_dir.join(MANIFEST_FILENAME);
                if !manifest_path.is_file() {
                    continue;
                }
                ensure_within_root(&manifest_path, &canonical_root)?;
                let manifest_bytes = fs::read(&manifest_path)?;
                let manifest: PluginManifest = serde_json::from_slice(&manifest_bytes)?;
                manifest.validate()?;

                let digest = compute_plugin_digest(&plugin_dir, &manifest_bytes)?;
                let compatibility = evaluate_host_requirement(&manifest, &host_version);

                let name = manifest.name.clone();
                if catalog.entries.contains_key(&name) {
                    let existing = catalog.entries.get(&name).expect("just checked");
                    return Err(PluginHostError::DuplicateName {
                        name,
                        left: existing.digest.clone(),
                        right: digest,
                    });
                }
                catalog.entries.insert(
                    name,
                    CatalogEntry { manifest, root: plugin_dir, digest, compatibility },
                );
            }
        }
        Ok(catalog)
    }

    pub fn validate_path(plugin_root: &PathBuf) -> Result<CatalogEntry, PluginHostError> {
        let canonical_root = canonical_plugin_root(plugin_root)?;
        let manifest_path = canonical_root.join(MANIFEST_FILENAME);
        ensure_within_root(&manifest_path, &canonical_root)?;
        let manifest_bytes = fs::read(&manifest_path)?;
        let manifest: PluginManifest = serde_json::from_slice(&manifest_bytes)?;
        manifest.validate()?;
        let digest = compute_plugin_digest(&canonical_root, &manifest_bytes)?;
        let host_version =
            Version::parse(HOST_PARQONAUT_VERSION).expect("host version must be semver");
        let compatibility = evaluate_host_requirement(&manifest, &host_version);
        Ok(CatalogEntry { manifest, root: canonical_root, digest, compatibility })
    }
}

fn evaluate_host_requirement(
    manifest: &PluginManifest,
    host_version: &Version,
) -> PluginCompatibility {
    let Some(req_str) = &manifest.requires_parqonaut else {
        return PluginCompatibility::Compatible;
    };
    let Ok(req) = VersionReq::parse(req_str) else {
        return PluginCompatibility::HostVersionMismatch;
    };
    if req.matches(host_version) {
        PluginCompatibility::Compatible
    } else {
        PluginCompatibility::HostVersionMismatch
    }
}
