use std::collections::BTreeMap;
use std::path::PathBuf;

use parqonaut_plugin_protocol::{PluginManifest, MANIFEST_FILENAME};

use crate::error::PluginHostError;

/// Resolved plugin entry in the host catalog (discovery only; does not execute).
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub manifest: PluginManifest,
    pub root: PathBuf,
    pub digest: String,
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

    pub fn entries(&self) -> impl Iterator<Item = (&str, &CatalogEntry)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Load manifests from immediate child directories of each root (placeholder digest).
    pub fn discover_from_roots(roots: &[PathBuf]) -> Result<Self, PluginHostError> {
        let mut catalog = Self::new();
        for root in roots {
            if !root.is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(root)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let manifest_path = path.join(MANIFEST_FILENAME);
                if !manifest_path.is_file() {
                    continue;
                }
                let bytes = std::fs::read(&manifest_path)?;
                let manifest: PluginManifest = serde_json::from_slice(&bytes)?;
                manifest.validate()?;
                let digest = "pending".to_string(); // v0.10: content-addressed digest
                let name = manifest.name.clone();
                if let Some(existing) = catalog.entries.get(&name) {
                    if existing.digest != digest {
                        return Err(PluginHostError::DuplicateName {
                            name,
                            left: existing.digest.clone(),
                            right: digest,
                        });
                    }
                } else {
                    catalog.entries.insert(name, CatalogEntry { manifest, root: path, digest });
                }
            }
        }
        Ok(catalog)
    }
}
