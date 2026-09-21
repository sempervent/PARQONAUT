//! Canonical plugin-root containment checks.

use std::path::{Component, Path, PathBuf};

use crate::error::PluginHostError;

pub fn canonical_plugin_root(root: &Path) -> Result<PathBuf, PluginHostError> {
    let canonical = root.canonicalize().map_err(|e| {
        PluginHostError::InvalidPath(format!("plugin root {}: {e}", root.display()))
    })?;
    Ok(canonical)
}

pub fn manifest_path_in_root(root: &Path, manifest_rel: &str) -> Result<PathBuf, PluginHostError> {
    if manifest_rel.contains("..") {
        return Err(PluginHostError::InvalidPath("manifest path traversal".into()));
    }
    let joined = root.join(manifest_rel);
    let canonical = joined
        .canonicalize()
        .map_err(|e| PluginHostError::InvalidPath(format!("manifest: {e}")))?;
    ensure_within_root(&canonical, root)?;
    Ok(canonical)
}

pub fn ensure_within_root(path: &Path, root: &Path) -> Result<(), PluginHostError> {
    let root = root.canonicalize().map_err(|e| PluginHostError::InvalidPath(e.to_string()))?;
    let path = path.canonicalize().map_err(|e| PluginHostError::InvalidPath(e.to_string()))?;
    if !path.starts_with(&root) {
        return Err(PluginHostError::InvalidPath(format!(
            "{} escapes plugin root {}",
            path.display(),
            root.display()
        )));
    }
    Ok(())
}

/// Rejects plugin roots whose path contains `..` before canonicalization.
pub fn reject_parent_components(root: &Path) -> Result<(), PluginHostError> {
    for c in root.components() {
        if matches!(c, Component::ParentDir) {
            return Err(PluginHostError::InvalidPath("plugin root must not contain ..".into()));
        }
    }
    Ok(())
}
