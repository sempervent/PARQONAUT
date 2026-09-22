//! Deterministic content identity for plugin packages (path-independent).

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::error::PluginHostError;

const DIGEST_VERSION: &str = "v1";

fn excluded_rel(path: &Path) -> bool {
    let components: Vec<_> = path.components().map(|c| c.as_os_str().to_string_lossy()).collect();
    for c in &components {
        match c.as_ref() {
            ".venv" | "__pycache__" | ".pytest_cache" | ".ruff_cache" | ".git" | "dist"
            | "build" => {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Computes a hex SHA-256 digest over canonical manifest bytes and sorted plugin files.
pub fn compute_plugin_digest(
    root: &Path,
    manifest_bytes: &[u8],
) -> Result<String, PluginHostError> {
    let root = root
        .canonicalize()
        .map_err(|e| PluginHostError::InvalidPath(format!("plugin root: {e}")))?;

    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in WalkDir::new(&root).follow_links(false).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        let rel = path
            .strip_prefix(&root)
            .map_err(|_| PluginHostError::InvalidPath("walkdir path outside plugin root".into()))?;
        if excluded_rel(rel) {
            continue;
        }
        paths.push(rel.to_path_buf());
    }
    paths.sort();

    let mut hasher = Sha256::new();
    hasher.update(DIGEST_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(manifest_bytes);
    hasher.update([0u8]);

    for rel in paths {
        let abs = root.join(&rel);
        let rel_str = rel.to_string_lossy();
        hasher.update(rel_str.as_bytes());
        hasher.update([0u8]);
        let mut file = fs::File::open(&abs)?;
        let mut buf = [0u8; 8192];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        hasher.update([0u8]);
    }

    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_plugin(tmp: &TempDir, manifest: &str, py: &str) -> PathBuf {
        let root = tmp.path().join("plugin");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("parqonaut-plugin.json"), manifest).unwrap();
        fs::write(root.join("example_rules.py"), py).unwrap();
        root
    }

    const MANIFEST: &str = r#"{
  "protocol_version": 1,
  "name": "example-rules",
  "version": "0.1.0",
  "entrypoint": "example_rules:analyze",
  "capabilities": { "scan": { "supported_formats": ["parquet"], "supported_phases": ["post_rules"] } }
}"#;

    #[test]
    fn same_content_different_root_same_digest() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        let ra = write_plugin(&a, MANIFEST, "x = 1\n");
        let rb = write_plugin(&b, MANIFEST, "x = 1\n");
        let da = compute_plugin_digest(&ra, MANIFEST.as_bytes()).unwrap();
        let db = compute_plugin_digest(&rb, MANIFEST.as_bytes()).unwrap();
        assert_eq!(da, db);
    }

    #[test]
    fn mtime_only_same_digest() {
        let tmp = TempDir::new().unwrap();
        let root = write_plugin(&tmp, MANIFEST, "x = 1\n");
        let d1 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        let py = root.join("example_rules.py");
        #[cfg(unix)]
        {
            std::process::Command::new("touch").arg(&py).status().unwrap();
        }
        let d2 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn edit_source_changes_digest() {
        let tmp = TempDir::new().unwrap();
        let root = write_plugin(&tmp, MANIFEST, "x = 1\n");
        let d1 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        fs::write(root.join("example_rules.py"), "x = 2\n").unwrap();
        let d2 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        assert_ne!(d1, d2);
    }

    #[test]
    fn edit_manifest_changes_digest() {
        let tmp = TempDir::new().unwrap();
        let root = write_plugin(&tmp, MANIFEST, "x = 1\n");
        let d1 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        let m2 = MANIFEST.replace("0.1.0", "0.1.1");
        fs::write(root.join("parqonaut-plugin.json"), &m2).unwrap();
        let d2 = compute_plugin_digest(&root, m2.as_bytes()).unwrap();
        assert_ne!(d1, d2);
    }

    #[test]
    fn pycache_ignored() {
        let tmp = TempDir::new().unwrap();
        let root = write_plugin(&tmp, MANIFEST, "x = 1\n");
        let d1 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        let cache = root.join("__pycache__");
        fs::create_dir_all(&cache).unwrap();
        fs::write(cache.join("example_rules.cpython-313.pyc"), b"junk").unwrap();
        let d2 = compute_plugin_digest(&root, MANIFEST.as_bytes()).unwrap();
        assert_eq!(d1, d2);
    }
}
