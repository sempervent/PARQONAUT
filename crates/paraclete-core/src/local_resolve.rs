//! Local filesystem target resolution → [`ScanPlan`].

use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use paraclete_types::{ScanOptions, ScanPlan, ScanTarget};
use walkdir::WalkDir;

use crate::format_detection::classify_format_from_path;
use crate::CoreError;

/// Resolves a local file or directory target into a [`ScanPlan`] with format classification.
pub fn resolve_local_scan_plan(
    target: &ScanTarget,
    options: &ScanOptions,
) -> Result<ScanPlan, CoreError> {
    match target {
        ScanTarget::LocalFile { path } => resolve_file(path, options),
        ScanTarget::LocalDirectory { path } => resolve_directory(path, options),
        ScanTarget::LogicalDataset { .. } => {
            Err(CoreError::Unsupported("logical dataset resolution"))
        }
        ScanTarget::ObjectStorePlaceholder { .. } => {
            Err(CoreError::Unsupported("object store resolution"))
        }
    }
}

fn resolve_file(path: &Utf8Path, _options: &ScanOptions) -> Result<ScanPlan, CoreError> {
    if !path.as_std_path().exists() {
        return Err(CoreError::MissingPath(path.to_owned()));
    }
    let meta = fs::metadata(path.as_std_path())?;
    let root = path.parent().map(Utf8Path::to_path_buf).unwrap_or_else(|| Utf8PathBuf::from("."));
    let format = classify_format_from_path(path);
    Ok(ScanPlan {
        root,
        assets: vec![paraclete_types::ResolvedAsset {
            path: path.to_owned(),
            format,
            size_bytes: meta.len(),
        }],
        truncated: false,
    })
}

fn resolve_directory(root: &Utf8Path, options: &ScanOptions) -> Result<ScanPlan, CoreError> {
    if !root.as_std_path().exists() {
        return Err(CoreError::MissingPath(root.to_owned()));
    }
    let cap = options.max_files.max(1).min(usize::MAX as u64) as usize;
    let mut assets = Vec::new();
    let mut truncated = false;
    for entry in
        WalkDir::new(root.as_std_path()).follow_links(false).into_iter().filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let utf8 = match Utf8PathBuf::from_path_buf(path.to_path_buf()) {
            Ok(p) => p,
            Err(p) => return Err(CoreError::NonUtf8Path(p.to_string_lossy().into())),
        };
        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let format = classify_format_from_path(utf8.as_path());
        assets.push(paraclete_types::ResolvedAsset { path: utf8, format, size_bytes: meta.len() });
        if assets.len() >= cap {
            truncated = true;
            break;
        }
    }
    Ok(ScanPlan { root: root.to_owned(), assets, truncated })
}

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf;

    use paraclete_types::{ScanOptions, ScanTarget};

    use super::resolve_local_scan_plan;

    #[test]
    fn resolves_single_file() {
        let root =
            Utf8PathBuf::from_path_buf(std::env::temp_dir().join("paraclete_resolve_single"))
                .unwrap();
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let f = root.join("a.csv");
        std::fs::write(&f, "h\n1\n").unwrap();
        let plan = resolve_local_scan_plan(
            &ScanTarget::LocalFile { path: f.clone() },
            &ScanOptions::default(),
        )
        .expect("resolve");
        assert_eq!(plan.assets.len(), 1);
        assert_eq!(plan.assets[0].path, f);
        assert!(!plan.truncated);
        let _ = std::fs::remove_dir_all(&root);
    }
}
