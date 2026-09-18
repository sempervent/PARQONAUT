//! Target resolution to concrete paths for local scans.

use camino::Utf8PathBuf;
use paraclete_types::{ScanOptions, ScanTarget};

use crate::local_resolve::resolve_local_scan_plan;
use crate::CoreError;

/// Resolves a [`ScanTarget`] into inspectable paths.
pub trait TargetResolver: Send + Sync {
    /// Returns concrete filesystem paths for downstream stages.
    fn resolve(&self, target: &ScanTarget) -> Result<Vec<Utf8PathBuf>, CoreError>;
}

/// Resolves local file and directory targets using default [`ScanOptions`] (for smoke tests).
#[derive(Debug, Default, Clone, Copy)]
#[allow(dead_code)] // Referenced from integration tests; not used inside the library crate itself.
pub struct LocalPathResolver;

impl TargetResolver for LocalPathResolver {
    fn resolve(&self, target: &ScanTarget) -> Result<Vec<Utf8PathBuf>, CoreError> {
        let plan = resolve_local_scan_plan(target, &ScanOptions::default())?;
        Ok(plan.assets.into_iter().map(|a| a.path).collect())
    }
}
