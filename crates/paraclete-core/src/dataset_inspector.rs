//! Dataset inventory and profiling boundary.

use camino::Utf8PathBuf;
use paraclete_types::Dataset;

use crate::CoreError;

/// Builds dataset models from resolved paths.
pub trait DatasetInspector: Send + Sync {
    /// Produces zero or more [`Dataset`] inventories.
    fn inspect_paths(&self, paths: &[Utf8PathBuf]) -> Result<Vec<Dataset>, CoreError>;
}
