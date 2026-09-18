//! Rule evaluation boundary (Rust rules and future plugin-fed rules).

use camino::Utf8PathBuf;
use paraclete_types::{Finding, ScanRequest};

use crate::CoreError;

/// Minimal scan context for rule evaluation in later phases.
#[derive(Debug, Clone)]
pub struct RuleScanContext {
    pub request: ScanRequest,
    pub paths: Vec<Utf8PathBuf>,
}

/// Executes built-in rules against scan context.
pub trait RuleEngine: Send + Sync {
    /// Returns findings produced by this engine stage.
    fn evaluate(&self, ctx: &RuleScanContext) -> Result<Vec<Finding>, CoreError>;
}
