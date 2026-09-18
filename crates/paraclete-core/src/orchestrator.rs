//! Scan orchestration skeleton wiring engine stages.

use paraclete_types::{DataFormat, ScanRequest};
use tracing::instrument;

use crate::dataset_inspector::DatasetInspector;
use crate::format_detection::FormatDetector;
use crate::plugin_invoker::PluginExecutor;
use crate::rule_engine::{RuleEngine, RuleScanContext};
use crate::target_resolution::TargetResolver;
use crate::CoreError;

/// Coordinates target resolution, detection, inspection, rules, and plugins.
pub struct ScanOrchestrator<'a> {
    pub targets: &'a dyn TargetResolver,
    pub formats: &'a dyn FormatDetector,
    pub datasets: &'a dyn DatasetInspector,
    pub rules: &'a dyn RuleEngine,
    /// Reserved for upcoming plugin phases; not invoked by Phase 0 orchestration helpers.
    pub plugins: &'a dyn PluginExecutor,
}

impl<'a> ScanOrchestrator<'a> {
    /// Phase 0 smoke path: resolve targets and classify formats by extension.
    #[instrument(skip(self), fields(scan_id = %request.scan_id))]
    pub fn classify_formats(&self, request: &ScanRequest) -> Result<Vec<DataFormat>, CoreError> {
        let paths = self.targets.resolve(&request.target)?;
        Ok(paths.iter().map(|path| self.formats.detect_format(path.as_path())).collect())
    }

    /// Runs the rule engine against resolved paths (plugins are not invoked yet).
    pub fn run_rules(
        &self,
        request: &ScanRequest,
    ) -> Result<Vec<paraclete_types::Finding>, CoreError> {
        let paths = self.targets.resolve(&request.target)?;
        let ctx = RuleScanContext { request: request.clone(), paths };
        self.rules.evaluate(&ctx)
    }

    /// Dataset inventory hook (placeholder pipeline stage).
    pub fn inventory_datasets(
        &self,
        request: &ScanRequest,
    ) -> Result<Vec<paraclete_types::Dataset>, CoreError> {
        let paths = self.targets.resolve(&request.target)?;
        self.datasets.inspect_paths(&paths)
    }
}
