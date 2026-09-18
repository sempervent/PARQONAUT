use camino::Utf8PathBuf;
use paraclete_core::{
    CoreError, DatasetInspector, ExtensionFormatDetector, PluginExecutor, RuleEngine,
    ScanOrchestrator, TargetResolver,
};
use paraclete_plugin_protocol::{PluginExecutorError, PluginRequest, PluginResponse};
use paraclete_types::{Dataset, Finding, ScanTarget};

struct LocalPathResolver;

impl TargetResolver for LocalPathResolver {
    fn resolve(&self, target: &ScanTarget) -> Result<Vec<Utf8PathBuf>, CoreError> {
        match target {
            ScanTarget::LocalFile { path } => Ok(vec![path.clone()]),
            ScanTarget::LocalDirectory { path } => Ok(vec![path.clone()]),
            ScanTarget::LogicalDataset { .. } => Err(CoreError::Unsupported("logical dataset")),
            ScanTarget::ObjectStorePlaceholder { .. } => {
                Err(CoreError::Unsupported("object store"))
            }
        }
    }
}

struct NoopDatasetInspector;

impl DatasetInspector for NoopDatasetInspector {
    fn inspect_paths(&self, _paths: &[Utf8PathBuf]) -> Result<Vec<Dataset>, CoreError> {
        Ok(Vec::new())
    }
}

struct EmptyRuleEngine;

impl RuleEngine for EmptyRuleEngine {
    fn evaluate(&self, _ctx: &paraclete_core::RuleScanContext) -> Result<Vec<Finding>, CoreError> {
        Ok(Vec::new())
    }
}

struct NotImplementedPlugins;

impl PluginExecutor for NotImplementedPlugins {
    fn execute(&self, _request: &PluginRequest) -> Result<PluginResponse, PluginExecutorError> {
        Err(PluginExecutorError::NotImplemented)
    }
}

fn orchestrator<'a>() -> ScanOrchestrator<'a> {
    static TARGETS: LocalPathResolver = LocalPathResolver;
    static FORMATS: ExtensionFormatDetector = ExtensionFormatDetector;
    static DATASETS: NoopDatasetInspector = NoopDatasetInspector;
    static RULES: EmptyRuleEngine = EmptyRuleEngine;
    static PLUGINS: NotImplementedPlugins = NotImplementedPlugins;
    ScanOrchestrator {
        targets: &TARGETS,
        formats: &FORMATS,
        datasets: &DATASETS,
        rules: &RULES,
        plugins: &PLUGINS,
    }
}

#[test]
fn orchestrator_classifies_parquet_by_extension() {
    let o = orchestrator();
    let req = paraclete_types::ScanRequest::new(
        ScanTarget::LocalFile { path: Utf8PathBuf::from("data/example.parquet") },
        paraclete_types::ScanProfile::Quick,
    );
    let formats = o.classify_formats(&req).expect("formats");
    assert_eq!(formats, vec![paraclete_types::DataFormat::Parquet]);
}

#[test]
fn orchestrator_runs_rules_with_empty_engine() {
    let o = orchestrator();
    let req = paraclete_types::ScanRequest::new(
        ScanTarget::LocalFile { path: Utf8PathBuf::from("data/example.csv") },
        paraclete_types::ScanProfile::Standard,
    );
    let findings = o.run_rules(&req).expect("rules");
    assert!(findings.is_empty());
}
