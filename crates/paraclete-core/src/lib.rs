//! Engine boundary: target resolution, format detection, orchestration seams.
//!
//! Phase 2 standardizes on [`ScanEngine::run`] as the single concrete local scan entrypoint
//! (resolve → inspection → multi-dataset inference → rules → validated reports). The
//! [`ScanOrchestrator`] remains for trait-level smoke tests only.

#![forbid(unsafe_code)]

mod dataset_infer;
mod dataset_inspector;
mod error;
mod failure_map;
mod format_detection;
mod local_resolve;
mod orchestrator;
mod parquet_inspect;
mod phase1_rules;
mod phase2_findings;
mod phase3_findings;
mod plugin_invoker;
mod rule_engine;
mod scan_engine;
mod shallow_inspect;
mod target_resolution;

pub use dataset_inspector::DatasetInspector;
pub use error::CoreError;
pub use format_detection::{classify_format_from_path, ExtensionFormatDetector, FormatDetector};
pub use local_resolve::resolve_local_scan_plan;
pub use orchestrator::ScanOrchestrator;
pub use parquet_inspect::{inspect_parquet_file, ParquetInspection, RowGroupSummary};
pub use plugin_invoker::PluginExecutor;
pub use rule_engine::{RuleEngine, RuleScanContext};
pub use scan_engine::{LocalScanEngine, ScanEngine};
pub use target_resolution::TargetResolver;
