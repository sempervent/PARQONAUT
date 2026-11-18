//! Engine boundary: target resolution, format detection, orchestration seams.
//!
//! [`ScanEngine::run`] is the single concrete local scan entrypoint
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
mod plugin_invoker;
mod rule_engine;
mod scan_engine;
mod scan_findings;
mod scan_plugins;
mod scan_rules;
mod schema_findings;
mod shallow_inspect;
mod target_resolution;

pub use dataset_infer::{
    dominant_format, infer_parquet_datasets, parquet_schema_signature, AnchorInferenceNote,
};
pub use dataset_inspector::DatasetInspector;
pub use error::CoreError;
pub use format_detection::{classify_format_from_path, ExtensionFormatDetector, FormatDetector};
pub use local_resolve::resolve_local_scan_plan;
pub use orchestrator::ScanOrchestrator;
pub use parquet_inspect::{
    footer_inspection_heap_bound, inspect_parquet_chunk_reader, inspect_parquet_file,
    inspect_parquet_footer_buffer, inspect_parquet_reader, ParquetInspection, RowGroupSummary,
};
pub use plugin_invoker::PluginExecutor;
pub use rule_engine::{RuleEngine, RuleScanContext};
pub use scan_engine::{LocalScanEngine, ScanEngine};
pub use scan_findings::{
    multiple_datasets_finding, parquet_read_failed_finding, unpartitioned_collection_finding,
};
pub use scan_plugins::ScanPluginHost;
pub use scan_rules::evaluate_scan_rules;
pub use schema_findings::grouping_ambiguous_from_notes;
pub use target_resolution::TargetResolver;
