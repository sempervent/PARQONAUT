//! Unified local scan engine: plan → inspect → infer datasets → rules → validated report.

use std::collections::{BTreeMap, BTreeSet};

use camino::Utf8PathBuf;
use chrono::Utc;
use paraclete_types::{
    contract_schema_version, report_format_version, validate_report, AssetRecord, DataFormat,
    Dataset, DatasetSummary, FormatSummary, InspectionStatus, ParseConfidence, PartitionLayout,
    ReportMetadata, ResolvedAsset, ScanReport, ScanRequest, ScanSummary,
};

use crate::dataset_infer::infer_parquet_datasets;
use crate::failure_map::failure_kind;
use crate::local_resolve::resolve_local_scan_plan;
use crate::parquet_inspect::inspect_parquet_file;
use crate::scan_findings::{
    multiple_datasets_finding, parquet_read_failed_finding, scan_truncated_finding,
    unpartitioned_collection_finding,
};
use crate::scan_plugins::ScanPluginHost;
use crate::scan_rules::evaluate_scan_rules;
use crate::schema_findings::{
    grouping_ambiguous_from_notes, inspection_partial_finding,
    inspection_skipped_aggregate_finding, text_probe_bounded_finding,
};
use crate::shallow_inspect::{inspect_csv_shallow, inspect_json_like_shallow};
use crate::CoreError;
use paraclete_types::{PluginExecutionRecord, ScanPluginMetadata};
use parqonaut_plugin_protocol::{PluginExecutionPhase, PluginScanContext};

/// Single entrypoint for the local scan pipeline.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScanEngine;

impl ScanEngine {
    /// Backwards-compatible alias for [`Self::run`].
    pub fn scan(request: &ScanRequest) -> Result<ScanReport, CoreError> {
        Self::run(request)
    }

    /// Executes a scan and returns a validated [`ScanReport`].
    pub fn run(request: &ScanRequest) -> Result<ScanReport, CoreError> {
        Self::run_with_plugins(request, None)
    }

    /// Local scan with optional analyzer plugins (explicit selection only).
    pub fn run_with_plugins(
        request: &ScanRequest,
        plugins: Option<&dyn ScanPluginHost>,
    ) -> Result<ScanReport, CoreError> {
        let plan = resolve_local_scan_plan(&request.target, &request.options)?;
        let mut assets_sorted: Vec<ResolvedAsset> = plan.assets.clone();
        assets_sorted.sort_by(|a, b| a.path.cmp(&b.path));

        let mut plugin_executions: Vec<PluginExecutionRecord> = Vec::new();
        let mut pre_scan_findings: Vec<paraclete_types::Finding> = Vec::new();
        if let Some(host) = plugins {
            if !request.options.requested_plugins.is_empty() {
                let ctx = plugin_context(
                    request,
                    assets_sorted.iter().map(|a| a.path.to_string()).collect(),
                    BTreeMap::new(),
                    Vec::new(),
                );
                let (extra, runs) =
                    host.run_phase(PluginExecutionPhase::PreScan, ctx, &BTreeSet::new())?;
                plugin_executions.extend(runs);
                pre_scan_findings.extend(extra);
            }
        }

        let scan_truncated = plan.truncated;
        let mut parquet_inspections: BTreeMap<Utf8PathBuf, _> = BTreeMap::new();
        let mut parquet_failures: Vec<(Utf8PathBuf, CoreError)> = Vec::new();
        let mut extra_findings: Vec<paraclete_types::Finding> = Vec::new();
        extra_findings.extend(pre_scan_findings);

        let mut asset_records: Vec<AssetRecord> = Vec::new();
        let mut skipped_paths: Vec<Utf8PathBuf> = Vec::new();

        for a in &assets_sorted {
            let mut rec = AssetRecord {
                path: a.path.clone(),
                format: a.format,
                size_bytes: a.size_bytes,
                inspection_status: InspectionStatus::Skipped,
                failure_kind: None,
                failure_message: None,
                dataset_id: None,
                probe: None,
                inspection_hints: None,
            };
            match a.format {
                DataFormat::Parquet => match inspect_parquet_file(&a.path) {
                    Ok(i) => {
                        parquet_inspections.insert(a.path.clone(), i);
                        rec.inspection_status = InspectionStatus::Inspected;
                    }
                    Err(e) => {
                        rec.inspection_status = InspectionStatus::Failed;
                        rec.failure_kind = Some(failure_kind(&e));
                        rec.failure_message = Some(e.to_string());
                        parquet_failures.push((a.path.clone(), e));
                    }
                },
                DataFormat::Csv => match inspect_csv_shallow(&a.path) {
                    Ok(out) => {
                        rec.inspection_status = InspectionStatus::Inspected;
                        rec.probe = Some(out.probe.clone());
                        rec.inspection_hints = Some(out.hints.clone());
                        extra_findings
                            .push(text_probe_bounded_finding(&a.path, &out.hints, &out.probe));
                        if matches!(
                            out.probe.parse_confidence,
                            ParseConfidence::Partial | ParseConfidence::Low
                        ) {
                            extra_findings.push(inspection_partial_finding(
                                &a.path,
                                "csv shallow probe used partial coverage or low-confidence parse",
                            ));
                        }
                    }
                    Err(e) => {
                        rec.inspection_status = InspectionStatus::Failed;
                        rec.failure_kind = Some(failure_kind(&e));
                        rec.failure_message = Some(e.to_string());
                    }
                },
                DataFormat::Json => match inspect_json_like_shallow(&a.path, false) {
                    Ok(out) => {
                        rec.inspection_status = InspectionStatus::Inspected;
                        rec.probe = Some(out.probe.clone());
                        rec.inspection_hints = Some(out.hints.clone());
                        extra_findings
                            .push(text_probe_bounded_finding(&a.path, &out.hints, &out.probe));
                        if matches!(
                            out.probe.parse_confidence,
                            ParseConfidence::Partial | ParseConfidence::Low
                        ) {
                            extra_findings.push(inspection_partial_finding(
                                &a.path,
                                "json probe used bounded bytes or partial structural parse",
                            ));
                        }
                    }
                    Err(e) => {
                        rec.inspection_status = InspectionStatus::Failed;
                        rec.failure_kind = Some(failure_kind(&e));
                        rec.failure_message = Some(e.to_string());
                    }
                },
                DataFormat::Ndjson => match inspect_json_like_shallow(&a.path, true) {
                    Ok(out) => {
                        rec.inspection_status = InspectionStatus::Inspected;
                        rec.probe = Some(out.probe.clone());
                        rec.inspection_hints = Some(out.hints.clone());
                        extra_findings
                            .push(text_probe_bounded_finding(&a.path, &out.hints, &out.probe));
                        extra_findings.push(inspection_partial_finding(
                            &a.path,
                            "ndjson probe evaluates first non-empty line only",
                        ));
                    }
                    Err(e) => {
                        rec.inspection_status = InspectionStatus::Failed;
                        rec.failure_kind = Some(failure_kind(&e));
                        rec.failure_message = Some(e.to_string());
                    }
                },
                DataFormat::Unknown => {
                    skipped_paths.push(a.path.clone());
                }
            }
            asset_records.push(rec);
        }

        for (path, err) in &parquet_failures {
            extra_findings.push(parquet_read_failed_finding(path, err));
        }

        if scan_truncated {
            extra_findings
                .push(scan_truncated_finding(plan.root.as_str(), request.options.max_files));
        }

        let skipped_count = skipped_paths.len() as u64;
        if skipped_count > 0 {
            extra_findings
                .push(inspection_skipped_aggregate_finding(skipped_count, &skipped_paths));
        }

        let parquet_ok_paths: Vec<Utf8PathBuf> = parquet_inspections.keys().cloned().collect();
        let (datasets, anchor_notes) =
            infer_parquet_datasets(&plan, &parquet_ok_paths, &parquet_inspections);
        extra_findings.extend(grouping_ambiguous_from_notes(&anchor_notes));

        let path_to_dataset: BTreeMap<Utf8PathBuf, String> = datasets
            .iter()
            .flat_map(|d| d.files.iter().map(move |f| (f.path.clone(), d.dataset_id.clone())))
            .collect();
        for rec in &mut asset_records {
            if rec.inspection_status == InspectionStatus::Inspected
                && rec.format == DataFormat::Parquet
            {
                if let Some(id) = path_to_dataset.get(&rec.path) {
                    rec.dataset_id = Some(id.clone());
                }
            }
        }

        if datasets.len() >= 2 {
            extra_findings.push(multiple_datasets_finding(plan.root.as_str(), datasets.len()));
        }

        for ds in &datasets {
            if ds.partition_layout == PartitionLayout::Unpartitioned && ds.files.len() >= 2 {
                extra_findings
                    .push(unpartitioned_collection_finding(&ds.dataset_id, ds.files.len()));
            }
        }

        if let Some(host) = plugins {
            if !request.options.requested_plugins.is_empty() {
                let mut inventory_summary = BTreeMap::new();
                inventory_summary.insert("dataset_count".into(), serde_json::json!(datasets.len()));
                inventory_summary
                    .insert("discovered_assets".into(), serde_json::json!(assets_sorted.len()));
                let ctx = plugin_context(
                    request,
                    assets_sorted.iter().map(|a| a.path.to_string()).collect(),
                    inventory_summary,
                    finding_summaries(&extra_findings),
                );
                let (extra, runs) = host.run_phase(
                    PluginExecutionPhase::PostInventory,
                    ctx,
                    &evidence_id_set(&extra_findings),
                )?;
                plugin_executions.extend(runs);
                extra_findings.extend(extra);
            }
        }

        let inspection_list: Vec<_> = parquet_inspections.values().cloned().collect();
        let mut findings = evaluate_scan_rules(&plan, &assets_sorted, &inspection_list, &datasets);
        findings.extend(extra_findings.clone());

        if let Some(host) = plugins {
            if !request.options.requested_plugins.is_empty() {
                let mut inventory_summary = BTreeMap::new();
                inventory_summary.insert("dataset_count".into(), serde_json::json!(datasets.len()));
                inventory_summary
                    .insert("discovered_assets".into(), serde_json::json!(assets_sorted.len()));
                let ctx = plugin_context(
                    request,
                    assets_sorted.iter().map(|a| a.path.to_string()).collect(),
                    inventory_summary,
                    finding_summaries(&findings),
                );
                let (extra, runs) = host.run_phase(
                    PluginExecutionPhase::PostRules,
                    ctx,
                    &evidence_id_set(&findings),
                )?;
                plugin_executions.extend(runs);
                findings.extend(extra);
            }
        }
        findings.sort_by(|a, b| a.code.as_str().cmp(b.code.as_str()));

        let format_summaries = summarize_formats(&assets_sorted);
        let dataset_summaries: Vec<DatasetSummary> = datasets
            .iter()
            .map(|d| DatasetSummary {
                dataset_id: d.dataset_id.clone(),
                file_count: d.files.len() as u64,
                dominant_format: dominant_format_in_dataset(&assets_sorted, d),
            })
            .collect();

        let findings_total = findings.len() as u64;
        let mut findings_by_severity = BTreeMap::new();
        for f in &findings {
            *findings_by_severity.entry(severity_key(f.severity).to_string()).or_insert(0) += 1;
        }

        let discovered = asset_records.len() as u64;
        let inspected = asset_records
            .iter()
            .filter(|r| r.inspection_status == InspectionStatus::Inspected)
            .count() as u64;
        let failed = asset_records
            .iter()
            .filter(|r| r.inspection_status == InspectionStatus::Failed)
            .count() as u64;
        let skipped = asset_records
            .iter()
            .filter(|r| r.inspection_status == InspectionStatus::Skipped)
            .count() as u64;
        let dataset_member_assets =
            asset_records.iter().filter(|r| r.dataset_id.is_some()).count() as u64;

        let partial_inspection = failed > 0 || scan_truncated;
        let plugin_metadata = if request.options.requested_plugins.is_empty() {
            None
        } else {
            let resolved = plugins
                .map(|h| h.resolved_plugin_order().to_vec())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| request.options.requested_plugins.clone());
            Some(ScanPluginMetadata {
                requested: request.options.requested_plugins.clone(),
                resolved_order: resolved,
                executions: plugin_executions,
            })
        };
        let report = ScanReport {
            request: request.clone(),
            metadata: ReportMetadata {
                scan_id: request.scan_id,
                generated_at: Utc::now(),
                contract_schema: contract_schema_version(),
                report_format: report_format_version(),
                engine_revision: Some("phase-3".into()),
            },
            summary: ScanSummary {
                discovered_assets: discovered,
                files_scanned: discovered,
                inspected_assets: inspected,
                failed_inspection_assets: failed,
                skipped_inspection_assets: skipped,
                dataset_member_assets,
                dataset_count: datasets.len() as u64,
                partial_inspection,
                scan_truncated,
                findings_total,
                findings_by_severity,
            },
            assets: asset_records,
            datasets,
            dataset_summaries,
            format_summaries,
            findings,
            plugin_metadata,
        };
        validate_report(&report).map_err(|e| CoreError::ReportValidation(e.to_string()))?;
        Ok(report)
    }
}

fn plugin_context(
    request: &ScanRequest,
    discovered_files: Vec<String>,
    inventory_summary: BTreeMap<String, serde_json::Value>,
    builtin_finding_summaries: Vec<serde_json::Value>,
) -> PluginScanContext {
    PluginScanContext {
        request: request.clone(),
        discovered_files,
        inventory_summary,
        builtin_finding_summaries,
        hints: BTreeMap::new(),
    }
}

fn finding_summaries(findings: &[paraclete_types::Finding]) -> Vec<serde_json::Value> {
    findings
        .iter()
        .map(|f| {
            serde_json::json!({
                "code": f.code.as_str(),
                "severity": format!("{:?}", f.severity).to_ascii_lowercase(),
                "summary": f.summary,
            })
        })
        .collect()
}

fn evidence_id_set(findings: &[paraclete_types::Finding]) -> BTreeSet<String> {
    findings.iter().flat_map(|f| f.evidence.iter().map(|e| e.id.to_string())).collect()
}

/// Back-compat alias for legacy call sites.
pub type LocalScanEngine = ScanEngine;

fn dominant_format_in_dataset(assets: &[ResolvedAsset], dataset: &Dataset) -> DataFormat {
    let paths: BTreeSet<_> = dataset.files.iter().map(|f| &f.path).collect();
    let subset: Vec<ResolvedAsset> =
        assets.iter().filter(|a| paths.contains(&a.path)).cloned().collect();
    crate::dataset_infer::dominant_format(&subset)
}

fn severity_key(s: paraclete_types::FindingSeverity) -> &'static str {
    match s {
        paraclete_types::FindingSeverity::Info => "info",
        paraclete_types::FindingSeverity::Low => "low",
        paraclete_types::FindingSeverity::Medium => "medium",
        paraclete_types::FindingSeverity::High => "high",
        paraclete_types::FindingSeverity::Critical => "critical",
    }
}

fn summarize_formats(assets: &[ResolvedAsset]) -> Vec<FormatSummary> {
    let mut m: BTreeMap<DataFormat, u64> = BTreeMap::new();
    for a in assets {
        *m.entry(a.format).or_insert(0) += 1;
    }
    m.into_iter().map(|(format, file_count)| FormatSummary { format, file_count }).collect()
}
