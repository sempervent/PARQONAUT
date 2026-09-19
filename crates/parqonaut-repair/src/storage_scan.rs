//! Storage-aware dataset scanning for local paths and object stores.

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use paraclete_core::{
    classify_format_from_path, infer_parquet_datasets, inspect_parquet_footer_buffer,
    parquet_read_failed_finding, ParquetInspection,
};
use paraclete_types::{
    contract_schema_version, report_format_version, validate_report, AssetRecord, DataFormat,
    DatasetSummary, FormatSummary, InspectionStatus, ObjectStoreTargetPlaceholder, ReportMetadata,
    ResolvedAsset, ScanOptions, ScanPlan, ScanProfile, ScanReport, ScanRequest, ScanSummary,
    ScanTarget,
};
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::inventory::{list_remote_inventory, relative_object_key};
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::memory::MemoryStorageBackend;
use parqonaut_storage::parquet_range::read_parquet_footer;
use parqonaut_storage::LocalStorageBackend;
use std::collections::{BTreeMap, BTreeSet};
use url::Url;
use uuid::Uuid;

use crate::error::RepairError;
use crate::verify::scan_directory;

/// Canonical scan root string for a [`DatasetLocation`].
pub fn location_root(location: &DatasetLocation) -> Utf8PathBuf {
    match location {
        DatasetLocation::Local(l) => l.path.clone(),
        DatasetLocation::S3(s) => Utf8PathBuf::from(s.display_uri()),
    }
}

/// Concrete storage backend selected for a dataset location.
pub enum RepairBackend {
    Local(LocalStorageBackend),
    Memory(MemoryStorageBackend),
    #[cfg(feature = "s3")]
    S3(parqonaut_storage::S3StorageBackend),
}

impl RepairBackend {
    pub async fn scan(&self, location: &DatasetLocation) -> Result<ScanReport, RepairError> {
        match self {
            Self::Local(b) => scan_dataset(location, b).await,
            Self::Memory(b) => scan_dataset(location, b).await,
            #[cfg(feature = "s3")]
            Self::S3(b) => scan_dataset(location, b).await,
        }
    }

    pub async fn execute_plan(
        &self,
        executor: &crate::RepairExecutor,
        plan: &crate::RepairPlan,
        output: &DatasetLocation,
        scan: &ScanReport,
        output_backend: &RepairBackend,
        run_id: &str,
    ) -> Result<crate::ExecutionReport, RepairError> {
        match (self, output_backend) {
            (Self::Local(s), Self::Local(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            (Self::Local(s), Self::Memory(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            (Self::Memory(s), Self::Local(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            (Self::Memory(s), Self::Memory(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            #[cfg(feature = "s3")]
            (Self::S3(s), Self::S3(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            #[cfg(feature = "s3")]
            (Self::Local(s), Self::S3(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            #[cfg(feature = "s3")]
            (Self::S3(s), Self::Local(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            #[cfg(feature = "s3")]
            (Self::Memory(s), Self::S3(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            #[cfg(feature = "s3")]
            (Self::S3(s), Self::Memory(o)) => {
                executor.execute_storage(plan, output, scan, s, o, run_id).await
            }
            _ => Err(RepairError::Storage("unsupported cross-backend repair pairing".into())),
        }
    }
}

/// Resolve a default storage backend for `location`.
pub async fn backend_for_location(
    location: &DatasetLocation,
) -> Result<RepairBackend, RepairError> {
    match location {
        DatasetLocation::Local(_) => Ok(RepairBackend::Local(LocalStorageBackend::direct())),
        DatasetLocation::S3(_) => backend_for_s3().await,
    }
}

async fn backend_for_s3() -> Result<RepairBackend, RepairError> {
    #[cfg(feature = "s3")]
    {
        use parqonaut_storage::{S3Config, S3StorageBackend};
        let config = S3Config::default();
        Ok(RepairBackend::S3(S3StorageBackend::new(config).await))
    }
    #[cfg(not(feature = "s3"))]
    {
        Err(RepairError::ScanFailed(
            "S3 datasets require the `s3` feature on parqonaut-repair".into(),
        ))
    }
}

/// Scan a dataset at `location` using `backend`.
///
/// Local locations delegate to [`scan_directory`]; remote locations list inventory and
/// read Parquet footers via ranged GETs without downloading whole objects.
pub async fn scan_dataset<B: StorageBackend>(
    location: &DatasetLocation,
    backend: &B,
) -> Result<ScanReport, RepairError> {
    match location {
        DatasetLocation::Local(l) => scan_directory(&l.path),
        DatasetLocation::S3(_) => scan_remote_dataset(location, backend).await,
    }
}

async fn scan_remote_dataset<B: StorageBackend>(
    location: &DatasetLocation,
    backend: &B,
) -> Result<ScanReport, RepairError> {
    let root = location_root(location);
    let inventory = list_remote_inventory(backend, location).await.map_err(storage_err)?;
    let scan_id = Uuid::new_v4();
    let uri = location.display_uri();
    let target = ScanTarget::ObjectStorePlaceholder {
        inner: ObjectStoreTargetPlaceholder {
            uri: Url::parse(&uri).map_err(|e| RepairError::ScanFailed(e.to_string()))?,
            description: None,
        },
    };
    let request = ScanRequest {
        scan_id,
        target,
        profile: ScanProfile::Standard,
        options: ScanOptions::default(),
    };

    let mut assets_sorted: Vec<ResolvedAsset> = Vec::new();
    for object in &inventory.objects {
        let Some(relative) = relative_object_key(location, &object.location) else {
            continue;
        };
        let path = Utf8PathBuf::from(object.location.display_uri());
        assets_sorted.push(ResolvedAsset {
            path,
            format: classify_format_from_path(Utf8Path::new(&relative)),
            size_bytes: object.size,
        });
    }
    assets_sorted.sort_by(|a, b| a.path.cmp(&b.path));

    let plan = ScanPlan { root: root.clone(), assets: assets_sorted.clone(), truncated: false };

    let mut parquet_inspections: BTreeMap<Utf8PathBuf, ParquetInspection> = BTreeMap::new();
    let mut parquet_failures: Vec<(Utf8PathBuf, paraclete_core::CoreError)> = Vec::new();
    let mut asset_records: Vec<AssetRecord> = Vec::new();

    for asset in &assets_sorted {
        let mut rec = AssetRecord {
            path: asset.path.clone(),
            format: asset.format,
            size_bytes: asset.size_bytes,
            inspection_status: InspectionStatus::Skipped,
            failure_kind: None,
            failure_message: None,
            dataset_id: None,
            probe: None,
            inspection_hints: None,
        };
        if asset.format == DataFormat::Parquet {
            match inspect_remote_parquet(backend, &asset.path, asset.size_bytes).await {
                Ok(insp) => {
                    rec.inspection_status = InspectionStatus::Inspected;
                    rec.inspection_hints = Some(parquet_inspection_hints(&insp));
                    parquet_inspections.insert(asset.path.clone(), insp);
                }
                Err(err) => {
                    rec.inspection_status = InspectionStatus::Failed;
                    rec.failure_kind = Some(paraclete_types::FailureKind::FormatReadError);
                    rec.failure_message = Some(err.to_string());
                    parquet_failures.push((asset.path.clone(), err));
                }
            }
        }
        asset_records.push(rec);
    }

    let mut extra_findings: Vec<paraclete_types::Finding> = Vec::new();
    for (path, err) in &parquet_failures {
        extra_findings.push(parquet_read_failed_finding(path, err));
    }

    let parquet_ok_paths: Vec<Utf8PathBuf> = parquet_inspections.keys().cloned().collect();
    let (datasets, anchor_notes) =
        infer_parquet_datasets(&plan, &parquet_ok_paths, &parquet_inspections);
    extra_findings.extend(paraclete_core::grouping_ambiguous_from_notes(&anchor_notes));

    let path_to_dataset: BTreeMap<Utf8PathBuf, String> = datasets
        .iter()
        .flat_map(|d| d.files.iter().map(move |f| (f.path.clone(), d.dataset_id.clone())))
        .collect();
    for rec in &mut asset_records {
        if rec.inspection_status == InspectionStatus::Inspected && rec.format == DataFormat::Parquet
        {
            if let Some(id) = path_to_dataset.get(&rec.path) {
                rec.dataset_id = Some(id.clone());
            }
        }
    }

    if datasets.len() >= 2 {
        extra_findings
            .push(paraclete_core::multiple_datasets_finding(root.as_str(), datasets.len()));
    }

    for ds in &datasets {
        if ds.partition_layout == paraclete_types::PartitionLayout::Unpartitioned
            && ds.files.len() >= 2
        {
            extra_findings.push(paraclete_core::unpartitioned_collection_finding(
                &ds.dataset_id,
                ds.files.len(),
            ));
        }
    }

    let inspection_list: Vec<_> = parquet_inspections.values().cloned().collect();
    let mut findings =
        paraclete_core::evaluate_phase1_rules(&plan, &assets_sorted, &inspection_list, &datasets);
    findings.extend(extra_findings);
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
    let inspected =
        asset_records.iter().filter(|r| r.inspection_status == InspectionStatus::Inspected).count()
            as u64;
    let failed =
        asset_records.iter().filter(|r| r.inspection_status == InspectionStatus::Failed).count()
            as u64;
    let skipped =
        asset_records.iter().filter(|r| r.inspection_status == InspectionStatus::Skipped).count()
            as u64;
    let dataset_member_assets =
        asset_records.iter().filter(|r| r.dataset_id.is_some()).count() as u64;
    let partial_inspection = failed > 0;

    let report = ScanReport {
        request: request.clone(),
        metadata: ReportMetadata {
            scan_id: request.scan_id,
            generated_at: Utc::now(),
            contract_schema: contract_schema_version(),
            report_format: report_format_version(),
            engine_revision: Some("phase-5-storage-scan".into()),
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
            scan_truncated: false,
            findings_total,
            findings_by_severity,
        },
        assets: asset_records,
        datasets,
        dataset_summaries,
        format_summaries,
        findings,
    };
    validate_report(&report).map_err(|e| RepairError::ScanFailed(e.to_string()))?;
    Ok(report)
}

async fn inspect_remote_parquet<B: StorageBackend>(
    backend: &B,
    path: &Utf8Path,
    object_size: u64,
) -> Result<ParquetInspection, paraclete_core::CoreError> {
    let object = object_location_from_display_uri(path.as_str())?;
    let (footer, _metrics) =
        read_parquet_footer(backend, &object).await.map_err(core_storage_err)?;
    inspect_parquet_footer_buffer(path.as_str().to_string(), object_size, &footer.footer)
}

fn object_location_from_display_uri(
    uri: &str,
) -> Result<ObjectLocation, paraclete_core::CoreError> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, key) = rest.split_once('/').ok_or_else(|| {
            paraclete_core::CoreError::Parquet(format!("invalid S3 object URI: {uri}"))
        })?;
        if key.is_empty() {
            return Err(paraclete_core::CoreError::Parquet(format!(
                "invalid S3 object URI (missing key): {uri}"
            )));
        }
        return Ok(ObjectLocation::S3 { bucket: bucket.to_string(), key: key.to_string() });
    }
    if let Ok(url) = Url::parse(uri) {
        if url.scheme() == "file" {
            let path = url.to_file_path().map_err(|_| {
                paraclete_core::CoreError::Parquet(format!("invalid file URI: {uri}"))
            })?;
            return Ok(ObjectLocation::from_local_path(Utf8PathBuf::from_path_buf(path).map_err(
                |_| paraclete_core::CoreError::Parquet(format!("non-UTF8 file URI: {uri}")),
            )?));
        }
    }
    Ok(ObjectLocation::from_local_path(Utf8PathBuf::from(uri)))
}

pub(crate) fn parquet_inspection_hints(insp: &ParquetInspection) -> serde_json::Value {
    serde_json::json!({
        "parqonaut_parquet_inspection": {
            "num_rows": insp.num_rows,
            "num_row_groups": insp.num_row_groups,
            "row_groups": insp.row_groups,
            "schema": insp.schema,
            "compression_codecs": insp.compression_codecs.iter().collect::<Vec<_>>(),
            "statistics_present": insp.statistics_present,
        }
    })
}

pub(crate) fn parquet_inspection_from_hints(
    hints: &serde_json::Value,
    path: &str,
) -> Result<ParquetInspection, RepairError> {
    let node = hints.get("parqonaut_parquet_inspection").ok_or_else(|| {
        RepairError::DatasetUnreadable(format!("missing cached Parquet inspection for {path}"))
    })?;
    let num_rows = node.get("num_rows").and_then(|v| v.as_i64()).ok_or_else(|| {
        RepairError::DatasetUnreadable(format!("invalid cached inspection num_rows for {path}"))
    })?;
    let num_row_groups =
        node.get("num_row_groups").and_then(|v| v.as_u64()).map(|v| v as usize).ok_or_else(
            || {
                RepairError::DatasetUnreadable(format!(
                    "invalid cached inspection num_row_groups for {path}"
                ))
            },
        )?;
    let row_groups: Vec<paraclete_core::RowGroupSummary> = serde_json::from_value(
        node.get("row_groups").cloned().unwrap_or_default(),
    )
    .map_err(|e| RepairError::DatasetUnreadable(format!("invalid row_groups for {path}: {e}")))?;
    let schema: paraclete_types::SchemaSnapshot =
        serde_json::from_value(node.get("schema").cloned().unwrap_or_default()).map_err(|e| {
            RepairError::DatasetUnreadable(format!("invalid schema for {path}: {e}"))
        })?;
    let compression_codecs: BTreeSet<String> = node
        .get("compression_codecs")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let statistics_present =
        node.get("statistics_present").and_then(|v| v.as_bool()).unwrap_or(false);
    Ok(ParquetInspection {
        path: path.to_string(),
        num_rows,
        num_row_groups,
        row_groups,
        schema,
        created_by: None,
        compression_codecs,
        statistics_present,
    })
}

fn summarize_formats(assets: &[ResolvedAsset]) -> Vec<FormatSummary> {
    let mut m: BTreeMap<DataFormat, u64> = BTreeMap::new();
    for a in assets {
        *m.entry(a.format).or_insert(0) += 1;
    }
    m.into_iter().map(|(format, file_count)| FormatSummary { format, file_count }).collect()
}

fn dominant_format_in_dataset(
    assets: &[ResolvedAsset],
    dataset: &paraclete_types::Dataset,
) -> DataFormat {
    let paths: BTreeSet<_> = dataset.files.iter().map(|f| &f.path).collect();
    let subset: Vec<ResolvedAsset> =
        assets.iter().filter(|a| paths.contains(&a.path)).cloned().collect();
    paraclete_core::dominant_format(&subset)
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

fn storage_err(err: parqonaut_storage::StorageError) -> RepairError {
    RepairError::ScanFailed(err.to_string())
}

fn core_storage_err(err: parqonaut_storage::StorageError) -> paraclete_core::CoreError {
    paraclete_core::CoreError::Parquet(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parqonaut_storage::capabilities::StorageCapabilities;
    use parqonaut_storage::conditional::ConditionalCreate;
    use parqonaut_storage::memory::MemoryStorageBackend;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    fn memory_backend() -> MemoryStorageBackend {
        MemoryStorageBackend::new(StorageCapabilities {
            conditional_create: true,
            ..StorageCapabilities::LOCAL
        })
    }

    async fn put_parquet(backend: &MemoryStorageBackend, bucket: &str, key: &str, rows: i32) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1_i32; rows as usize]))],
        )
        .unwrap();
        let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        backend
            .conditional_create(
                &ObjectLocation::S3 { bucket: bucket.into(), key: key.into() },
                ConditionalCreate::must_not_exist(),
                Bytes::from(buf),
            )
            .await
            .expect("conditional create");
    }

    #[tokio::test]
    async fn remote_scan_builds_report_from_inventory_and_footer() {
        let backend = Arc::new(memory_backend());
        let dataset = DatasetLocation::parse("s3://scan-bucket/data/").unwrap();
        put_parquet(&backend, "scan-bucket", "data/a.parquet", 10).await;
        put_parquet(&backend, "scan-bucket", "data/b.parquet", 20).await;

        let report = scan_dataset(&dataset, backend.as_ref()).await.unwrap();
        assert_eq!(report.assets.len(), 2);
        assert_eq!(report.summary.inspected_assets, 2);
        assert_eq!(report.datasets.len(), 1);
        assert!(report.findings.iter().all(|f| f.fingerprint.is_some()));
    }

    #[test]
    fn footer_buffer_inspection_matches_local_file() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
                .unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let object_size = buf.len() as u64;
        let trailer_start = object_size - 8;
        let metadata_len = u32::from_le_bytes(
            buf[trailer_start as usize..trailer_start as usize + 4].try_into().unwrap(),
        );
        let footer_start = object_size - u64::from(metadata_len) - 8;
        let footer = &buf[footer_start as usize..];
        let remote =
            inspect_parquet_footer_buffer("s3://b/k.parquet".into(), object_size, footer).unwrap();
        let local =
            paraclete_core::inspect_parquet_reader("local.parquet".into(), bytes::Bytes::from(buf))
                .unwrap();
        assert_eq!(remote.num_rows, local.num_rows);
        assert_eq!(remote.num_row_groups, local.num_row_groups);
    }
}
