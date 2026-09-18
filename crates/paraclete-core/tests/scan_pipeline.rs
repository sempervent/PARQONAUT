use std::path::PathBuf;

use camino::Utf8PathBuf;
use paraclete_types::{system, InspectionStatus, ScanProfile, ScanRequest, ScanTarget};
use uuid::Uuid;

use paraclete_core::{resolve_local_scan_plan, ScanEngine};

fn fixture(p: &str) -> Utf8PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(p);
    Utf8PathBuf::from_path_buf(path).expect("utf8 fixture path")
}

fn request_for_dir(rel: &str) -> ScanRequest {
    let mut req =
        ScanRequest::new(ScanTarget::LocalDirectory { path: fixture(rel) }, ScanProfile::Standard);
    req.scan_id = Uuid::parse_str("00000000-0000-4000-8000-0000000000a1").unwrap();
    req
}

fn request_for_file(rel: &str) -> ScanRequest {
    let mut req =
        ScanRequest::new(ScanTarget::LocalFile { path: fixture(rel) }, ScanProfile::Standard);
    req.scan_id = Uuid::parse_str("00000000-0000-4000-8000-0000000000a2").unwrap();
    req
}

fn codes(report: &paraclete_types::ScanReport) -> Vec<String> {
    let mut c: Vec<String> = report.findings.iter().map(|f| f.code.as_str().to_string()).collect();
    c.sort();
    c
}

#[test]
fn scan_single_parquet_produces_valid_report() {
    let req = request_for_file("phase1/single_parquet/data.parquet");
    let report = ScanEngine::scan(&req).expect("scan");
    paraclete_types::validate_report(&report).unwrap();
    assert_eq!(report.summary.files_scanned, 1);
    assert_eq!(report.summary.discovered_assets, 1);
    assert_eq!(report.assets.len(), 1);
    assert_eq!(report.assets[0].inspection_status, InspectionStatus::Inspected);
    assert!(report.assets[0].dataset_id.is_some());
    assert_eq!(report.summary.dataset_member_assets, 1);
    assert!(report.datasets.len() <= 1);
}

#[test]
fn scan_parquet_dataset_directory() {
    let req = request_for_dir("phase1/parquet_dataset");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(report.summary.files_scanned >= 2);
    assert!(!report.datasets.is_empty());
}

#[test]
fn mixed_format_emits_mixed_dataset() {
    let req = request_for_dir("phase1/mixed_dir");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::FORMAT_MIXED_DATASET.to_string()));
}

#[test]
fn tiny_parquet_emits_file_too_small() {
    let req = request_for_file("phase1/tiny_parquet/micro.parquet");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::METADATA_FILE_TOO_SMALL.to_string()));
}

#[test]
fn fragmented_row_groups_emit_finding() {
    let req = request_for_file("phase1/fragmented_rg/fragmented.parquet");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::METADATA_ROW_GROUP_SUSPICIOUSLY_SMALL.to_string()));
}

#[test]
fn hive_inconsistent_partition_keys() {
    let req = request_for_dir("phase1/hive_inconsistent");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::PARTITION_INCONSISTENT_KEYS.to_string()));
}

#[test]
fn unknown_extension_emits_format_unknown() {
    let dir = std::env::temp_dir().join(format!("paraclete_unknown_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("weird.bin");
    std::fs::write(&file, b"x").unwrap();
    let path = Utf8PathBuf::from_path_buf(file).unwrap();
    let mut req = ScanRequest::new(ScanTarget::LocalFile { path }, ScanProfile::Quick);
    req.scan_id = Uuid::nil();
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::FORMAT_UNKNOWN.to_string()));
    assert!(codes(&report).contains(&system::ASSET_INSPECTION_SKIPPED.to_string()));
    assert_eq!(report.summary.skipped_inspection_assets, 1);
    let _ = std::fs::remove_dir_all(&dir);
}

fn validate(report: &paraclete_types::ScanReport) {
    paraclete_types::validate_report(report).unwrap();
}

#[test]
fn local_scan_plan_counts_parquet_files() {
    let target = ScanTarget::LocalDirectory { path: fixture("phase1/parquet_dataset") };
    let plan =
        resolve_local_scan_plan(&target, &paraclete_types::ScanOptions::default()).expect("plan");
    assert_eq!(plan.assets.len(), 2);
    assert!(!plan.truncated);
}

#[test]
fn single_parquet_scan_json_roundtrips() {
    let req = request_for_file("phase1/single_parquet/data.parquet");
    let report = ScanEngine::scan(&req).expect("scan");
    let v = serde_json::to_value(&report).unwrap();
    let back: paraclete_types::ScanReport = serde_json::from_value(v.clone()).unwrap();
    paraclete_types::validate_report(&back).unwrap();
    assert_eq!(serde_json::to_value(&back).unwrap(), v);
}

#[test]
fn corrupt_parquet_surfaces_read_failure() {
    let req = request_for_file("phase2/corrupt_parquet/bad.parquet");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::FORMAT_PARQUET_READ_FAILED.to_string()));
    assert!(report.summary.partial_inspection);
    assert_eq!(report.summary.failed_inspection_assets, 1);
    assert_eq!(report.summary.dataset_count, 0);
}

#[test]
fn multi_dataset_detection() {
    let req = request_for_dir("phase2/multi_dataset");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(report.datasets.len() >= 2);
    assert!(codes(&report).contains(&system::DATASET_MULTIPLE_DETECTED.to_string()));
    assert_eq!(report.summary.dataset_count, report.datasets.len() as u64);
}

#[test]
fn shallow_csv_and_json_in_mixed_siblings() {
    let req = request_for_dir("phase2/mixed_siblings");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    let c = codes(&report);
    let bounded = c.iter().filter(|x| **x == system::FORMAT_TEXT_PROBE_BOUNDED).count();
    assert!(bounded >= 2, "expected bounded text probes for csv+json: {c:?}");
    assert!(report.summary.inspected_assets >= 3);
    assert_eq!(report.summary.discovered_assets, report.assets.len() as u64);
    assert_eq!(
        report.summary.inspected_assets
            + report.summary.failed_inspection_assets
            + report.summary.skipped_inspection_assets,
        report.summary.discovered_assets
    );
}

#[test]
fn unpartitioned_collection_finding() {
    let req = request_for_dir("phase2/unpartitioned_pair");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(codes(&report).contains(&system::PARTITION_UNPARTITIONED_COLLECTION.to_string()));
}

#[test]
fn ndjson_shallow_inspection() {
    let req = request_for_dir("phase2/shallow_text");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    let c = codes(&report);
    assert!(c.contains(&system::FORMAT_TEXT_PROBE_BOUNDED.to_string()));
    assert!(c.contains(&system::ASSET_INSPECTION_PARTIAL.to_string()));
}

#[test]
fn truncation_emits_scan_truncated_finding() {
    let dir = std::env::temp_dir().join(format!("paraclete_trunc_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    for i in 0..5 {
        std::fs::write(dir.join(format!("f{i}.csv")), b"a,b\n1,2\n").unwrap();
    }
    let path = Utf8PathBuf::from_path_buf(dir.clone()).unwrap();
    let mut req = ScanRequest::new(ScanTarget::LocalDirectory { path }, ScanProfile::Quick);
    req.scan_id = Uuid::nil();
    req.options.max_files = 2;
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(report.summary.scan_truncated);
    assert!(report.summary.partial_inspection);
    assert!(codes(&report).contains(&system::TARGET_SCAN_TRUNCATED.to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn mixed_multi_dataset_report_summary_stable() {
    let req = request_for_dir("phase2/mixed_siblings");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert_eq!(report.summary.files_scanned, 3);
    assert_eq!(report.summary.dataset_count, 1);
    assert!(!report.summary.scan_truncated);
}

#[test]
fn schema_split_flat_emits_grouping_ambiguous() {
    let req = request_for_dir("phase3/schema_split_flat");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(report.datasets.len() >= 2);
    assert!(codes(&report).contains(&system::DATASET_GROUPING_AMBIGUOUS.to_string()));
}

#[test]
fn nested_anchor_produces_multiple_datasets() {
    let req = request_for_dir("phase3/nested_mixed_anchor");
    let report = ScanEngine::scan(&req).expect("scan");
    validate(&report);
    assert!(report.datasets.len() >= 2);
    assert!(codes(&report).contains(&system::DATASET_MULTIPLE_DETECTED.to_string()));
}
