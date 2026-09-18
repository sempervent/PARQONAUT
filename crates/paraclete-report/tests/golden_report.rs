use std::path::PathBuf;

use chrono::{TimeZone, Utc};
use paraclete_report::validate_report;
use paraclete_types::{
    contract_schema_version, report_format_version, ReportMetadata, ScanMode, ScanOptions,
    ScanProfile, ScanReport, ScanRequest, ScanSummary, ScanTarget,
};
use serde_json::Value;
use uuid::Uuid;

fn deterministic_report() -> ScanReport {
    let scan_id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
    let target = ScanTarget::LocalDirectory { path: camino::Utf8PathBuf::from("fixtures/csv") };
    let mut request = ScanRequest::new(target, ScanProfile::Quick);
    request.scan_id = scan_id;
    request.options =
        ScanOptions { mode: ScanMode::Full, max_files: 100_000, format_hints: Vec::new() };
    ScanReport {
        request: request.clone(),
        metadata: ReportMetadata {
            scan_id,
            generated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            contract_schema: contract_schema_version(),
            report_format: report_format_version(),
            engine_revision: Some("phase-4-scaffold".into()),
        },
        summary: ScanSummary {
            discovered_assets: 0,
            files_scanned: 0,
            inspected_assets: 0,
            failed_inspection_assets: 0,
            skipped_inspection_assets: 0,
            dataset_member_assets: 0,
            dataset_count: 0,
            partial_inspection: false,
            scan_truncated: false,
            findings_total: 0,
            findings_by_severity: std::collections::BTreeMap::new(),
        },
        assets: Vec::new(),
        datasets: Vec::new(),
        dataset_summaries: Vec::new(),
        format_summaries: Vec::new(),
        findings: Vec::new(),
    }
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/reports/minimal_report.json")
}

#[test]
fn minimal_report_fixture_roundtrips() {
    if std::env::var("PARACLETE_UPDATE_FIXTURES").ok().as_deref() == Some("1") {
        let report = deterministic_report();
        std::fs::write(fixture_path(), serde_json::to_string_pretty(&report).expect("serialize"))
            .expect("write fixture");
    }
    let raw = std::fs::read_to_string(fixture_path()).expect("fixture");
    let value: Value = serde_json::from_str(&raw).expect("parse json");
    let report: paraclete_types::ScanReport =
        serde_json::from_value(value.clone()).expect("report");
    validate_report(&report).expect("valid report");
    let again = serde_json::to_value(&report).expect("serialize");
    assert_eq!(value, again);
}

#[test]
fn deterministic_report_matches_fixture() {
    let expected = serde_json::to_value(deterministic_report()).unwrap();
    let raw = std::fs::read_to_string(fixture_path()).expect("fixture");
    let from_disk: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(expected, from_disk);
}
