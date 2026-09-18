//! `diff_reports` and `apply_redaction_policy` behavior.

use camino::Utf8PathBuf;
use std::path::PathBuf;

use paraclete_types::{
    apply_redaction_policy, diff_reports, system, validate_report, AssetRecord, DataFormat,
    FailureKind, Finding, FindingCategory, FindingCode, FindingSeverity, InspectionStatus,
    ParseConfidence, ProbeDepth, ProbeMetadata, RedactionPolicy, RunDiff, RunId, ScanReport,
    ScanSummary,
};
use uuid::Uuid;

fn sample_finding() -> Finding {
    Finding {
        id: Uuid::from_u128(42),
        code: FindingCode::try_new(system::FORMAT_UNKNOWN).unwrap(),
        severity: FindingSeverity::Low,
        category: FindingCategory::Format,
        summary: "x".into(),
        detail: "y".into(),
        evidence: Vec::new(),
        locations: Vec::new(),
        recommendations: Vec::new(),
        fingerprint: None,
        attributes: Default::default(),
    }
}

#[test]
fn diff_reports_detects_added_finding_fingerprint() {
    let ra = RunId(Uuid::from_u128(1));
    let rb = RunId(Uuid::from_u128(2));
    let a = ScanReport::minimal_example();
    let mut b = a.clone();
    let mut f = sample_finding();
    f.fingerprint = Some(f.compute_fingerprint());
    b.findings.push(f.clone());
    b.summary.findings_total = 1;
    validate_report(&a).unwrap();
    validate_report(&b).unwrap();
    let d = diff_reports(ra, rb, &a, &b);
    assert_eq!(d.findings.removed.len(), 0);
    assert_eq!(d.findings.added, vec![f.fingerprint.as_ref().unwrap().digest.clone()]);
    assert_eq!(d.summary_delta.findings_total_delta, 1);
}

#[test]
fn redaction_strips_probe_and_truncates_failure_message() {
    let mut report = ScanReport::minimal_example();
    report.summary = ScanSummary {
        discovered_assets: 1,
        files_scanned: 1,
        inspected_assets: 1,
        failed_inspection_assets: 0,
        skipped_inspection_assets: 0,
        dataset_member_assets: 0,
        dataset_count: 0,
        partial_inspection: false,
        scan_truncated: false,
        findings_total: 0,
        findings_by_severity: Default::default(),
    };
    report.assets.push(AssetRecord {
        path: Utf8PathBuf::from("a.csv"),
        format: DataFormat::Csv,
        size_bytes: 10,
        inspection_status: InspectionStatus::Inspected,
        failure_kind: None,
        failure_message: None,
        dataset_id: None,
        probe: Some(ProbeMetadata {
            bytes_sampled: 5,
            file_size_bytes: Some(10),
            encoding_assumption: "utf-8".into(),
            probe_depth: ProbeDepth::PartialHead,
            parse_confidence: ParseConfidence::High,
            notes: None,
        }),
        inspection_hints: Some(serde_json::json!({"k": 1})),
    });
    validate_report(&report).unwrap();
    let out = apply_redaction_policy(
        report,
        &RedactionPolicy {
            strip_probe_metadata: true,
            strip_probe_inspection_hints: true,
            strip_finding_evidence_payloads: false,
            truncate_failure_messages_to: None,
        },
    );
    assert!(out.assets[0].probe.is_none());
    assert!(out.assets[0].inspection_hints.is_none());
}

#[test]
fn redaction_truncates_failure_message_on_failed_asset() {
    let mut report = ScanReport::minimal_example();
    report.summary = ScanSummary {
        discovered_assets: 1,
        files_scanned: 1,
        inspected_assets: 0,
        failed_inspection_assets: 1,
        skipped_inspection_assets: 0,
        dataset_member_assets: 0,
        dataset_count: 0,
        partial_inspection: true,
        scan_truncated: false,
        findings_total: 0,
        findings_by_severity: Default::default(),
    };
    report.assets.push(AssetRecord {
        path: Utf8PathBuf::from("bad.parquet"),
        format: DataFormat::Parquet,
        size_bytes: 3,
        inspection_status: InspectionStatus::Failed,
        failure_kind: Some(FailureKind::FormatReadError),
        failure_message: Some("abcdefgh".into()),
        dataset_id: None,
        probe: None,
        inspection_hints: None,
    });
    validate_report(&report).unwrap();
    let out = apply_redaction_policy(
        report,
        &RedactionPolicy {
            strip_probe_metadata: false,
            strip_probe_inspection_hints: false,
            strip_finding_evidence_payloads: false,
            truncate_failure_messages_to: Some(4),
        },
    );
    assert_eq!(out.assets[0].failure_message.as_deref(), Some("abcd"));
}

#[test]
fn sample_run_diff_fixture_roundtrips() {
    let p =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/store/sample_run_diff.json");
    let raw = std::fs::read_to_string(p).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let d: RunDiff = serde_json::from_value(v.clone()).unwrap();
    let again = serde_json::to_value(&d).unwrap();
    assert_eq!(v, again);
}
