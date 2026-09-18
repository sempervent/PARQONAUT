use camino::Utf8PathBuf;
use paraclete_types::{
    system, validate_finding, validate_report, ContractSchemaVersion, DataFormat, Dataset,
    DatasetFile, Evidence, EvidenceKind, EvidenceLocationRef, EvidenceReference, Finding,
    FindingCategory, FindingCode, FindingLocation, FindingSeverity, FormatSupportTier,
    GroupingKind, LogicalDatasetTarget, ObjectStoreTargetPlaceholder, PartitionLayout,
    PartitionSegment, ReportFormatVersion, ScanMode, ScanProfile, ScanReport, ScanRequest,
    ScanTarget, TargetKind,
};
use url::Url;
use uuid::Uuid;

#[test]
fn scan_target_roundtrips_json() {
    let targets = vec![
        ScanTarget::LocalFile { path: Utf8PathBuf::from("data/file.parquet") },
        ScanTarget::LocalDirectory { path: Utf8PathBuf::from("data/lake") },
        ScanTarget::LogicalDataset {
            inner: LogicalDatasetTarget {
                dataset_id: "sales_curated".into(),
                resolver_hint: Some("catalog:prod".into()),
            },
        },
        ScanTarget::ObjectStorePlaceholder {
            inner: ObjectStoreTargetPlaceholder {
                uri: Url::parse("s3://bucket/prefix/").unwrap(),
                description: Some("future seam".into()),
            },
        },
    ];

    for target in targets {
        let json = serde_json::to_string(&target).unwrap();
        let back: ScanTarget = serde_json::from_str(&json).unwrap();
        assert_eq!(target, back);
    }
}

#[test]
fn finding_code_serializes_as_plain_string() {
    let code = FindingCode::try_new(system::FORMAT_UNKNOWN).unwrap();
    let v = serde_json::to_value(code).unwrap();
    assert_eq!(v, serde_json::json!(system::FORMAT_UNKNOWN));
}

#[test]
fn format_support_tier_defaults_match_policy() {
    assert_eq!(DataFormat::Parquet.default_support_tier(), FormatSupportTier::FirstClass);
    assert_eq!(DataFormat::Csv.default_support_tier(), FormatSupportTier::SecondClass);
}

#[test]
fn scan_request_defaults_options() {
    let req = ScanRequest::new(
        ScanTarget::LocalDirectory { path: Utf8PathBuf::from(".") },
        ScanProfile::Standard,
    );
    assert_eq!(req.options.mode, ScanMode::Full);
}

#[test]
fn target_kind_derivation() {
    let t = ScanTarget::LocalFile { path: Utf8PathBuf::from("x.parquet") };
    assert_eq!(t.target_kind(), TargetKind::LocalFile);
}

#[test]
fn finding_validation_and_fingerprint() {
    let finding = Finding {
        id: Uuid::nil(),
        code: FindingCode::try_new(system::FORMAT_UNKNOWN).unwrap(),
        severity: FindingSeverity::Medium,
        category: FindingCategory::Format,
        summary: "Unknown format".into(),
        detail: "Could not infer a supported format.".into(),
        evidence: vec![Evidence {
            id: Uuid::nil(),
            kind: EvidenceKind::FileSlice,
            summary: "magic bytes".into(),
            location_ref: Some(EvidenceLocationRef {
                path: Some("file.bin".into()),
                row_group_index: None,
                partition: Vec::new(),
            }),
            payload: None,
            references: vec![EvidenceReference {
                label: "path".into(),
                target: Some("file.bin".into()),
            }],
        }],
        locations: vec![FindingLocation {
            file: Some(Utf8PathBuf::from("file.bin")),
            columns: Vec::new(),
            partition: vec![PartitionSegment { key: "dt".into(), value: "2024-01-01".into() }],
            row_group: None,
        }],
        recommendations: Vec::new(),
        fingerprint: None,
        attributes: Default::default(),
    };
    validate_finding(&finding).unwrap();
    let fp = finding.compute_fingerprint();
    assert!(!fp.digest.is_empty());
}

#[test]
fn report_validation_counts() {
    let mut report = ScanReport::minimal_example();
    report.summary.findings_total = 1;
    report.summary.dataset_count = 0;
    report.summary.partial_inspection = false;
    report.summary.scan_truncated = false;
    report.summary.discovered_assets = 0;
    report.summary.files_scanned = 0;
    report.summary.inspected_assets = 0;
    report.summary.failed_inspection_assets = 0;
    report.summary.skipped_inspection_assets = 0;
    report.summary.dataset_member_assets = 0;
    report.assets.clear();
    report.findings.push(Finding {
        id: Uuid::nil(),
        code: FindingCode::try_new(system::METADATA_FILE_TOO_SMALL).unwrap(),
        severity: FindingSeverity::Low,
        category: FindingCategory::Metadata,
        summary: "small".into(),
        detail: "file is tiny".into(),
        evidence: Vec::new(),
        locations: Vec::new(),
        recommendations: Vec::new(),
        fingerprint: None,
        attributes: Default::default(),
    });
    validate_report(&report).unwrap();
}

#[test]
fn version_newtypes_roundtrip() {
    let c = ContractSchemaVersion("0.5.0".into());
    let r = ReportFormatVersion("0.5.0".into());
    let cj = serde_json::to_string(&c).unwrap();
    let rj = serde_json::to_string(&r).unwrap();
    assert!(cj.contains("0.5.0"));
    assert!(rj.contains("0.5.0"));
}

#[test]
fn dataset_inventory_roundtrip() {
    let ds = Dataset {
        dataset_id: "ds1".into(),
        grouping_kind: GroupingKind::PathAnchor,
        partition_layout: PartitionLayout::HiveDirectoryKeys,
        files: vec![DatasetFile {
            path: Utf8PathBuf::from("p=1/a.parquet"),
            format: DataFormat::Parquet,
            partitions: vec![PartitionSegment { key: "p".into(), value: "1".into() }],
            byte_length: Some(12),
        }],
        schema: None,
        column_profiles: Vec::new(),
    };
    let json = serde_json::to_string(&ds).unwrap();
    let back: Dataset = serde_json::from_str(&json).unwrap();
    assert_eq!(ds, back);
}
