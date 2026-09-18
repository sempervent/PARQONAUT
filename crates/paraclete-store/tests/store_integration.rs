//! Persistence round-trip, projections, listing, diff, and redaction (SQLite).

use std::path::PathBuf;

use camino::Utf8PathBuf;
use chrono::{Duration, Utc};
use paraclete_core::ScanEngine;
use paraclete_store::{diff_reports, RedactionPolicy, RunId, SqliteScanStore, TargetIdentity};
use paraclete_types::{
    validate_report, FailureKind, InspectionStatus, ScanProfile, ScanRequest, ScanTarget,
};
use uuid::Uuid;

fn fixture(rel: &str) -> Utf8PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(rel);
    Utf8PathBuf::from_path_buf(path).expect("utf8 fixture path")
}

fn sqlite_url(dir: &tempfile::TempDir) -> String {
    let p = dir.path().join("store.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    format!("sqlite://{}", abs.display())
}

#[tokio::test]
async fn migrations_and_connect_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let url = sqlite_url(&dir);
    let _ = SqliteScanStore::connect(&url).await.expect("connect");
    let _ = SqliteScanStore::connect(&url).await.expect("reconnect applies cleanly");
}

#[tokio::test]
async fn persist_load_roundtrip_and_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let mut req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("phase1/single_parquet/data.parquet") },
        ScanProfile::Standard,
    );
    req.scan_id = Uuid::parse_str("00000000-0000-4000-8000-0000000000b1").unwrap();
    let report = ScanEngine::scan(&req).expect("scan");
    validate_report(&report).unwrap();
    let run_id = RunId::new();
    let started = Utc::now();
    let completed = Utc::now();
    store
        .persist_scan_run(run_id, started, completed, &report, &RedactionPolicy::default())
        .await
        .unwrap();
    let loaded = store.load_report(run_id).await.unwrap();
    assert_eq!(serde_json::to_string(&report).unwrap(), serde_json::to_string(&loaded).unwrap());
    store.verify_projection_integrity(run_id).await.unwrap();
}

#[tokio::test]
async fn list_runs_for_same_target() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let mut req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("phase1/single_parquet/data.parquet") },
        ScanProfile::Standard,
    );
    req.scan_id = Uuid::new_v4();
    let report = ScanEngine::scan(&req).unwrap();
    let identity = TargetIdentity::from_scan_target(&report.request.target);
    let r1 = RunId::new();
    let r2 = RunId::new();
    let t0 = Utc::now();
    store.persist_scan_run(r1, t0, t0, &report, &RedactionPolicy::default()).await.unwrap();
    let t1 = t0 + Duration::seconds(1);
    store.persist_scan_run(r2, t1, t1, &report, &RedactionPolicy::default()).await.unwrap();
    let listed = store.list_runs_for_target(&identity, 10).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|x| x.run_id == r2));
}

#[tokio::test]
async fn diff_two_loaded_runs() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let mut a_req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("phase1/tiny_parquet/micro.parquet") },
        ScanProfile::Standard,
    );
    a_req.scan_id = Uuid::new_v4();
    let rep_a = ScanEngine::scan(&a_req).unwrap();
    let mut b_req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("phase1/single_parquet/data.parquet") },
        ScanProfile::Standard,
    );
    b_req.scan_id = Uuid::new_v4();
    let rep_b = ScanEngine::scan(&b_req).unwrap();
    let ra = RunId::new();
    let rb = RunId::new();
    let t = Utc::now();
    store.persist_scan_run(ra, t, t, &rep_a, &RedactionPolicy::default()).await.unwrap();
    store.persist_scan_run(rb, t, t, &rep_b, &RedactionPolicy::default()).await.unwrap();
    let la = store.load_report(ra).await.unwrap();
    let lb = store.load_report(rb).await.unwrap();
    let d = diff_reports(ra, rb, &la, &lb);
    assert!(!d.findings.added.is_empty() || !d.findings.removed.is_empty());
}

#[tokio::test]
async fn redaction_strips_probe_before_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let mut req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("phase1/single_parquet/data.parquet") },
        ScanProfile::Standard,
    );
    req.scan_id = Uuid::new_v4();
    let report = ScanEngine::scan(&req).unwrap();
    let run_id = RunId::new();
    let t = Utc::now();
    let policy = RedactionPolicy {
        strip_probe_metadata: true,
        strip_probe_inspection_hints: true,
        ..Default::default()
    };
    store.persist_scan_run(run_id, t, t, &report, &policy).await.unwrap();
    let loaded = store.load_report(run_id).await.unwrap();
    for a in &loaded.assets {
        assert!(a.probe.is_none());
        assert!(a.inspection_hints.is_none());
    }
}

#[tokio::test]
async fn failed_asset_roundtrip_keeps_failure_kind() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    let mut req = ScanRequest::new(
        ScanTarget::LocalFile { path: fixture("phase2/corrupt_parquet/bad.parquet") },
        ScanProfile::Standard,
    );
    req.scan_id = Uuid::new_v4();
    let report = ScanEngine::scan(&req).unwrap();
    let failed = report
        .assets
        .iter()
        .find(|a| a.inspection_status == InspectionStatus::Failed)
        .expect("expected failed asset in fixture");
    assert_eq!(failed.failure_kind, Some(FailureKind::FormatReadError));
    let run_id = RunId::new();
    let t = Utc::now();
    store.persist_scan_run(run_id, t, t, &report, &RedactionPolicy::default()).await.unwrap();
    let loaded = store.load_report(run_id).await.unwrap();
    let lf =
        loaded.assets.iter().find(|a| a.inspection_status == InspectionStatus::Failed).unwrap();
    assert_eq!(lf.failure_kind, Some(FailureKind::FormatReadError));
}
