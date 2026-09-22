//! Application contract smoke tests (no HTTP/CLI).

use parqonaut_app::{ParqonautApp, ScanRequest};
use parqonaut_storage::location::DatasetLocation;
use parqonaut_types::ScanProfile;

#[tokio::test]
async fn scan_local_fixture_through_app() {
    let root = camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/scan/single_parquet/data.parquet");
    let location = DatasetLocation::parse(root.as_str()).unwrap();
    let app = ParqonautApp::cli();
    let out = app.scan(ScanRequest::new(location, ScanProfile::Standard)).await.unwrap();
    assert!(out.report.summary.files_scanned >= 1);
}
