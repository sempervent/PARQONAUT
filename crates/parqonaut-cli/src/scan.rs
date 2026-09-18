use camino::Utf8PathBuf;
use paraclete_core::ScanEngine;
use paraclete_types::{ScanProfile, ScanRequest, ScanTarget};
use std::path::PathBuf;

pub async fn run_scan(
    path: PathBuf,
    profile: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let utf8_path = Utf8PathBuf::from_path_buf(path.clone())
        .map_err(|_| format!("non-UTF8 path: {}", path.display()))?;

    let scan_profile = match profile {
        "quick" => ScanProfile::Quick,
        "deep" => ScanProfile::Deep,
        _ => ScanProfile::Standard,
    };

    let target = if utf8_path.is_file() {
        ScanTarget::LocalFile { path: utf8_path }
    } else {
        ScanTarget::LocalDirectory { path: utf8_path }
    };

    let request = ScanRequest::new(target, scan_profile);
    let report = ScanEngine::run(&request)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Scan complete: {}", report.request.scan_id);
        println!("  Files scanned: {}", report.summary.files_scanned);
        println!("  Assets: {}", report.summary.discovered_assets);
        println!("  Findings: {}", report.findings.len());
        println!("  Datasets: {}", report.datasets.len());
        for finding in &report.findings {
            println!("  [{:?}] {} — {}", finding.severity, finding.code, finding.summary);
        }
    }

    Ok(())
}
