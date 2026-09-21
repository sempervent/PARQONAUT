use paraclete_types::{ScanProfile, ScanReport};
use parqonaut_app::{ParqonautApp, ScanRequest};

use crate::location::parse_dataset_location;

pub async fn run_scan(
    path: String,
    profile: &str,
    plugins: Vec<String>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    let scan_profile = match profile {
        "quick" => ScanProfile::Quick,
        "deep" => ScanProfile::Deep,
        _ => ScanProfile::Standard,
    };
    let app = ParqonautApp::cli();
    let mut req = ScanRequest::new(location, scan_profile);
    req.plugins = plugins;
    let out = app.scan(req).await.map_err(|e| e.to_string())?;
    print_scan_report(&out.report, json)?;
    Ok(())
}

fn print_scan_report(report: &ScanReport, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
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
