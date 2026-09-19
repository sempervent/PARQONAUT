use paraclete_types::ScanReport;

use crate::location::{parse_dataset_location, resolve_backend};

pub async fn run_scan(
    path: String,
    profile: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let location = parse_dataset_location(&path)?;
    if location.backend_name() == "local" {
        let report = run_local_scan(&location, profile)?;
        print_scan_report(&report, json)?;
        return Ok(());
    }

    let backend = resolve_backend(&location).await?;
    let report = backend.scan(&location).await?;
    print_scan_report(&report, json)?;
    Ok(())
}

fn run_local_scan(
    location: &parqonaut_storage::location::DatasetLocation,
    profile: &str,
) -> Result<ScanReport, Box<dyn std::error::Error>> {
    use camino::Utf8PathBuf;
    use paraclete_core::ScanEngine;
    use paraclete_types::{ScanProfile, ScanRequest, ScanTarget};

    let root = match location {
        parqonaut_storage::location::DatasetLocation::Local(l) => l.path.clone(),
        _ => unreachable!("local scan requested for non-local location"),
    };

    let scan_profile = match profile {
        "quick" => ScanProfile::Quick,
        "deep" => ScanProfile::Deep,
        _ => ScanProfile::Standard,
    };

    let target = if root.is_file() {
        ScanTarget::LocalFile { path: root }
    } else {
        ScanTarget::LocalDirectory { path: root }
    };

    let request = ScanRequest::new(target, scan_profile);
    Ok(ScanEngine::run(&request)?)
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
