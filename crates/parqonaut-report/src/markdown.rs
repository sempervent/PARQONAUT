//! Markdown rendering stub for future human-facing summaries.

use parqonaut_types::ScanReport;

/// Renders a minimal Markdown summary for dashboards or terminals.
pub fn render_markdown_stub(report: &ScanReport) -> String {
    format!(
        "# PARQONAUT Scan Report\n\n- scan_id: `{}`\n- files_scanned: {}\n- findings_total: {}\n",
        report.metadata.scan_id, report.summary.files_scanned, report.summary.findings_total
    )
}
