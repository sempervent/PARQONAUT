//! Forensic scan use case (local engine + storage-backed remote datasets).

use parqonaut_core::ScanEngine;
use parqonaut_repair::backend_for_location;
use parqonaut_storage::location::DatasetLocation;
use parqonaut_types::{ScanProfile, ScanReport, ScanRequest, ScanTarget};

use crate::error::ApplicationError;
use crate::plugins::ScanPluginBridge;
use crate::policy::StoragePolicy;

#[derive(Debug, Clone)]
pub struct AppScanRequest {
    pub location: DatasetLocation,
    pub profile: ScanProfile,
    pub plugins: Vec<String>,
}

impl AppScanRequest {
    pub fn from_scan_target(
        target: ScanTarget,
        profile: ScanProfile,
        policy: &StoragePolicy,
    ) -> Result<(Self, ScanTarget), ApplicationError> {
        let location = crate::location::dataset_location_from_scan_target(&target)?;
        policy.validate_dataset(&location)?;
        Ok((Self { location, profile, plugins: Vec::new() }, target))
    }
}

pub async fn run_scan(
    req: &AppScanRequest,
    policy: &StoragePolicy,
) -> Result<ScanReport, ApplicationError> {
    run_scan_with_bridge(req, policy, None).await
}

pub async fn run_scan_with_bridge(
    req: &AppScanRequest,
    policy: &StoragePolicy,
    bridge: Option<&ScanPluginBridge>,
) -> Result<ScanReport, ApplicationError> {
    run_scan_with_bridge_and_progress(
        req,
        policy,
        bridge,
        &parqonaut_workflow::NoOpProgressObserver,
    )
    .await
}

pub async fn run_scan_with_bridge_and_progress(
    req: &AppScanRequest,
    policy: &StoragePolicy,
    bridge: Option<&ScanPluginBridge>,
    progress: &dyn parqonaut_workflow::ProgressObserver,
) -> Result<ScanReport, ApplicationError> {
    use parqonaut_workflow::{ProgressEvent, ProgressEventKind, ProgressMetrics};
    policy.validate_dataset(&req.location)?;
    progress.emit(ProgressEvent {
        kind: ProgressEventKind::ExecutionStarted,
        path: Some(req.location.display_uri()),
        metrics: ProgressMetrics::default(),
        message: Some(format!("scan started (profile={:?})", req.profile)),
    });
    match &req.location {
        DatasetLocation::Local(l) => {
            if !req.plugins.is_empty() {
                let bridge = match bridge {
                    Some(b) => b,
                    None => {
                        return Err(ApplicationError::InvalidRequest(
                            "internal: plugins requested without bridge".into(),
                        ));
                    }
                };
                let target = if l.path.is_file() {
                    ScanTarget::LocalFile { path: l.path.clone() }
                } else {
                    ScanTarget::LocalDirectory { path: l.path.clone() }
                };
                let mut scan_req = ScanRequest::new(target, req.profile);
                scan_req.options.requested_plugins = req.plugins.clone();
                let report = ScanEngine::run_with_plugins(&scan_req, Some(bridge))?;
                emit_scan_completion_events(progress, &report);
                Ok(report)
            } else if bridge.is_some() {
                Err(ApplicationError::InvalidRequest(
                    "plugins requested but selection empty".into(),
                ))
            } else {
                let target = if l.path.is_file() {
                    ScanTarget::LocalFile { path: l.path.clone() }
                } else {
                    ScanTarget::LocalDirectory { path: l.path.clone() }
                };
                let scan_req = ScanRequest::new(target, req.profile);
                let report = ScanEngine::run(&scan_req)?;
                emit_scan_completion_events(progress, &report);
                Ok(report)
            }
        }
        DatasetLocation::S3(_) => {
            if !req.plugins.is_empty() {
                return Err(ApplicationError::InvalidRequest(
                    "analyzer plugins are supported for local scans only in v0.10".into(),
                ));
            }
            let backend = backend_for_location(&req.location).await?;
            let report = backend.scan(&req.location).await.map_err(ApplicationError::from)?;
            emit_scan_completion_events(progress, &report);
            Ok(report)
        }
    }
}

fn emit_scan_completion_events(
    progress: &dyn parqonaut_workflow::ProgressObserver,
    report: &ScanReport,
) {
    use parqonaut_workflow::{ProgressEvent, ProgressEventKind, ProgressMetrics};
    if let Some(meta) = &report.plugin_metadata {
        for exec in &meta.executions {
            let kind = if exec.status == "succeeded" {
                ProgressEventKind::PluginCompleted
            } else {
                ProgressEventKind::PluginFailed
            };
            progress.emit(ProgressEvent {
                kind,
                path: Some(exec.name.clone()),
                metrics: ProgressMetrics { elapsed_ms: exec.duration_ms, ..Default::default() },
                message: Some(format!(
                    "plugin {} ({}) phase={} findings={}",
                    exec.name, exec.digest, exec.phase, exec.findings_contributed
                )),
            });
        }
    }
    progress.emit(ProgressEvent {
        kind: ProgressEventKind::InputsDiscovered,
        path: None,
        metrics: ProgressMetrics {
            files_total: report.summary.discovered_assets,
            files_done: report.summary.inspected_assets,
            ..Default::default()
        },
        message: Some(format!(
            "discovered {} assets, {} findings",
            report.summary.discovered_assets, report.summary.findings_total
        )),
    });
}
