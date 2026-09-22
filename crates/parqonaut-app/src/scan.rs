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
    policy.validate_dataset(&req.location)?;
    match &req.location {
        DatasetLocation::Local(l) => {
            if !req.plugins.is_empty() {
                let bridge = ScanPluginBridge::new(req.plugins.clone())?;
                let target = if l.path.is_file() {
                    ScanTarget::LocalFile { path: l.path.clone() }
                } else {
                    ScanTarget::LocalDirectory { path: l.path.clone() }
                };
                let mut scan_req = ScanRequest::new(target, req.profile);
                scan_req.options.requested_plugins = req.plugins.clone();
                Ok(ScanEngine::run_with_plugins(&scan_req, Some(&bridge))?)
            } else {
                let target = if l.path.is_file() {
                    ScanTarget::LocalFile { path: l.path.clone() }
                } else {
                    ScanTarget::LocalDirectory { path: l.path.clone() }
                };
                let scan_req = ScanRequest::new(target, req.profile);
                Ok(ScanEngine::run(&scan_req)?)
            }
        }
        DatasetLocation::S3(_) => {
            if !req.plugins.is_empty() {
                return Err(ApplicationError::InvalidRequest(
                    "analyzer plugins are supported for local scans only in v0.10".into(),
                ));
            }
            let backend = backend_for_location(&req.location).await?;
            backend.scan(&req.location).await.map_err(ApplicationError::from)
        }
    }
}
