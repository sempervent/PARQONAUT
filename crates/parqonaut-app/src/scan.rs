//! Forensic scan use case (local engine + storage-backed remote datasets).

use paraclete_core::ScanEngine;
use paraclete_types::{ScanProfile, ScanReport, ScanRequest, ScanTarget};
use parqonaut_repair::backend_for_location;
use parqonaut_storage::location::DatasetLocation;

use crate::error::ApplicationError;
use crate::policy::StoragePolicy;

#[derive(Debug, Clone)]
pub struct AppScanRequest {
    pub location: DatasetLocation,
    pub profile: ScanProfile,
}

impl AppScanRequest {
    pub fn from_scan_target(
        target: ScanTarget,
        profile: ScanProfile,
        policy: &StoragePolicy,
    ) -> Result<(Self, ScanTarget), ApplicationError> {
        let location = crate::location::dataset_location_from_scan_target(&target)?;
        policy.validate_dataset(&location)?;
        Ok((Self { location, profile }, target))
    }
}

pub async fn run_scan(
    req: &AppScanRequest,
    policy: &StoragePolicy,
) -> Result<ScanReport, ApplicationError> {
    policy.validate_dataset(&req.location)?;
    match &req.location {
        DatasetLocation::Local(l) => {
            let target = if l.path.is_file() {
                ScanTarget::LocalFile { path: l.path.clone() }
            } else {
                ScanTarget::LocalDirectory { path: l.path.clone() }
            };
            let scan_req = ScanRequest::new(target, req.profile);
            Ok(ScanEngine::run(&scan_req)?)
        }
        DatasetLocation::S3(_) => {
            let backend = backend_for_location(&req.location).await?;
            backend.scan(&req.location).await.map_err(ApplicationError::from)
        }
    }
}
