//! Parse dataset locations and resolve storage backends for CLI commands.

use parqonaut_repair::{backend_for_location, RepairBackend, RepairError};
use parqonaut_storage::location::DatasetLocation;

pub fn parse_dataset_location(input: &str) -> Result<DatasetLocation, RepairError> {
    DatasetLocation::parse(input).map_err(|e| RepairError::ScanFailed(e.to_string()))
}

pub async fn resolve_backend(location: &DatasetLocation) -> Result<RepairBackend, RepairError> {
    backend_for_location(location).await
}
