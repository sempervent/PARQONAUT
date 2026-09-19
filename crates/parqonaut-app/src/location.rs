//! Convert scan targets and URI strings into [`DatasetLocation`].

use camino::Utf8PathBuf;
use paraclete_types::ScanTarget;
use parqonaut_storage::location::DatasetLocation;
use url::Url;

use crate::error::ApplicationError;

pub fn dataset_location_from_scan_target(
    target: &ScanTarget,
) -> Result<DatasetLocation, ApplicationError> {
    match target {
        ScanTarget::LocalFile { path } | ScanTarget::LocalDirectory { path } => {
            Ok(DatasetLocation::Local(parqonaut_storage::location::LocalLocation {
                path: path.clone(),
            }))
        }
        ScanTarget::ObjectStorePlaceholder { inner } => object_uri_to_location(&inner.uri),
        ScanTarget::LogicalDataset { inner } => Err(ApplicationError::InvalidRequest(format!(
            "logical dataset `{}` is not supported over the application server yet",
            inner.dataset_id
        ))),
    }
}

pub fn parse_dataset_location(input: &str) -> Result<DatasetLocation, ApplicationError> {
    DatasetLocation::parse(input).map_err(ApplicationError::from)
}

fn object_uri_to_location(uri: &Url) -> Result<DatasetLocation, ApplicationError> {
    if uri.scheme() == "s3" {
        let bucket = uri
            .host_str()
            .ok_or_else(|| ApplicationError::InvalidRequest("s3 URI missing bucket host".into()))?;
        let prefix = uri.path().trim_start_matches('/');
        return DatasetLocation::parse(&format!("s3://{bucket}/{prefix}"))
            .map_err(ApplicationError::from);
    }
    if uri.scheme() == "file" {
        let path = uri
            .to_file_path()
            .map_err(|_| ApplicationError::InvalidRequest("invalid file:// URI".into()))?;
        let utf = Utf8PathBuf::from_path_buf(path)
            .map_err(|_| ApplicationError::InvalidRequest("file path is not valid UTF-8".into()))?;
        return Ok(DatasetLocation::Local(parqonaut_storage::location::LocalLocation {
            path: utf,
        }));
    }
    Err(ApplicationError::InvalidRequest(format!(
        "unsupported object store URI scheme `{}`",
        uri.scheme()
    )))
}
