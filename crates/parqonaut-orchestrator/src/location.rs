//! Config path parsing and resolution into [`DatasetLocation`].

use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use parqonaut_storage::location::{DatasetLocation, LocalLocation, S3Location};
use parqonaut_storage::StorageError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::OrchestratorError;

/// TOML string field parsed as a [`DatasetLocation`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigLocation(pub DatasetLocation);

impl ConfigLocation {
    pub fn resolve(&self, base: &Utf8Path) -> Result<DatasetLocation, OrchestratorError> {
        resolve_dataset_location(base, &self.0)
    }

    pub fn canonicalize_local(
        &self,
        base: &Utf8Path,
    ) -> Result<DatasetLocation, OrchestratorError> {
        canonicalize_local_location(base, &self.0)
    }

    pub fn as_location(&self) -> &DatasetLocation {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ConfigLocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        DatasetLocation::parse(&raw)
            .map(ConfigLocation)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

impl Serialize for ConfigLocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.display_uri())
    }
}

pub fn resolve_dataset_location(
    base: &Utf8Path,
    location: &DatasetLocation,
) -> Result<DatasetLocation, OrchestratorError> {
    match location {
        DatasetLocation::Local(local) => {
            let path =
                if local.path.is_absolute() { local.path.clone() } else { base.join(&local.path) };
            Ok(DatasetLocation::Local(LocalLocation { path }))
        }
        DatasetLocation::S3(_) => Ok(location.clone()),
    }
}

pub fn canonicalize_local_location(
    base: &Utf8Path,
    location: &DatasetLocation,
) -> Result<DatasetLocation, OrchestratorError> {
    match resolve_dataset_location(base, location)? {
        DatasetLocation::Local(local) => {
            let canonical = fs::canonicalize(local.path.as_std_path())
                .map_err(OrchestratorError::Io)?
                .try_into()
                .map_err(|_| OrchestratorError::InvalidConfig("non-UTF8 path".into()))?;
            Ok(DatasetLocation::Local(LocalLocation { path: canonical }))
        }
        other => Ok(other),
    }
}

pub fn join_output_segment(
    root: &DatasetLocation,
    segment: &str,
) -> Result<DatasetLocation, OrchestratorError> {
    if segment.is_empty() || segment.contains("..") || segment.contains('/') {
        return Err(OrchestratorError::InvalidConfig(format!(
            "invalid output segment `{segment}`"
        )));
    }
    match root {
        DatasetLocation::Local(local) => {
            Ok(DatasetLocation::Local(LocalLocation { path: local.path.join(segment) }))
        }
        DatasetLocation::S3(s3) => {
            let prefix = if s3.prefix.is_empty() {
                segment.to_string()
            } else {
                format!("{}/{}", s3.prefix.trim_end_matches('/'), segment)
            };
            Ok(DatasetLocation::S3(S3Location { bucket: s3.bucket.clone(), prefix }))
        }
    }
}

pub fn parse_output_override(
    raw: &str,
    base: &Utf8Path,
) -> Result<DatasetLocation, OrchestratorError> {
    let trimmed = raw.trim();
    if trimmed.contains("://") || Utf8Path::new(trimmed).is_absolute() {
        let loc = DatasetLocation::parse(trimmed).map_err(map_storage_err)?;
        resolve_dataset_location(base, &loc)
    } else {
        Err(OrchestratorError::InvalidConfig(format!(
            "output override `{raw}` is not an absolute path or URI"
        )))
    }
}

pub fn location_display(location: &DatasetLocation) -> String {
    location.display_uri()
}

pub fn local_path_for_policy(base: &Utf8Path, path: &Utf8Path) -> Utf8PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn map_storage_err(err: StorageError) -> OrchestratorError {
    OrchestratorError::InvalidConfig(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn resolves_relative_local_against_base() {
        let base = Utf8PathBuf::from("/cfg");
        let loc = ConfigLocation(DatasetLocation::parse("datasets/a").unwrap());
        match loc.resolve(&base).unwrap() {
            DatasetLocation::Local(l) => assert_eq!(l.path.as_str(), "/cfg/datasets/a"),
            _ => panic!("expected local"),
        }
    }

    #[test]
    fn joins_s3_output_segment() {
        let root = DatasetLocation::parse("s3://bucket/batch/").unwrap();
        let out = join_output_segment(&root, "ds-a").unwrap();
        match out {
            DatasetLocation::S3(s) => assert_eq!(s.prefix, "batch/ds-a"),
            _ => panic!("expected s3"),
        }
    }

    #[test]
    fn canonicalize_skips_s3() {
        let loc = DatasetLocation::parse("s3://bucket/p/").unwrap();
        let out = canonicalize_local_location(Utf8Path::new("/tmp"), &loc).unwrap();
        assert_eq!(out, loc);
    }

    #[test]
    fn canonicalize_local_path() {
        let tmp = TempDir::new().unwrap();
        let base = Utf8PathBuf::from(tmp.path().to_str().unwrap());
        let nested = base.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let loc = ConfigLocation(DatasetLocation::parse("nested").unwrap());
        let canon = loc.canonicalize_local(&base).unwrap();
        assert!(matches!(canon, DatasetLocation::Local(_)));
    }
}
