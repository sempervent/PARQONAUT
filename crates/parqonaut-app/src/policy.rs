//! Server-side location policy (local roots and S3 buckets/prefixes).

use camino::{Utf8Path, Utf8PathBuf};
use parqonaut_storage::location::{DatasetLocation, S3Location};
use serde::{Deserialize, Serialize};

use crate::error::ApplicationError;

/// Where durable server artifacts (plans, manifests) may be written.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoragePolicy {
    /// When true, any local path is accepted (CLI default).
    #[serde(default)]
    pub allow_unrestricted_local: bool,
    #[serde(default)]
    pub allowed_local_roots: Vec<Utf8PathBuf>,
    #[serde(default)]
    pub allowed_s3_buckets: Vec<String>,
    /// `bucket/prefix` entries; empty prefix means whole bucket.
    #[serde(default)]
    pub allowed_s3_prefixes: Vec<String>,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            allow_unrestricted_local: false,
            allowed_local_roots: Vec::new(),
            allowed_s3_buckets: Vec::new(),
            allowed_s3_prefixes: Vec::new(),
        }
    }
}

impl StoragePolicy {
    /// CLI and direct local workflows: preserve existing unrestricted local access.
    #[must_use]
    pub fn cli_unrestricted_local() -> Self {
        Self { allow_unrestricted_local: true, ..Self::default() }
    }

    pub fn validate_dataset(&self, location: &DatasetLocation) -> Result<(), ApplicationError> {
        match location {
            DatasetLocation::Local(l) => self.validate_local_path(&l.path),
            DatasetLocation::S3(s) => self.validate_s3(s),
        }
    }

    pub fn validate_local_path(&self, path: &Utf8Path) -> Result<(), ApplicationError> {
        if self.allow_unrestricted_local {
            return Ok(());
        }
        if self.allowed_local_roots.is_empty() {
            return Err(ApplicationError::LocationNotAllowed(
                "local filesystem access is disabled; configure server.storage.allowed_local_roots"
                    .into(),
            ));
        }
        let abs = path
            .canonicalize_utf8()
            .map_err(|e| ApplicationError::InvalidRequest(e.to_string()))?;
        for root in &self.allowed_local_roots {
            let root_abs = root.canonicalize_utf8().unwrap_or_else(|_| root.clone());
            if abs.starts_with(&root_abs) {
                return Ok(());
            }
        }
        Err(ApplicationError::LocationNotAllowed(format!(
            "path `{}` is outside configured allowed_local_roots",
            path
        )))
    }

    fn validate_s3(&self, loc: &S3Location) -> Result<(), ApplicationError> {
        if !self.allowed_s3_buckets.is_empty()
            && !self.allowed_s3_buckets.iter().any(|b| b == &loc.bucket)
        {
            return Err(ApplicationError::LocationNotAllowed(format!(
                "s3 bucket `{}` is not allowed",
                loc.bucket
            )));
        }
        if self.allowed_s3_prefixes.is_empty() {
            return Ok(());
        }
        let uri = loc.display_uri();
        let allowed = self.allowed_s3_prefixes.iter().any(|entry| {
            if let Some((bucket, prefix)) = entry.split_once('/') {
                loc.bucket == bucket
                    && (prefix.is_empty() || loc.prefix.starts_with(prefix.trim_end_matches('/')))
            } else {
                loc.bucket == *entry
            }
        });
        if allowed {
            Ok(())
        } else {
            Err(ApplicationError::LocationNotAllowed(format!(
                "s3 location `{uri}` is outside configured allowed_s3_prefixes"
            )))
        }
    }
}
