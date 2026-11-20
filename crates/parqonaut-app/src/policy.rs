//! Server-side location policy (local roots and S3 buckets/prefixes).

use camino::{Utf8Path, Utf8PathBuf};
use parqonaut_storage::location::{DatasetLocation, S3Location};
use serde::{Deserialize, Serialize};

use crate::error::ApplicationError;

/// Where durable server artifacts (plans, manifests) may be written.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
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
        let abs = resolve_lexical_or_canonical(path)?;
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

fn resolve_lexical_or_canonical(path: &Utf8Path) -> Result<Utf8PathBuf, ApplicationError> {
    if let Ok(abs) = path.canonicalize_utf8() {
        return Ok(abs);
    }
    let mut suffix: Vec<&str> = Vec::new();
    let mut cursor = path;
    loop {
        if let Ok(abs) = cursor.canonicalize_utf8() {
            let mut out = abs;
            for seg in suffix.into_iter().rev() {
                out.push(seg);
            }
            return Ok(out);
        }
        let Some(comp) = cursor.components().next_back() else {
            return Err(ApplicationError::InvalidRequest(format!(
                "cannot resolve local path `{path}`"
            )));
        };
        match comp {
            camino::Utf8Component::Normal(s) => suffix.push(s),
            camino::Utf8Component::CurDir => {}
            camino::Utf8Component::ParentDir => {
                return Err(ApplicationError::LocationNotAllowed(format!(
                    "parent directory traversal (`..`) is not allowed in `{path}`"
                )));
            }
            _ => {
                return Err(ApplicationError::InvalidRequest(format!(
                    "cannot resolve local path `{path}`"
                )));
            }
        }
        cursor = cursor.parent().ok_or_else(|| {
            ApplicationError::InvalidRequest(format!("cannot resolve local path `{path}`"))
        })?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_non_existent_output_under_allowed_root_and_rejects_traversal() {
        let base =
            std::env::temp_dir().join(format!("parqonaut-policy-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let root = Utf8PathBuf::from_path_buf(base.clone()).unwrap();
        let policy = StoragePolicy {
            allow_unrestricted_local: false,
            allowed_local_roots: vec![root.clone()],
            ..StoragePolicy::default()
        };

        let nested_out = root.join("nested").join("repaired-ds");
        assert!(policy.validate_local_path(&nested_out).is_ok());

        let traversal = root.join("nested").join("..").join("..").join("etc");
        assert!(matches!(
            policy.validate_local_path(&traversal),
            Err(ApplicationError::LocationNotAllowed(_))
        ));

        let outside = Utf8PathBuf::from("/tmp/definitely-outside-parqonaut-test-root");
        assert!(matches!(
            policy.validate_local_path(&outside),
            Err(ApplicationError::LocationNotAllowed(_))
        ));
        let _ = std::fs::remove_dir_all(&base);
    }
}
