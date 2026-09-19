use std::fmt;
use std::path::{Component, Path, PathBuf};

use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::StorageError;
use crate::redact::RedactUri;

/// A dataset prefix (directory or object-store prefix).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum DatasetLocation {
    Local(LocalLocation),
    S3(S3Location),
}

/// A single object within a backend.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum ObjectLocation {
    Local { path: Utf8PathBuf },
    S3 { bucket: String, key: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocalLocation {
    pub path: Utf8PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct S3Location {
    pub bucket: String,
    /// Object key prefix without leading slash. Empty string = bucket root.
    pub prefix: String,
}

impl DatasetLocation {
    pub fn parse(input: &str) -> Result<Self, StorageError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(StorageError::InvalidLocation {
                message: "location must not be empty".into(),
            });
        }
        if trimmed.starts_with("s3://") {
            return Ok(Self::S3(parse_s3_dataset(trimmed)?));
        }
        Ok(Self::Local(LocalLocation { path: parse_local_dataset(trimmed)? }))
    }

    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::Local(_) => "local",
            Self::S3(_) => "s3",
        }
    }

    pub fn display_uri(&self) -> String {
        match self {
            Self::Local(l) => format!("file://{}", l.path.as_str()),
            Self::S3(s) => s.display_uri(),
        }
    }
}

impl S3Location {
    pub fn display_uri(&self) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}/", self.bucket)
        } else {
            format!("s3://{}/{}", self.bucket, self.prefix)
        }
    }

    pub fn object_key(&self, relative: &str) -> Result<String, StorageError> {
        let rel = relative.trim_start_matches('/');
        if rel.contains("..") {
            return Err(StorageError::InvalidLocation {
                message: "object key must not contain '..'".into(),
            });
        }
        let key = if self.prefix.is_empty() {
            rel.to_string()
        } else if rel.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), rel)
        };
        Ok(key)
    }
}

impl ObjectLocation {
    pub fn from_local_path(path: impl AsRef<Utf8Path>) -> Self {
        Self::Local { path: path.as_ref().to_path_buf() }
    }

    pub fn display_uri(&self) -> String {
        match self {
            Self::Local { path } => format!("file://{}", path.as_str()),
            Self::S3 { bucket, key } => format!("s3://{bucket}/{key}"),
        }
    }
}

impl fmt::Display for DatasetLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_uri().redact_uri())
    }
}

fn parse_local_dataset(input: &str) -> Result<Utf8PathBuf, StorageError> {
    let path = if let Ok(url) = Url::parse(input) {
        if url.scheme() == "file" {
            url.to_file_path().map_err(|_| StorageError::InvalidLocation {
                message: format!("invalid file URI: {input}"),
            })?
        } else {
            return Err(StorageError::InvalidLocation {
                message: format!("unsupported URI scheme for local dataset: {input}"),
            });
        }
    } else {
        PathBuf::from(input)
    };
    let normalized = normalize_local_path(&path);
    Utf8PathBuf::from_path_buf(normalized).map_err(|_| StorageError::InvalidLocation {
        message: "local path must be valid UTF-8".into(),
    })
}

fn normalize_local_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(c) => out.push(c),
        }
    }
    out
}

fn parse_s3_dataset(input: &str) -> Result<S3Location, StorageError> {
    let url = Url::parse(input)
        .map_err(|e| StorageError::InvalidLocation { message: format!("invalid S3 URI: {e}") })?;
    if url.scheme() != "s3" {
        return Err(StorageError::InvalidLocation {
            message: format!("expected s3:// URI, got {input}"),
        });
    }
    if url.username() != "" || url.password().is_some() {
        return Err(StorageError::InvalidLocation {
            message: "credentials must not be embedded in S3 URIs".into(),
        });
    }
    let bucket = url
        .host_str()
        .ok_or_else(|| StorageError::InvalidLocation { message: "S3 URI missing bucket".into() })?
        .to_string();
    let prefix = url.path().trim_start_matches('/').trim_end_matches('/').to_string();
    Ok(S3Location { bucket, prefix })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_relative_local_path() {
        let loc = DatasetLocation::parse("./data/set").unwrap();
        assert!(matches!(loc, DatasetLocation::Local(_)));
    }

    #[test]
    fn parses_file_uri() {
        let loc = DatasetLocation::parse("file:///tmp/data").unwrap();
        match loc {
            DatasetLocation::Local(l) => assert!(l.path.as_str().ends_with("tmp/data")),
            _ => panic!("expected local"),
        }
    }

    #[test]
    fn parses_s3_prefix_variants() {
        let a = DatasetLocation::parse("s3://bucket").unwrap();
        let b = DatasetLocation::parse("s3://bucket/").unwrap();
        let c = DatasetLocation::parse("s3://bucket/prefix/").unwrap();
        match (a, b, c) {
            (DatasetLocation::S3(sa), DatasetLocation::S3(sb), DatasetLocation::S3(sc)) => {
                assert_eq!(sa.bucket, "bucket");
                assert_eq!(sb.prefix, "");
                assert_eq!(sc.prefix, "prefix");
            }
            _ => panic!("expected s3"),
        }
    }

    #[test]
    fn rejects_embedded_credentials() {
        let err = DatasetLocation::parse("s3://AKIA:key@bucket/path").unwrap_err();
        assert!(matches!(err, StorageError::InvalidLocation { .. }));
    }

    #[test]
    fn s3_object_key_rejects_parent_segments() {
        let loc = S3Location { bucket: "b".into(), prefix: "p".into() };
        assert!(loc.object_key("../secret").is_err());
    }
}
