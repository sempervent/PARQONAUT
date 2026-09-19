use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::location::ObjectLocation;

/// Backend-neutral object metadata. `etag` is opaque — never interpret as MD5.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub location: ObjectLocation,
    pub size: u64,
    pub etag: Option<String>,
    pub version_id: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

impl ObjectMetadata {
    /// ETag is opaque metadata from the backend. Do not treat as content hash.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}
