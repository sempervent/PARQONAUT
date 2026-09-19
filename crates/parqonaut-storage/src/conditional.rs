use serde::{Deserialize, Serialize};

use crate::metadata::ObjectMetadata;

/// Create object only if it does not already exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalCreate {
    pub if_absent: bool,
}

impl ConditionalCreate {
    pub fn must_not_exist() -> Self {
        Self { if_absent: true }
    }
}

/// Replace object only if current identity matches expected metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalReplace {
    pub expected_etag: Option<String>,
    pub expected_version_id: Option<String>,
}

impl ConditionalReplace {
    pub fn matching(metadata: &ObjectMetadata) -> Self {
        Self {
            expected_etag: metadata.etag.clone(),
            expected_version_id: metadata.version_id.clone(),
        }
    }
}
