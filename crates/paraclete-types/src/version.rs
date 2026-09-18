//! Explicit versioning for contracts and serialized reports.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Semantic version of the durable JSON schema for core Paraclete contracts.
///
/// Bump this when breaking serialized shapes that consumers rely on.
pub const CONTRACT_SCHEMA_VERSION: &str = "0.5.0";

/// Semantic version of the scan report envelope and summary fields.
pub const REPORT_FORMAT_VERSION: &str = "0.5.0";

/// Newtype wrapper for contract schema version strings.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct ContractSchemaVersion(pub String);

/// Newtype wrapper for report format version strings.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct ReportFormatVersion(pub String);

/// Returns the current contract schema version as a structured value.
pub fn contract_schema_version() -> ContractSchemaVersion {
    ContractSchemaVersion(CONTRACT_SCHEMA_VERSION.to_owned())
}

/// Returns the current report format version as a structured value.
pub fn report_format_version() -> ReportFormatVersion {
    ReportFormatVersion(REPORT_FORMAT_VERSION.to_owned())
}
