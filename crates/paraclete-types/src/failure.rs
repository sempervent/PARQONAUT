//! Machine-readable failure taxonomy (storage and APIs).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Typed failure classes for assets and projections (human `failure_message` complements this).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    IoError,
    FormatReadError,
    UnsupportedFormat,
    ProbeDecodeError,
    TruncatedByPolicy,
    InternalEngineError,
}
