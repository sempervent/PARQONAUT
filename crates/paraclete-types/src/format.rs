//! Data format classification and support tiers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Detected or declared on-disk / logical data format.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DataFormat {
    Parquet,
    Csv,
    Json,
    Ndjson,
    Unknown,
}

/// Declares how deeply Paraclete can inspect a format in a given phase.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FormatSupportTier {
    /// Deepest diagnostics and metadata access (Parquet-first).
    FirstClass,
    /// Supported through shared abstractions with reduced guarantees.
    SecondClass,
    /// Exploratory support; contracts may change more frequently.
    Experimental,
}

impl DataFormat {
    /// Default support tier for this format in Phase 0 policy.
    pub fn default_support_tier(self) -> FormatSupportTier {
        match self {
            DataFormat::Parquet => FormatSupportTier::FirstClass,
            DataFormat::Csv | DataFormat::Json | DataFormat::Ndjson => {
                FormatSupportTier::SecondClass
            }
            DataFormat::Unknown => FormatSupportTier::Experimental,
        }
    }
}
