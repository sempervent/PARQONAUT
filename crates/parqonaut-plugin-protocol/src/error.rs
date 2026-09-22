use thiserror::Error;

/// Errors surfaced when validating plugin protocol payloads.
#[derive(Debug, Error)]
pub enum PluginProtocolError {
    #[error("plugin manifest name must not be empty")]
    EmptyPluginName,
    #[error("plugin manifest version must not be empty")]
    EmptyPluginVersion,
    #[error("plugin manifest version must be valid semantic version: {0}")]
    InvalidPluginVersion(String),
    #[error("invalid requires_parqonaut semver requirement: {0}")]
    InvalidRequiresParqonaut(String),
    #[error("duplicate scan phase declaration: {0:?}")]
    DuplicateScanPhase(String),
    #[error("invalid plugin name: {0}")]
    InvalidPluginName(String),
    #[error("invalid entrypoint: {0}")]
    InvalidEntrypoint(String),
    #[error("manifest must declare at least one capability (scan or batch_transform)")]
    NoCapabilities,
    #[error("unsupported plugin protocol version: expected {expected}, found {found}")]
    ProtocolVersionMismatch { expected: u32, found: u32 },
    #[error("JSON serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}
