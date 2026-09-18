use thiserror::Error;

/// Errors surfaced when validating plugin protocol payloads.
#[derive(Debug, Error)]
pub enum PluginProtocolError {
    #[error("plugin manifest name must not be empty")]
    EmptyPluginName,
    #[error("plugin manifest version must not be empty")]
    EmptyPluginVersion,
    #[error("JSON serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}
