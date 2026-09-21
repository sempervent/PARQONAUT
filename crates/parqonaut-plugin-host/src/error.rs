use thiserror::Error;

#[derive(Debug, Error)]
pub enum PluginHostError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] parqonaut_plugin_protocol::PluginProtocolError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("plugin not found: {0}")]
    NotFound(String),
    #[error("duplicate plugin name {name} (digest {left} vs {right})")]
    DuplicateName { name: String, left: String, right: String },
}
