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
    #[error("invalid manifest at {path}: {detail}")]
    InvalidManifest { path: String, detail: String },
    #[error("protocol version mismatch: {detail}")]
    ProtocolVersionMismatch { detail: String },
    #[error("host version mismatch for plugin {name}: requires {requirement}")]
    HostVersionMismatch { name: String, requirement: String },
    #[error("plugin runtime unavailable: {0}")]
    RuntimeUnavailable(String),
    #[error("invalid entrypoint for plugin {name}: {detail}")]
    EntrypointInvalid { name: String, detail: String },
    #[error("failed to spawn plugin process: {0}")]
    SpawnFailed(String),
    #[error("plugin timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    #[error("plugin process failed (exit {code:?}): {detail}")]
    ProcessFailed { code: Option<i32>, detail: String },
    #[error("plugin protocol violation: {0}")]
    ProtocolViolation(String),
    #[error("plugin response too large ({size} bytes, max {max})")]
    ResponseTooLarge { size: usize, max: usize },
    #[error("invalid plugin response: {0}")]
    InvalidResponse(String),
    #[error("plugin digest mismatch: expected {expected}, got {actual}")]
    PluginDigestMismatch { expected: String, actual: String },
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("plugin referenced unknown evidence id: {0}")]
    InvalidEvidence(String),
    #[error("plugin result exceeds policy: {0}")]
    ResultPolicyViolation(String),
    #[error("plugin {name} does not support phase {phase:?}")]
    UnsupportedPhase { name: String, phase: String },
    #[error("plugin schema mismatch: {0}")]
    PluginSchemaMismatch(String),
    #[error("plugin output expansion exceeded: {0}")]
    PluginOutputExpansionExceeded(String),
    #[error("stale plugin: expected digest {expected}, current {actual}")]
    StalePlugin { expected: String, actual: String },
}
