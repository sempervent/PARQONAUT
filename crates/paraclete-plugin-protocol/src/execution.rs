use crate::{PluginRequest, PluginResponse};

/// Execution boundary for invoking plugins without prescribing an embedding.
pub trait PluginExecutor {
    /// Invokes a plugin for the supplied request.
    ///
    /// Phase 0 ships no concrete executor; engine crates may provide stubs in tests.
    fn execute(&self, request: &PluginRequest) -> Result<PluginResponse, PluginExecutorError>;
}

/// Errors for plugin execution (transport, timeout, user code).
#[derive(Debug, thiserror::Error)]
pub enum PluginExecutorError {
    #[error("plugin execution is not implemented in this build")]
    NotImplemented,
}
