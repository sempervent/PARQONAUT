//! Contracts for crossing the Rust ↔ Python plugin membrane.
//!
//! Execution embedding is intentionally deferred; this crate defines durable
//! request/response shapes and a trait boundary the engine can depend on.

#![forbid(unsafe_code)]

mod error;
mod execution;
mod messages;

pub use error::PluginProtocolError;
pub use execution::{PluginExecutor, PluginExecutorError};
pub use messages::{
    PluginCapabilities, PluginExecutionPhase, PluginFindingContribution, PluginManifest,
    PluginRequest, PluginResponse, PluginResult, PluginScanContext,
};
