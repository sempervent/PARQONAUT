//! Durable contracts for PARQONAUT plugin execution (protocol version 1).
//!
//! This crate defines manifests, scan JSON request/response shapes, and shared error
//! types. Subprocess management lives in `parqonaut-plugin-host`.

#![forbid(unsafe_code)]

mod error;
mod execution;
mod messages;
mod version;

pub use error::PluginProtocolError;
pub use execution::{PluginExecutor, PluginExecutorError};
pub use messages::{
    BatchTransformCapabilities, PluginCapabilities, PluginExecutionPhase,
    PluginFindingContribution, PluginManifest, PluginRequest, PluginResponse, PluginResult,
    PluginScanContext, ScanAnalyzerCapabilities,
};
pub use version::{PluginProtocolVersion, MANIFEST_FILENAME, PLUGIN_PROTOCOL_VERSION};
