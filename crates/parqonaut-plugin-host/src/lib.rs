//! Plugin catalog, host execution, and resource policy.

#![forbid(unsafe_code)]

mod batch_arrow;
mod batch_bridge;
mod batch_context;
mod batch_execute;
mod batch_ipc;
mod batch_schema;
mod catalog;
mod digest;
mod env;
mod error;
mod host;
mod normalize;
mod path_safety;
mod policy;
mod scan_execute;

pub use batch_arrow::validate_schema_supported;
pub use batch_bridge::{BatchBridgeMetrics, BatchPluginBridge};
pub use batch_execute::BatchPluginSession;
pub use catalog::{CatalogEntry, PluginCatalog, PluginCompatibility, HOST_PARQONAUT_VERSION};
pub use env::plugin_child_env;
pub use error::PluginHostError;
pub use host::{merge_plugin_metadata, phase_label, PluginHost, ResolvedPlugin};
pub use policy::{BatchResourcePolicy, PluginResourcePolicy};
pub use scan_execute::{CancelToken, PluginRuntimeConfig, ScanPluginExecutor};
