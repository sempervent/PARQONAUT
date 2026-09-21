//! Plugin catalog, host execution, and resource policy.

#![forbid(unsafe_code)]

mod catalog;
mod digest;
mod env;
mod error;
mod host;
mod normalize;
mod path_safety;
mod policy;
mod scan_execute;

pub use catalog::{CatalogEntry, PluginCatalog, PluginCompatibility, HOST_PARQONAUT_VERSION};
pub use env::plugin_child_env;
pub use error::PluginHostError;
pub use host::{merge_plugin_metadata, phase_label, PluginHost, ResolvedPlugin};
pub use policy::PluginResourcePolicy;
pub use scan_execute::{CancelToken, PluginRuntimeConfig, ScanPluginExecutor};
