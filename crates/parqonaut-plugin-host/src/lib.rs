//! Plugin catalog, host execution, and resource policy (implementation in progress for v0.10).

#![forbid(unsafe_code)]

mod catalog;
mod error;

pub use catalog::PluginCatalog;
pub use error::PluginHostError;
