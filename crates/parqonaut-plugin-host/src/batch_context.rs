//! Host-owned batch plugin sidecar context (no storage topology or secrets).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPluginContextFile {
    pub protocol_version: u32,
    pub plugin: String,
    pub plugin_version: String,
    pub plugin_digest: String,
    pub entrypoint: String,
    pub execution_id: String,
    pub config: serde_json::Value,
    pub max_output_rows_per_input_batch: u64,
    pub max_output_expansion_factor: f64,
}
