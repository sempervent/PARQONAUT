use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamExecutionIdentity {
    pub contract_version: String,
    pub input_fingerprint: String,
    pub output_location: String,
    pub output_format: String,
    pub schema_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamCheckpoint {
    pub identity: StreamExecutionIdentity,
    pub processed_files: Vec<String>,
    pub committed: bool,
}
