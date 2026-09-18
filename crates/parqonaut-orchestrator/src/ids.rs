use serde::{Deserialize, Serialize};

use parqonaut_repair::stable_id::{canonical_json, stable_hex_id};

/// Stable identifier for a batch plan derived from config + dataset fingerprints.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BatchPlanId(pub String);

impl BatchPlanId {
    pub fn derive(payload: &serde_json::Value) -> Self {
        Self(stable_hex_id("batch-plan", &canonical_json(payload)))
    }
}

/// Unique identifier for a batch execution run (runtime, not plan-stable).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub String);

impl RunId {
    pub fn new() -> Self {
        Self(format!("run-{}", uuid::Uuid::new_v4()))
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

/// User-declared dataset identifier within a batch config.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatasetId(pub String);
