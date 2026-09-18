use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::authorization::OperationAuditRecord;
use crate::fingerprint::DatasetFingerprint;
use crate::verify::VerificationReport;

pub const MANIFEST_VERSION: u32 = 1;

/// Machine-readable audit receipt for a repair execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionManifest {
    pub parqonaut_manifest_version: u32,
    pub plan_id: String,
    pub execution_id: String,
    pub parqonaut_version: String,
    pub source_fingerprint: DatasetFingerprint,
    pub output_fingerprint: DatasetFingerprint,
    pub policy_fingerprint: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub operations: Vec<OperationAuditRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<VerificationReport>,
}

impl ExecutionManifest {
    pub fn write_json(&self, path: &camino::Utf8Path) -> Result<(), crate::error::RepairError> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path.as_std_path(), json)?;
        Ok(())
    }

    pub fn read_json(path: &camino::Utf8Path) -> Result<Self, crate::error::RepairError> {
        let bytes = std::fs::read(path.as_std_path())?;
        serde_json::from_slice(&bytes)
            .map_err(|e| crate::error::RepairError::InvalidPlan(e.to_string()))
    }
}
