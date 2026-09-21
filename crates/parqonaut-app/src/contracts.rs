//! Transport-neutral application request/response contracts.

use camino::Utf8PathBuf;
use paraclete_types::{ScanProfile, ScanReport};
use parqonaut_orchestrator::{BatchPlan, BatchStatusReport};
use parqonaut_repair::{
    CheckReport, DiagnosisReport, EffectivePolicy, FieldDescriptor, RepairPlan, VerificationReport,
};
use parqonaut_storage::location::DatasetLocation;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const APP_REQUEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRequest {
    pub schema_version: u32,
    pub location: DatasetLocation,
    pub profile: ScanProfile,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScanResult {
    pub report: ScanReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnoseRequest {
    pub schema_version: u32,
    pub location: DatasetLocation,
    pub policy: EffectivePolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiagnoseResult {
    pub report: DiagnosisReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRequest {
    pub schema_version: u32,
    pub location: DatasetLocation,
    pub policy: EffectivePolicy,
    pub target_schema: Option<Vec<FieldDescriptor>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanResult {
    pub plan: RepairPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckRequest {
    pub schema_version: u32,
    pub location: DatasetLocation,
    pub policy: EffectivePolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub report: CheckReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairRequest {
    pub schema_version: u32,
    pub source: DatasetLocation,
    pub output: DatasetLocation,
    pub plan: RepairPlan,
    pub authorize: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepairResult {
    pub execution: parqonaut_repair::ExecutionReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyRequest {
    pub schema_version: u32,
    pub before: DatasetLocation,
    pub after_local_path: Utf8PathBuf,
    pub manifest_path: Option<Utf8PathBuf>,
    pub policy: EffectivePolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyResult {
    pub report: VerificationReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCheckRequest {
    pub schema_version: u32,
    pub config_path: Utf8PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPlanRequest {
    pub schema_version: u32,
    pub config_path: Utf8PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchPlanResult {
    pub plan: BatchPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRepairRequest {
    pub schema_version: u32,
    pub plan_path: Utf8PathBuf,
    pub jobs: Option<u32>,
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupt_after_completed: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct BatchRepairResult {
    pub execution: parqonaut_orchestrator::BatchExecutionResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStatusRequest {
    pub schema_version: u32,
    pub run_dir: Utf8PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchStatusResult {
    pub status: BatchStatusReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResumeRequest {
    pub schema_version: u32,
    pub run_dir: Utf8PathBuf,
    pub jobs: Option<u32>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchVerifyRequest {
    pub schema_version: u32,
    pub run_dir: Utf8PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchVerifyResult {
    pub report: parqonaut_orchestrator::BatchVerifyReport,
}

impl ScanRequest {
    pub fn new(location: DatasetLocation, profile: ScanProfile) -> Self {
        Self { schema_version: APP_REQUEST_SCHEMA_VERSION, location, profile, plugins: Vec::new() }
    }
}

impl DiagnoseRequest {
    pub fn new(location: DatasetLocation, policy: EffectivePolicy) -> Self {
        Self { schema_version: APP_REQUEST_SCHEMA_VERSION, location, policy }
    }
}

impl PlanRequest {
    pub fn new(
        location: DatasetLocation,
        policy: EffectivePolicy,
        target_schema: Option<Vec<FieldDescriptor>>,
    ) -> Self {
        Self { schema_version: APP_REQUEST_SCHEMA_VERSION, location, policy, target_schema }
    }
}

impl CheckRequest {
    pub fn new(location: DatasetLocation, policy: EffectivePolicy) -> Self {
        Self { schema_version: APP_REQUEST_SCHEMA_VERSION, location, policy }
    }
}

/// Durable job payload envelope stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableJobPayload {
    pub schema_version: u32,
    pub body: serde_json::Value,
}

impl DurableJobPayload {
    pub fn wrap<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        Ok(Self { schema_version: APP_REQUEST_SCHEMA_VERSION, body: serde_json::to_value(value)? })
    }
}

/// Reference persisted when a job completes (not full result blobs).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobResultReference {
    ScanRun { run_id: Uuid },
    RepairManifest { manifest_path: String },
    BatchRun { run_dir: String },
}
