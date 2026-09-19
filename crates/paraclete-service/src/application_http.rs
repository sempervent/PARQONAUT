//! HTTP DTOs for repair/batch application endpoints (convert to [`parqonaut_app`] contracts).

use std::path::PathBuf;

use camino::Utf8PathBuf;
use paraclete_types::TargetIdentity;
use parqonaut_app::{
    BatchCheckRequest, BatchPlanRequest, BatchRepairRequest, BatchResumeRequest,
    BatchStatusRequest, BatchVerifyRequest, CheckRequest, DiagnoseRequest, DurableJobPayload,
    PlanRequest, RepairRequest, VerifyRequest, APP_REQUEST_SCHEMA_VERSION,
};
use parqonaut_repair::{EffectivePolicy, FieldDescriptor, RepairPlan};
use parqonaut_storage::location::DatasetLocation;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use parqonaut_app::location;

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct LocationPolicyBody {
    /// Dataset location (`/path`, `file://…`, `s3://bucket/prefix`).
    pub location: String,
    /// Optional repair policy TOML; default policy when omitted.
    #[serde(default)]
    pub policy_toml: Option<String>,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct PlanBody {
    pub location: String,
    #[serde(default)]
    pub policy_toml: Option<String>,
    /// Optional JSON file containing target schema field descriptors.
    #[serde(default)]
    pub target_schema_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct RepairJobBody {
    pub source: String,
    pub output: String,
    #[schema(value_type = Object)]
    pub plan: RepairPlan,
    #[serde(default)]
    pub authorize: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct VerifyBody {
    pub before: String,
    pub after_local_path: String,
    #[serde(default)]
    pub manifest_path: Option<String>,
    #[serde(default)]
    pub policy_toml: Option<String>,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct BatchConfigBody {
    pub config_path: String,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct BatchRepairJobBody {
    pub plan_path: String,
    #[serde(default)]
    pub jobs: Option<u32>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct BatchResumeBody {
    #[serde(default)]
    pub jobs: Option<u32>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DiagnoseResponse {
    #[schema(value_type = Object)]
    pub report: parqonaut_repair::DiagnosisReport,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PlanResponse {
    #[schema(value_type = Object)]
    pub plan: RepairPlan,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CheckResponse {
    #[schema(value_type = Object)]
    pub report: parqonaut_repair::CheckReport,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct VerifyResponse {
    #[schema(value_type = Object)]
    pub report: parqonaut_repair::VerificationReport,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BatchCheckResponse {
    #[schema(value_type = Object)]
    pub report: parqonaut_orchestrator::BatchCheckReport,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BatchPlanResponse {
    #[schema(value_type = Object)]
    pub plan: parqonaut_orchestrator::BatchPlan,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BatchStatusResponse {
    #[schema(value_type = Object)]
    pub status: parqonaut_orchestrator::BatchStatusReport,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BatchVerifyResponse {
    #[schema(value_type = Object)]
    pub report: parqonaut_orchestrator::BatchVerifyReport,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct JobCancelResponse {
    pub job_id: uuid::Uuid,
    pub status: paraclete_types::JobStatus,
    pub message: String,
}

pub fn effective_policy(toml: Option<&str>) -> Result<EffectivePolicy, AppError> {
    EffectivePolicy::from_toml(toml)
        .map_err(|e| AppError::InvalidRequest(format!("invalid repair policy TOML: {e}")))
}

pub fn parse_location(s: &str) -> Result<DatasetLocation, AppError> {
    location::parse_dataset_location(s).map_err(AppError::from)
}

pub fn target_identity_for_location(loc: &DatasetLocation) -> TargetIdentity {
    TargetIdentity { target_kind: "dataset_location".into(), normalized_key: loc.to_string() }
}

pub fn target_identity_for_path(label: &str, path: &str) -> TargetIdentity {
    TargetIdentity { target_kind: label.into(), normalized_key: path.to_string() }
}

pub fn to_diagnose_request(body: LocationPolicyBody) -> Result<DiagnoseRequest, AppError> {
    let loc = parse_location(&body.location)?;
    let policy = effective_policy(body.policy_toml.as_deref())?;
    Ok(DiagnoseRequest::new(loc, policy))
}

pub fn to_plan_request(body: PlanBody) -> Result<PlanRequest, AppError> {
    let loc = parse_location(&body.location)?;
    let policy = effective_policy(body.policy_toml.as_deref())?;
    let target_schema = load_target_schema(body.target_schema_path.as_deref())?;
    Ok(PlanRequest::new(loc, policy, target_schema))
}

pub fn to_check_request(body: LocationPolicyBody) -> Result<CheckRequest, AppError> {
    let loc = parse_location(&body.location)?;
    let policy = effective_policy(body.policy_toml.as_deref())?;
    Ok(CheckRequest::new(loc, policy))
}

pub fn to_verify_request(body: VerifyBody) -> Result<VerifyRequest, AppError> {
    let before = parse_location(&body.before)?;
    let policy = effective_policy(body.policy_toml.as_deref())?;
    Ok(VerifyRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        before,
        after_local_path: Utf8PathBuf::from(body.after_local_path),
        manifest_path: body.manifest_path.map(Utf8PathBuf::from),
        policy,
    })
}

pub fn to_repair_request(body: RepairJobBody) -> Result<RepairRequest, AppError> {
    Ok(RepairRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        source: parse_location(&body.source)?,
        output: parse_location(&body.output)?,
        plan: body.plan,
        authorize: body.authorize,
    })
}

pub fn to_batch_check_request(body: BatchConfigBody) -> Result<BatchCheckRequest, AppError> {
    Ok(BatchCheckRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        config_path: Utf8PathBuf::from(body.config_path),
    })
}

pub fn to_batch_plan_request(body: BatchConfigBody) -> Result<BatchPlanRequest, AppError> {
    Ok(BatchPlanRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        config_path: Utf8PathBuf::from(body.config_path),
    })
}

pub fn to_batch_repair_request(body: BatchRepairJobBody) -> Result<BatchRepairRequest, AppError> {
    Ok(BatchRepairRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        plan_path: Utf8PathBuf::from(body.plan_path),
        jobs: body.jobs,
        dry_run: body.dry_run,
        interrupt_after_completed: None,
    })
}

pub fn to_batch_resume_request(
    run_dir: Utf8PathBuf,
    body: BatchResumeBody,
) -> Result<BatchResumeRequest, AppError> {
    Ok(BatchResumeRequest {
        schema_version: APP_REQUEST_SCHEMA_VERSION,
        run_dir,
        jobs: body.jobs,
        dry_run: body.dry_run,
    })
}

pub fn batch_status_request(run_dir: Utf8PathBuf) -> BatchStatusRequest {
    BatchStatusRequest { schema_version: APP_REQUEST_SCHEMA_VERSION, run_dir }
}

pub fn batch_verify_request(run_dir: Utf8PathBuf) -> BatchVerifyRequest {
    BatchVerifyRequest { schema_version: APP_REQUEST_SCHEMA_VERSION, run_dir }
}

pub fn durable_payload<T: Serialize>(value: &T) -> Result<String, AppError> {
    Ok(serde_json::to_string(&DurableJobPayload::wrap(value)?)?)
}

fn load_target_schema(path: Option<&str>) -> Result<Option<Vec<FieldDescriptor>>, AppError> {
    let Some(p) = path else {
        return Ok(None);
    };
    let bytes = std::fs::read(PathBuf::from(p))
        .map_err(|e| AppError::InvalidRequest(format!("target_schema_path read failed: {e}")))?;
    let fields: Vec<FieldDescriptor> = serde_json::from_slice(&bytes)?;
    Ok(Some(fields))
}
