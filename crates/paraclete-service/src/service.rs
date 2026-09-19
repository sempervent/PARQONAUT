//! Application use cases: orchestrates `ScanEngine` and the store backend (`StoreBackend`).

use std::time::Instant;

use chrono::{DateTime, Utc};
use paraclete_store::{
    AuthTokenSummary, JobCancelOutcome, ScanJobRow, StoreBackend, StoredAssetRow, StoredFindingRow,
};
use paraclete_types::{
    validate_report, AuthTokenId, AuthTokenStatus, JobErrorCode, JobId, JobKind, JobStatus,
    RedactionPolicy, RunId, RunOutcome, ScanReport, ScanRunListItem, TargetIdentity,
};
use parqonaut_app::{location, JobResultReference, ParqonautApp, ScanRequest, StoragePolicy};
use parqonaut_orchestrator::CancelFlag;

use crate::api_types::{
    AuthTokenCreateRequest, AuthTokenCreateResponse, AuthTokenListResponse,
    AuthTokenRotateResponse, AuthTokenSummaryView, JobFailureBody, JobListQuery, PageQuery,
    RunSummaryView, ScanJobListResponse, ScanJobSubmissionResponse, ScanJobView, StartScanRequest,
    StartScanResponse,
};
use crate::application_http::{
    self, BatchCheckResponse, BatchConfigBody, BatchPlanResponse, BatchRepairJobBody,
    BatchResumeBody, BatchStatusResponse, BatchVerifyResponse, CheckResponse, DiagnoseResponse,
    JobCancelResponse, LocationPolicyBody, PlanBody, PlanResponse, RepairJobBody, VerifyBody,
    VerifyResponse,
};
use crate::error::AppError;
use crate::observability::audit;

#[derive(Debug, Clone)]
pub struct ParacleteService {
    store: StoreBackend,
    app: ParqonautApp,
}

impl ParacleteService {
    pub fn new(store: impl Into<StoreBackend>) -> Self {
        Self::with_storage_policy(store, StoragePolicy::cli_unrestricted_local())
    }

    pub fn with_storage_policy(store: impl Into<StoreBackend>, policy: StoragePolicy) -> Self {
        Self { store: store.into(), app: ParqonautApp::new(policy) }
    }

    pub fn store(&self) -> &StoreBackend {
        &self.store
    }

    pub fn app(&self) -> &ParqonautApp {
        &self.app
    }

    /// Runs a scan via [`ParqonautApp`], applies redaction policy, validates, and persists.
    pub async fn execute_scan_and_persist(
        &self,
        req: StartScanRequest,
    ) -> Result<StartScanResponse, AppError> {
        let location = location::dataset_location_from_scan_target(&req.target)?;
        self.app.policy().validate_dataset(&location)?;
        let scan_req = ScanRequest::new(location, req.profile);

        let started = Utc::now();
        let engine_start = Instant::now();
        let mut report = self.app.scan(scan_req).await.map_err(AppError::from)?.report;
        if let Some(id) = req.scan_id {
            report.request.scan_id = id;
        }
        validate_report(&report)?;
        let completed = Utc::now();
        metrics::histogram!("parqonaut_scan_engine_duration_seconds")
            .record(engine_start.elapsed().as_secs_f64());

        let redaction =
            req.redaction.clone().unwrap_or_else(RedactionPolicy::transport_safe_persist);

        let run_id = RunId::new();
        let persist_start = Instant::now();
        self.store.persist_scan_run(run_id, started, completed, &report, &redaction).await?;
        metrics::histogram!("parqonaut_run_persist_duration_seconds")
            .record(persist_start.elapsed().as_secs_f64());
        metrics::counter!("parqonaut_runs_persisted_total").increment(1);

        let outcome = if report.summary.partial_inspection {
            RunOutcome::CompletedPartial
        } else {
            RunOutcome::Completed
        };
        let identity = TargetIdentity::from_scan_target(&req.target);
        audit::run_persisted(run_id.0, &identity.target_kind, &identity.normalized_key);
        Ok(StartScanResponse {
            run_id: run_id.0,
            target_kind: identity.target_kind,
            normalized_target_key: identity.normalized_key,
            run_outcome: outcome,
            summary: report.summary,
            report_url: format!("/api/v1/runs/{}/report", run_id.0),
        })
    }

    /// Synchronous scan + persist (dev/tests via `POST /api/v1/scans/sync`).
    pub async fn start_scan_and_persist(
        &self,
        req: StartScanRequest,
    ) -> Result<StartScanResponse, AppError> {
        self.execute_scan_and_persist(req).await
    }

    pub async fn submit_scan_job(
        &self,
        req: StartScanRequest,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let loc = location::dataset_location_from_scan_target(&req.target)?;
        self.app.policy().validate_dataset(&loc)?;
        let jid = JobId::new();
        let identity = TargetIdentity::from_scan_target(&req.target);
        let request_json = serde_json::to_string(&req)?;
        self.store.insert_scan_job_queued(jid, &identity, &request_json).await?;
        metrics::counter!("parqonaut_jobs_submitted_total").increment(1);
        audit::scan_submitted(jid.0, &identity.target_kind, &identity.normalized_key);
        let submitted_at = Utc::now();
        Ok(ScanJobSubmissionResponse {
            job_id: jid.0,
            status: JobStatus::Queued,
            submitted_at,
            target_kind: identity.target_kind,
            normalized_target_key: identity.normalized_key,
            job_url: format!("/api/v1/jobs/{}", jid.0),
        })
    }

    pub async fn get_scan_job_view(&self, job_id: JobId) -> Result<ScanJobView, AppError> {
        let row = self.store.get_scan_job(job_id).await?;
        scan_job_row_to_view(row)
    }

    pub async fn list_scan_jobs(&self, q: &JobListQuery) -> Result<ScanJobListResponse, AppError> {
        let status_filter = match q.status.as_deref() {
            None => None,
            Some(s) => Some(normalize_job_status_filter(s)?),
        };
        let limit = q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE) as u64;
        let offset = q.offset as u64;
        let total = self.store.count_scan_jobs(status_filter).await?;
        let rows = self.store.list_scan_jobs_page(status_filter, offset, limit).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(scan_job_row_to_view(row)?);
        }
        Ok(ScanJobListResponse {
            items,
            total,
            limit: q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE),
            offset: q.offset,
        })
    }

    pub async fn get_run_summary_view(&self, run_id: RunId) -> Result<RunSummaryView, AppError> {
        let meta = self.store.get_run_public_meta(run_id).await?;
        Ok(RunSummaryView {
            run_id: meta.run_id.0,
            request_scan_id: meta.request_scan_id,
            target_kind: meta.target_identity.target_kind,
            normalized_target_key: meta.target_identity.normalized_key,
            started_at: meta.started_at,
            completed_at: meta.completed_at,
            run_outcome: meta.run_outcome,
            engine_revision: meta.engine_revision,
            contract_schema_version: meta.contract_schema_version,
            report_format_version: meta.report_format_version,
            report_sha256: meta.report_sha256,
            summary: meta.summary,
        })
    }

    pub async fn get_run_report(&self, run_id: RunId) -> Result<ScanReport, AppError> {
        Ok(self.store.load_report(run_id).await?)
    }

    pub async fn list_runs_for_target(
        &self,
        identity: &TargetIdentity,
        limit: i64,
    ) -> Result<Vec<ScanRunListItem>, AppError> {
        Ok(self.store.list_runs_for_target(identity, limit).await?)
    }

    pub async fn diff_runs(
        &self,
        left: RunId,
        right: RunId,
    ) -> Result<paraclete_types::RunDiff, AppError> {
        let a = self.store.load_report(left).await?;
        let b = self.store.load_report(right).await?;
        Ok(paraclete_store::diff_reports(left, right, &a, &b))
    }

    pub async fn list_assets_page(
        &self,
        run_id: RunId,
        q: &PageQuery,
    ) -> Result<(Vec<StoredAssetRow>, u64), AppError> {
        let (limit, offset) = q.clamped();
        let st = q.inspection_status.as_deref();
        let total = self.store.count_assets(run_id, st).await?;
        let items = self.store.list_assets_page(run_id, st, offset, limit).await?;
        Ok((items, total))
    }

    pub async fn list_findings_page(
        &self,
        run_id: RunId,
        q: &PageQuery,
    ) -> Result<(Vec<StoredFindingRow>, u64), AppError> {
        let (limit, offset) = q.clamped();
        let sev = q.severity.as_deref();
        let code = q.code.as_deref();
        let total = self.store.count_findings(run_id, sev, code).await?;
        let items = self.store.list_findings_page(run_id, sev, code, offset, limit).await?;
        Ok((items, total))
    }

    pub async fn admin_create_token(
        &self,
        req: AuthTokenCreateRequest,
    ) -> Result<AuthTokenCreateResponse, AppError> {
        let label = req.label.trim();
        if label.is_empty() {
            return Err(AppError::InvalidRequest("label must not be empty".into()));
        }
        let note = req.note.as_deref().map(str::trim).filter(|s| !s.is_empty());
        let (id, secret) = self.store().create_auth_token(label, req.role, note).await?;
        let row = self
            .store()
            .get_auth_token_summary(id)
            .await?
            .ok_or_else(|| AppError::Internal("token row missing after create".into()))?;
        let out = AuthTokenCreateResponse {
            token_id: id.0,
            label: row.label.clone(),
            role: row.role,
            created_at: row.created_at,
            token_secret: secret,
            token_prefix: row.token_prefix.clone(),
            note: row.note.clone(),
        };
        audit::token_created(id.0, &row.label, row.role);
        Ok(out)
    }

    pub async fn admin_list_tokens(&self) -> Result<AuthTokenListResponse, AppError> {
        let rows = self.store().list_auth_tokens().await?;
        Ok(AuthTokenListResponse { items: rows.into_iter().map(auth_token_summary_view).collect() })
    }

    pub async fn admin_get_token(&self, id: AuthTokenId) -> Result<AuthTokenSummaryView, AppError> {
        let row = self.store().get_auth_token_summary(id).await?.ok_or(AppError::TokenNotFound)?;
        Ok(auth_token_summary_view(row))
    }

    pub async fn admin_disable_token(
        &self,
        id: AuthTokenId,
    ) -> Result<AuthTokenSummaryView, AppError> {
        let row = self.store().disable_auth_token(id).await?;
        Ok(auth_token_summary_view(row))
    }

    /// Rotates a token: mints a new secret, disables the old row, sets `replaced_by_token_id` on the old row.
    pub async fn diagnose_sync(
        &self,
        body: LocationPolicyBody,
    ) -> Result<DiagnoseResponse, AppError> {
        let req = application_http::to_diagnose_request(body)?;
        let report = self.app.diagnose(req).await.map_err(AppError::from)?.report;
        Ok(DiagnoseResponse { report })
    }

    pub async fn plan_sync(&self, body: PlanBody) -> Result<PlanResponse, AppError> {
        let req = application_http::to_plan_request(body)?;
        let plan = self.app.plan(req).await.map_err(AppError::from)?.plan;
        Ok(PlanResponse { plan })
    }

    pub async fn check_sync(&self, body: LocationPolicyBody) -> Result<CheckResponse, AppError> {
        let req = application_http::to_check_request(body)?;
        let report = self.app.check(req).await.map_err(AppError::from)?.report;
        Ok(CheckResponse { report })
    }

    pub async fn verify_sync(&self, body: VerifyBody) -> Result<VerifyResponse, AppError> {
        let req = application_http::to_verify_request(body)?;
        let report = self.app.verify(req).await.map_err(AppError::from)?.report;
        Ok(VerifyResponse { report })
    }

    pub async fn batch_check_sync(
        &self,
        body: BatchConfigBody,
    ) -> Result<BatchCheckResponse, AppError> {
        let req = application_http::to_batch_check_request(body)?;
        let report = self.app.batch_check(req);
        Ok(BatchCheckResponse { report })
    }

    pub async fn batch_plan_sync(
        &self,
        body: BatchConfigBody,
    ) -> Result<BatchPlanResponse, AppError> {
        let req = application_http::to_batch_plan_request(body)?;
        let plan = self.app.batch_plan(req)?.plan;
        Ok(BatchPlanResponse { plan })
    }

    pub async fn batch_status_sync(
        &self,
        run_dir: camino::Utf8PathBuf,
    ) -> Result<BatchStatusResponse, AppError> {
        let req = application_http::batch_status_request(run_dir);
        let status = self.app.batch_status(req)?.status;
        Ok(BatchStatusResponse { status })
    }

    pub async fn batch_verify_sync(
        &self,
        run_dir: camino::Utf8PathBuf,
    ) -> Result<BatchVerifyResponse, AppError> {
        let req = application_http::batch_verify_request(run_dir);
        let report = self.app.batch_verify(req)?.report;
        Ok(BatchVerifyResponse { report })
    }

    pub async fn submit_repair_job(
        &self,
        body: RepairJobBody,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let req = application_http::to_repair_request(body)?;
        self.app.policy().validate_dataset(&req.source)?;
        self.app.policy().validate_dataset(&req.output)?;
        let identity = application_http::target_identity_for_location(&req.source);
        let jid = JobId::new();
        let payload = application_http::durable_payload(&req)?;
        self.store.insert_application_job(jid, JobKind::Repair, &identity, &payload).await?;
        metrics::counter!("parqonaut_jobs_submitted_total", "kind" => "repair").increment(1);
        Ok(job_submission_view(jid, &identity))
    }

    pub async fn submit_batch_repair_job(
        &self,
        body: BatchRepairJobBody,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let req = application_http::to_batch_repair_request(body)?;
        if !req.plan_path.exists() {
            return Err(AppError::InvalidRequest(format!(
                "batch plan not found: {}",
                req.plan_path
            )));
        }
        let identity =
            application_http::target_identity_for_path("batch_repair", req.plan_path.as_str());
        let jid = JobId::new();
        let payload = application_http::durable_payload(&req)?;
        self.store.insert_application_job(jid, JobKind::BatchRepair, &identity, &payload).await?;
        metrics::counter!("parqonaut_jobs_submitted_total", "kind" => "batch_repair").increment(1);
        Ok(job_submission_view(jid, &identity))
    }

    pub async fn submit_batch_resume_job(
        &self,
        run_dir: camino::Utf8PathBuf,
        body: BatchResumeBody,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let req = application_http::to_batch_resume_request(run_dir.clone(), body)?;
        let identity = application_http::target_identity_for_path("batch_resume", run_dir.as_str());
        let jid = JobId::new();
        let payload = application_http::durable_payload(&req)?;
        self.store.insert_application_job(jid, JobKind::BatchResume, &identity, &payload).await?;
        metrics::counter!("parqonaut_jobs_submitted_total", "kind" => "batch_resume").increment(1);
        Ok(job_submission_view(jid, &identity))
    }

    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobCancelResponse, AppError> {
        let outcome = self.store.request_cancel_job(job_id).await?;
        let status = match outcome {
            JobCancelOutcome::CanceledFromQueued | JobCancelOutcome::AlreadyCanceled => {
                JobStatus::Canceled
            }
            JobCancelOutcome::CancelRequestedForRunning => JobStatus::Running,
        };
        let message = match outcome {
            JobCancelOutcome::CanceledFromQueued => "job canceled".into(),
            JobCancelOutcome::AlreadyCanceled => "job already canceled".into(),
            JobCancelOutcome::CancelRequestedForRunning => {
                "cancel requested; worker will stop at next safe boundary".into()
            }
        };
        metrics::counter!("parqonaut_jobs_cancel_total").increment(1);
        Ok(JobCancelResponse { job_id: job_id.0, status, message })
    }

    pub async fn admin_rotate_token(
        &self,
        old_id: AuthTokenId,
    ) -> Result<AuthTokenRotateResponse, AppError> {
        let (new_id, secret) = self.store().rotate_auth_token(old_id).await?;
        let row = self
            .store()
            .get_auth_token_summary(new_id)
            .await?
            .ok_or_else(|| AppError::Internal("token row missing after rotate".into()))?;
        let old_row =
            self.store().get_auth_token_summary(old_id).await?.ok_or_else(|| {
                AppError::Internal("previous token row missing after rotate".into())
            })?;
        let previous_disabled_at = old_row.disabled_at.ok_or_else(|| {
            AppError::Internal("previous token not marked disabled after rotate".into())
        })?;
        Ok(AuthTokenRotateResponse {
            token_id: new_id.0,
            previous_token_id: old_id.0,
            label: row.label,
            role: row.role,
            created_at: row.created_at,
            token_secret: secret,
            token_prefix: row.token_prefix,
            note: row.note,
            previous_disabled_at,
        })
    }

    pub(crate) fn decode_durable_payload<T: serde::de::DeserializeOwned>(
        payload_json: &str,
    ) -> Result<T, AppError> {
        let envelope: parqonaut_app::DurableJobPayload = serde_json::from_str(payload_json)?;
        Ok(serde_json::from_value(envelope.body)?)
    }

    pub(crate) async fn run_repair_job(
        &self,
        payload_json: &str,
    ) -> Result<JobResultReference, AppError> {
        let req: parqonaut_app::RepairRequest = Self::decode_durable_payload(payload_json)?;
        let result = self.app.repair(req).await.map_err(AppError::from)?;
        Ok(JobResultReference::RepairManifest { manifest_path: result.execution.manifest_path })
    }

    pub(crate) async fn run_batch_repair_job(
        &self,
        payload_json: &str,
        cancel: CancelFlag,
    ) -> Result<JobResultReference, AppError> {
        let req: parqonaut_app::BatchRepairRequest = Self::decode_durable_payload(payload_json)?;
        let out = self.app.batch_repair(req, cancel).await.map_err(AppError::from)?;
        Ok(JobResultReference::BatchRun { run_dir: out.execution.run_dir.as_str().to_string() })
    }

    pub(crate) async fn run_batch_resume_job(
        &self,
        payload_json: &str,
        cancel: CancelFlag,
    ) -> Result<JobResultReference, AppError> {
        let req: parqonaut_app::BatchResumeRequest = Self::decode_durable_payload(payload_json)?;
        let out = self.app.batch_resume(req, cancel).await.map_err(AppError::from)?;
        Ok(JobResultReference::BatchRun { run_dir: out.execution.run_dir.as_str().to_string() })
    }
}

fn auth_token_summary_view(s: AuthTokenSummary) -> AuthTokenSummaryView {
    let status =
        if s.disabled_at.is_some() { AuthTokenStatus::Disabled } else { AuthTokenStatus::Active };
    AuthTokenSummaryView {
        token_id: s.token_id.0,
        label: s.label,
        role: s.role,
        status,
        created_at: s.created_at,
        disabled_at: s.disabled_at,
        token_prefix: s.token_prefix,
        note: s.note,
        last_used_at: s.last_used_at,
        replaced_by_token_id: s.replaced_by_token_id.map(|id| id.0),
    }
}

fn normalize_job_status_filter(s: &str) -> Result<&'static str, AppError> {
    match s.trim().to_lowercase().as_str() {
        "queued" => Ok("queued"),
        "running" => Ok("running"),
        "succeeded" => Ok("succeeded"),
        "failed" => Ok("failed"),
        "canceled" => Ok("canceled"),
        _ => Err(AppError::InvalidRequest(format!("unknown job status filter `{s}`"))),
    }
}

fn job_submission_view(jid: JobId, identity: &TargetIdentity) -> ScanJobSubmissionResponse {
    ScanJobSubmissionResponse {
        job_id: jid.0,
        status: JobStatus::Queued,
        submitted_at: Utc::now(),
        target_kind: identity.target_kind.clone(),
        normalized_target_key: identity.normalized_key.clone(),
        job_url: format!("/api/v1/jobs/{}", jid.0),
    }
}

fn scan_job_row_to_view(row: ScanJobRow) -> Result<ScanJobView, AppError> {
    let job_id = uuid::Uuid::parse_str(&row.job_id)
        .map_err(|_| AppError::Internal("invalid job_id in store".into()))?;
    let status = parse_job_status_label(&row.status)?;
    let submitted_at = parse_rfc3339(&row.submitted_at)?;
    let started_at = row.started_at.as_deref().map(parse_rfc3339).transpose()?;
    let completed_at = row.completed_at.as_deref().map(parse_rfc3339).transpose()?;
    let run_id = row
        .run_id
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|_| AppError::Internal("invalid run_id on job".into()))?;

    let job_kind = JobKind::parse(&row.job_kind)
        .ok_or_else(|| AppError::Internal(format!("unknown job_kind `{}`", row.job_kind)))?;

    let failure = if status == JobStatus::Failed {
        let code = row
            .failure_code
            .as_deref()
            .map(parse_failure_code)
            .unwrap_or(JobErrorCode::InternalError);
        let message = row.failure_message.unwrap_or_default();
        Some(JobFailureBody { code, message })
    } else if status == JobStatus::Canceled {
        let message = row.failure_message.unwrap_or_else(|| "canceled".into());
        Some(JobFailureBody { code: JobErrorCode::InvalidRequest, message })
    } else {
        None
    };

    let result_ref = row
        .result_ref_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| AppError::Internal("invalid result_ref_json on job".into()))?;

    let heartbeat_at = row.heartbeat_at.as_deref().map(parse_rfc3339).transpose()?;
    let leased_until = row.leased_until.as_deref().map(parse_rfc3339).transpose()?;

    Ok(ScanJobView {
        job_id,
        job_kind,
        status,
        submitted_at,
        started_at,
        completed_at,
        target_kind: row.target_kind,
        normalized_target_key: row.normalized_target_key,
        attempt_count: row.attempt_count,
        worker_id: row.worker_id,
        heartbeat_at,
        leased_until,
        recovery_note: row.recovery_note,
        run_id,
        result_ref,
        cancel_requested: row.cancel_requested != 0,
        failure,
        job_url: format!("/api/v1/jobs/{job_id}"),
    })
}

fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>, AppError> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|_| AppError::Internal("invalid timestamp in job row".into()))
}

fn parse_job_status_label(s: &str) -> Result<JobStatus, AppError> {
    match s {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "succeeded" => Ok(JobStatus::Succeeded),
        "failed" => Ok(JobStatus::Failed),
        "canceled" => Ok(JobStatus::Canceled),
        _ => Err(AppError::Internal(format!("unknown job status `{s}`"))),
    }
}

fn parse_failure_code(s: &str) -> JobErrorCode {
    match s {
        "invalid_request" => JobErrorCode::InvalidRequest,
        "target_not_found" => JobErrorCode::TargetNotFound,
        "scan_failed" => JobErrorCode::ScanFailed,
        "store_error" => JobErrorCode::StoreError,
        "internal_error" => JobErrorCode::InternalError,
        "worker_lost" => JobErrorCode::WorkerLost,
        "job_retries_exhausted" => JobErrorCode::JobRetriesExhausted,
        _ => JobErrorCode::InternalError,
    }
}
