//! Application use cases: orchestrates `ScanEngine` and the store backend (`StoreBackend`).

use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use hex::encode as hex_encode;
use parqonaut_app::plugins::{pinned_to_resolved, ScanPluginBridge};
use parqonaut_app::server_plugins::{public_capabilities_summary, ServerPluginState};
use parqonaut_app::{location, JobResultReference, ParqonautApp, ScanRequest, StoragePolicy};
use parqonaut_orchestrator::CancelFlag;
use parqonaut_plugin_host::CancelToken;
use parqonaut_store::{
    AuthTokenSummary, IdempotencyInsertResult, JobCancelOutcome, ScanJobRow, StoreBackend,
    StoredAssetRow, StoredFindingRow,
};
use parqonaut_types::{
    validate_report, AuthTokenId, AuthTokenStatus, JobErrorCode, JobEventRetentionPolicy, JobId,
    JobKind, JobStage, JobStatus, RedactionPolicy, RunId, RunOutcome, ScanReport, ScanRunListItem,
    TargetIdentity,
};
use parqonaut_workflow::{
    NoOpProgressObserver, ProgressEventKind, ProgressMetrics, ProgressObserver,
};
use sha2::{Digest, Sha256};

use crate::api_types::{
    AuthTokenCreateRequest, AuthTokenCreateResponse, AuthTokenListResponse,
    AuthTokenRotateResponse, AuthTokenSummaryView, JobEventListQuery, JobEventListResponse,
    JobFailureBody, JobListQuery, OverviewResponse, PageQuery, PluginCatalogEntryView,
    PluginCatalogListResponse, RunListQuery, RunListResponse, RunSummaryView, ScanJobListResponse,
    ScanJobSubmissionResponse, ScanJobView, StartScanRequest, StartScanResponse,
};
use crate::application_http::{
    self, BatchCheckResponse, BatchConfigBody, BatchPlanResponse, BatchRepairJobBody,
    BatchResumeBody, BatchStatusResponse, BatchVerifyResponse, CheckResponse, DiagnoseResponse,
    JobCancelResponse, LocationPolicyBody, PlanBody, PlanResponse, RepairJobBody, VerifyBody,
    VerifyResponse,
};
use crate::error::AppError;
use crate::events::{lifecycle_event, JobEventRecorder};
use crate::observability::audit;
use crate::scan_job::ScanJobPayload;

#[derive(Debug, Clone)]
pub struct ParqonautService {
    store: StoreBackend,
    app: ParqonautApp,
    server_plugins: ServerPluginState,
    events: JobEventRecorder,
}

impl ParqonautService {
    pub fn new(store: impl Into<StoreBackend>) -> Self {
        Self::with_storage_policy(store, StoragePolicy::cli_unrestricted_local())
    }

    pub fn with_storage_policy(store: impl Into<StoreBackend>, policy: StoragePolicy) -> Self {
        Self::with_storage_policy_and_plugins(store, policy, ServerPluginState::disabled())
    }

    pub fn with_storage_policy_and_plugins(
        store: impl Into<StoreBackend>,
        policy: StoragePolicy,
        server_plugins: ServerPluginState,
    ) -> Self {
        let store = store.into();
        let events = JobEventRecorder::new(store.clone(), JobEventRetentionPolicy::from_env());
        Self { store, app: ParqonautApp::new(policy), server_plugins, events }
    }

    pub fn events(&self) -> &JobEventRecorder {
        &self.events
    }

    pub fn server_plugins(&self) -> &ServerPluginState {
        &self.server_plugins
    }

    pub fn list_plugin_catalog(&self) -> PluginCatalogListResponse {
        let plugins = self
            .server_plugins
            .allowed_catalog_entries()
            .into_iter()
            .map(|e| PluginCatalogEntryView {
                name: e.manifest.name.clone(),
                version: e.manifest.version.clone(),
                protocol_version: e.manifest.protocol_version,
                digest: e.digest.clone(),
                compatible: e.compatibility
                    == parqonaut_plugin_host::PluginCompatibility::Compatible,
                capabilities: public_capabilities_summary(e),
            })
            .collect();
        PluginCatalogListResponse {
            plugins,
            plugins_enabled: self.server_plugins.plugins_enabled(),
        }
    }

    pub fn get_plugin_catalog_entry(&self, name: &str) -> Result<PluginCatalogEntryView, AppError> {
        let Some(entry) = self.server_plugins.get_allowed_entry(name) else {
            return Err(AppError::PluginNotFound(name.to_string()));
        };
        Ok(PluginCatalogEntryView {
            name: entry.manifest.name.clone(),
            version: entry.manifest.version.clone(),
            protocol_version: entry.manifest.protocol_version,
            digest: entry.digest.clone(),
            compatible: entry.compatibility
                == parqonaut_plugin_host::PluginCompatibility::Compatible,
            capabilities: public_capabilities_summary(entry),
        })
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
        self.execute_scan_and_persist_with_cancel(req, None).await
    }

    pub async fn execute_scan_job_payload(
        &self,
        payload: ScanJobPayload,
        cancel: Option<CancelToken>,
    ) -> Result<StartScanResponse, AppError> {
        self.execute_scan_job_payload_with_progress(payload, cancel, None).await
    }

    pub async fn execute_scan_job_payload_with_progress(
        &self,
        payload: ScanJobPayload,
        cancel: Option<CancelToken>,
        progress: Option<&dyn ProgressObserver>,
    ) -> Result<StartScanResponse, AppError> {
        let req = StartScanRequest {
            target: payload.target,
            profile: payload.profile,
            options: payload.options,
            scan_id: payload.scan_id,
            redaction: payload.redaction,
            plugins: payload.plugins.clone(),
        };
        self.execute_scan_and_persist_internal(req, payload.resolved_plugins, cancel, progress)
            .await
    }

    async fn execute_scan_and_persist_with_cancel(
        &self,
        req: StartScanRequest,
        cancel: Option<CancelToken>,
    ) -> Result<StartScanResponse, AppError> {
        let resolved = self.server_plugins.resolve_for_enqueue(&req.plugins)?;
        self.execute_scan_and_persist_internal(req, resolved, cancel, None).await
    }

    async fn execute_scan_and_persist_internal(
        &self,
        req: StartScanRequest,
        resolved_plugins: Vec<parqonaut_types::ResolvedPluginSelection>,
        cancel: Option<CancelToken>,
        progress: Option<&dyn ProgressObserver>,
    ) -> Result<StartScanResponse, AppError> {
        let location = location::dataset_location_from_scan_target(&req.target)?;
        self.app.policy().validate_dataset(&location)?;
        let scan_req = ScanRequest {
            schema_version: parqonaut_app::APP_REQUEST_SCHEMA_VERSION,
            location,
            profile: req.profile,
            plugins: req.plugins.clone(),
        };

        let bridge = if req.plugins.is_empty() && resolved_plugins.is_empty() {
            None
        } else {
            if resolved_plugins.len() != req.plugins.len()
                || resolved_plugins.iter().zip(req.plugins.iter()).any(|(p, n)| &p.name != n)
            {
                return Err(AppError::InvalidRequest(
                    "pinned plugin selections do not match requested plugins".into(),
                ));
            }
            let resolved = pinned_to_resolved(&self.server_plugins, &resolved_plugins)?;
            let token = cancel.unwrap_or_default();
            Some(ScanPluginBridge::from_pinned_resolved(
                self.server_plugins.catalog().clone(),
                self.server_plugins.runtime().clone(),
                resolved,
                req.plugins.clone(),
                token,
            ))
        };

        let started = Utc::now();
        let engine_start = Instant::now();
        let noop = NoOpProgressObserver;
        let observer = progress.unwrap_or(&noop);
        let mut report = self
            .app
            .scan_with_bridge_and_progress(scan_req, bridge.as_ref(), observer)
            .await
            .map_err(AppError::from)?
            .report;
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
        self.submit_scan_job_with_idempotency(req, None).await
    }

    pub async fn submit_scan_job_with_idempotency(
        &self,
        req: StartScanRequest,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let loc = location::dataset_location_from_scan_target(&req.target)?;
        self.app.policy().validate_dataset(&loc)?;
        let resolved = self.server_plugins.resolve_for_enqueue(&req.plugins)?;
        let payload = ScanJobPayload::from_http(
            req.target.clone(),
            req.profile,
            req.options,
            req.scan_id,
            req.redaction,
            req.plugins,
            resolved,
        );
        let identity = TargetIdentity::from_scan_target(&req.target);
        let request_json = serde_json::to_string(&payload)?;
        let sha = sha256_hex(&request_json);
        let jid = JobId::new();
        let ttl = ChronoDuration::seconds(self.events.policy().idempotency_ttl_secs);
        let expires = Utc::now() + ttl;
        let idem = idempotency_key.map(|k| ("scan", k, sha.as_str(), expires));
        match self
            .store
            .insert_application_job_with_idempotency(
                jid,
                JobKind::Scan,
                &identity,
                &request_json,
                idem,
            )
            .await?
        {
            IdempotencyInsertResult::Created { job_id, submitted_at } => {
                metrics::counter!("parqonaut_jobs_submitted_total").increment(1);
                audit::scan_submitted(job_id.0, &identity.target_kind, &identity.normalized_key);
                let _ = self
                    .events
                    .record_lifecycle(lifecycle_event(
                        job_id,
                        ProgressEventKind::JobQueued,
                        JobStage::Queue,
                        ProgressMetrics::default(),
                        format!("scan job queued for {}", identity.normalized_key),
                    ))
                    .await;
                Ok(ScanJobSubmissionResponse {
                    job_id: job_id.0,
                    status: JobStatus::Queued,
                    submitted_at,
                    target_kind: identity.target_kind,
                    normalized_target_key: identity.normalized_key,
                    job_url: format!("/api/v1/jobs/{}", job_id.0),
                })
            }
            IdempotencyInsertResult::Replayed { existing } => submission_from_row(&existing),
        }
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
        let kind_filter = match q.kind.as_deref() {
            None => None,
            Some(k) => Some(normalize_job_kind_filter(k)?),
        };
        let limit = q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE) as u64;
        let offset = q.offset as u64;
        let (rows, total) = self
            .store
            .list_application_jobs_filtered(kind_filter, status_filter, offset, limit)
            .await?;
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

    pub async fn list_job_events(
        &self,
        job_id: JobId,
        q: &JobEventListQuery,
    ) -> Result<JobEventListResponse, AppError> {
        let kind_filter = match q.kind.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            None => None,
            Some(k) => {
                let parsed = ProgressEventKind::parse(k).ok_or_else(|| {
                    AppError::InvalidRequest(format!("unknown event kind filter `{k}`"))
                })?;
                Some(parsed.as_str())
            }
        };
        let stage_filter = match q.stage.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            None => None,
            Some(s) => {
                let parsed = JobStage::parse(s).ok_or_else(|| {
                    AppError::InvalidRequest(format!("unknown event stage filter `{s}`"))
                })?;
                Some(parsed.as_str())
            }
        };
        let limit = q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE) as u64;
        let mut items = self
            .store
            .list_job_events(job_id, q.after_seq, limit + 1, kind_filter, stage_filter)
            .await?;
        let has_more = items.len() as u64 > limit;
        if has_more {
            items.truncate(limit as usize);
        }
        let next_after_seq = items.last().map(|e| e.seq).unwrap_or(q.after_seq);
        Ok(JobEventListResponse {
            job_id: job_id.0,
            items,
            next_after_seq,
            has_more,
            retention_max_per_job: self.events.policy().max_events_per_job as u64,
        })
    }

    pub async fn list_runs_page(&self, q: &RunListQuery) -> Result<RunListResponse, AppError> {
        let limit = q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE) as u64;
        let offset = q.offset as u64;
        let (items, total) = self.store.list_scan_runs_page(offset, limit).await?;
        Ok(RunListResponse {
            items,
            total,
            limit: q.limit.clamp(1, crate::api_types::MAX_PAGE_SIZE),
            offset: q.offset,
        })
    }

    pub async fn get_overview(&self) -> Result<OverviewResponse, AppError> {
        let job_counts = self.store.job_status_counts().await?;
        let (rows, _) = self.store.list_application_jobs_filtered(None, None, 0, 10).await?;
        let mut recent_jobs = Vec::with_capacity(rows.len());
        for r in rows {
            recent_jobs.push(scan_job_row_to_view(r)?);
        }
        let (recent_runs, _) = self.store.list_scan_runs_page(0, 10).await?;
        let catalog = self.list_plugin_catalog();
        Ok(OverviewResponse {
            status: "ok",
            job_counts,
            plugins_enabled: catalog.plugins_enabled,
            allowed_plugins_count: catalog.plugins.len(),
            recent_jobs,
            recent_runs,
        })
    }

    pub async fn get_job_batch_status(
        &self,
        job_id: JobId,
    ) -> Result<BatchStatusResponse, AppError> {
        let row = self.store.get_scan_job(job_id).await?;
        let kind = JobKind::parse(&row.job_kind)
            .ok_or_else(|| AppError::Internal(format!("unknown job_kind `{}`", row.job_kind)))?;
        if !matches!(kind, JobKind::BatchRepair | JobKind::BatchResume) {
            return Err(AppError::InvalidRequest(format!(
                "job {} is kind `{}` (not a batch job)",
                job_id.0, row.job_kind
            )));
        }
        if let Some(ref_json) = row.result_ref_json.as_deref() {
            if let Ok(JobResultReference::BatchRun { run_dir }) = serde_json::from_str(ref_json) {
                return self.batch_status_sync(camino::Utf8PathBuf::from(run_dir)).await;
            }
        }
        match kind {
            JobKind::BatchResume => {
                let req: parqonaut_app::BatchResumeRequest =
                    Self::decode_durable_payload(&row.request_json)?;
                if !req.run_dir.join(parqonaut_orchestrator::JOURNAL_FILE).exists() {
                    return Err(AppError::Conflict("batch run journal not initialized yet".into()));
                }
                self.batch_status_sync(req.run_dir).await
            }
            JobKind::BatchRepair => {
                let req: parqonaut_app::BatchRepairRequest =
                    Self::decode_durable_payload(&row.request_json)?;
                let plan_bytes = std::fs::read(req.plan_path.as_std_path()).map_err(|e| {
                    AppError::InvalidRequest(format!("unable to read batch plan: {e}"))
                })?;
                let plan = parqonaut_orchestrator::BatchPlan::from_json(&plan_bytes)
                    .map_err(|e| AppError::InvalidRequest(format!("invalid batch plan: {e}")))?;
                let runs_dir = plan.effective_run_root().join(parqonaut_orchestrator::RUNS_DIR);
                if let Some(found) = find_matching_batch_run_dir(&runs_dir, &plan.batch_plan_id.0) {
                    return self.batch_status_sync(found).await;
                }
                Err(AppError::Conflict("batch run has not initialized its journal yet".into()))
            }
            _ => unreachable!(),
        }
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
    ) -> Result<parqonaut_types::RunDiff, AppError> {
        let a = self.store.load_report(left).await?;
        let b = self.store.load_report(right).await?;
        Ok(parqonaut_store::diff_reports(left, right, &a, &b))
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
        self.submit_repair_job_with_idempotency(body, None).await
    }

    pub async fn submit_repair_job_with_idempotency(
        &self,
        body: RepairJobBody,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let req = application_http::to_repair_request(body)?;
        self.app.policy().validate_dataset(&req.source)?;
        self.app.policy().validate_dataset(&req.output)?;
        let identity = application_http::target_identity_for_location(&req.source);
        let jid = JobId::new();
        let payload = application_http::durable_payload(&req)?;
        let sha = sha256_hex(&payload);
        let expires =
            Utc::now() + ChronoDuration::seconds(self.events.policy().idempotency_ttl_secs);
        let idem = idempotency_key.map(|k| ("repair", k, sha.as_str(), expires));
        match self
            .store
            .insert_application_job_with_idempotency(
                jid,
                JobKind::Repair,
                &identity,
                &payload,
                idem,
            )
            .await?
        {
            IdempotencyInsertResult::Created { job_id, submitted_at } => {
                metrics::counter!("parqonaut_jobs_submitted_total", "kind" => "repair")
                    .increment(1);
                let _ = self
                    .events
                    .record_lifecycle(lifecycle_event(
                        job_id,
                        ProgressEventKind::JobQueued,
                        JobStage::Queue,
                        ProgressMetrics::default(),
                        format!(
                            "repair job queued (plan={}, authorize={:?})",
                            req.plan.plan_id, req.authorize
                        ),
                    ))
                    .await;
                Ok(job_submission_view_at(job_id, &identity, submitted_at))
            }
            IdempotencyInsertResult::Replayed { existing } => submission_from_row(&existing),
        }
    }

    pub async fn submit_batch_repair_job(
        &self,
        body: BatchRepairJobBody,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        self.submit_batch_repair_job_with_idempotency(body, None).await
    }

    pub async fn submit_batch_repair_job_with_idempotency(
        &self,
        body: BatchRepairJobBody,
        idempotency_key: Option<&str>,
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
        let sha = sha256_hex(&payload);
        let expires =
            Utc::now() + ChronoDuration::seconds(self.events.policy().idempotency_ttl_secs);
        let idem = idempotency_key.map(|k| ("batch_repair", k, sha.as_str(), expires));
        match self
            .store
            .insert_application_job_with_idempotency(
                jid,
                JobKind::BatchRepair,
                &identity,
                &payload,
                idem,
            )
            .await?
        {
            IdempotencyInsertResult::Created { job_id, submitted_at } => {
                metrics::counter!("parqonaut_jobs_submitted_total", "kind" => "batch_repair")
                    .increment(1);
                let _ = self
                    .events
                    .record_lifecycle(lifecycle_event(
                        job_id,
                        ProgressEventKind::JobQueued,
                        JobStage::Queue,
                        ProgressMetrics::default(),
                        format!("batch repair queued ({})", req.plan_path),
                    ))
                    .await;
                Ok(job_submission_view_at(job_id, &identity, submitted_at))
            }
            IdempotencyInsertResult::Replayed { existing } => submission_from_row(&existing),
        }
    }

    pub async fn submit_batch_resume_job(
        &self,
        run_dir: camino::Utf8PathBuf,
        body: BatchResumeBody,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        self.submit_batch_resume_job_with_idempotency(run_dir, body, None).await
    }

    pub async fn submit_batch_resume_job_with_idempotency(
        &self,
        run_dir: camino::Utf8PathBuf,
        body: BatchResumeBody,
        idempotency_key: Option<&str>,
    ) -> Result<ScanJobSubmissionResponse, AppError> {
        let req = application_http::to_batch_resume_request(run_dir.clone(), body)?;
        let identity = application_http::target_identity_for_path("batch_resume", run_dir.as_str());
        let jid = JobId::new();
        let payload = application_http::durable_payload(&req)?;
        let sha = sha256_hex(&payload);
        let expires =
            Utc::now() + ChronoDuration::seconds(self.events.policy().idempotency_ttl_secs);
        let idem = idempotency_key.map(|k| ("batch_resume", k, sha.as_str(), expires));
        match self
            .store
            .insert_application_job_with_idempotency(
                jid,
                JobKind::BatchResume,
                &identity,
                &payload,
                idem,
            )
            .await?
        {
            IdempotencyInsertResult::Created { job_id, submitted_at } => {
                metrics::counter!("parqonaut_jobs_submitted_total", "kind" => "batch_resume")
                    .increment(1);
                let _ = self
                    .events
                    .record_lifecycle(lifecycle_event(
                        job_id,
                        ProgressEventKind::JobQueued,
                        JobStage::Queue,
                        ProgressMetrics::default(),
                        format!("batch resume queued ({})", run_dir),
                    ))
                    .await;
                Ok(job_submission_view_at(job_id, &identity, submitted_at))
            }
            IdempotencyInsertResult::Replayed { existing } => submission_from_row(&existing),
        }
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
            JobCancelOutcome::CanceledFromQueued => {
                let _ = self
                    .events
                    .record_lifecycle(lifecycle_event(
                        job_id,
                        ProgressEventKind::JobCancelled,
                        JobStage::Cancel,
                        ProgressMetrics::default(),
                        "job canceled before worker execution",
                    ))
                    .await;
                "job canceled".into()
            }
            JobCancelOutcome::AlreadyCanceled => "job already canceled".into(),
            JobCancelOutcome::CancelRequestedForRunning => {
                let _ = self
                    .events
                    .record_lifecycle(lifecycle_event(
                        job_id,
                        ProgressEventKind::CancelRequested,
                        JobStage::Cancel,
                        ProgressMetrics::default(),
                        "cancel requested; worker stopping at next safe boundary",
                    ))
                    .await;
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
        progress: Option<&dyn ProgressObserver>,
    ) -> Result<JobResultReference, AppError> {
        let req: parqonaut_app::RepairRequest = Self::decode_durable_payload(payload_json)?;
        let noop = NoOpProgressObserver;
        let result = self
            .app
            .repair_with_progress(req, progress.unwrap_or(&noop))
            .await
            .map_err(AppError::from)?;
        Ok(JobResultReference::RepairManifest { manifest_path: result.execution.manifest_path })
    }

    pub(crate) async fn run_batch_repair_job(
        &self,
        payload_json: &str,
        cancel: CancelFlag,
        progress: Option<Arc<dyn ProgressObserver>>,
    ) -> Result<JobResultReference, AppError> {
        let req: parqonaut_app::BatchRepairRequest = Self::decode_durable_payload(payload_json)?;
        let out = self
            .app
            .batch_repair_with_progress(
                req,
                cancel,
                progress.unwrap_or_else(|| Arc::new(NoOpProgressObserver)),
            )
            .await
            .map_err(AppError::from)?;
        Ok(JobResultReference::BatchRun { run_dir: out.execution.run_dir.as_str().to_string() })
    }

    pub(crate) async fn run_batch_resume_job(
        &self,
        payload_json: &str,
        cancel: CancelFlag,
        progress: Option<Arc<dyn ProgressObserver>>,
    ) -> Result<JobResultReference, AppError> {
        let req: parqonaut_app::BatchResumeRequest = Self::decode_durable_payload(payload_json)?;
        let out = self
            .app
            .batch_resume_with_progress(
                req,
                cancel,
                progress.unwrap_or_else(|| Arc::new(NoOpProgressObserver)),
            )
            .await
            .map_err(AppError::from)?;
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

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex_encode(h.finalize())
}

fn submission_from_row(row: &ScanJobRow) -> Result<ScanJobSubmissionResponse, AppError> {
    let job_id = uuid::Uuid::parse_str(&row.job_id)
        .map_err(|_| AppError::Internal("invalid job_id in store".into()))?;
    let status = parse_job_status_label(&row.status)?;
    let submitted_at = parse_rfc3339(&row.submitted_at)?;
    Ok(ScanJobSubmissionResponse {
        job_id,
        status,
        submitted_at,
        target_kind: row.target_kind.clone(),
        normalized_target_key: row.normalized_target_key.clone(),
        job_url: format!("/api/v1/jobs/{job_id}"),
    })
}

fn job_submission_view_at(
    jid: JobId,
    identity: &TargetIdentity,
    submitted_at: DateTime<Utc>,
) -> ScanJobSubmissionResponse {
    ScanJobSubmissionResponse {
        job_id: jid.0,
        status: JobStatus::Queued,
        submitted_at,
        target_kind: identity.target_kind.clone(),
        normalized_target_key: identity.normalized_key.clone(),
        job_url: format!("/api/v1/jobs/{}", jid.0),
    }
}

fn normalize_job_kind_filter(k: &str) -> Result<&'static str, AppError> {
    let norm = k.trim().to_ascii_lowercase();
    JobKind::parse(&norm)
        .map(JobKind::as_str)
        .ok_or_else(|| AppError::InvalidRequest(format!("unknown job kind filter `{k}`")))
}

fn find_matching_batch_run_dir(
    runs_dir: &camino::Utf8Path,
    expected_plan_id: &str,
) -> Option<camino::Utf8PathBuf> {
    let entries = std::fs::read_dir(runs_dir.as_std_path()).ok()?;
    let mut best: Option<(String, camino::Utf8PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(utf8) = camino::Utf8PathBuf::from_path_buf(path) else {
            continue;
        };
        let status = parqonaut_orchestrator::read_status(&utf8).ok()?;
        if status.batch_plan_id != expected_plan_id {
            continue;
        }
        let replace = best.as_ref().map_or(true, |(ts, _)| status.started_at > *ts);
        if replace {
            best = Some((status.started_at, utf8));
        }
    }
    best.map(|(_, p)| p)
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
        attempt_count: i64::from(row.attempt_count),
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
