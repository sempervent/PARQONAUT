//! In-process background worker: claims queued application jobs and delegates to [`ParqonautService`].

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use futures_util::FutureExt;
use parqonaut_orchestrator::CancelFlag;
use parqonaut_types::{JobId, JobKind, JobRecoveryPolicy, RunId};
use tracing::Instrument;
use uuid::Uuid;

use crate::api_types::StartScanRequest;
use crate::error::AppError;
use crate::observability::audit;
use crate::service::ParqonautService;

pub(crate) fn spawn_scan_job_workers(
    service: ParqonautService,
    workers: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..workers).map(|_| spawn_application_job_worker(service.clone())).collect()
}

fn spawn_application_job_worker(service: ParqonautService) -> tokio::task::JoinHandle<()> {
    let policy = JobRecoveryPolicy::default();
    let worker_id = Uuid::new_v4().to_string();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let now = Utc::now();
            match service.store().recover_stale_scan_jobs(now, policy).await {
                Ok(stats) => {
                    if stats.requeued > 0 {
                        metrics::counter!("parqonaut_jobs_requeued_total")
                            .increment(stats.requeued);
                    }
                    if stats.failed_retries_exhausted > 0 {
                        metrics::counter!("parqonaut_job_retries_exhausted_total")
                            .increment(stats.failed_retries_exhausted);
                    }
                    if stats.requeued > 0 || stats.failed_retries_exhausted > 0 {
                        audit::job_recovered(stats.requeued, stats.failed_retries_exhausted);
                    }
                }
                Err(e) => tracing::error!(error = %e, "recover_stale_scan_jobs"),
            }

            if let (Ok(q), Ok(r)) = (
                service.store().count_scan_jobs(Some("queued")).await,
                service.store().count_scan_jobs(Some("running")).await,
            ) {
                metrics::gauge!("parqonaut_jobs_queued").set(q as f64);
                metrics::gauge!("parqonaut_jobs_running").set(r as f64);
            }

            let hb = Utc::now();
            let leased_until = hb + ChronoDuration::seconds(policy.lease_duration_secs as i64);
            let job = match service
                .store()
                .claim_next_queued_scan_job(&worker_id, hb, leased_until)
                .await
            {
                Ok(Some(j)) => j,
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!(error = %e, "claim_next_queued_scan_job");
                    continue;
                }
            };
            let job_uuid = match Uuid::parse_str(&job.job_id) {
                Ok(u) => u,
                Err(_) => continue,
            };
            let jid = JobId(job_uuid);
            let kind = JobKind::parse(&job.job_kind).unwrap_or(JobKind::Scan);
            metrics::counter!("parqonaut_jobs_claimed_total", "kind" => kind.as_str()).increment(1);
            audit::job_claimed(job_uuid, job.attempt_count, &worker_id);

            let job_span = tracing::info_span!(
                "parqonaut.job",
                job_id = %jid,
                worker_id = %worker_id,
                attempt_count = job.attempt_count,
                job_kind = kind.as_str(),
            );

            let cancel_flag = CancelFlag::new();
            let svc = service.clone();
            let wid = worker_id.clone();
            let hb_jid = jid;
            let hb_policy = policy;
            let hb_cancel = cancel_flag.clone();
            let heartbeat = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(hb_policy.heartbeat_interval_secs))
                        .await;
                    if hb_cancel.is_cancelled() {
                        break;
                    }
                    if svc.store().job_cancel_requested(hb_jid).await.unwrap_or(false) {
                        hb_cancel.cancel();
                    }
                    let n = Utc::now();
                    let lu = n + ChronoDuration::seconds(hb_policy.lease_duration_secs as i64);
                    match svc.store().renew_scan_job_lease(hb_jid, &wid, n, lu).await {
                        Ok(true) => {
                            metrics::counter!("parqonaut_job_lease_renewals_total").increment(1);
                            audit::job_heartbeat(hb_jid.0);
                        }
                        Ok(false) => break,
                        Err(e) => {
                            tracing::warn!(error = %e, job_id = %hb_jid, "renew_scan_job_lease");
                            break;
                        }
                    }
                }
            });

            let run_outcome = async {
                let work = AssertUnwindSafe(run_claimed_job(
                    &service,
                    jid,
                    kind,
                    &job.request_json,
                    cancel_flag.clone(),
                ));
                let result = work.catch_unwind().await;
                heartbeat.abort();
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => fail_job(&service, jid, &e).await,
                    Err(panic) => {
                        let msg = panic_message(panic);
                        let _ = service
                            .store()
                            .complete_scan_job_failure(jid, "internal_error", &msg)
                            .await;
                        metrics::counter!("parqonaut_jobs_failed_total", "code" => "internal_error")
                            .increment(1);
                        audit::job_failed(jid.0, "internal_error");
                    }
                }
            }
            .instrument(job_span);
            run_outcome.await;
        }
    })
}

async fn run_claimed_job(
    service: &ParqonautService,
    jid: JobId,
    kind: JobKind,
    request_json: &str,
    cancel: CancelFlag,
) -> Result<(), AppError> {
    if service.store().job_cancel_requested(jid).await? {
        cancel.cancel();
    }

    match kind {
        JobKind::Scan => run_scan_job(service, jid, request_json).await,
        JobKind::Repair => run_repair_job(service, jid, request_json, cancel).await,
        JobKind::BatchRepair => run_batch_repair_job(service, jid, request_json, cancel).await,
        JobKind::BatchResume => run_batch_resume_job(service, jid, request_json, cancel).await,
    }
}

async fn run_scan_job(
    service: &ParqonautService,
    jid: JobId,
    request_json: &str,
) -> Result<(), AppError> {
    let req: StartScanRequest = serde_json::from_str(request_json)
        .map_err(|e| AppError::InvalidRequest(format!("invalid scan job payload: {e}")))?;
    let resp = service.execute_scan_and_persist(req).await?;
    service.store().complete_scan_job_success(jid, RunId(resp.run_id)).await?;
    metrics::counter!("parqonaut_jobs_succeeded_total", "kind" => "scan").increment(1);
    audit::job_completed(jid.0, resp.run_id);
    Ok(())
}

async fn run_repair_job(
    service: &ParqonautService,
    jid: JobId,
    request_json: &str,
    cancel: CancelFlag,
) -> Result<(), AppError> {
    if cancel.is_cancelled() {
        return cancel_running_job(service, jid).await;
    }
    let result_ref = service.run_repair_job(request_json).await?;
    if cancel.is_cancelled() {
        return cancel_running_job(service, jid).await;
    }
    complete_with_result_ref(service, jid, None, &result_ref).await
}

async fn run_batch_repair_job(
    service: &ParqonautService,
    jid: JobId,
    request_json: &str,
    cancel: CancelFlag,
) -> Result<(), AppError> {
    if cancel.is_cancelled() {
        return cancel_running_job(service, jid).await;
    }
    let result_ref = service.run_batch_repair_job(request_json, cancel.clone()).await?;
    if cancel.is_cancelled() {
        return cancel_running_job(service, jid).await;
    }
    complete_with_result_ref(service, jid, None, &result_ref).await
}

async fn run_batch_resume_job(
    service: &ParqonautService,
    jid: JobId,
    request_json: &str,
    cancel: CancelFlag,
) -> Result<(), AppError> {
    if cancel.is_cancelled() {
        return cancel_running_job(service, jid).await;
    }
    let result_ref = service.run_batch_resume_job(request_json, cancel.clone()).await?;
    if cancel.is_cancelled() {
        return cancel_running_job(service, jid).await;
    }
    complete_with_result_ref(service, jid, None, &result_ref).await
}

async fn complete_with_result_ref(
    service: &ParqonautService,
    jid: JobId,
    run_id: Option<RunId>,
    result_ref: &parqonaut_app::JobResultReference,
) -> Result<(), AppError> {
    let json = serde_json::to_string(result_ref)?;
    service.store().complete_application_job_success(jid, run_id, Some(&json)).await?;
    metrics::counter!("parqonaut_jobs_succeeded_total").increment(1);
    if let parqonaut_app::JobResultReference::ScanRun { run_id } = result_ref {
        audit::job_completed(jid.0, *run_id);
    } else {
        audit::job_completed(jid.0, jid.0);
    }
    Ok(())
}

async fn cancel_running_job(service: &ParqonautService, jid: JobId) -> Result<(), AppError> {
    service.store().complete_application_job_canceled(jid).await?;
    metrics::counter!("parqonaut_jobs_cancel_total").increment(1);
    audit::job_failed(jid.0, "canceled");
    Ok(())
}

async fn fail_job(service: &ParqonautService, jid: JobId, e: &AppError) {
    let code = app_error_to_job_failure_label(e);
    let _ = service.store().complete_scan_job_failure(jid, code, &e.to_string()).await;
    metrics::counter!("parqonaut_jobs_failed_total", "code" => code).increment(1);
    audit::job_failed(jid.0, code);
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        format!("worker panic: {s}")
    } else if let Some(s) = panic.downcast_ref::<String>() {
        format!("worker panic: {s}")
    } else {
        "worker panic".into()
    }
}

fn app_error_to_job_failure_label(e: &AppError) -> &'static str {
    match e {
        AppError::InvalidRequest(_) | AppError::InvalidJsonRequest { .. } => "invalid_request",
        AppError::TargetNotFound(_) => "target_not_found",
        AppError::RunNotFound => "store_error",
        AppError::ScanFailed(_) => "scan_failed",
        AppError::Store(_) => "store_error",
        AppError::Internal(_) => "internal_error",
        AppError::JobNotFound => "internal_error",
        AppError::TokenNotFound => "internal_error",
        AppError::Unauthorized(_) | AppError::Forbidden(_) | AppError::LocationNotAllowed(_) => {
            "internal_error"
        }
    }
}
