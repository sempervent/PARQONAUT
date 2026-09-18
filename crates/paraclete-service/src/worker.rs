//! In-process background worker: claims queued jobs and calls the same scan path as synchronous API.

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use paraclete_types::{JobId, JobRecoveryPolicy, RunId};
use tracing::Instrument;
use uuid::Uuid;

use crate::api_types::StartScanRequest;
use crate::error::AppError;
use crate::observability::audit;
use crate::service::ParacleteService;

pub(crate) fn spawn_scan_job_worker(service: ParacleteService) -> tokio::task::JoinHandle<()> {
    let policy = JobRecoveryPolicy::default();
    let worker_id = Uuid::new_v4().to_string();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let now = Utc::now();
            match service.store().recover_stale_scan_jobs(now, policy).await {
                Ok(stats) => {
                    if stats.requeued > 0 {
                        metrics::counter!("paraclete_jobs_requeued_total")
                            .increment(stats.requeued);
                    }
                    if stats.failed_retries_exhausted > 0 {
                        metrics::counter!("paraclete_job_retries_exhausted_total")
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
                metrics::gauge!("paraclete_jobs_queued").set(q as f64);
                metrics::gauge!("paraclete_jobs_running").set(r as f64);
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
            metrics::counter!("paraclete_jobs_claimed_total").increment(1);
            audit::job_claimed(job_uuid, job.attempt_count, &worker_id);

            let job_span = tracing::info_span!(
                "paraclete.job",
                job_id = %jid,
                worker_id = %worker_id,
                attempt_count = job.attempt_count,
            );

            let req: StartScanRequest = match serde_json::from_str(&job.request_json) {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    let _ = async {
                        let _ = service
                            .store()
                            .complete_scan_job_failure(jid, "invalid_request", &msg)
                            .await;
                        metrics::counter!("paraclete_jobs_failed_total", "code" => "invalid_request")
                            .increment(1);
                        audit::job_failed(job_uuid, "invalid_request");
                    }
                    .instrument(job_span)
                    .await;
                    continue;
                }
            };

            let svc = service.clone();
            let wid = worker_id.clone();
            let hb_jid = jid;
            let hb_policy = policy;
            let heartbeat = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(hb_policy.heartbeat_interval_secs))
                        .await;
                    let n = Utc::now();
                    let lu = n + ChronoDuration::seconds(hb_policy.lease_duration_secs as i64);
                    match svc.store().renew_scan_job_lease(hb_jid, &wid, n, lu).await {
                        Ok(true) => {
                            metrics::counter!("paraclete_job_lease_renewals_total").increment(1);
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
                match service.execute_scan_and_persist(req).await {
                    Ok(resp) => {
                        heartbeat.abort();
                        match service
                            .store()
                            .complete_scan_job_success(jid, RunId(resp.run_id))
                            .await
                        {
                            Ok(()) => {
                                metrics::counter!("paraclete_jobs_succeeded_total").increment(1);
                                audit::job_completed(jid.0, resp.run_id);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "complete_scan_job_success");
                            }
                        }
                    }
                    Err(e) => {
                        heartbeat.abort();
                        let code = app_error_to_job_failure_label(&e);
                        let _ = service
                            .store()
                            .complete_scan_job_failure(jid, code, &e.to_string())
                            .await;
                        metrics::counter!("paraclete_jobs_failed_total", "code" => code)
                            .increment(1);
                        audit::job_failed(jid.0, code);
                    }
                }
            }
            .instrument(job_span);
            run_outcome.await;
        }
    })
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
        AppError::Unauthorized(_) | AppError::Forbidden(_) => "internal_error",
    }
}
