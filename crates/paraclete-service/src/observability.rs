//! Metrics (Prometheus) and structured audit logging (`target = "paraclete_audit"`).

use std::sync::OnceLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();

/// Installs the global Prometheus metrics recorder (once) and returns a handle for `/metrics`.
pub fn metrics_handle() -> PrometheusHandle {
    PROMETHEUS
        .get_or_init(|| {
            PrometheusBuilder::new()
                .set_buckets_for_metric(
                    Matcher::Full("paraclete_http_request_duration_seconds".to_string()),
                    &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
                )
                .expect("http histogram buckets")
                .set_buckets_for_metric(
                    Matcher::Full("paraclete_scan_engine_duration_seconds".to_string()),
                    &[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0],
                )
                .expect("scan histogram buckets")
                .set_buckets_for_metric(
                    Matcher::Full("paraclete_run_persist_duration_seconds".to_string()),
                    &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
                )
                .expect("persist histogram buckets")
                .install_recorder()
                .expect("install Prometheus metrics recorder")
        })
        .clone()
}

/// Structured audit events (never log raw tokens).
pub mod audit {
    use paraclete_types::AuthRole;
    use uuid::Uuid;

    pub fn auth_accepted(token_id: Uuid, label: &str, role: AuthRole) {
        tracing::info!(
            target: "paraclete_audit",
            event = "auth.accepted",
            token_id = %token_id,
            token_label = label,
            role = ?role,
        );
    }

    pub fn auth_rejected(reason: &'static str) {
        tracing::info!(target: "paraclete_audit", event = "auth.rejected", reason = reason,);
    }

    pub fn auth_forbidden(token_id: uuid::Uuid, label: &str, role: AuthRole) {
        tracing::info!(
            target: "paraclete_audit",
            event = "auth.forbidden",
            token_id = %token_id,
            token_label = label,
            role = ?role,
        );
    }

    pub fn scan_submitted(job_id: Uuid, target_kind: &str, normalized_target_key: &str) {
        tracing::info!(
            target: "paraclete_audit",
            event = "scan.submitted",
            job_id = %job_id,
            target_kind = target_kind,
            normalized_target_key = normalized_target_key,
        );
    }

    pub fn job_claimed(job_id: uuid::Uuid, attempt_count: i64, worker_id: &str) {
        tracing::info!(
            target: "paraclete_audit",
            event = "job.claimed",
            job_id = %job_id,
            attempt_count = attempt_count,
            worker_id = worker_id,
        );
    }

    pub fn job_heartbeat(job_id: uuid::Uuid) {
        tracing::debug!(target: "paraclete_audit", event = "job.heartbeat", job_id = %job_id,);
    }

    pub fn job_recovered(requeued: u64, retries_exhausted: u64) {
        tracing::info!(
            target: "paraclete_audit",
            event = "job.recovered",
            requeued = requeued,
            retries_exhausted = retries_exhausted,
        );
    }

    pub fn job_completed(job_id: uuid::Uuid, run_id: uuid::Uuid) {
        tracing::info!(
            target: "paraclete_audit",
            event = "job.completed",
            job_id = %job_id,
            run_id = %run_id,
        );
    }

    pub fn job_failed(job_id: uuid::Uuid, failure_code: &str) {
        tracing::info!(
            target: "paraclete_audit",
            event = "job.failed",
            job_id = %job_id,
            failure_code = failure_code,
        );
    }

    pub fn run_persisted(run_id: uuid::Uuid, target_kind: &str, normalized_target_key: &str) {
        tracing::info!(
            target: "paraclete_audit",
            event = "run.persisted",
            run_id = %run_id,
            target_kind = target_kind,
            normalized_target_key = normalized_target_key,
        );
    }

    pub fn token_created(new_token_id: uuid::Uuid, label: &str, role: AuthRole) {
        tracing::info!(
            target: "paraclete_audit",
            event = "token.created",
            token_id = %new_token_id,
            token_label = label,
            role = ?role,
        );
    }

    pub fn token_disabled(target_token_id: uuid::Uuid, actor_token_id: uuid::Uuid) {
        tracing::info!(
            target: "paraclete_audit",
            event = "token.disabled",
            target_token_id = %target_token_id,
            actor_token_id = %actor_token_id,
        );
    }

    pub fn token_rotated(
        previous_token_id: uuid::Uuid,
        new_token_id: uuid::Uuid,
        actor_token_id: uuid::Uuid,
    ) {
        tracing::info!(
            target: "paraclete_audit",
            event = "token.rotated",
            previous_token_id = %previous_token_id,
            new_token_id = %new_token_id,
            actor_token_id = %actor_token_id,
        );
    }
}

#[cfg(test)]
mod audit_log_tests {
    use paraclete_types::AuthRole;
    use tracing_test::traced_test;
    use uuid::Uuid;

    use super::audit;

    #[traced_test]
    #[test]
    fn stable_audit_event_fields_appear_in_logs() {
        audit::auth_rejected("missing_bearer");
        assert!(logs_contain("missing_bearer"));
        audit::auth_forbidden(Uuid::nil(), "lab", AuthRole::Reader);
        assert!(logs_contain("auth.forbidden"));
    }
}
