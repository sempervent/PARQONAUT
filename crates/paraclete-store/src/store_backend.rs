//! Explicit backend selection: SQLite (single-node) or Postgres (multi-writer capable).

use chrono::{DateTime, Utc};
use paraclete_types::{
    AuthPrincipal, AuthRole, AuthTokenId, JobId, JobRecoveryPolicy, RedactionPolicy, RunId,
    ScanReport, ScanRun, ScanRunListItem, ScanSummary, TargetIdentity,
};

use crate::auth_store::AuthTokenSummary;
use crate::error::StoreError;
use crate::models::{
    RunPublicMeta, ScanJobRow, StaleRecoveryStats, StoredAssetRow, StoredFindingRow,
};
use crate::postgres_store::PostgresScanStore;
use crate::sqlite_store::SqliteScanStore;

/// Which physical store backs Paraclete.
#[derive(Debug, Clone)]
pub enum StoreBackend {
    Sqlite(SqliteScanStore),
    Postgres(PostgresScanStore),
}

impl StoreBackend {
    /// Connects using `PARACLETE_DATABASE_URL` semantics: `postgres://` / `postgresql://` use Postgres; otherwise SQLite.
    pub async fn connect(database_url: impl AsRef<str>) -> Result<Self, StoreError> {
        let url = database_url.as_ref().trim();
        if url.is_empty() {
            return Err(StoreError::AuthToken("PARACLETE_DATABASE_URL is empty".into()));
        }
        if is_postgres_url(url) {
            Ok(StoreBackend::Postgres(PostgresScanStore::connect(url).await?))
        } else {
            Ok(StoreBackend::Sqlite(SqliteScanStore::connect(url).await?))
        }
    }

    pub async fn persist_scan_run(
        &self,
        run_id: RunId,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        report: &ScanReport,
        redaction: &RedactionPolicy,
    ) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.persist_scan_run(run_id, started_at, completed_at, report, redaction).await
            }
            StoreBackend::Postgres(p) => {
                p.persist_scan_run(run_id, started_at, completed_at, report, redaction).await
            }
        }
    }

    pub async fn load_stored_scan_summary(&self, run_id: RunId) -> Result<ScanSummary, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.load_stored_scan_summary(run_id).await,
            StoreBackend::Postgres(p) => p.load_stored_scan_summary(run_id).await,
        }
    }

    pub async fn count_assets(
        &self,
        run_id: RunId,
        inspection_status: Option<&str>,
    ) -> Result<u64, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.count_assets(run_id, inspection_status).await,
            StoreBackend::Postgres(p) => p.count_assets(run_id, inspection_status).await,
        }
    }

    pub async fn list_assets_page(
        &self,
        run_id: RunId,
        inspection_status: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<StoredAssetRow>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.list_assets_page(run_id, inspection_status, offset, limit).await
            }
            StoreBackend::Postgres(p) => {
                p.list_assets_page(run_id, inspection_status, offset, limit).await
            }
        }
    }

    pub async fn count_findings(
        &self,
        run_id: RunId,
        severity: Option<&str>,
        code: Option<&str>,
    ) -> Result<u64, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.count_findings(run_id, severity, code).await,
            StoreBackend::Postgres(p) => p.count_findings(run_id, severity, code).await,
        }
    }

    pub async fn list_findings_page(
        &self,
        run_id: RunId,
        severity: Option<&str>,
        code: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<StoredFindingRow>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.list_findings_page(run_id, severity, code, offset, limit).await
            }
            StoreBackend::Postgres(p) => {
                p.list_findings_page(run_id, severity, code, offset, limit).await
            }
        }
    }

    pub async fn load_report(&self, run_id: RunId) -> Result<ScanReport, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.load_report(run_id).await,
            StoreBackend::Postgres(p) => p.load_report(run_id).await,
        }
    }

    pub async fn list_runs_for_target(
        &self,
        identity: &TargetIdentity,
        limit: i64,
    ) -> Result<Vec<ScanRunListItem>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.list_runs_for_target(identity, limit).await,
            StoreBackend::Postgres(p) => p.list_runs_for_target(identity, limit).await,
        }
    }

    pub async fn get_run_header(&self, run_id: RunId) -> Result<ScanRun, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.get_run_header(run_id).await,
            StoreBackend::Postgres(p) => p.get_run_header(run_id).await,
        }
    }

    pub async fn get_run_public_meta(&self, run_id: RunId) -> Result<RunPublicMeta, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.get_run_public_meta(run_id).await,
            StoreBackend::Postgres(p) => p.get_run_public_meta(run_id).await,
        }
    }

    pub async fn verify_projection_integrity(&self, run_id: RunId) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.verify_projection_integrity(run_id).await,
            StoreBackend::Postgres(p) => p.verify_projection_integrity(run_id).await,
        }
    }

    pub async fn insert_scan_job_queued(
        &self,
        job_id: JobId,
        identity: &TargetIdentity,
        request_json: &str,
    ) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.insert_scan_job_queued(job_id, identity, request_json).await
            }
            StoreBackend::Postgres(p) => {
                p.insert_scan_job_queued(job_id, identity, request_json).await
            }
        }
    }

    pub async fn claim_next_queued_scan_job(
        &self,
        worker_id: &str,
        heartbeat_at: DateTime<Utc>,
        leased_until: DateTime<Utc>,
    ) -> Result<Option<ScanJobRow>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.claim_next_queued_scan_job(worker_id, heartbeat_at, leased_until).await
            }
            StoreBackend::Postgres(p) => {
                p.claim_next_queued_scan_job(worker_id, heartbeat_at, leased_until).await
            }
        }
    }

    pub async fn renew_scan_job_lease(
        &self,
        job_id: JobId,
        worker_id: &str,
        heartbeat_at: DateTime<Utc>,
        leased_until: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.renew_scan_job_lease(job_id, worker_id, heartbeat_at, leased_until).await
            }
            StoreBackend::Postgres(p) => {
                p.renew_scan_job_lease(job_id, worker_id, heartbeat_at, leased_until).await
            }
        }
    }

    pub async fn recover_stale_scan_jobs(
        &self,
        now: DateTime<Utc>,
        policy: JobRecoveryPolicy,
    ) -> Result<StaleRecoveryStats, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.recover_stale_scan_jobs(now, policy).await,
            StoreBackend::Postgres(p) => p.recover_stale_scan_jobs(now, policy).await,
        }
    }

    pub async fn complete_scan_job_success(
        &self,
        job_id: JobId,
        run_id: RunId,
    ) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.complete_scan_job_success(job_id, run_id).await,
            StoreBackend::Postgres(p) => p.complete_scan_job_success(job_id, run_id).await,
        }
    }

    pub async fn complete_scan_job_failure(
        &self,
        job_id: JobId,
        failure_code: &str,
        failure_message: &str,
    ) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => {
                s.complete_scan_job_failure(job_id, failure_code, failure_message).await
            }
            StoreBackend::Postgres(p) => {
                p.complete_scan_job_failure(job_id, failure_code, failure_message).await
            }
        }
    }

    pub async fn get_scan_job(&self, job_id: JobId) -> Result<ScanJobRow, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.get_scan_job(job_id).await,
            StoreBackend::Postgres(p) => p.get_scan_job(job_id).await,
        }
    }

    pub async fn count_scan_jobs(&self, status: Option<&str>) -> Result<u64, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.count_scan_jobs(status).await,
            StoreBackend::Postgres(p) => p.count_scan_jobs(status).await,
        }
    }

    pub async fn list_scan_jobs_page(
        &self,
        status: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ScanJobRow>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.list_scan_jobs_page(status, offset, limit).await,
            StoreBackend::Postgres(p) => p.list_scan_jobs_page(status, offset, limit).await,
        }
    }

    pub async fn insert_auth_token(
        &self,
        label: &str,
        plaintext: &str,
        role: AuthRole,
    ) -> Result<AuthTokenId, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.insert_auth_token(label, plaintext, role).await,
            StoreBackend::Postgres(p) => p.insert_auth_token(label, plaintext, role).await,
        }
    }

    pub async fn upsert_bootstrap_token(
        &self,
        plaintext: &str,
        role: AuthRole,
    ) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.upsert_bootstrap_token(plaintext, role).await,
            StoreBackend::Postgres(p) => p.upsert_bootstrap_token(plaintext, role).await,
        }
    }

    pub async fn create_auth_token(
        &self,
        label: &str,
        role: AuthRole,
        note: Option<&str>,
    ) -> Result<(AuthTokenId, String), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.create_auth_token(label, role, note).await,
            StoreBackend::Postgres(p) => p.create_auth_token(label, role, note).await,
        }
    }

    pub async fn verify_bearer_token(
        &self,
        plaintext: &str,
    ) -> Result<Option<AuthPrincipal>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.verify_bearer_token(plaintext).await,
            StoreBackend::Postgres(p) => p.verify_bearer_token(plaintext).await,
        }
    }

    pub async fn rotate_auth_token(
        &self,
        old_id: AuthTokenId,
    ) -> Result<(AuthTokenId, String), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.rotate_auth_token(old_id).await,
            StoreBackend::Postgres(p) => p.rotate_auth_token(old_id).await,
        }
    }

    pub async fn count_auth_tokens(&self) -> Result<i64, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.count_auth_tokens().await,
            StoreBackend::Postgres(p) => p.count_auth_tokens().await,
        }
    }

    pub async fn list_auth_tokens(&self) -> Result<Vec<AuthTokenSummary>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.list_auth_tokens().await,
            StoreBackend::Postgres(p) => p.list_auth_tokens().await,
        }
    }

    pub async fn get_auth_token_summary(
        &self,
        token_id: AuthTokenId,
    ) -> Result<Option<AuthTokenSummary>, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.get_auth_token_summary(token_id).await,
            StoreBackend::Postgres(p) => p.get_auth_token_summary(token_id).await,
        }
    }

    pub async fn disable_auth_token(
        &self,
        token_id: AuthTokenId,
    ) -> Result<AuthTokenSummary, StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.disable_auth_token(token_id).await,
            StoreBackend::Postgres(p) => p.disable_auth_token(token_id).await,
        }
    }

    pub async fn bootstrap_auth_from_env(&self) -> Result<(), StoreError> {
        match self {
            StoreBackend::Sqlite(s) => s.bootstrap_auth_from_env().await,
            StoreBackend::Postgres(p) => p.bootstrap_auth_from_env().await,
        }
    }
}

impl From<SqliteScanStore> for StoreBackend {
    fn from(value: SqliteScanStore) -> Self {
        StoreBackend::Sqlite(value)
    }
}

impl From<PostgresScanStore> for StoreBackend {
    fn from(value: PostgresScanStore) -> Self {
        StoreBackend::Postgres(value)
    }
}

fn is_postgres_url(url: &str) -> bool {
    let u = url.trim();
    u.starts_with("postgres://") || u.starts_with("postgresql://")
}
