//! Postgres-backed scan history (runs, reports, projections, jobs, auth).

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use hex::encode as hex_encode;
use paraclete_types::{
    apply_redaction_policy, validate_report, FailureKind, InspectionStatus, JobId,
    JobRecoveryPolicy, RedactionPolicy, RunId, RunOutcome, ScanReport, ScanRun, ScanRunListItem,
    ScanSummary, TargetIdentity,
};
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::StoreError;
use crate::labels::{
    category_label, data_format_label, grouping_kind_label, parse_data_format,
    partition_layout_label, severity_label,
};
use crate::models::{
    RunPublicMeta, ScanJobRow, StaleRecoveryStats, StoredAssetRow, StoredFindingRow,
};

/// Postgres persistence for scan runs.
#[derive(Debug, Clone)]
pub struct PostgresScanStore {
    pool: PgPool,
}

impl PostgresScanStore {
    /// Opens Postgres, applies migrations from `migrations/postgres`, and returns a pool-backed store.
    pub async fn connect(database_url: impl AsRef<str>) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(10).connect(database_url.as_ref()).await?;
        sqlx::migrate!("./migrations/postgres").run(&pool).await?;
        Ok(Self { pool })
    }

    /// Persists a validated scan: canonical JSON blob plus normalized rows.
    pub async fn persist_scan_run(
        &self,
        run_id: RunId,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        report: &ScanReport,
        redaction: &RedactionPolicy,
    ) -> Result<(), StoreError> {
        validate_report(report).map_err(|e| StoreError::ReportValidation(e.to_string()))?;
        let stored = apply_redaction_policy(report.clone(), redaction);
        validate_report(&stored).map_err(|e| StoreError::ReportValidation(e.to_string()))?;

        let identity = TargetIdentity::from_scan_target(&stored.request.target);
        let target_json = serde_json::to_value(&stored.request.target)?;
        let outcome = if stored.summary.partial_inspection {
            RunOutcome::CompletedPartial
        } else {
            RunOutcome::Completed
        };
        let json = serde_json::to_string(&stored)?;
        let digest = Sha256::digest(json.as_bytes());
        let sha = hex_encode(digest);
        let summary_json = serde_json::to_string(&stored.summary)?;

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"INSERT INTO scan_runs (
                run_id, request_scan_id, target_kind, normalized_target_key, target_json,
                started_at, completed_at, run_outcome, engine_revision,
                contract_schema_version, report_format_version, report_sha256, summary_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#,
        )
        .bind(run_id.0.to_string())
        .bind(stored.request.scan_id.to_string())
        .bind(&identity.target_kind)
        .bind(&identity.normalized_key)
        .bind(target_json.to_string())
        .bind(started_at.to_rfc3339())
        .bind(completed_at.to_rfc3339())
        .bind(run_outcome_db(outcome))
        .bind(stored.metadata.engine_revision.clone())
        .bind(stored.metadata.contract_schema.0.clone())
        .bind(stored.metadata.report_format.0.clone())
        .bind(&sha)
        .bind(&summary_json)
        .execute(&mut *tx)
        .await?;

        sqlx::query("INSERT INTO scan_reports (run_id, report_json) VALUES ($1, $2)")
            .bind(run_id.0.to_string())
            .bind(&json)
            .execute(&mut *tx)
            .await?;

        for a in &stored.assets {
            sqlx::query(
                r#"INSERT INTO scan_assets
                (run_id, path, format, inspection_status, failure_kind, failure_message, dataset_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            )
            .bind(run_id.0.to_string())
            .bind(a.path.as_str())
            .bind(data_format_label(a.format))
            .bind(inspection_status_db(a.inspection_status))
            .bind(a.failure_kind.map(failure_kind_db))
            .bind(a.failure_message.clone())
            .bind(a.dataset_id.clone())
            .execute(&mut *tx)
            .await?;
        }

        for d in &stored.datasets {
            sqlx::query(
                r#"INSERT INTO scan_datasets
                (run_id, dataset_id, grouping_kind, partition_layout, file_count)
                VALUES ($1, $2, $3, $4, $5)"#,
            )
            .bind(run_id.0.to_string())
            .bind(&d.dataset_id)
            .bind(grouping_kind_label(&d.grouping_kind))
            .bind(partition_layout_label(&d.partition_layout))
            .bind(d.files.len() as i64)
            .execute(&mut *tx)
            .await?;
        }

        for f in &stored.findings {
            let fp =
                f.fingerprint.as_ref().map(|x| x.digest.as_str()).unwrap_or("missing_fingerprint");
            let asset_path =
                f.locations.first().and_then(|l| l.file.as_ref()).map(|p| p.as_str().to_string());
            let dataset_id: Option<String> =
                f.attributes.get("dataset_id").and_then(|v| v.as_str().map(String::from));
            sqlx::query(
                r#"INSERT INTO scan_findings
                (run_id, fingerprint, code, severity, category, asset_path, dataset_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            )
            .bind(run_id.0.to_string())
            .bind(fp)
            .bind(f.code.as_str())
            .bind(severity_label(f.severity))
            .bind(category_label(f.category))
            .bind(asset_path)
            .bind(dataset_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn load_stored_scan_summary(&self, run_id: RunId) -> Result<ScanSummary, StoreError> {
        let s: String = sqlx::query_scalar("SELECT summary_json FROM scan_runs WHERE run_id = $1")
            .bind(run_id.0.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::RunNotFound(run_id.0))?;
        match serde_json::from_str::<ScanSummary>(&s) {
            Ok(sum) => Ok(sum),
            Err(_) => Ok(self.load_report(run_id).await?.summary),
        }
    }

    pub async fn count_assets(
        &self,
        run_id: RunId,
        inspection_status: Option<&str>,
    ) -> Result<u64, StoreError> {
        self.ensure_run_exists(run_id).await?;
        let total: i64 = if let Some(st) = inspection_status {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM scan_assets WHERE run_id = $1 AND inspection_status = $2",
            )
            .bind(run_id.0.to_string())
            .bind(st)
            .fetch_one(&self.pool)
            .await?
        } else {
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_assets WHERE run_id = $1")
                .bind(run_id.0.to_string())
                .fetch_one(&self.pool)
                .await?
        };
        Ok(total as u64)
    }

    pub async fn list_assets_page(
        &self,
        run_id: RunId,
        inspection_status: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<StoredAssetRow>, StoreError> {
        self.ensure_run_exists(run_id).await?;
        let rows = if let Some(st) = inspection_status {
            sqlx::query(
                r#"SELECT path, format, inspection_status, failure_kind, failure_message, dataset_id
                   FROM scan_assets WHERE run_id = $1 AND inspection_status = $2
                   ORDER BY path ASC LIMIT $3 OFFSET $4"#,
            )
            .bind(run_id.0.to_string())
            .bind(st)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"SELECT path, format, inspection_status, failure_kind, failure_message, dataset_id
                   FROM scan_assets WHERE run_id = $1
                   ORDER BY path ASC LIMIT $2 OFFSET $3"#,
            )
            .bind(run_id.0.to_string())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(stored_asset_row_from_pg(&r)?);
        }
        Ok(out)
    }

    pub async fn count_findings(
        &self,
        run_id: RunId,
        severity: Option<&str>,
        code: Option<&str>,
    ) -> Result<u64, StoreError> {
        self.ensure_run_exists(run_id).await?;
        let total: i64 = match (severity, code) {
            (Some(sev), Some(cd)) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM scan_findings WHERE run_id = $1 AND severity = $2 AND code = $3",
            )
            .bind(run_id.0.to_string())
            .bind(sev)
            .bind(cd)
            .fetch_one(&self.pool)
            .await?,
            (Some(sev), None) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM scan_findings WHERE run_id = $1 AND severity = $2",
                )
                .bind(run_id.0.to_string())
                .bind(sev)
                .fetch_one(&self.pool)
                .await?
            }
            (None, Some(cd)) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM scan_findings WHERE run_id = $1 AND code = $2",
                )
                .bind(run_id.0.to_string())
                .bind(cd)
                .fetch_one(&self.pool)
                .await?
            }
            (None, None) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM scan_findings WHERE run_id = $1")
                    .bind(run_id.0.to_string())
                    .fetch_one(&self.pool)
                    .await?
            }
        };
        Ok(total as u64)
    }

    pub async fn list_findings_page(
        &self,
        run_id: RunId,
        severity: Option<&str>,
        code: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<StoredFindingRow>, StoreError> {
        self.ensure_run_exists(run_id).await?;
        let rows = match (severity, code) {
            (Some(sev), Some(cd)) => {
                sqlx::query(
                    r#"SELECT fingerprint, code, severity, category, asset_path, dataset_id
                       FROM scan_findings WHERE run_id = $1 AND severity = $2 AND code = $3
                       ORDER BY fingerprint ASC LIMIT $4 OFFSET $5"#,
                )
                .bind(run_id.0.to_string())
                .bind(sev)
                .bind(cd)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await?
            }
            (Some(sev), None) => {
                sqlx::query(
                    r#"SELECT fingerprint, code, severity, category, asset_path, dataset_id
                       FROM scan_findings WHERE run_id = $1 AND severity = $2
                       ORDER BY fingerprint ASC LIMIT $3 OFFSET $4"#,
                )
                .bind(run_id.0.to_string())
                .bind(sev)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await?
            }
            (None, Some(cd)) => {
                sqlx::query(
                    r#"SELECT fingerprint, code, severity, category, asset_path, dataset_id
                       FROM scan_findings WHERE run_id = $1 AND code = $2
                       ORDER BY fingerprint ASC LIMIT $3 OFFSET $4"#,
                )
                .bind(run_id.0.to_string())
                .bind(cd)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await?
            }
            (None, None) => {
                sqlx::query(
                    r#"SELECT fingerprint, code, severity, category, asset_path, dataset_id
                       FROM scan_findings WHERE run_id = $1
                       ORDER BY fingerprint ASC LIMIT $2 OFFSET $3"#,
                )
                .bind(run_id.0.to_string())
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(stored_finding_row_from_pg(&r)?);
        }
        Ok(out)
    }

    async fn ensure_run_exists(&self, run_id: RunId) -> Result<(), StoreError> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_runs WHERE run_id = $1")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        if n == 0 {
            return Err(StoreError::RunNotFound(run_id.0));
        }
        Ok(())
    }

    pub async fn load_report(&self, run_id: RunId) -> Result<ScanReport, StoreError> {
        let row: String =
            sqlx::query_scalar("SELECT report_json FROM scan_reports WHERE run_id = $1")
                .bind(run_id.0.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or(StoreError::RunNotFound(run_id.0))?;
        Ok(serde_json::from_str(&row)?)
    }

    pub async fn list_runs_for_target(
        &self,
        identity: &TargetIdentity,
        limit: i64,
    ) -> Result<Vec<ScanRunListItem>, StoreError> {
        let rows = sqlx::query(
            r#"SELECT run_id, request_scan_id, completed_at, run_outcome, target_kind, normalized_target_key
               FROM scan_runs
               WHERE target_kind = $1 AND normalized_target_key = $2
               ORDER BY completed_at DESC
               LIMIT $3"#,
        )
        .bind(&identity.target_kind)
        .bind(&identity.normalized_key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::new();
        for r in rows {
            let run_id = RunId(
                Uuid::parse_str(r.try_get::<String, _>(0)?.as_str())
                    .map_err(|_| StoreError::ReportValidation("invalid run_id".into()))?,
            );
            let request_scan_id = Uuid::parse_str(r.try_get::<String, _>(1)?.as_str())
                .map_err(|_| StoreError::ReportValidation("invalid request_scan_id".into()))?;
            let completed_at = DateTime::parse_from_rfc3339(r.try_get::<String, _>(2)?.as_str())
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| StoreError::ReportValidation("invalid completed_at".into()))?;
            let outcome = parse_run_outcome(r.try_get::<String, _>(3)?.as_str())?;
            let tid = TargetIdentity {
                target_kind: r.try_get::<String, _>(4)?,
                normalized_key: r.try_get::<String, _>(5)?,
            };
            out.push(ScanRunListItem {
                run_id,
                request_scan_id,
                completed_at,
                run_outcome: outcome,
                target_identity: tid,
            });
        }
        Ok(out)
    }

    pub async fn get_run_header(&self, run_id: RunId) -> Result<ScanRun, StoreError> {
        let meta = self.get_run_public_meta(run_id).await?;
        Ok(ScanRun {
            run_id: meta.run_id,
            request_scan_id: meta.request_scan_id,
            target_identity: meta.target_identity,
            target_json: meta.target_json,
            started_at: meta.started_at,
            completed_at: meta.completed_at,
            run_outcome: meta.run_outcome,
            engine_revision: meta.engine_revision,
            contract_schema_version: meta.contract_schema_version,
            report_format_version: meta.report_format_version,
        })
    }

    pub async fn get_run_public_meta(&self, run_id: RunId) -> Result<RunPublicMeta, StoreError> {
        let r = sqlx::query(
            r#"SELECT run_id, request_scan_id, target_kind, normalized_target_key, target_json,
                      started_at, completed_at, run_outcome, engine_revision,
                      contract_schema_version, report_format_version, report_sha256, summary_json
               FROM scan_runs WHERE run_id = $1"#,
        )
        .bind(run_id.0.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::RunNotFound(run_id.0))?;

        let target_json: serde_json::Value =
            serde_json::from_str(r.try_get::<String, _>(4)?.as_str())
                .map_err(|_| StoreError::ReportValidation("invalid target_json".into()))?;
        let summary_raw: String = r.try_get::<String, _>(12)?;
        let summary = match serde_json::from_str::<ScanSummary>(&summary_raw) {
            Ok(s) => s,
            Err(_) => self.load_report(run_id).await?.summary,
        };
        Ok(RunPublicMeta {
            run_id: RunId(
                Uuid::parse_str(r.try_get::<String, _>(0)?.as_str())
                    .map_err(|_| StoreError::ReportValidation("invalid run_id".into()))?,
            ),
            request_scan_id: Uuid::parse_str(r.try_get::<String, _>(1)?.as_str())
                .map_err(|_| StoreError::ReportValidation("invalid request_scan_id".into()))?,
            target_identity: TargetIdentity {
                target_kind: r.try_get::<String, _>(2)?,
                normalized_key: r.try_get::<String, _>(3)?,
            },
            target_json,
            started_at: DateTime::parse_from_rfc3339(r.try_get::<String, _>(5)?.as_str())
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| StoreError::ReportValidation("invalid started_at".into()))?,
            completed_at: DateTime::parse_from_rfc3339(r.try_get::<String, _>(6)?.as_str())
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| StoreError::ReportValidation("invalid completed_at".into()))?,
            run_outcome: parse_run_outcome(r.try_get::<String, _>(7)?.as_str())?,
            engine_revision: r.try_get::<Option<String>, _>(8)?,
            contract_schema_version: r.try_get::<String, _>(9)?,
            report_format_version: r.try_get::<String, _>(10)?,
            report_sha256: r.try_get::<String, _>(11)?,
            summary,
        })
    }

    pub async fn verify_projection_integrity(&self, run_id: RunId) -> Result<(), StoreError> {
        let report = self.load_report(run_id).await?;
        let ac: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_assets WHERE run_id = $1")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        let dc: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_datasets WHERE run_id = $1")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        let fc: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_findings WHERE run_id = $1")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        if ac != report.assets.len() as i64
            || dc != report.datasets.len() as i64
            || fc != report.findings.len() as i64
        {
            return Err(StoreError::ReportValidation(
                "projection row counts do not match report".into(),
            ));
        }
        Ok(())
    }

    pub async fn insert_scan_job_queued(
        &self,
        job_id: JobId,
        identity: &TargetIdentity,
        request_json: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO scan_jobs (
                job_id, submitted_at, status, target_kind, normalized_target_key, request_json, attempt_count
            ) VALUES ($1, $2, 'queued', $3, $4, $5, 0)"#,
        )
        .bind(job_id.0.to_string())
        .bind(&now)
        .bind(&identity.target_kind)
        .bind(&identity.normalized_key)
        .bind(request_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Claims the next queued job using `FOR UPDATE SKIP LOCKED` (concurrent workers).
    pub async fn claim_next_queued_scan_job(
        &self,
        worker_id: &str,
        heartbeat_at: DateTime<Utc>,
        leased_until: DateTime<Utc>,
    ) -> Result<Option<ScanJobRow>, StoreError> {
        let started = Utc::now().to_rfc3339();
        let hb = heartbeat_at.to_rfc3339();
        let lu = leased_until.to_rfc3339();
        let row = sqlx::query_as::<_, ScanJobRow>(
            r#"WITH picked AS (
                 SELECT job_id FROM scan_jobs
                 WHERE status = 'queued'
                 ORDER BY submitted_at ASC
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED
               )
               UPDATE scan_jobs AS j
               SET status = 'running',
                   started_at = $1,
                   worker_id = $2,
                   heartbeat_at = $3,
                   leased_until = $4,
                   attempt_count = j.attempt_count + 1
               FROM picked
               WHERE j.job_id = picked.job_id
               RETURNING j.job_id, j.submitted_at, j.started_at, j.completed_at, j.status,
                         j.target_kind, j.normalized_target_key, j.request_json,
                         j.failure_code, j.failure_message, j.run_id,
                         j.worker_id, j.attempt_count, j.heartbeat_at, j.leased_until, j.recovery_note"#,
        )
        .bind(&started)
        .bind(worker_id)
        .bind(&hb)
        .bind(&lu)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn renew_scan_job_lease(
        &self,
        job_id: JobId,
        worker_id: &str,
        heartbeat_at: DateTime<Utc>,
        leased_until: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let res = sqlx::query(
            r#"UPDATE scan_jobs SET heartbeat_at = $1, leased_until = $2
               WHERE job_id = $3 AND worker_id = $4 AND status = 'running'"#,
        )
        .bind(heartbeat_at.to_rfc3339())
        .bind(leased_until.to_rfc3339())
        .bind(job_id.0.to_string())
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn recover_stale_scan_jobs(
        &self,
        now: DateTime<Utc>,
        policy: JobRecoveryPolicy,
    ) -> Result<StaleRecoveryStats, StoreError> {
        let now_s = now.to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let rows: Vec<(String, i64)> = sqlx::query_as(
            r#"SELECT job_id, attempt_count FROM scan_jobs
               WHERE status = 'running'
                 AND leased_until IS NOT NULL
                 AND leased_until < $1"#,
        )
        .bind(&now_s)
        .fetch_all(&mut *tx)
        .await?;

        let mut stats = StaleRecoveryStats::default();
        let max = policy.max_attempts as i64;

        for (job_id, attempt_count) in rows {
            if attempt_count >= max {
                sqlx::query(
                    r#"UPDATE scan_jobs SET status = 'failed', completed_at = $1, failure_code = $2, failure_message = $3,
                       worker_id = NULL, heartbeat_at = NULL, leased_until = NULL,
                       recovery_note = $4
                       WHERE job_id = $5 AND status = 'running'"#,
                )
                .bind(now.to_rfc3339())
                .bind("job_retries_exhausted")
                .bind(
                    "maximum job execution attempts reached after worker loss or lease expiry",
                )
                .bind("retries_exhausted")
                .bind(&job_id)
                .execute(&mut *tx)
                .await?;
                stats.failed_retries_exhausted += 1;
            } else {
                sqlx::query(
                    r#"UPDATE scan_jobs SET status = 'queued',
                       started_at = NULL, worker_id = NULL, heartbeat_at = NULL, leased_until = NULL,
                       recovery_note = $1
                       WHERE job_id = $2 AND status = 'running'"#,
                )
                .bind(
                    "requeued after lease expired without completion (worker lost or process died)",
                )
                .bind(&job_id)
                .execute(&mut *tx)
                .await?;
                stats.requeued += 1;
            }
        }
        tx.commit().await?;
        Ok(stats)
    }

    pub async fn complete_scan_job_success(
        &self,
        job_id: JobId,
        run_id: RunId,
    ) -> Result<(), StoreError> {
        let completed = Utc::now().to_rfc3339();
        let res = sqlx::query(
            r#"UPDATE scan_jobs SET status = 'succeeded', completed_at = $1, run_id = $2,
               worker_id = NULL, heartbeat_at = NULL, leased_until = NULL
               WHERE job_id = $3 AND status = 'running'"#,
        )
        .bind(&completed)
        .bind(run_id.0.to_string())
        .bind(job_id.0.to_string())
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(StoreError::ReportValidation(
                "scan job not running or missing when completing success".into(),
            ));
        }
        Ok(())
    }

    pub async fn complete_scan_job_failure(
        &self,
        job_id: JobId,
        failure_code: &str,
        failure_message: &str,
    ) -> Result<(), StoreError> {
        let completed = Utc::now().to_rfc3339();
        let res = sqlx::query(
            r#"UPDATE scan_jobs SET status = 'failed', completed_at = $1, failure_code = $2, failure_message = $3,
               worker_id = NULL, heartbeat_at = NULL, leased_until = NULL
               WHERE job_id = $4 AND status = 'running'"#,
        )
        .bind(&completed)
        .bind(failure_code)
        .bind(failure_message)
        .bind(job_id.0.to_string())
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(StoreError::ReportValidation(
                "scan job not running or missing when completing failure".into(),
            ));
        }
        Ok(())
    }

    pub async fn get_scan_job(&self, job_id: JobId) -> Result<ScanJobRow, StoreError> {
        sqlx::query_as::<_, ScanJobRow>(
            r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                      target_kind, normalized_target_key, request_json,
                      failure_code, failure_message, run_id,
                      worker_id, attempt_count, heartbeat_at, leased_until, recovery_note
               FROM scan_jobs WHERE job_id = $1"#,
        )
        .bind(job_id.0.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::JobNotFound(job_id.0))
    }

    pub async fn count_scan_jobs(&self, status: Option<&str>) -> Result<u64, StoreError> {
        let n: i64 = if let Some(st) = status {
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_jobs WHERE status = $1")
                .bind(st)
                .fetch_one(&self.pool)
                .await?
        } else {
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_jobs").fetch_one(&self.pool).await?
        };
        Ok(n as u64)
    }

    pub async fn list_scan_jobs_page(
        &self,
        status: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ScanJobRow>, StoreError> {
        let rows = if let Some(st) = status {
            sqlx::query_as::<_, ScanJobRow>(
                r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                          target_kind, normalized_target_key, request_json,
                          failure_code, failure_message, run_id,
                          worker_id, attempt_count, heartbeat_at, leased_until, recovery_note
                   FROM scan_jobs WHERE status = $1
                   ORDER BY submitted_at DESC LIMIT $2 OFFSET $3"#,
            )
            .bind(st)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, ScanJobRow>(
                r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                          target_kind, normalized_target_key, request_json,
                          failure_code, failure_message, run_id,
                          worker_id, attempt_count, heartbeat_at, leased_until, recovery_note
                   FROM scan_jobs
                   ORDER BY submitted_at DESC LIMIT $1 OFFSET $2"#,
            )
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

fn run_outcome_db(o: RunOutcome) -> &'static str {
    match o {
        RunOutcome::Completed => "completed",
        RunOutcome::CompletedPartial => "completed_partial",
    }
}

fn parse_run_outcome(s: &str) -> Result<RunOutcome, StoreError> {
    match s {
        "completed" => Ok(RunOutcome::Completed),
        "completed_partial" => Ok(RunOutcome::CompletedPartial),
        _ => Err(StoreError::ReportValidation(format!("unknown run_outcome {s}"))),
    }
}

fn inspection_status_db(s: paraclete_types::InspectionStatus) -> &'static str {
    match s {
        paraclete_types::InspectionStatus::Inspected => "inspected",
        paraclete_types::InspectionStatus::Failed => "failed",
        paraclete_types::InspectionStatus::Skipped => "skipped",
    }
}

fn failure_kind_db(k: paraclete_types::FailureKind) -> String {
    serde_json::to_value(k)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "internal_engine_error".into())
}

fn stored_asset_row_from_pg(r: &PgRow) -> Result<StoredAssetRow, StoreError> {
    let path_str: String = r.try_get::<&str, _>(0usize)?.to_string();
    let path = Utf8PathBuf::from(path_str);
    let format = parse_data_format(r.try_get::<&str, _>(1usize)?)
        .ok_or_else(|| StoreError::ReportValidation("unknown asset format label".into()))?;
    let inspection_status = parse_inspection_status_pg(r.try_get::<&str, _>(2usize)?)?;
    let fk_raw: Option<String> = r.try_get(3usize)?;
    let failure_kind =
        fk_raw.and_then(|s| serde_json::from_str::<FailureKind>(&format!("\"{s}\"")).ok());
    Ok(StoredAssetRow {
        path,
        format,
        inspection_status,
        failure_kind,
        failure_message: r.try_get(4usize)?,
        dataset_id: r.try_get(5usize)?,
    })
}

fn stored_finding_row_from_pg(r: &PgRow) -> Result<StoredFindingRow, StoreError> {
    Ok(StoredFindingRow {
        fingerprint: r.try_get::<&str, _>(0usize)?.to_string(),
        code: r.try_get::<&str, _>(1usize)?.to_string(),
        severity: r.try_get::<&str, _>(2usize)?.to_string(),
        category: r.try_get::<&str, _>(3usize)?.to_string(),
        asset_path: r.try_get(4usize)?,
        dataset_id: r.try_get(5usize)?,
    })
}

fn parse_inspection_status_pg(s: &str) -> Result<InspectionStatus, StoreError> {
    match s {
        "inspected" => Ok(InspectionStatus::Inspected),
        "failed" => Ok(InspectionStatus::Failed),
        "skipped" => Ok(InspectionStatus::Skipped),
        _ => Err(StoreError::ReportValidation(format!("unknown inspection_status {s}"))),
    }
}
