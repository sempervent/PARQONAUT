//! SQLite-backed scan history (runs, canonical report blobs, indexed projections).

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use hex::encode as hex_encode;
use paraclete_types::{
    apply_redaction_policy, validate_report, FailureKind, InspectionStatus, JobId,
    JobRecoveryPolicy, RedactionPolicy, RunId, RunOutcome, ScanReport, ScanRun, ScanRunListItem,
    ScanSummary, TargetIdentity,
};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::error::StoreError;
use crate::labels::{
    category_label, data_format_label, grouping_kind_label, parse_data_format,
    partition_layout_label, severity_label,
};
use crate::sqlite_config::SqliteStoreConfig;
use crate::sqlite_connection::connect_options;

pub use crate::models::{
    RunPublicMeta, ScanJobRow, StaleRecoveryStats, StoredAssetRow, StoredFindingRow,
};

/// SQLite persistence for scan runs.
#[derive(Debug, Clone)]
pub struct SqliteScanStore {
    pool: SqlitePool,
}

impl SqliteScanStore {
    /// Opens or creates a SQLite database with hardened defaults ([`SqliteStoreConfig::default`]) and applies embedded migrations.
    pub async fn connect(database_url: impl AsRef<str>) -> Result<Self, StoreError> {
        Self::connect_with_config(database_url.as_ref(), &SqliteStoreConfig::default()).await
    }

    /// Same as [`Self::connect`], with explicit [`SqliteStoreConfig`] (journal mode, synchronous mode, busy timeout, pool size).
    pub async fn connect_with_config(
        database_url: impl AsRef<str>,
        config: &SqliteStoreConfig,
    ) -> Result<Self, StoreError> {
        let opts = connect_options(database_url.as_ref(), config)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(opts)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
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

        sqlx::query("INSERT INTO scan_reports (run_id, report_json) VALUES (?, ?)")
            .bind(run_id.0.to_string())
            .bind(&json)
            .execute(&mut *tx)
            .await?;

        for a in &stored.assets {
            sqlx::query(
                r#"INSERT INTO scan_assets
                (run_id, path, format, inspection_status, failure_kind, failure_message, dataset_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)"#,
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
                VALUES (?, ?, ?, ?, ?)"#,
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
                VALUES (?, ?, ?, ?, ?, ?, ?)"#,
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

    /// Loads the denormalized [`ScanSummary`] written at persist time (no full report blob I/O).
    pub async fn load_stored_scan_summary(&self, run_id: RunId) -> Result<ScanSummary, StoreError> {
        let s: String = sqlx::query_scalar("SELECT summary_json FROM scan_runs WHERE run_id = ?")
            .bind(run_id.0.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::RunNotFound(run_id.0))?;
        match serde_json::from_str::<ScanSummary>(&s) {
            Ok(sum) => Ok(sum),
            Err(_) => Ok(self.load_report(run_id).await?.summary),
        }
    }

    /// Counts assets for a run, optionally filtered by `inspection_status` label (`inspected` / `failed` / `skipped`).
    pub async fn count_assets(
        &self,
        run_id: RunId,
        inspection_status: Option<&str>,
    ) -> Result<u64, StoreError> {
        self.ensure_run_exists(run_id).await?;
        let total: i64 = if let Some(st) = inspection_status {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM scan_assets WHERE run_id = ? AND inspection_status = ?",
            )
            .bind(run_id.0.to_string())
            .bind(st)
            .fetch_one(&self.pool)
            .await?
        } else {
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_assets WHERE run_id = ?")
                .bind(run_id.0.to_string())
                .fetch_one(&self.pool)
                .await?
        };
        Ok(total as u64)
    }

    /// Paginated assets ordered deterministically by `path` ascending.
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
                   FROM scan_assets WHERE run_id = ? AND inspection_status = ?
                   ORDER BY path ASC LIMIT ? OFFSET ?"#,
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
                   FROM scan_assets WHERE run_id = ?
                   ORDER BY path ASC LIMIT ? OFFSET ?"#,
            )
            .bind(run_id.0.to_string())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(stored_asset_row_from_sql(r)?);
        }
        Ok(out)
    }

    /// Counts findings for a run with optional `severity` / `code` filters (exact match).
    pub async fn count_findings(
        &self,
        run_id: RunId,
        severity: Option<&str>,
        code: Option<&str>,
    ) -> Result<u64, StoreError> {
        self.ensure_run_exists(run_id).await?;
        let total: i64 = match (severity, code) {
            (Some(sev), Some(cd)) => sqlx::query_scalar(
                "SELECT COUNT(*) FROM scan_findings WHERE run_id = ? AND severity = ? AND code = ?",
            )
            .bind(run_id.0.to_string())
            .bind(sev)
            .bind(cd)
            .fetch_one(&self.pool)
            .await?,
            (Some(sev), None) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM scan_findings WHERE run_id = ? AND severity = ?",
                )
                .bind(run_id.0.to_string())
                .bind(sev)
                .fetch_one(&self.pool)
                .await?
            }
            (None, Some(cd)) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM scan_findings WHERE run_id = ? AND code = ?",
                )
                .bind(run_id.0.to_string())
                .bind(cd)
                .fetch_one(&self.pool)
                .await?
            }
            (None, None) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM scan_findings WHERE run_id = ?")
                    .bind(run_id.0.to_string())
                    .fetch_one(&self.pool)
                    .await?
            }
        };
        Ok(total as u64)
    }

    /// Paginated findings ordered by `fingerprint` ascending.
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
                       FROM scan_findings WHERE run_id = ? AND severity = ? AND code = ?
                       ORDER BY fingerprint ASC LIMIT ? OFFSET ?"#,
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
                       FROM scan_findings WHERE run_id = ? AND severity = ?
                       ORDER BY fingerprint ASC LIMIT ? OFFSET ?"#,
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
                       FROM scan_findings WHERE run_id = ? AND code = ?
                       ORDER BY fingerprint ASC LIMIT ? OFFSET ?"#,
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
                       FROM scan_findings WHERE run_id = ?
                       ORDER BY fingerprint ASC LIMIT ? OFFSET ?"#,
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
            out.push(stored_finding_row_from_sql(r)?);
        }
        Ok(out)
    }

    async fn ensure_run_exists(&self, run_id: RunId) -> Result<(), StoreError> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_runs WHERE run_id = ?")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        if n == 0 {
            return Err(StoreError::RunNotFound(run_id.0));
        }
        Ok(())
    }

    /// Loads the canonical JSON report for a run.
    pub async fn load_report(&self, run_id: RunId) -> Result<ScanReport, StoreError> {
        let row: String =
            sqlx::query_scalar("SELECT report_json FROM scan_reports WHERE run_id = ?")
                .bind(run_id.0.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or(StoreError::RunNotFound(run_id.0))?;
        Ok(serde_json::from_str(&row)?)
    }

    /// Lists runs for the same normalized target identity (most recent first).
    pub async fn list_runs_for_target(
        &self,
        identity: &TargetIdentity,
        limit: i64,
    ) -> Result<Vec<ScanRunListItem>, StoreError> {
        let rows = sqlx::query(
            r#"SELECT run_id, request_scan_id, completed_at, run_outcome, target_kind, normalized_target_key
               FROM scan_runs
               WHERE target_kind = ? AND normalized_target_key = ?
               ORDER BY completed_at DESC
               LIMIT ?"#,
        )
        .bind(&identity.target_kind)
        .bind(&identity.normalized_key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::new();
        for r in rows {
            let run_id = RunId(
                Uuid::parse_str(r.get::<&str, _>(0))
                    .map_err(|_| StoreError::ReportValidation("invalid run_id".into()))?,
            );
            let request_scan_id = Uuid::parse_str(r.get::<&str, _>(1))
                .map_err(|_| StoreError::ReportValidation("invalid request_scan_id".into()))?;
            let completed_at = DateTime::parse_from_rfc3339(r.get::<&str, _>(2))
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| StoreError::ReportValidation("invalid completed_at".into()))?;
            let outcome = parse_run_outcome(r.get::<&str, _>(3))?;
            let tid = TargetIdentity {
                target_kind: r.get::<String, _>(4),
                normalized_key: r.get::<String, _>(5),
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

    /// Returns stored run header metadata (without loading the full JSON blob).
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

    /// Header + denormalized summary + blob digest (for HTTP run detail without `report_json`).
    pub async fn get_run_public_meta(&self, run_id: RunId) -> Result<RunPublicMeta, StoreError> {
        let r = sqlx::query(
            r#"SELECT run_id, request_scan_id, target_kind, normalized_target_key, target_json,
                      started_at, completed_at, run_outcome, engine_revision,
                      contract_schema_version, report_format_version, report_sha256, summary_json
               FROM scan_runs WHERE run_id = ?"#,
        )
        .bind(run_id.0.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::RunNotFound(run_id.0))?;

        let target_json: serde_json::Value = serde_json::from_str(r.get::<&str, _>(4))
            .map_err(|_| StoreError::ReportValidation("invalid target_json".into()))?;
        let summary_raw: String = r.get::<String, _>(12);
        let summary = match serde_json::from_str::<ScanSummary>(&summary_raw) {
            Ok(s) => s,
            Err(_) => self.load_report(run_id).await?.summary,
        };
        Ok(RunPublicMeta {
            run_id: RunId(
                Uuid::parse_str(r.get::<&str, _>(0))
                    .map_err(|_| StoreError::ReportValidation("invalid run_id".into()))?,
            ),
            request_scan_id: Uuid::parse_str(r.get::<&str, _>(1))
                .map_err(|_| StoreError::ReportValidation("invalid request_scan_id".into()))?,
            target_identity: TargetIdentity {
                target_kind: r.get::<String, _>(2),
                normalized_key: r.get::<String, _>(3),
            },
            target_json,
            started_at: DateTime::parse_from_rfc3339(r.get::<&str, _>(5))
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| StoreError::ReportValidation("invalid started_at".into()))?,
            completed_at: DateTime::parse_from_rfc3339(r.get::<&str, _>(6))
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| StoreError::ReportValidation("invalid completed_at".into()))?,
            run_outcome: parse_run_outcome(r.get::<&str, _>(7))?,
            engine_revision: r.get::<Option<String>, _>(8),
            contract_schema_version: r.get::<String, _>(9),
            report_format_version: r.get::<String, _>(10),
            report_sha256: r.get::<String, _>(11),
            summary,
        })
    }

    /// Verifies projection row counts match the deserialized report (integrity check).
    pub async fn verify_projection_integrity(&self, run_id: RunId) -> Result<(), StoreError> {
        let report = self.load_report(run_id).await?;
        let ac: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_assets WHERE run_id = ?")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        let dc: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_datasets WHERE run_id = ?")
            .bind(run_id.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        let fc: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scan_findings WHERE run_id = ?")
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

    /// Inserts a queued scan job (HTTP submission path).
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
            ) VALUES (?, ?, 'queued', ?, ?, ?, 0)"#,
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

    /// Atomically picks the oldest queued job, marks it `running`, and assigns lease metadata.
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
            r#"UPDATE scan_jobs
               SET status = 'running',
                   started_at = ?,
                   worker_id = ?,
                   heartbeat_at = ?,
                   leased_until = ?,
                   attempt_count = attempt_count + 1
               WHERE job_id = (
                 SELECT job_id FROM scan_jobs
                 WHERE status = 'queued'
                 ORDER BY submitted_at ASC
                 LIMIT 1
               )
               RETURNING job_id, submitted_at, started_at, completed_at, status,
                         target_kind, normalized_target_key, request_json,
                         failure_code, failure_message, run_id,
                         worker_id, attempt_count, heartbeat_at, leased_until, recovery_note"#,
        )
        .bind(&started)
        .bind(worker_id)
        .bind(&hb)
        .bind(&lu)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Renews heartbeat + lease while a worker holds a running job.
    pub async fn renew_scan_job_lease(
        &self,
        job_id: JobId,
        worker_id: &str,
        heartbeat_at: DateTime<Utc>,
        leased_until: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let res = sqlx::query(
            r#"UPDATE scan_jobs SET heartbeat_at = ?, leased_until = ?
               WHERE job_id = ? AND worker_id = ? AND status = 'running'"#,
        )
        .bind(heartbeat_at.to_rfc3339())
        .bind(leased_until.to_rfc3339())
        .bind(job_id.0.to_string())
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Requeues `running` jobs whose lease expired, or fails them with `job_retries_exhausted`.
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
                 AND leased_until < ?"#,
        )
        .bind(&now_s)
        .fetch_all(&mut *tx)
        .await?;

        let mut stats = StaleRecoveryStats::default();
        let max = policy.max_attempts as i64;

        for (job_id, attempt_count) in rows {
            if attempt_count >= max {
                sqlx::query(
                    r#"UPDATE scan_jobs SET status = 'failed', completed_at = ?, failure_code = ?, failure_message = ?,
                       worker_id = NULL, heartbeat_at = NULL, leased_until = NULL,
                       recovery_note = ?
                       WHERE job_id = ? AND status = 'running'"#,
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
                       recovery_note = ?
                       WHERE job_id = ? AND status = 'running'"#,
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
            r#"UPDATE scan_jobs SET status = 'succeeded', completed_at = ?, run_id = ?,
               worker_id = NULL, heartbeat_at = NULL, leased_until = NULL
               WHERE job_id = ? AND status = 'running'"#,
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
            r#"UPDATE scan_jobs SET status = 'failed', completed_at = ?, failure_code = ?, failure_message = ?,
               worker_id = NULL, heartbeat_at = NULL, leased_until = NULL
               WHERE job_id = ? AND status = 'running'"#,
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
               FROM scan_jobs WHERE job_id = ?"#,
        )
        .bind(job_id.0.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::JobNotFound(job_id.0))
    }

    pub async fn count_scan_jobs(&self, status: Option<&str>) -> Result<u64, StoreError> {
        let n: i64 = if let Some(st) = status {
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_jobs WHERE status = ?")
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
                   FROM scan_jobs WHERE status = ?
                   ORDER BY submitted_at DESC LIMIT ? OFFSET ?"#,
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
                   ORDER BY submitted_at DESC LIMIT ? OFFSET ?"#,
            )
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    /// Access to the underlying pool (integration tests, diagnostics).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

fn stored_asset_row_from_sql(r: SqliteRow) -> Result<StoredAssetRow, StoreError> {
    let path_str: String = r.get::<&str, _>(0).to_string();
    let path = Utf8PathBuf::from(path_str);
    let format = parse_data_format(r.get::<&str, _>(1))
        .ok_or_else(|| StoreError::ReportValidation("unknown asset format label".into()))?;
    let inspection_status = parse_inspection_status(r.get::<&str, _>(2))?;
    let failure_kind = r
        .get::<Option<String>, _>(3)
        .and_then(|s| serde_json::from_str::<FailureKind>(&format!("\"{s}\"")).ok());
    Ok(StoredAssetRow {
        path,
        format,
        inspection_status,
        failure_kind,
        failure_message: r.get(4),
        dataset_id: r.get(5),
    })
}

fn stored_finding_row_from_sql(r: SqliteRow) -> Result<StoredFindingRow, StoreError> {
    Ok(StoredFindingRow {
        fingerprint: r.get::<&str, _>(0).to_string(),
        code: r.get::<&str, _>(1).to_string(),
        severity: r.get::<&str, _>(2).to_string(),
        category: r.get::<&str, _>(3).to_string(),
        asset_path: r.get(4),
        dataset_id: r.get(5),
    })
}

fn parse_inspection_status(s: &str) -> Result<InspectionStatus, StoreError> {
    match s {
        "inspected" => Ok(InspectionStatus::Inspected),
        "failed" => Ok(InspectionStatus::Failed),
        "skipped" => Ok(InspectionStatus::Skipped),
        _ => Err(StoreError::ReportValidation(format!("unknown inspection_status {s}"))),
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

fn inspection_status_db(s: InspectionStatus) -> &'static str {
    match s {
        InspectionStatus::Inspected => "inspected",
        InspectionStatus::Failed => "failed",
        InspectionStatus::Skipped => "skipped",
    }
}

fn failure_kind_db(k: FailureKind) -> String {
    serde_json::to_value(k)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "internal_engine_error".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use paraclete_types::{JobId, JobRecoveryPolicy};
    use uuid::Uuid;

    #[tokio::test]
    async fn migration_creates_scan_tables() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.sqlite");
        std::fs::File::create(&p).unwrap();
        let abs = p.canonicalize().unwrap();
        let url = format!("sqlite://{}", abs.display());
        let store = SqliteScanStore::connect(&url).await.unwrap();
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'scan_%'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(n, 6);
    }

    #[tokio::test]
    async fn scan_job_failure_without_run() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.sqlite");
        std::fs::File::create(&p).unwrap();
        let abs = p.canonicalize().unwrap();
        let url = format!("sqlite://{}", abs.display());
        let store = SqliteScanStore::connect(&url).await.unwrap();

        let jid = JobId::new();
        let identity = TargetIdentity {
            target_kind: "local_file".into(),
            normalized_key: "/tmp/x.parquet".into(),
        };
        store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();

        let now = Utc::now();
        let row = store
            .claim_next_queued_scan_job("test-worker", now, now + chrono::Duration::seconds(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "running");
        assert_eq!(row.attempt_count, 1);
        assert_eq!(row.worker_id.as_deref(), Some("test-worker"));
        assert!(row.started_at.is_some());
        let claimed = JobId(Uuid::parse_str(&row.job_id).unwrap());
        assert_eq!(claimed.0, jid.0);

        store.complete_scan_job_failure(claimed, "scan_failed", "boom").await.unwrap();
        let got = store.get_scan_job(jid).await.unwrap();
        assert_eq!(got.status, "failed");
        assert_eq!(got.failure_code.as_deref(), Some("scan_failed"));
    }

    #[tokio::test]
    async fn scan_job_success_links_existing_run() {
        use paraclete_types::ScanRequest;

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.sqlite");
        std::fs::File::create(&p).unwrap();
        let abs = p.canonicalize().unwrap();
        let url = format!("sqlite://{}", abs.display());
        let store = SqliteScanStore::connect(&url).await.unwrap();

        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/phase1/single_parquet/data.parquet");
        let path = Utf8PathBuf::from_path_buf(fixture).unwrap();
        let target = paraclete_types::ScanTarget::LocalFile { path };
        let scan_req = ScanRequest::new(target, paraclete_types::ScanProfile::Standard);
        let report = paraclete_core::ScanEngine::run(&scan_req).unwrap();
        let run_id = RunId::new();
        store
            .persist_scan_run(
                run_id,
                Utc::now(),
                Utc::now(),
                &report,
                &RedactionPolicy::transport_safe_persist(),
            )
            .await
            .unwrap();

        let jid = JobId::new();
        let identity = TargetIdentity::from_scan_target(&scan_req.target);
        store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();
        let now = Utc::now();
        store
            .claim_next_queued_scan_job("test-worker", now, now + chrono::Duration::seconds(60))
            .await
            .unwrap()
            .unwrap();
        store.complete_scan_job_success(jid, run_id).await.unwrap();

        let got = store.get_scan_job(jid).await.unwrap();
        assert_eq!(got.status, "succeeded");
        assert_eq!(got.run_id.as_deref(), Some(run_id.0.to_string().as_str()));
    }

    #[tokio::test]
    async fn renew_scan_job_lease_updates_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.sqlite");
        std::fs::File::create(&p).unwrap();
        let abs = p.canonicalize().unwrap();
        let url = format!("sqlite://{}", abs.display());
        let store = SqliteScanStore::connect(&url).await.unwrap();

        let jid = JobId::new();
        let identity = TargetIdentity {
            target_kind: "local_file".into(),
            normalized_key: "/tmp/x.parquet".into(),
        };
        store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();
        let t0 = Utc::now();
        store
            .claim_next_queued_scan_job("w-renew", t0, t0 + Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();

        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(45);
        assert!(store.renew_scan_job_lease(jid, "w-renew", t1, t2).await.unwrap());

        let got = store.get_scan_job(jid).await.unwrap();
        let hb_expected = t1.to_rfc3339();
        let lu_expected = t2.to_rfc3339();
        assert_eq!(got.heartbeat_at.as_deref(), Some(hb_expected.as_str()));
        assert_eq!(got.leased_until.as_deref(), Some(lu_expected.as_str()));
    }

    #[tokio::test]
    async fn recover_stale_requeues_running_job() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.sqlite");
        std::fs::File::create(&p).unwrap();
        let abs = p.canonicalize().unwrap();
        let url = format!("sqlite://{}", abs.display());
        let store = SqliteScanStore::connect(&url).await.unwrap();

        let jid = JobId::new();
        let identity = TargetIdentity {
            target_kind: "local_file".into(),
            normalized_key: "/tmp/x.parquet".into(),
        };
        store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();
        let now = Utc::now();
        store
            .claim_next_queued_scan_job("w", now, now + Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();

        sqlx::query("UPDATE scan_jobs SET leased_until = ? WHERE job_id = ?")
            .bind("1999-01-01T00:00:00Z")
            .bind(jid.0.to_string())
            .execute(store.pool())
            .await
            .unwrap();

        let stats =
            store.recover_stale_scan_jobs(Utc::now(), JobRecoveryPolicy::default()).await.unwrap();
        assert_eq!(stats.requeued, 1);
        assert_eq!(stats.failed_retries_exhausted, 0);

        let got = store.get_scan_job(jid).await.unwrap();
        assert_eq!(got.status, "queued");
        assert!(got.recovery_note.is_some());
        assert_eq!(got.attempt_count, 1);
    }

    #[tokio::test]
    async fn recover_stale_fails_when_max_attempts_reached() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.sqlite");
        std::fs::File::create(&p).unwrap();
        let abs = p.canonicalize().unwrap();
        let url = format!("sqlite://{}", abs.display());
        let store = SqliteScanStore::connect(&url).await.unwrap();

        let policy = JobRecoveryPolicy {
            lease_duration_secs: 30,
            heartbeat_interval_secs: 10,
            max_attempts: 2,
        };

        let jid = JobId::new();
        let identity = TargetIdentity {
            target_kind: "local_file".into(),
            normalized_key: "/tmp/x.parquet".into(),
        };
        store.insert_scan_job_queued(jid, &identity, "{}").await.unwrap();

        let now = Utc::now();
        store
            .claim_next_queued_scan_job("w", now, now + Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE scan_jobs SET leased_until = ? WHERE job_id = ?")
            .bind("1999-01-01T00:00:00Z")
            .bind(jid.0.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        store.recover_stale_scan_jobs(Utc::now(), policy).await.unwrap();

        let now = Utc::now();
        store
            .claim_next_queued_scan_job("w", now, now + Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE scan_jobs SET leased_until = ? WHERE job_id = ?")
            .bind("1999-01-01T00:00:00Z")
            .bind(jid.0.to_string())
            .execute(store.pool())
            .await
            .unwrap();

        let stats = store.recover_stale_scan_jobs(Utc::now(), policy).await.unwrap();
        assert_eq!(stats.requeued, 0);
        assert_eq!(stats.failed_retries_exhausted, 1);

        let got = store.get_scan_job(jid).await.unwrap();
        assert_eq!(got.status, "failed");
        assert_eq!(got.failure_code.as_deref(), Some("job_retries_exhausted"));
    }
}
