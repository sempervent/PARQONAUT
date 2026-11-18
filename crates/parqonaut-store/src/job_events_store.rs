//! Durable job event journal, idempotency keys, filtered job queries, and run listing.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use parqonaut_types::{
    JobEvent, JobId, JobKind, JobStage, RunId, RunOutcome, ScanRunListItem, TargetIdentity,
    JOB_EVENT_SCHEMA_VERSION,
};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use sqlx::{Postgres, Sqlite, Transaction};
use uuid::Uuid;

use crate::error::StoreError;
use crate::models::{AppendJobEvent, IdempotencyInsertResult, ScanJobRow};
use crate::postgres_store::PostgresScanStore;
use crate::sqlite_store::SqliteScanStore;

#[derive(Debug, sqlx::FromRow)]
struct JobEventSqlRow {
    event_id: String,
    job_id: String,
    seq: i64,
    schema_version: i32,
    occurred_at: String,
    kind: String,
    stage: String,
    is_terminal: i32,
    metrics_json: String,
    message: Option<String>,
    run_id: Option<String>,
    plugin_name: Option<String>,
    plugin_digest: Option<String>,
}

impl JobEventSqlRow {
    fn into_event(self) -> Result<JobEvent, StoreError> {
        let event_id = Uuid::parse_str(&self.event_id)
            .map_err(|_| StoreError::ReportValidation("invalid event_id".into()))?;
        let job_id = JobId(
            Uuid::parse_str(&self.job_id)
                .map_err(|_| StoreError::ReportValidation("invalid job_id".into()))?,
        );
        let occurred_at = DateTime::parse_from_rfc3339(&self.occurred_at)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|_| StoreError::ReportValidation("invalid occurred_at".into()))?;
        let kind = ProgressEventKind::parse(&self.kind).ok_or_else(|| {
            StoreError::ReportValidation(format!("unknown event kind `{}`", self.kind))
        })?;
        let stage = JobStage::parse(&self.stage).ok_or_else(|| {
            StoreError::ReportValidation(format!("unknown event stage `{}`", self.stage))
        })?;
        let metrics: ProgressMetrics = serde_json::from_str(&self.metrics_json)?;
        let run_id = self
            .run_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|_| StoreError::ReportValidation("invalid event run_id".into()))?;
        Ok(JobEvent {
            event_id,
            job_id,
            seq: self.seq,
            schema_version: self.schema_version as u32,
            occurred_at,
            kind,
            stage,
            is_terminal: self.is_terminal != 0,
            metrics,
            message: self.message,
            run_id,
            plugin_name: self.plugin_name,
            plugin_digest: self.plugin_digest,
        })
    }
}

pub(crate) async fn append_event_sqlite_tx(
    tx: &mut Transaction<'_, Sqlite>,
    input: &AppendJobEvent,
    max_events_per_job: usize,
) -> Result<JobEvent, StoreError> {
    let job_id_str = input.job_id.0.to_string();
    let seq: Option<i64> = sqlx::query_scalar(
        "UPDATE application_jobs SET last_event_seq = last_event_seq + 1 WHERE job_id = ? RETURNING last_event_seq",
    )
    .bind(&job_id_str)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(seq) = seq else {
        return Err(StoreError::JobNotFound(input.job_id.0));
    };

    let event_id = Uuid::new_v4();
    let occurred_at_str = input.occurred_at.to_rfc3339();
    let kind_str = input.kind.as_str();
    let stage_str = input.stage.as_str();
    let is_terminal = i32::from(input.is_terminal);
    let metrics_json = serde_json::to_string(&input.metrics)?;
    let run_id_str = input.run_id.map(|r| r.to_string());

    sqlx::query(
        r#"INSERT INTO application_job_events (
            event_id, job_id, seq, schema_version, occurred_at, kind, stage, is_terminal,
            metrics_json, message, run_id, plugin_name, plugin_digest
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
    )
    .bind(event_id.to_string())
    .bind(&job_id_str)
    .bind(seq)
    .bind(JOB_EVENT_SCHEMA_VERSION as i32)
    .bind(&occurred_at_str)
    .bind(kind_str)
    .bind(stage_str)
    .bind(is_terminal)
    .bind(&metrics_json)
    .bind(&input.message)
    .bind(&run_id_str)
    .bind(&input.plugin_name)
    .bind(&input.plugin_digest)
    .execute(&mut **tx)
    .await?;

    if max_events_per_job > 0 {
        sqlx::query(
            r#"DELETE FROM application_job_events
               WHERE job_id = ? AND is_terminal = 0
                 AND kind NOT IN ('job_queued', 'job_claimed', 'job_recovered', 'cancel_requested', 'job_cancelled')
                 AND seq NOT IN (
                   SELECT seq FROM application_job_events
                   WHERE job_id = ? AND is_terminal = 0
                     AND kind NOT IN ('job_queued', 'job_claimed', 'job_recovered', 'cancel_requested', 'job_cancelled')
                   ORDER BY seq DESC
                   LIMIT ?
                 )"#,
        )
        .bind(&job_id_str)
        .bind(&job_id_str)
        .bind(max_events_per_job as i64)
        .execute(&mut **tx)
        .await?;
    }

    Ok(JobEvent {
        event_id,
        job_id: input.job_id,
        seq,
        schema_version: JOB_EVENT_SCHEMA_VERSION,
        occurred_at: input.occurred_at,
        kind: input.kind,
        stage: input.stage,
        is_terminal: input.is_terminal,
        metrics: input.metrics.clone(),
        message: input.message.clone(),
        run_id: input.run_id,
        plugin_name: input.plugin_name.clone(),
        plugin_digest: input.plugin_digest.clone(),
    })
}

pub(crate) async fn append_event_postgres_tx(
    tx: &mut Transaction<'_, Postgres>,
    input: &AppendJobEvent,
    max_events_per_job: usize,
) -> Result<JobEvent, StoreError> {
    let job_id_str = input.job_id.0.to_string();
    let seq: Option<i64> = sqlx::query_scalar(
        "UPDATE application_jobs SET last_event_seq = last_event_seq + 1 WHERE job_id = $1 RETURNING last_event_seq",
    )
    .bind(&job_id_str)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(seq) = seq else {
        return Err(StoreError::JobNotFound(input.job_id.0));
    };

    let event_id = Uuid::new_v4();
    let occurred_at_str = input.occurred_at.to_rfc3339();
    let kind_str = input.kind.as_str();
    let stage_str = input.stage.as_str();
    let is_terminal = i32::from(input.is_terminal);
    let metrics_json = serde_json::to_string(&input.metrics)?;
    let run_id_str = input.run_id.map(|r| r.to_string());

    sqlx::query(
        r#"INSERT INTO application_job_events (
            event_id, job_id, seq, schema_version, occurred_at, kind, stage, is_terminal,
            metrics_json, message, run_id, plugin_name, plugin_digest
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#,
    )
    .bind(event_id.to_string())
    .bind(&job_id_str)
    .bind(seq)
    .bind(JOB_EVENT_SCHEMA_VERSION as i32)
    .bind(&occurred_at_str)
    .bind(kind_str)
    .bind(stage_str)
    .bind(is_terminal)
    .bind(&metrics_json)
    .bind(&input.message)
    .bind(&run_id_str)
    .bind(&input.plugin_name)
    .bind(&input.plugin_digest)
    .execute(&mut **tx)
    .await?;

    if max_events_per_job > 0 {
        sqlx::query(
            r#"DELETE FROM application_job_events
               WHERE job_id = $1 AND is_terminal = 0
                 AND kind NOT IN ('job_queued', 'job_claimed', 'job_recovered', 'cancel_requested', 'job_cancelled')
                 AND seq NOT IN (
                   SELECT seq FROM application_job_events
                   WHERE job_id = $2 AND is_terminal = 0
                     AND kind NOT IN ('job_queued', 'job_claimed', 'job_recovered', 'cancel_requested', 'job_cancelled')
                   ORDER BY seq DESC
                   LIMIT $3
                 )"#,
        )
        .bind(&job_id_str)
        .bind(&job_id_str)
        .bind(max_events_per_job as i64)
        .execute(&mut **tx)
        .await?;
    }

    Ok(JobEvent {
        event_id,
        job_id: input.job_id,
        seq,
        schema_version: JOB_EVENT_SCHEMA_VERSION,
        occurred_at: input.occurred_at,
        kind: input.kind,
        stage: input.stage,
        is_terminal: input.is_terminal,
        metrics: input.metrics.clone(),
        message: input.message.clone(),
        run_id: input.run_id,
        plugin_name: input.plugin_name.clone(),
        plugin_digest: input.plugin_digest.clone(),
    })
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = err {
        if let Some(code) = db.code() {
            let c = code.as_ref();
            return c == "23505" || c == "1555" || c == "2067";
        }
    }
    false
}

impl SqliteScanStore {
    pub async fn append_job_event(
        &self,
        input: AppendJobEvent,
        max_events_per_job: usize,
    ) -> Result<JobEvent, StoreError> {
        let mut tx = self.pool().begin().await?;
        let ev = append_event_sqlite_tx(&mut tx, &input, max_events_per_job).await?;
        tx.commit().await?;
        Ok(ev)
    }

    pub async fn list_job_events(
        &self,
        job_id: JobId,
        after_seq: i64,
        limit: u64,
        kind: Option<&str>,
        stage: Option<&str>,
    ) -> Result<Vec<JobEvent>, StoreError> {
        let job_str = job_id.0.to_string();
        let exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM application_jobs WHERE job_id = ?")
                .bind(&job_str)
                .fetch_one(self.pool())
                .await?;
        if exists == 0 {
            return Err(StoreError::JobNotFound(job_id.0));
        }

        let rows = sqlx::query_as::<_, JobEventSqlRow>(
            r#"SELECT event_id, job_id, seq, schema_version, occurred_at, kind, stage, is_terminal,
                      metrics_json, message, run_id, plugin_name, plugin_digest
               FROM application_job_events
               WHERE job_id = ?
                 AND seq > ?
                 AND (? IS NULL OR kind = ?)
                 AND (? IS NULL OR stage = ?)
               ORDER BY seq ASC
               LIMIT ?"#,
        )
        .bind(&job_str)
        .bind(after_seq)
        .bind(kind)
        .bind(kind)
        .bind(stage)
        .bind(stage)
        .bind(limit as i64)
        .fetch_all(self.pool())
        .await?;

        rows.into_iter().map(JobEventSqlRow::into_event).collect()
    }

    pub async fn count_job_events(&self, job_id: JobId) -> Result<u64, StoreError> {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM application_job_events WHERE job_id = ?")
                .bind(job_id.0.to_string())
                .fetch_one(self.pool())
                .await?;
        Ok(n as u64)
    }

    pub async fn insert_application_job_with_idempotency(
        &self,
        job_id: JobId,
        kind: JobKind,
        identity: &TargetIdentity,
        request_json: &str,
        idempotency: Option<(&str, &str, &str, DateTime<Utc>)>,
    ) -> Result<IdempotencyInsertResult, StoreError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let mut tx = self.pool().begin().await?;

        if let Some((scope, key, req_sha, expires_at)) = idempotency {
            sqlx::query(
                "DELETE FROM idempotency_keys WHERE scope = ? AND idempotency_key = ? AND expires_at <= ?",
            )
            .bind(scope)
            .bind(key)
            .bind(&now_str)
            .execute(&mut *tx)
            .await?;

            if let Some((existing_sha, existing_job_id)) = sqlx::query_as::<_, (String, String)>(
                "SELECT request_sha256, job_id FROM idempotency_keys WHERE scope = ? AND idempotency_key = ?",
            )
            .bind(scope)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
            {
                if existing_sha != req_sha {
                    return Err(StoreError::IdempotencyConflict(
                        "idempotency key already used with a different request payload".into(),
                    ));
                }
                let row = sqlx::query_as::<_, ScanJobRow>(
                    r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                              target_kind, normalized_target_key, request_json,
                              failure_code, failure_message, run_id,
                              worker_id, attempt_count, heartbeat_at, leased_until, recovery_note,
                              job_kind, payload_schema_version, cancel_requested, result_ref_json
                       FROM application_jobs WHERE job_id = ?"#,
                )
                .bind(&existing_job_id)
                .fetch_one(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(IdempotencyInsertResult::Replayed { existing: Box::new(row) });
            }

            sqlx::query(
                r#"INSERT INTO application_jobs (
                    job_id, submitted_at, status, target_kind, normalized_target_key, request_json,
                    attempt_count, job_kind, payload_schema_version
                ) VALUES (?, ?, 'queued', ?, ?, ?, 0, ?, 1)"#,
            )
            .bind(job_id.0.to_string())
            .bind(&now_str)
            .bind(&identity.target_kind)
            .bind(&identity.normalized_key)
            .bind(request_json)
            .bind(kind.as_str())
            .execute(&mut *tx)
            .await?;

            let insert_res = sqlx::query(
                r#"INSERT INTO idempotency_keys (scope, idempotency_key, request_sha256, job_id, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?)"#,
            )
            .bind(scope)
            .bind(key)
            .bind(req_sha)
            .bind(job_id.0.to_string())
            .bind(&now_str)
            .bind(expires_at.to_rfc3339())
            .execute(&mut *tx)
            .await;

            match insert_res {
                Ok(_) => {
                    tx.commit().await?;
                    return Ok(IdempotencyInsertResult::Created { job_id, submitted_at: now });
                }
                Err(e) if is_unique_violation(&e) => {
                    tx.rollback().await?;
                    let (existing_sha, existing_job_id): (String, String) = sqlx::query_as(
                        "SELECT request_sha256, job_id FROM idempotency_keys WHERE scope = ? AND idempotency_key = ?",
                    )
                    .bind(scope)
                    .bind(key)
                    .fetch_one(self.pool())
                    .await?;
                    if existing_sha != req_sha {
                        return Err(StoreError::IdempotencyConflict(
                            "idempotency key already used with a different request payload".into(),
                        ));
                    }
                    let existing_uuid = Uuid::parse_str(&existing_job_id)
                        .map_err(|_| StoreError::ReportValidation("invalid job_id".into()))?;
                    let row = self.get_scan_job(JobId(existing_uuid)).await?;
                    return Ok(IdempotencyInsertResult::Replayed { existing: Box::new(row) });
                }
                Err(e) => return Err(StoreError::Sql(e)),
            }
        }

        sqlx::query(
            r#"INSERT INTO application_jobs (
                job_id, submitted_at, status, target_kind, normalized_target_key, request_json,
                attempt_count, job_kind, payload_schema_version
            ) VALUES (?, ?, 'queued', ?, ?, ?, 0, ?, 1)"#,
        )
        .bind(job_id.0.to_string())
        .bind(&now_str)
        .bind(&identity.target_kind)
        .bind(&identity.normalized_key)
        .bind(request_json)
        .bind(kind.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(IdempotencyInsertResult::Created { job_id, submitted_at: now })
    }

    pub async fn list_application_jobs_filtered(
        &self,
        kind: Option<&str>,
        status: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<ScanJobRow>, u64), StoreError> {
        let total: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM application_jobs
               WHERE (? IS NULL OR job_kind = ?)
                 AND (? IS NULL OR status = ?)"#,
        )
        .bind(kind)
        .bind(kind)
        .bind(status)
        .bind(status)
        .fetch_one(self.pool())
        .await?;

        let rows = sqlx::query_as::<_, ScanJobRow>(
            r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                      target_kind, normalized_target_key, request_json,
                      failure_code, failure_message, run_id,
                      worker_id, attempt_count, heartbeat_at, leased_until, recovery_note,
                      job_kind, payload_schema_version, cancel_requested, result_ref_json
               FROM application_jobs
               WHERE (? IS NULL OR job_kind = ?)
                 AND (? IS NULL OR status = ?)
               ORDER BY submitted_at DESC
               LIMIT ? OFFSET ?"#,
        )
        .bind(kind)
        .bind(kind)
        .bind(status)
        .bind(status)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.pool())
        .await?;

        Ok((rows, total as u64))
    }

    pub async fn job_status_counts(&self) -> Result<BTreeMap<String, u64>, StoreError> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT status, COUNT(*) FROM application_jobs GROUP BY status")
                .fetch_all(self.pool())
                .await?;
        let mut counts = BTreeMap::new();
        for s in ["queued", "running", "succeeded", "failed", "canceled"] {
            counts.insert(s.to_string(), 0);
        }
        for (status, n) in rows {
            counts.insert(status, n.max(0) as u64);
        }
        Ok(counts)
    }

    pub async fn list_scan_runs_page(
        &self,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<ScanRunListItem>, u64), StoreError> {
        let total: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_runs").fetch_one(self.pool()).await?;
        let rows: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
            r#"SELECT run_id, request_scan_id, completed_at, run_outcome, target_kind, normalized_target_key
               FROM scan_runs
               ORDER BY completed_at DESC
               LIMIT ? OFFSET ?"#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.pool())
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (rid, rsid, comp, outcome, tk, nk) in rows {
            out.push(parse_run_list_item(&rid, &rsid, &comp, &outcome, tk, nk)?);
        }
        Ok((out, total as u64))
    }
}

impl PostgresScanStore {
    pub async fn append_job_event(
        &self,
        input: AppendJobEvent,
        max_events_per_job: usize,
    ) -> Result<JobEvent, StoreError> {
        let mut tx = self.pool().begin().await?;
        let ev = append_event_postgres_tx(&mut tx, &input, max_events_per_job).await?;
        tx.commit().await?;
        Ok(ev)
    }

    pub async fn list_job_events(
        &self,
        job_id: JobId,
        after_seq: i64,
        limit: u64,
        kind: Option<&str>,
        stage: Option<&str>,
    ) -> Result<Vec<JobEvent>, StoreError> {
        let job_str = job_id.0.to_string();
        let exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM application_jobs WHERE job_id = $1")
                .bind(&job_str)
                .fetch_one(self.pool())
                .await?;
        if exists == 0 {
            return Err(StoreError::JobNotFound(job_id.0));
        }

        let rows = sqlx::query_as::<_, JobEventSqlRow>(
            r#"SELECT event_id, job_id, seq, schema_version, occurred_at, kind, stage, is_terminal,
                      metrics_json, message, run_id, plugin_name, plugin_digest
               FROM application_job_events
               WHERE job_id = $1
                 AND seq > $2
                 AND ($3::TEXT IS NULL OR kind = $3)
                 AND ($4::TEXT IS NULL OR stage = $4)
               ORDER BY seq ASC
               LIMIT $5"#,
        )
        .bind(&job_str)
        .bind(after_seq)
        .bind(kind)
        .bind(stage)
        .bind(limit as i64)
        .fetch_all(self.pool())
        .await?;

        rows.into_iter().map(JobEventSqlRow::into_event).collect()
    }

    pub async fn count_job_events(&self, job_id: JobId) -> Result<u64, StoreError> {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM application_job_events WHERE job_id = $1")
                .bind(job_id.0.to_string())
                .fetch_one(self.pool())
                .await?;
        Ok(n as u64)
    }

    pub async fn insert_application_job_with_idempotency(
        &self,
        job_id: JobId,
        kind: JobKind,
        identity: &TargetIdentity,
        request_json: &str,
        idempotency: Option<(&str, &str, &str, DateTime<Utc>)>,
    ) -> Result<IdempotencyInsertResult, StoreError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let mut tx = self.pool().begin().await?;

        if let Some((scope, key, req_sha, expires_at)) = idempotency {
            sqlx::query(
                "DELETE FROM idempotency_keys WHERE scope = $1 AND idempotency_key = $2 AND expires_at <= $3",
            )
            .bind(scope)
            .bind(key)
            .bind(&now_str)
            .execute(&mut *tx)
            .await?;

            if let Some((existing_sha, existing_job_id)) = sqlx::query_as::<_, (String, String)>(
                "SELECT request_sha256, job_id FROM idempotency_keys WHERE scope = $1 AND idempotency_key = $2",
            )
            .bind(scope)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
            {
                if existing_sha != req_sha {
                    return Err(StoreError::IdempotencyConflict(
                        "idempotency key already used with a different request payload".into(),
                    ));
                }
                let row = sqlx::query_as::<_, ScanJobRow>(
                    r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                              target_kind, normalized_target_key, request_json,
                              failure_code, failure_message, run_id,
                              worker_id, attempt_count, heartbeat_at, leased_until, recovery_note,
                              job_kind, payload_schema_version, cancel_requested, result_ref_json
                       FROM application_jobs WHERE job_id = $1"#,
                )
                .bind(&existing_job_id)
                .fetch_one(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(IdempotencyInsertResult::Replayed { existing: Box::new(row) });
            }

            sqlx::query(
                r#"INSERT INTO application_jobs (
                    job_id, submitted_at, status, target_kind, normalized_target_key, request_json,
                    attempt_count, job_kind, payload_schema_version
                ) VALUES ($1, $2, 'queued', $3, $4, $5, 0, $6, 1)"#,
            )
            .bind(job_id.0.to_string())
            .bind(&now_str)
            .bind(&identity.target_kind)
            .bind(&identity.normalized_key)
            .bind(request_json)
            .bind(kind.as_str())
            .execute(&mut *tx)
            .await?;

            let insert_res = sqlx::query(
                r#"INSERT INTO idempotency_keys (scope, idempotency_key, request_sha256, job_id, created_at, expires_at)
                   VALUES ($1, $2, $3, $4, $5, $6)"#,
            )
            .bind(scope)
            .bind(key)
            .bind(req_sha)
            .bind(job_id.0.to_string())
            .bind(&now_str)
            .bind(expires_at.to_rfc3339())
            .execute(&mut *tx)
            .await;

            match insert_res {
                Ok(_) => {
                    tx.commit().await?;
                    return Ok(IdempotencyInsertResult::Created { job_id, submitted_at: now });
                }
                Err(e) if is_unique_violation(&e) => {
                    tx.rollback().await?;
                    let (existing_sha, existing_job_id): (String, String) = sqlx::query_as(
                        "SELECT request_sha256, job_id FROM idempotency_keys WHERE scope = $1 AND idempotency_key = $2",
                    )
                    .bind(scope)
                    .bind(key)
                    .fetch_one(self.pool())
                    .await?;
                    if existing_sha != req_sha {
                        return Err(StoreError::IdempotencyConflict(
                            "idempotency key already used with a different request payload".into(),
                        ));
                    }
                    let existing_uuid = Uuid::parse_str(&existing_job_id)
                        .map_err(|_| StoreError::ReportValidation("invalid job_id".into()))?;
                    let row = self.get_scan_job(JobId(existing_uuid)).await?;
                    return Ok(IdempotencyInsertResult::Replayed { existing: Box::new(row) });
                }
                Err(e) => return Err(StoreError::Sql(e)),
            }
        }

        sqlx::query(
            r#"INSERT INTO application_jobs (
                job_id, submitted_at, status, target_kind, normalized_target_key, request_json,
                attempt_count, job_kind, payload_schema_version
            ) VALUES ($1, $2, 'queued', $3, $4, $5, 0, $6, 1)"#,
        )
        .bind(job_id.0.to_string())
        .bind(&now_str)
        .bind(&identity.target_kind)
        .bind(&identity.normalized_key)
        .bind(request_json)
        .bind(kind.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(IdempotencyInsertResult::Created { job_id, submitted_at: now })
    }

    pub async fn list_application_jobs_filtered(
        &self,
        kind: Option<&str>,
        status: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<ScanJobRow>, u64), StoreError> {
        let total: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM application_jobs
               WHERE ($1::TEXT IS NULL OR job_kind = $1)
                 AND ($2::TEXT IS NULL OR status = $2)"#,
        )
        .bind(kind)
        .bind(status)
        .fetch_one(self.pool())
        .await?;

        let rows = sqlx::query_as::<_, ScanJobRow>(
            r#"SELECT job_id, submitted_at, started_at, completed_at, status,
                      target_kind, normalized_target_key, request_json,
                      failure_code, failure_message, run_id,
                      worker_id, attempt_count, heartbeat_at, leased_until, recovery_note,
                      job_kind, payload_schema_version, cancel_requested, result_ref_json
               FROM application_jobs
               WHERE ($1::TEXT IS NULL OR job_kind = $1)
                 AND ($2::TEXT IS NULL OR status = $2)
               ORDER BY submitted_at DESC
               LIMIT $3 OFFSET $4"#,
        )
        .bind(kind)
        .bind(status)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.pool())
        .await?;

        Ok((rows, total as u64))
    }

    pub async fn job_status_counts(&self) -> Result<BTreeMap<String, u64>, StoreError> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT status, COUNT(*) FROM application_jobs GROUP BY status")
                .fetch_all(self.pool())
                .await?;
        let mut counts = BTreeMap::new();
        for s in ["queued", "running", "succeeded", "failed", "canceled"] {
            counts.insert(s.to_string(), 0);
        }
        for (status, n) in rows {
            counts.insert(status, n.max(0) as u64);
        }
        Ok(counts)
    }

    pub async fn list_scan_runs_page(
        &self,
        offset: u64,
        limit: u64,
    ) -> Result<(Vec<ScanRunListItem>, u64), StoreError> {
        let total: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM scan_runs").fetch_one(self.pool()).await?;
        let rows: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
            r#"SELECT run_id, request_scan_id, completed_at, run_outcome, target_kind, normalized_target_key
               FROM scan_runs
               ORDER BY completed_at DESC
               LIMIT $1 OFFSET $2"#,
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.pool())
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (rid, rsid, comp, outcome, tk, nk) in rows {
            out.push(parse_run_list_item(&rid, &rsid, &comp, &outcome, tk, nk)?);
        }
        Ok((out, total as u64))
    }
}

fn parse_run_list_item(
    rid: &str,
    rsid: &str,
    comp: &str,
    outcome: &str,
    target_kind: String,
    normalized_key: String,
) -> Result<ScanRunListItem, StoreError> {
    let run_id = RunId(
        Uuid::parse_str(rid).map_err(|_| StoreError::ReportValidation("invalid run_id".into()))?,
    );
    let request_scan_id = Uuid::parse_str(rsid)
        .map_err(|_| StoreError::ReportValidation("invalid request_scan_id".into()))?;
    let completed_at = DateTime::parse_from_rfc3339(comp)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|_| StoreError::ReportValidation("invalid completed_at".into()))?;
    let run_outcome = match outcome {
        "completed" => RunOutcome::Completed,
        "completed_partial" => RunOutcome::CompletedPartial,
        other => return Err(StoreError::ReportValidation(format!("unknown run_outcome {other}"))),
    };
    Ok(ScanRunListItem {
        run_id,
        request_scan_id,
        completed_at,
        run_outcome,
        target_identity: TargetIdentity { target_kind, normalized_key },
    })
}
