use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteSynchronous};
use sqlx::Row;

use crate::error::OrchestratorError;
use crate::ids::{BatchPlanId, DatasetId, RunId};
use crate::journal::{DatasetRunRecord, RunIdentity, RunJournal, JOURNAL_SCHEMA_VERSION};
use crate::state::DatasetState;

pub struct SqliteRunJournal {
    pool: SqlitePool,
}

impl SqliteRunJournal {
    pub async fn open_existing(path: &camino::Utf8Path) -> Result<Self, OrchestratorError> {
        let opts = SqliteConnectOptions::new()
            .filename(path.as_str())
            .create_if_missing(false)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true);
        let pool = SqlitePool::connect_with(opts)
            .await
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
        let journal = Self { pool };
        journal.migrate().await?;
        Ok(journal)
    }

    pub fn open_existing_sync(path: &camino::Utf8Path) -> Result<Self, OrchestratorError> {
        let path = path.to_path_buf();
        tokio_task_block(async move { Self::open_existing(&path).await })
    }

    pub async fn open(
        path: &camino::Utf8Path,
        identity: &RunIdentity,
    ) -> Result<Self, OrchestratorError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent.as_std_path())?;
        }
        let opts = SqliteConnectOptions::new()
            .filename(path.as_str())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true);
        let pool = SqlitePool::connect_with(opts)
            .await
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
        let journal = Self { pool };
        journal.migrate().await?;
        journal.ensure_run(identity).await?;
        Ok(journal)
    }

    pub fn open_sync(
        path: &camino::Utf8Path,
        identity: &RunIdentity,
    ) -> Result<Self, OrchestratorError> {
        let path = path.to_path_buf();
        let identity = identity.clone();
        tokio_task_block(async move { Self::open(&path, &identity).await })
    }

    async fn migrate(&self) -> Result<(), OrchestratorError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
        sqlx::query("INSERT OR REPLACE INTO parqonaut_journal_meta (key, value) VALUES ('schema_version', ?)")
            .bind(JOURNAL_SCHEMA_VERSION.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
        Ok(())
    }

    async fn ensure_run(&self, identity: &RunIdentity) -> Result<(), OrchestratorError> {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO batch_runs (
                run_id, batch_plan_id, config_fingerprint, batch_name, output_root,
                parqonaut_version, plan_digest, started_at, updated_at, completed_at, cancelled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&identity.run_id.0)
        .bind(&identity.batch_plan_id.0)
        .bind(&identity.config_fingerprint)
        .bind(&identity.batch_name)
        .bind(&identity.output_root)
        .bind(&identity.parqonaut_version)
        .bind(&identity.plan_digest)
        .bind(identity.started_at.to_rfc3339())
        .bind(identity.updated_at.to_rfc3339())
        .bind(identity.completed_at.map(|t| t.to_rfc3339()))
        .bind(identity.cancelled_at.map(|t| t.to_rfc3339()))
        .execute(&self.pool)
        .await
        .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
        Ok(())
    }
}

impl RunJournal for SqliteRunJournal {
    fn run_identity(&self) -> Result<Option<RunIdentity>, OrchestratorError> {
        let pool = self.pool.clone();
        tokio_task_block(async move {
            let row = sqlx::query(
                "SELECT run_id, batch_plan_id, config_fingerprint, batch_name, output_root, parqonaut_version, plan_digest, started_at, updated_at, completed_at, cancelled_at FROM batch_runs LIMIT 1",
            )
            .fetch_optional(&pool)
            .await
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            Ok(row.map(map_identity_row))
        })
    }

    fn list_datasets(&self) -> Result<Vec<DatasetRunRecord>, OrchestratorError> {
        let pool = self.pool.clone();
        tokio_task_block(async move {
            let rows = sqlx::query("SELECT * FROM dataset_runs ORDER BY dataset_id")
                .fetch_all(&pool)
                .await
                .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            rows.into_iter().map(map_dataset_row).collect()
        })
    }

    fn dataset(&self, id: &DatasetId) -> Result<Option<DatasetRunRecord>, OrchestratorError> {
        let pool = self.pool.clone();
        let dataset_id = id.0.clone();
        tokio_task_block(async move {
            let row = sqlx::query("SELECT * FROM dataset_runs WHERE dataset_id = ?")
                .bind(dataset_id)
                .fetch_optional(&pool)
                .await
                .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            row.map(map_dataset_row).transpose()
        })
    }

    fn upsert_dataset(&self, record: &DatasetRunRecord) -> Result<(), OrchestratorError> {
        let pool = self.pool.clone();
        let record = record.clone();
        tokio_task_block(async move {
            let run_id = sqlx::query_scalar::<_, String>("SELECT run_id FROM batch_runs LIMIT 1")
                .fetch_one(&pool)
                .await
                .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            sqlx::query(
                r#"
                INSERT INTO dataset_runs (
                    run_id, dataset_id, output_path, repair_plan_id, policy_fingerprint,
                    state, attempts, source_fingerprint, output_fingerprint,
                    error_class, error_message, started_at, updated_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, dataset_id) DO UPDATE SET
                    output_path=excluded.output_path,
                    repair_plan_id=excluded.repair_plan_id,
                    policy_fingerprint=excluded.policy_fingerprint,
                    state=excluded.state,
                    attempts=excluded.attempts,
                    source_fingerprint=excluded.source_fingerprint,
                    output_fingerprint=excluded.output_fingerprint,
                    error_class=excluded.error_class,
                    error_message=excluded.error_message,
                    started_at=excluded.started_at,
                    updated_at=excluded.updated_at,
                    completed_at=excluded.completed_at
                "#,
            )
            .bind(run_id)
            .bind(&record.dataset_id.0)
            .bind(&record.output_path)
            .bind(&record.repair_plan_id)
            .bind(&record.policy_fingerprint)
            .bind(state_to_str(record.state))
            .bind(record.attempts as i64)
            .bind(&record.source_fingerprint)
            .bind(&record.output_fingerprint)
            .bind(record.error_class.map(|c| format!("{c:?}").to_lowercase()))
            .bind(&record.error_message)
            .bind(record.started_at.map(|t| t.to_rfc3339()))
            .bind(record.updated_at.to_rfc3339())
            .bind(record.completed_at.map(|t| t.to_rfc3339()))
            .execute(&pool)
            .await
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            Ok(())
        })
    }

    fn mark_run_completed(&self, at: DateTime<Utc>) -> Result<(), OrchestratorError> {
        let pool = self.pool.clone();
        tokio_task_block(async move {
            sqlx::query("UPDATE batch_runs SET completed_at = ?, updated_at = ?")
                .bind(at.to_rfc3339())
                .bind(at.to_rfc3339())
                .execute(&pool)
                .await
                .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            Ok(())
        })
    }

    fn mark_run_cancelled(&self, at: DateTime<Utc>) -> Result<(), OrchestratorError> {
        let pool = self.pool.clone();
        tokio_task_block(async move {
            sqlx::query("UPDATE batch_runs SET cancelled_at = ?, updated_at = ?")
                .bind(at.to_rfc3339())
                .bind(at.to_rfc3339())
                .execute(&pool)
                .await
                .map_err(|e| OrchestratorError::Journal(e.to_string()))?;
            Ok(())
        })
    }
}

fn map_identity_row(r: sqlx::sqlite::SqliteRow) -> RunIdentity {
    RunIdentity {
        run_id: RunId(r.get("run_id")),
        batch_plan_id: BatchPlanId(r.get("batch_plan_id")),
        config_fingerprint: r.get("config_fingerprint"),
        batch_name: r.get("batch_name"),
        output_root: r.get("output_root"),
        parqonaut_version: r.get("parqonaut_version"),
        plan_digest: r.get("plan_digest"),
        started_at: parse_ts(r.get::<String, _>("started_at")),
        updated_at: parse_ts(r.get::<String, _>("updated_at")),
        completed_at: r.get::<Option<String>, _>("completed_at").map(parse_ts),
        cancelled_at: r.get::<Option<String>, _>("cancelled_at").map(parse_ts),
    }
}

fn map_dataset_row(r: sqlx::sqlite::SqliteRow) -> Result<DatasetRunRecord, OrchestratorError> {
    Ok(DatasetRunRecord {
        dataset_id: DatasetId(r.try_get("dataset_id").map_err(journal_err)?),
        output_path: r.try_get("output_path").map_err(journal_err)?,
        repair_plan_id: r.try_get("repair_plan_id").map_err(journal_err)?,
        policy_fingerprint: r.try_get("policy_fingerprint").map_err(journal_err)?,
        state: parse_state(&r.try_get::<String, _>("state").map_err(journal_err)?),
        attempts: r.try_get::<i64, _>("attempts").map_err(journal_err)? as u32,
        source_fingerprint: r.try_get("source_fingerprint").ok(),
        output_fingerprint: r.try_get("output_fingerprint").ok(),
        error_class: parse_failure(r.try_get("error_class").ok()),
        error_message: r.try_get("error_message").ok(),
        started_at: r.try_get::<Option<String>, _>("started_at").ok().flatten().map(parse_ts),
        updated_at: parse_ts(r.try_get::<String, _>("updated_at").map_err(journal_err)?),
        completed_at: r.try_get::<Option<String>, _>("completed_at").ok().flatten().map(parse_ts),
    })
}

fn parse_ts(s: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&s).map(|t| t.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now())
}

fn state_to_str(state: DatasetState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "pending".into())
}

fn parse_state(raw: &str) -> DatasetState {
    serde_json::from_value(serde_json::Value::String(raw.to_string()))
        .unwrap_or(DatasetState::Pending)
}

fn parse_failure(raw: Option<String>) -> Option<crate::error::FailureClass> {
    raw.and_then(|s| match s.as_str() {
        "recoverable" => Some(crate::error::FailureClass::Recoverable),
        "permanent" => Some(crate::error::FailureClass::Permanent),
        "blocked" => Some(crate::error::FailureClass::Blocked),
        "stalesource" => Some(crate::error::FailureClass::StaleSource),
        "verificationfailed" => Some(crate::error::FailureClass::VerificationFailed),
        "cancelled" => Some(crate::error::FailureClass::Cancelled),
        _ => None,
    })
}

fn journal_err(e: sqlx::Error) -> OrchestratorError {
    OrchestratorError::Journal(e.to_string())
}

fn tokio_task_block<F, T>(future: F) -> Result<T, OrchestratorError>
where
    F: std::future::Future<Output = Result<T, OrchestratorError>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| OrchestratorError::Journal(e.to_string()))?
            .block_on(future)
    })
    .join()
    .map_err(|_| OrchestratorError::Journal("journal worker thread panicked".into()))?
}
