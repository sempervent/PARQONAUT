//! Build [`sqlx::sqlite::SqliteConnectOptions`] with Paraclete hardening (ADR 0025).

use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;

use crate::error::StoreError;
use crate::sqlite_config::SqliteStoreConfig;

/// Applies [`SqliteStoreConfig`] and enforces **foreign keys** (SQLx also defaults `foreign_keys=ON`).
pub fn connect_options(
    database_url: impl AsRef<str>,
    config: &SqliteStoreConfig,
) -> Result<SqliteConnectOptions, StoreError> {
    let opts = SqliteConnectOptions::from_str(database_url.as_ref())?;
    Ok(opts
        .journal_mode(config.journal_mode)
        .synchronous(config.synchronous)
        .foreign_keys(true)
        .busy_timeout(config.busy_timeout))
}
