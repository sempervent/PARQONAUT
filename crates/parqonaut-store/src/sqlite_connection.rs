//! Build [`sqlx::sqlite::SqliteConnectOptions`] with PARQONAUT hardening (ADR 0025).

use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;

use crate::error::StoreError;
use crate::sqlite_config::SqliteStoreConfig;

/// Applies [`SqliteStoreConfig`] and enforces **foreign keys** (SQLx also defaults `foreign_keys=ON`).
pub fn connect_options(
    database_url: impl AsRef<str>,
    config: &SqliteStoreConfig,
) -> Result<SqliteConnectOptions, StoreError> {
    let raw = database_url.as_ref();
    let mut opts = SqliteConnectOptions::from_str(raw)?;
    let explicit_mode = raw
        .split_once('?')
        .map(|(_, q)| q.split('&').any(|kv| matches!(kv, "mode=ro" | "mode=rw" | "mode=memory")))
        .unwrap_or(false);
    if !explicit_mode {
        opts = opts.create_if_missing(true);
    }
    Ok(opts
        .journal_mode(config.journal_mode)
        .synchronous(config.synchronous)
        .foreign_keys(true)
        .busy_timeout(config.busy_timeout))
}
