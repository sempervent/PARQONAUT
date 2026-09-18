//! Persistence for Paraclete scan runs (history, projections, integrity checks): **SQLite** and **Postgres**.
//!
//! SQLite connections use hardened defaults (**WAL**, **`synchronous=NORMAL`**, **`foreign_keys=ON`**, busy timeout);
//! see [`SqliteStoreConfig`] and ADR **0025**. Postgres uses **`PostgresScanStore::connect`** and **`migrations/postgres/`**.
//! **`StoreBackend`** selects the implementation; see ADR **0029**.
//!
//! The scan engine (`paraclete-core`) produces [`paraclete_types::ScanReport`] values; this crate
//! stores canonical JSON blobs plus normalized tables for listing and diffing without coupling
//! storage to scan logic.

#![forbid(unsafe_code)]

mod auth_store;
mod error;
mod labels;
mod models;
mod postgres_store;
mod sqlite_config;
mod sqlite_connection;
mod sqlite_store;
mod store_backend;

pub use auth_store::AuthTokenSummary;
pub use error::StoreError;
pub use models::{RunPublicMeta, ScanJobRow, StaleRecoveryStats, StoredAssetRow, StoredFindingRow};
pub use paraclete_types::{
    apply_redaction_policy, diff_reports, RedactionPolicy, RetentionPolicy, RunDiff, RunId,
    RunOutcome, ScanRun, ScanRunListItem, StoredReportRef, SummaryDelta, TargetIdentity,
};
pub use postgres_store::PostgresScanStore;
pub use sqlite_config::SqliteStoreConfig;
pub use sqlite_store::SqliteScanStore;
pub use sqlx::sqlite::{SqliteJournalMode, SqliteSynchronous};
pub use store_backend::StoreBackend;
