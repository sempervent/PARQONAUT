//! Tunable SQLite connection settings for single-node deployments (see ADR 0025).

use std::time::Duration;

use sqlx::sqlite::{SqliteJournalMode, SqliteSynchronous};

/// Defaults used by [`crate::SqliteScanStore::connect`] and [`crate::SqliteScanStore::connect_with_config`].
///
/// Values favor **WAL** + **`synchronous=NORMAL`**: durable enough for typical server use while avoiding
/// the extra fsync cost of **`FULL`** when WAL is enabled. Adjust only with operational justification.
#[derive(Debug, Clone)]
pub struct SqliteStoreConfig {
    /// Connection pool size. SQLite still serializes writers; keep this modest (default **5**).
    pub max_connections: u32,
    /// [`PRAGMA busy_timeout`](https://www.sqlite.org/pragma.html#pragma_busy_timeout) (default **5s**).
    pub busy_timeout: Duration,
    /// [`PRAGMA journal_mode`](https://www.sqlite.org/pragma.html#pragma_journal_mode) (default **WAL**).
    pub journal_mode: SqliteJournalMode,
    /// [`PRAGMA synchronous`](https://www.sqlite.org/pragma.html#pragma_synchronous) (default **NORMAL** with WAL).
    pub synchronous: SqliteSynchronous,
}

impl Default for SqliteStoreConfig {
    fn default() -> Self {
        Self {
            max_connections: 5,
            busy_timeout: Duration::from_secs(5),
            journal_mode: SqliteJournalMode::Wal,
            synchronous: SqliteSynchronous::Normal,
        }
    }
}
