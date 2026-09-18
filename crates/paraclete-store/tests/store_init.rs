//! SQLite startup: hardened pragmas and migration path.

#[tokio::test]
async fn hardened_sqlite_pragmas_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("init.sqlite");
    std::fs::File::create(&p).unwrap();
    let url = format!("sqlite://{}", p.canonicalize().unwrap().display());

    let store = paraclete_store::SqliteScanStore::connect(&url).await.expect("connect");

    let journal: String = sqlx::query_scalar("SELECT journal_mode FROM pragma_journal_mode")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(journal.to_ascii_lowercase(), "wal");

    let sync: i64 = sqlx::query_scalar("PRAGMA synchronous").fetch_one(store.pool()).await.unwrap();
    // NORMAL = 1
    assert_eq!(sync, 1);

    let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys").fetch_one(store.pool()).await.unwrap();
    assert_eq!(fk, 1);

    let busy_ms: i64 =
        sqlx::query_scalar("PRAGMA busy_timeout").fetch_one(store.pool()).await.unwrap();
    assert_eq!(busy_ms, 5000);
}
