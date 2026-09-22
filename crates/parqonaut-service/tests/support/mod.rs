//! Shared helpers for HTTP integration tests (`http_api`, `api_integration`, …).

use std::path::PathBuf;

use camino::Utf8PathBuf;
use parqonaut_store::SqliteScanStore;
use parqonaut_types::AuthRole;

pub const TEST_BEARER_SECRET: &str = "parqonaut-http-test-token";

pub fn bearer(secret: &str) -> String {
    format!("Bearer {secret}")
}

pub fn bearer_operator() -> String {
    bearer(TEST_BEARER_SECRET)
}

pub fn sqlite_url(dir: &tempfile::TempDir) -> String {
    let p = dir.path().join("t.sqlite");
    std::fs::File::create(&p).unwrap();
    let abs = p.canonicalize().unwrap();
    format!("sqlite://{}", abs.display())
}

pub async fn connect_store_with_token(
    role: AuthRole,
    label: &str,
) -> (tempfile::TempDir, SqliteScanStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteScanStore::connect(&sqlite_url(&dir)).await.unwrap();
    store.insert_auth_token(label, TEST_BEARER_SECRET, role).await.unwrap();
    (dir, store)
}

pub fn fixture(rel: &str) -> Utf8PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(rel);
    Utf8PathBuf::from_path_buf(path).unwrap()
}
