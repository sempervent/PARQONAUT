use parqonaut_storage::contract::storage_backend_contract;
use parqonaut_storage::LocalStorageBackend;

#[tokio::test]
async fn local_backend_satisfies_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = LocalStorageBackend::new(dir.path());
    storage_backend_contract(&backend).await;
}
