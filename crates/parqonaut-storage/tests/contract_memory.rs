use parqonaut_storage::contract::storage_backend_contract;
use parqonaut_storage::{MemoryStorageBackend, StorageCapabilities};

#[tokio::test]
async fn memory_backend_satisfies_contract() {
    let backend = MemoryStorageBackend::new(StorageCapabilities {
        range_reads: true,
        stream_reads: true,
        stream_writes: true,
        multipart_upload: false,
        conditional_create: true,
        conditional_replace: true,
        server_side_copy: false,
        version_ids: true,
    });
    storage_backend_contract(&backend).await;
}
