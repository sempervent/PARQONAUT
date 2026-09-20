#![cfg(feature = "s3")]

mod common;

use std::sync::Arc;

use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};

#[tokio::test(flavor = "multi_thread")]
async fn raw_write_stream_head() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required");
    };
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());
    let key = format!("raw-write/{}.bin", uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket, key };

    let mut w = backend.write_stream(&loc, None).await.expect("open");
    w.write_all(b"hello").await.expect("write");
    let n = w.finish().await.expect("finish");
    assert_eq!(n, 5);
    backend.head(&loc).await.expect("head");
}

// Multipart finalize is covered by `write_s3_roundtrip` (Parquet path) in CI.
