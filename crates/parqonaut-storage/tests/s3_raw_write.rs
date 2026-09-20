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

#[tokio::test(flavor = "multi_thread")]
async fn raw_multipart_write_stream_head() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required");
    };
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());
    let key = format!("raw-write/large-{}.bin", uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket, key };

    let before = backend.metrics().multipart_parts;
    let mut w = backend.write_stream(&loc, None).await.expect("open");
    let chunk = vec![0_u8; 5 * 1024 * 1024 + 1024];
    w.write_all(&chunk).await.expect("write part1");
    w.write_all(b"tail").await.expect("write tail");
    let n = w.finish().await.expect("finish");
    assert!(n > 5 * 1024 * 1024);
    assert!(backend.metrics().multipart_parts > before);
    backend.head(&loc).await.expect("head");
}
