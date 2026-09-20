#![cfg(all(feature = "columnar", feature = "s3"))]

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use parqonaut_columnar::BatchSource;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::columnar::StorageParquetBatchSource;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};

#[tokio::test]
async fn s3_parquet_batch_source_reads_fixture_without_full_get() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        eprintln!("skipping S3 columnar test: set PARQONAUT_S3_ENDPOINT (or run just s3-up)");
        return;
    };

    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = common::fogbank_bucket();
    let prefix =
        std::env::var("FOGBANK_DATASET_PREFIX").unwrap_or_else(|_| "datasets".into());
    let key = format!("{prefix}/healthy/part-0000.parquet");
    let object = ObjectLocation::S3 { bucket, key };

    let head = match backend.head(&object).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("skipping S3 columnar test: fixture missing ({e})");
            return;
        }
    };

    let before = backend.metrics();
    let source = StorageParquetBatchSource::new(backend.clone(), object);
    let mut stream = Box::new(source).into_stream().expect("stream");
    let batch = stream.next().await.expect("item").expect("batch");
    assert!(batch.num_rows() > 0);

    let after = backend.metrics();
    let range_bytes = after.range_bytes_read.saturating_sub(before.range_bytes_read);
    assert!(
        range_bytes < head.size,
        "expected range reads only, got {range_bytes} of {} object bytes",
        head.size
    );
    let full_gets = after.full_get_requests.saturating_sub(before.full_get_requests);
    assert_eq!(full_gets, 0, "full object download must not occur");
}
