#![cfg(all(feature = "columnar", feature = "s3"))]

mod common;

use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parqonaut_columnar::BatchSource;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::columnar::{write_parquet_batch_stream, StorageParquetBatchSource};
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
use parqonaut_workflow::NoOpProgressObserver;

#[tokio::test(flavor = "multi_thread")]
async fn write_parquet_batch_stream_small_put_roundtrip() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required");
    };
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());
    let key = format!("write-roundtrip/small-{}.parquet", uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket, key };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(StringArray::from(vec!["a"]))],
    )
    .unwrap();
    let stream: parqonaut_columnar::BatchStream = Box::pin(futures::stream::iter(vec![Ok(batch)]));

    write_parquet_batch_stream(
        Arc::clone(&backend),
        loc.clone(),
        schema,
        stream,
        &NoOpProgressObserver,
    )
    .await
    .expect("write");

    backend.head(&loc).await.expect("head before read");
    let source = StorageParquetBatchSource::new(backend, loc);
    let mut read = Box::new(source).into_stream().expect("read stream");
    assert_eq!(read.next().await.expect("batch").expect("ok").num_rows(), 1);
}

/// Multipart finalize and read-back (large incompressible payload, default 5 MiB part size).
#[tokio::test(flavor = "multi_thread")]
async fn write_parquet_batch_stream_multipart_roundtrip() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required");
    };
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());
    let key = format!("write-roundtrip/large-{}.parquet", uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket, key };

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
    ]));
    let payload = vec![7_u8; 512 * 1024];
    let mut batches = Vec::new();
    for i in 0..24 {
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![i; 64])),
                    Arc::new(BinaryArray::from(vec![payload.as_slice(); 64])),
                ],
            )
            .unwrap(),
        );
    }
    let expected_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let stream: parqonaut_columnar::BatchStream =
        Box::pin(futures::stream::iter(batches.into_iter().map(Ok)));

    let summary = write_parquet_batch_stream(
        Arc::clone(&backend),
        loc.clone(),
        schema,
        stream,
        &NoOpProgressObserver,
    )
    .await
    .expect("write");

    assert_eq!(summary.rows_written, expected_rows);
    assert!(summary.bytes_written > 0);
    backend.head(&loc).await.expect("head after streaming write");
    let source = StorageParquetBatchSource::new(Arc::clone(&backend), loc);
    let mut read = Box::new(source).into_stream().expect("read stream");
    let mut rows = 0usize;
    while let Some(b) = read.next().await {
        rows += b.expect("batch").num_rows();
    }
    assert_eq!(rows as u64, expected_rows);
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_create_destination_conflict() {
    use bytes::Bytes;
    use parqonaut_storage::conditional::ConditionalCreate;

    let Some(endpoint) = common::require_s3_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required");
    };
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());
    let key = format!("write-roundtrip/conflict-{}.bin", uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket, key };

    backend
        .conditional_create(&loc, ConditionalCreate::must_not_exist(), Bytes::from_static(b"seed"))
        .await
        .expect("seed object");

    let err = backend
        .conditional_create(&loc, ConditionalCreate::must_not_exist(), Bytes::from_static(b"other"))
        .await
        .expect_err("create-only must not overwrite");

    assert!(
        err.to_string().contains("exist")
            || err.to_string().contains("409")
            || err.to_string().contains("412")
            || err.to_string().contains("Conflict"),
        "unexpected conflict error: {err}"
    );
    let head = backend.head(&loc).await.expect("seed still present");
    assert_eq!(head.size, 4);
}
