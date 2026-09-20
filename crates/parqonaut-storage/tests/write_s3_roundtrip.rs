#![cfg(all(feature = "columnar", feature = "s3"))]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parqonaut_columnar::BatchSource;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::columnar::{write_parquet_batch_stream, StorageParquetBatchSource};
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
use parqonaut_workflow::NoOpProgressObserver;

#[tokio::test(flavor = "multi_thread")]
async fn write_parquet_batch_stream_visible_on_rustfs() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        panic!("PARQONAUT_S3_ENDPOINT required");
    };
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());
    let key = format!("write-roundtrip/{}.parquet", uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };
    let uri = loc.display_uri();

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
        schema.clone(),
        stream,
        &NoOpProgressObserver,
    )
    .await
    .expect("write");

    backend.head(&loc).await.expect("head after write");

    let source = StorageParquetBatchSource::new(backend, loc);
    let mut read = Box::new(source).into_stream().expect("read stream");
    let read_batch = read.next().await.expect("batch").expect("ok");
    assert_eq!(read_batch.num_rows(), 1);
    let _ = uri;
}
