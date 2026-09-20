#![cfg(feature = "columnar")]

//! Columnar batch source/sink tests (MemoryStorageBackend + local roundtrip).

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parqonaut_columnar::{BatchSink, BatchSource};
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::capabilities::StorageCapabilities;
use parqonaut_storage::columnar::{StorageParquetBatchSink, StorageParquetBatchSource};
use parqonaut_storage::conditional::ConditionalCreate;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::memory::MemoryStorageBackend;
use parqonaut_workflow::NoOpProgressObserver;

fn write_parquet_bytes(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![rows, rows + 1])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_backend_parquet_read_uses_bounded_ranges() {
    let backend = Arc::new(MemoryStorageBackend::new(StorageCapabilities::LOCAL));
    let object = ObjectLocation::S3 {
        bucket: "batch".into(),
        key: "data.parquet".into(),
    };
    let bytes = write_parquet_bytes(1);
    let full_len = bytes.len() as u64;
    backend
        .conditional_create(&object, ConditionalCreate::must_not_exist(), Bytes::from(bytes))
        .await
        .expect("seed object");

    let before = backend.metrics();
    let source = StorageParquetBatchSource::new(Arc::clone(&backend), object);
    let mut stream = Box::new(source).into_stream().expect("stream");
    let batch = stream.next().await.expect("batch").expect("ok");
    assert_eq!(batch.num_rows(), 2);

    let after = backend.metrics();
    let range_bytes = after.range_bytes_read.saturating_sub(before.range_bytes_read);
    assert!(
        range_bytes < full_len,
        "expected ranged reads only, got {range_bytes} bytes for object size {full_len}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn local_to_local_rewrite_via_storage_backend() {
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("in.parquet");
    let output_path = dir.path().join("out.parquet");
    std::fs::write(&input_path, write_parquet_bytes(10)).unwrap();

    let backend: Arc<dyn StorageBackend> =
        Arc::new(parqonaut_storage::LocalStorageBackend::direct());
    let input = input_path.to_string_lossy().to_string();
    let output = output_path.to_string_lossy().to_string();

    let source = StorageParquetBatchSource::new(Arc::clone(&backend), ObjectLocation::parse(&input).unwrap());
    let schema = source.schema().expect("schema");
    let stream = Box::new(source).into_stream().expect("stream");
    let mut sink = StorageParquetBatchSink::new(backend, ObjectLocation::parse(&output).unwrap());
    sink.write_stream(schema, stream, &NoOpProgressObserver).expect("write");

    assert!(output_path.exists());
}
