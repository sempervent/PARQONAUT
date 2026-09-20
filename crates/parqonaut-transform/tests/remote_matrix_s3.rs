#![cfg(feature = "s3")]

use std::sync::Arc;

use bytes::Bytes;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::conditional::ConditionalCreate;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
use parqonaut_transform::columnar_io::ColumnarPipelineIo;
use parqonaut_transform::{
    classify_io, merge_parquet_storage, rewrite_parquet_storage, RemoteIoKind,
};

fn require_s3_endpoint() -> Option<String> {
    std::env::var("PARQONAUT_S3_ENDPOINT").ok().or_else(|| std::env::var("MINIO_ENDPOINT").ok())
}

fn test_bucket() -> String {
    std::env::var("PARQONAUT_S3_BUCKET")
        .or_else(|_| std::env::var("MINIO_BUCKET"))
        .unwrap_or_else(|_| "parqonaut-test".into())
}

fn parquet_fixture() -> Vec<u8> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(StringArray::from(vec!["x", "y"]))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

async fn s3_io() -> Option<(ColumnarPipelineIo, Arc<S3StorageBackend>)> {
    let endpoint = require_s3_endpoint()?;
    let s3 = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let io = ColumnarPipelineIo::for_test_s3(Arc::clone(&s3));
    Some((io, s3))
}

async fn put_bytes(backend: &S3StorageBackend, bucket: &str, key: &str, data: &[u8]) {
    let object = ObjectLocation::S3 { bucket: bucket.into(), key: key.into() };
    backend
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::copy_from_slice(data),
        )
        .await
        .expect("put fixture");
}

#[test]
fn classify_all_remote_io_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("a.parquet");
    let s3 = "s3://bucket/key.parquet";
    assert_eq!(
        classify_io(&local.to_string_lossy(), &local.to_string_lossy()).unwrap(),
        RemoteIoKind::LocalToLocal
    );
    assert_eq!(classify_io(&local.to_string_lossy(), s3).unwrap(), RemoteIoKind::LocalToRemote);
    assert_eq!(classify_io(s3, &local.to_string_lossy()).unwrap(), RemoteIoKind::RemoteToLocal);
    assert_eq!(classify_io(s3, s3).unwrap(), RemoteIoKind::RemoteToRemote);
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_rewrite_and_merge_remote_to_remote() {
    let Some((io, s3)) = s3_io().await else {
        eprintln!("skipping remote_matrix_s3: set PARQONAUT_S3_ENDPOINT (just s3-up)");
        return;
    };

    let fixture = parquet_fixture();
    let bucket = test_bucket();
    let prefix = format!("remote-matrix/{}", uuid::Uuid::new_v4());

    let s3_a = format!("s3://{bucket}/{prefix}/a.parquet");
    let s3_b = format!("s3://{bucket}/{prefix}/b.parquet");
    let s3_out = format!("s3://{bucket}/{prefix}/r2r.parquet");
    let s3_merged = format!("s3://{bucket}/{prefix}/merged.parquet");

    put_bytes(&s3, &bucket, &format!("{prefix}/a.parquet"), &fixture).await;
    put_bytes(&s3, &bucket, &format!("{prefix}/b.parquet"), &fixture).await;

    rewrite_parquet_storage(&io, &s3_a, &s3_out).expect("r2r rewrite");
    merge_parquet_storage(&io, &[s3_a, s3_b], &s3_merged).expect("s3 merge");
}

#[test]
fn local_to_local_rewrite_uses_local_backend() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.parquet");
    let out = dir.path().join("out.parquet");
    std::fs::write(&a, parquet_fixture()).unwrap();
    let io = ColumnarPipelineIo::for_test();
    rewrite_parquet_storage(&io, &a.to_string_lossy(), &out.to_string_lossy()).expect("l2l");
    assert!(out.exists());
}
