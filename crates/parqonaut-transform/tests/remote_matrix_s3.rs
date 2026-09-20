#![cfg(feature = "s3")]

use std::sync::Arc;

use bytes::Bytes;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::conditional::ConditionalCreate;
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
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

async fn s3_backend() -> Option<Arc<S3StorageBackend>> {
    let endpoint = require_s3_endpoint()?;
    Some(Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await))
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

#[tokio::test]
async fn remote_matrix_rewrite_and_merge_all_io_kinds() {
    let Some(backend) = s3_backend().await else {
        eprintln!("skipping remote_matrix_s3: set PARQONAUT_S3_ENDPOINT (just s3-up)");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let local_a = dir.path().join("a.parquet");
    let local_b = dir.path().join("b.parquet");
    let local_out = dir.path().join("out.parquet");
    let fixture = parquet_fixture();
    std::fs::write(&local_a, &fixture).unwrap();
    std::fs::write(&local_b, &fixture).unwrap();

    let bucket = test_bucket();
    let prefix = format!("remote-matrix/{}", uuid::Uuid::new_v4());

    let s3_a = format!("s3://{bucket}/{prefix}/a.parquet");
    let s3_b = format!("s3://{bucket}/{prefix}/b.parquet");
    let s3_copy = format!("s3://{bucket}/{prefix}/copy.parquet");
    let s3_merged = format!("s3://{bucket}/{prefix}/merged.parquet");

    put_bytes(&backend, &bucket, &format!("{prefix}/a.parquet"), &fixture).await;
    put_bytes(&backend, &bucket, &format!("{prefix}/b.parquet"), &fixture).await;

    let backend_dyn: Arc<dyn StorageBackend> = backend.clone();

    // local → local
    assert_eq!(
        classify_io(&local_a.to_string_lossy(), &local_out.to_string_lossy()).unwrap(),
        RemoteIoKind::LocalToLocal
    );
    rewrite_parquet_storage(
        Arc::clone(&backend_dyn),
        &local_a.to_string_lossy(),
        &local_out.to_string_lossy(),
    )
    .expect("l2l rewrite");

    // local → remote
    assert_eq!(
        classify_io(&local_a.to_string_lossy(), &s3_copy).unwrap(),
        RemoteIoKind::LocalToRemote
    );
    rewrite_parquet_storage(Arc::clone(&backend_dyn), &local_a.to_string_lossy(), &s3_copy)
        .expect("l2r rewrite");

    // remote → local
    let local_from_s3 = dir.path().join("from-s3.parquet");
    assert_eq!(
        classify_io(&s3_a, &local_from_s3.to_string_lossy()).unwrap(),
        RemoteIoKind::RemoteToLocal
    );
    rewrite_parquet_storage(Arc::clone(&backend_dyn), &s3_a, &local_from_s3.to_string_lossy())
        .expect("r2l rewrite");

    // remote → remote
    let s3_out = format!("s3://{bucket}/{prefix}/r2r.parquet");
    assert_eq!(classify_io(&s3_a, &s3_out).unwrap(), RemoteIoKind::RemoteToRemote);
    rewrite_parquet_storage(Arc::clone(&backend_dyn), &s3_a, &s3_out).expect("r2r rewrite");

    merge_parquet_storage(backend_dyn, &[s3_a.clone(), s3_b.clone()], &s3_merged)
        .expect("s3 merge");
}
