#![cfg(feature = "s3")]

use bytes::Bytes;
use parqonaut_storage::backend::{ByteRange, ListOptions, StorageBackend};
use parqonaut_storage::conditional::{ConditionalCreate, ConditionalReplace};
use parqonaut_storage::contract::storage_backend_contract;
use parqonaut_storage::error::StorageError;
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::{S3Config, S3StorageBackend};
use uuid::Uuid;

fn minio_config() -> Option<S3Config> {
    let endpoint = std::env::var("MINIO_ENDPOINT").ok()?;
    Some(S3Config {
        endpoint: Some(endpoint),
        region: std::env::var("MINIO_REGION")
            .ok()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or(Some("us-east-1".into())),
        path_style: std::env::var("MINIO_PATH_STYLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true),
        profile: std::env::var("AWS_PROFILE").ok(),
    })
}

fn test_bucket() -> String {
    std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "parqonaut-test".into())
}

fn unique_key(prefix: &str) -> String {
    format!("{prefix}/{}", Uuid::new_v4())
}

async fn backend() -> Option<S3StorageBackend> {
    let config = minio_config()?;
    Some(S3StorageBackend::new(config).await)
}

#[tokio::test]
async fn s3_backend_satisfies_contract_when_minio_available() {
    let Some(backend) = backend().await else {
        eprintln!("skipping MinIO integration test: MINIO_ENDPOINT not set");
        return;
    };

    storage_backend_contract(&backend).await;
}

#[tokio::test]
async fn conditional_create_and_replace_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping MinIO integration test: MINIO_ENDPOINT not set");
        return;
    };

    let bucket = test_bucket();
    let key = unique_key("phase5");
    let object = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };

    let created = backend
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::from_static(b"payload"),
        )
        .await
        .expect("create");

    assert_eq!(created.size, 7);
    assert!(created.etag.is_some());

    let head = backend.head(&object).await.expect("head");
    let replaced = backend
        .conditional_replace(
            &object,
            ConditionalReplace::matching(&head),
            Bytes::from_static(b"payload-v2"),
        )
        .await
        .expect("replace");
    assert_eq!(replaced.size, 10);

    let range = backend.read_range(&object, ByteRange::new(0, 4).unwrap()).await.expect("range");
    assert_eq!(&range[..], b"paylo");

    backend.delete_owned_object(&object).await.expect("delete");
    assert!(matches!(backend.head(&object).await, Err(StorageError::NotFound { .. })));
}

#[tokio::test]
async fn list_with_pagination_prefix() {
    let Some(backend) = backend().await else {
        eprintln!("skipping MinIO integration test: MINIO_ENDPOINT not set");
        return;
    };

    let bucket = test_bucket();
    let prefix = unique_key("list");
    let dataset = DatasetLocation::parse(&format!("s3://{bucket}/{prefix}/")).unwrap();

    let keys =
        [format!("{prefix}/a.bin"), format!("{prefix}/b.bin"), format!("{prefix}/nested/c.bin")];

    for key in &keys[..2] {
        let object = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };
        backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"x"),
            )
            .await
            .expect("seed");
    }

    let page = backend
        .list(&dataset, ListOptions { recursive: false, max_keys: Some(10) }, None)
        .await
        .expect("list");

    assert!(page.objects.iter().any(|o| o.location.display_uri().contains("/a.bin")));
    assert!(page.objects.iter().any(|o| o.location.display_uri().contains("/b.bin")));

    for key in &keys {
        let object = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };
        let _ = backend.delete_owned_object(&object).await;
    }
}

#[tokio::test]
async fn write_stream_multipart_small_object() {
    let Some(backend) = backend().await else {
        eprintln!("skipping MinIO integration test: MINIO_ENDPOINT not set");
        return;
    };

    let bucket = test_bucket();
    let key = unique_key("stream");
    let object = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };

    let mut stream = backend.write_stream(&object, None).await.expect("write_stream");
    stream.write_all(b"hello-stream").await.expect("write");
    let written = stream.finish().await.expect("finish");
    assert_eq!(written, 12);

    let head = backend.head(&object).await.expect("head");
    assert_eq!(head.size, 12);

    backend.delete_owned_object(&object).await.expect("delete");
}

#[test]
fn s3_config_has_no_secret_fields() {
    let cfg = S3Config::minio("http://127.0.0.1:9000");
    let debug = format!("{cfg:?}");
    assert!(!debug.to_lowercase().contains("secret"));
    assert!(!debug.to_lowercase().contains("password"));
}
