#![cfg(feature = "s3")]

mod common;

use bytes::Bytes;
use parqonaut_storage::backend::{ByteRange, ListOptions, StorageBackend};
use parqonaut_storage::conditional::{ConditionalCreate, ConditionalReplace};
use parqonaut_storage::contract::storage_backend_contract;
use parqonaut_storage::error::StorageError;
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::{S3Config, S3StorageBackend};
use uuid::Uuid;

fn s3_config() -> Option<S3Config> {
    common::require_s3_endpoint().map(|endpoint| {
        let mut cfg = S3Config::minio(endpoint);
        if let Ok(region) = std::env::var("AWS_REGION") {
            cfg.region = Some(region);
        }
        cfg
    })
}

fn test_bucket() -> String {
    std::env::var("PARQONAUT_S3_BUCKET")
        .or_else(|_| std::env::var("MINIO_BUCKET"))
        .unwrap_or_else(|_| "parqonaut-test".into())
}

fn unique_key(prefix: &str) -> String {
    format!("{prefix}/{}", Uuid::new_v4())
}

async fn backend() -> Option<S3StorageBackend> {
    let config = s3_config()?;
    Some(S3StorageBackend::new(config).await)
}

#[tokio::test]
async fn s3_backend_satisfies_contract_when_s3_available() {
    let Some(backend) = backend().await else {
        eprintln!("skipping S3 integration test: no endpoint configured");
        return;
    };

    storage_backend_contract(&backend).await;
}

#[tokio::test]
async fn conditional_create_and_replace_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping S3 integration test: no endpoint configured");
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
        eprintln!("skipping S3 integration test: no endpoint configured");
        return;
    };

    let bucket = test_bucket();
    let prefix = unique_key("list");
    let dataset = DatasetLocation::parse(&format!("s3://{bucket}/")).unwrap();

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
        .list(&dataset, ListOptions { recursive: true, max_keys: Some(10) }, None)
        .await
        .expect("list");

    assert!(
        page.objects.iter().any(|o| o.location.display_uri().contains(&format!("{prefix}/a.bin"))),
        "objects: {:?}",
        page.objects.iter().map(|o| o.location.display_uri()).collect::<Vec<_>>()
    );
    assert!(page
        .objects
        .iter()
        .any(|o| o.location.display_uri().contains(&format!("{prefix}/b.bin"))));

    for key in &keys {
        let object = ObjectLocation::S3 { bucket: bucket.clone(), key: key.clone() };
        let _ = backend.delete_owned_object(&object).await;
    }
}

#[test]
fn s3_config_has_no_secret_fields() {
    let cfg = S3Config::minio("http://127.0.0.1:9000");
    let debug = format!("{cfg:?}");
    assert!(!debug.to_lowercase().contains("secret"));
    assert!(!debug.to_lowercase().contains("password"));
}
