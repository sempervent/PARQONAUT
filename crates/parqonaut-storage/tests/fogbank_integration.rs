#![cfg(feature = "s3")]

//! FOGBANK laboratory integration tests (MinIO required).
//!
//! Run after `just phase5-up && just phase5-fixtures`.

use parqonaut_storage::backend::{ByteRange, StorageBackend};
use parqonaut_storage::fingerprint::compute_remote_fingerprint;
use parqonaut_storage::inventory::list_remote_inventory;
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::{S3Config, S3StorageBackend};

fn minio_config() -> Option<S3Config> {
    let endpoint = std::env::var("MINIO_ENDPOINT").ok()?;
    Some(S3Config::minio(endpoint))
}

fn fogbank_bucket() -> String {
    std::env::var("FOGBANK_BUCKET").unwrap_or_else(|_| "fogbank".into())
}

fn dataset_prefix() -> String {
    std::env::var("FOGBANK_DATASET_PREFIX").unwrap_or_else(|_| "datasets".into())
}

fn fogbank_manifest(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/phase5/manifests")
        .join(name)
}

async fn backend() -> Option<S3StorageBackend> {
    let config = minio_config()?;
    Some(S3StorageBackend::new(config).await)
}

#[tokio::test]
async fn fogbank_healthy_dataset_present() {
    let Some(backend) = backend().await else {
        eprintln!("skipping FOGBANK test: MINIO_ENDPOINT not set");
        return;
    };

    let bucket = fogbank_bucket();
    let prefix = dataset_prefix();
    let dataset =
        DatasetLocation::parse(&format!("s3://{bucket}/{prefix}/healthy/")).expect("parse");

    let inventory = list_remote_inventory(&backend, &dataset).await.expect("inventory");

    assert!(
        inventory.objects.len() >= 2,
        "expected healthy dataset objects, got {}",
        inventory.objects.len()
    );

    let fp = compute_remote_fingerprint(&inventory);
    assert!(!fp.digest.is_empty());
    assert_eq!(fp.entries.len(), inventory.objects.len());
}

#[tokio::test]
async fn fogbank_large_parquet_supports_footer_range_read() {
    let Some(backend) = backend().await else {
        eprintln!("skipping FOGBANK test: MINIO_ENDPOINT not set");
        return;
    };

    let bucket = fogbank_bucket();
    let prefix = dataset_prefix();
    let object = ObjectLocation::S3 {
        bucket: bucket.clone(),
        key: format!("{prefix}/large-parquet/wide-table.parquet"),
    };

    let head = match backend.head(&object).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("skipping FOGBANK large-parquet test: {e} (run just phase5-fixtures)");
            return;
        }
    };

    assert!(
        head.size >= 1_000_000,
        "large-parquet fixture should exceed 1 MiB, got {} bytes",
        head.size
    );

    let tail_start = head.size.saturating_sub(8191);
    let range = ByteRange::new(tail_start, head.size - 1).expect("range");
    let bytes = backend.read_range(&object, range).await.expect("read_range");
    assert!(!bytes.is_empty());
    assert!(bytes.len() <= 8192);
}

#[tokio::test]
async fn fogbank_stale_baseline_matches_manifest_when_present() {
    let Some(backend) = backend().await else {
        eprintln!("skipping FOGBANK test: MINIO_ENDPOINT not set");
        return;
    };

    let bucket = fogbank_bucket();
    let prefix = dataset_prefix();
    let dataset =
        DatasetLocation::parse(&format!("s3://{bucket}/{prefix}/stale-source/")).expect("parse");

    let inventory = list_remote_inventory(&backend, &dataset).await.expect("inventory");

    assert_eq!(
        inventory.objects.len(),
        3,
        "stale-source baseline should have 3 snapshots before mutation"
    );

    let manifest_path = fogbank_manifest("stale-baseline-fingerprint.json");
    if !manifest_path.exists() {
        eprintln!(
            "skipping manifest comparison: {} not found (run phase5-fixtures)",
            manifest_path.display()
        );
        return;
    }

    let expected: parqonaut_storage::fingerprint::RemoteFingerprint =
        serde_json::from_slice(&std::fs::read(manifest_path).expect("read manifest"))
            .expect("json");
    let current = compute_remote_fingerprint(&inventory);
    expected.verify_against(&current).expect("baseline fingerprint");
}
