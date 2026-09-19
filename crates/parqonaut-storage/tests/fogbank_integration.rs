#![cfg(feature = "s3")]

//! FOGBANK laboratory integration tests (S3-compatible backend, RustFS in CI).
//!
//! Run after `just phase5-up && just phase5-fixtures`.

mod common;

use parqonaut_storage::backend::{ByteRange, StorageBackend};
use parqonaut_storage::fingerprint::compute_remote_fingerprint;
use parqonaut_storage::inventory::list_remote_inventory;
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::{S3Config, S3StorageBackend};

fn s3_config() -> Option<S3Config> {
    common::require_s3_endpoint().map(S3Config::minio)
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
    let config = s3_config()?;
    Some(S3StorageBackend::new(config).await)
}

fn integration_required() -> bool {
    std::env::var("PARQONAUT_S3_INTEGRATION").as_deref() == Ok("1")
}

#[tokio::test]
async fn fogbank_healthy_dataset_present() {
    let Some(backend) = backend().await else {
        eprintln!("skipping FOGBANK test: no S3 endpoint configured");
        return;
    };

    let bucket = common::fogbank_bucket();
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
        eprintln!("skipping FOGBANK test: no S3 endpoint configured");
        return;
    };

    let bucket = common::fogbank_bucket();
    let prefix = dataset_prefix();
    let object = ObjectLocation::S3 {
        bucket: bucket.clone(),
        key: format!("{prefix}/large-parquet/wide-table.parquet"),
    };

    let head = match backend.head(&object).await {
        Ok(h) => h,
        Err(e) if integration_required() => {
            panic!("FOGBANK large-parquet head failed in integration CI: {e}");
        }
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
        eprintln!("skipping FOGBANK test: no S3 endpoint configured");
        return;
    };

    let bucket = common::fogbank_bucket();
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
        if integration_required() {
            panic!(
                "integration CI requires manifest at {}",
                manifest_path.display()
            );
        }
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
