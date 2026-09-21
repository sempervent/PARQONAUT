#![cfg(feature = "s3")]

mod common;

use std::sync::Arc;

use parqonaut_storage::backend::{ByteRange, StorageBackend};
use parqonaut_storage::location::ObjectLocation;
use parqonaut_storage::{S3Config, S3StorageBackend};
use sha2::{Digest, Sha256};

const MIB: usize = 1024 * 1024;
const PART: usize = 5 * MIB;

fn deterministic_payload(total: usize) -> Vec<u8> {
    (0..total).map(|i| (i % 251) as u8).collect()
}

async fn stream_and_verify(
    backend: &Arc<S3StorageBackend>,
    bucket: &str,
    payload: &[u8],
    chunk: usize,
) {
    let key =
        format!("multipart-boundary/{}-{}-{}.bin", payload.len(), chunk, uuid::Uuid::new_v4());
    let loc = ObjectLocation::S3 { bucket: bucket.into(), key };
    let expected_hash = hex::encode(Sha256::digest(payload));

    let mut w = backend.write_stream(&loc, None).await.expect("open write stream");
    let mut off = 0usize;
    while off < payload.len() {
        let end = (off + chunk).min(payload.len());
        w.write_all(&payload[off..end]).await.expect("write chunk");
        off = end;
    }
    let accepted = w.finish().await.expect("finish");
    assert_eq!(accepted, payload.len() as u64);

    let meta = backend.head(&loc).await.expect("head");
    assert_eq!(meta.size, payload.len() as u64);

    let end = payload.len().saturating_sub(1);
    let got = backend
        .read_range(&loc, ByteRange::new(0, end as u64).expect("range"))
        .await
        .expect("read object");
    assert_eq!(got.len(), payload.len());
    assert_eq!(hex::encode(Sha256::digest(&got)), expected_hash);
}

#[tokio::test(flavor = "multi_thread")]
async fn multipart_default_part_size_boundary_matrix() {
    let Some(endpoint) = common::require_s3_endpoint() else {
        return;
    };
    std::env::remove_var("PARQONAUT_S3_PART_SIZE_BYTES");
    let backend = Arc::new(S3StorageBackend::new(S3Config::minio(endpoint)).await);
    let bucket = std::env::var("PARQONAUT_S3_BUCKET").unwrap_or_else(|_| "parqonaut-test".into());

    let sizes = [
        PART - 1,
        PART,
        PART + 1,
        (5 * MIB) + (300 * 1024),
        2 * PART - 1,
        2 * PART,
        2 * PART + 1,
        (3 * PART) + (PART / 2),
    ];
    let chunks = [1usize, 4 * 1024, MIB, 3 * MIB, 6 * MIB];

    for size in sizes {
        let payload = deterministic_payload(size);
        for chunk in chunks {
            stream_and_verify(&backend, &bucket, &payload, chunk).await;
        }
    }
}
