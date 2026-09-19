//! Range-based Parquet footer retrieval via [`StorageBackend::read_range`].
//!
//! Reads the 8-byte footer trailer first, then fetches the metadata block in a second
//! ranged request when needed. Request and byte counters come from backend metrics.

use bytes::Bytes;

use crate::backend::{ByteRange, StorageBackend};
use crate::error::StorageError;
use crate::location::ObjectLocation;
use crate::metrics::StorageMetrics;

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
const FOOTER_TRAILER_LEN: u64 = 8;

/// Metrics delta observed while reading a Parquet footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParquetRangeMetrics {
    pub head_requests: u64,
    pub range_get_requests: u64,
    pub range_bytes_read: u64,
}

/// Raw Parquet footer bytes (Thrift metadata + 8-byte trailer ending in `PAR1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFooterBytes {
    pub footer: Bytes,
    pub metadata_len: u32,
    pub object_size: u64,
}

impl ParquetFooterBytes {
    pub fn magic_valid(&self) -> bool {
        self.footer.len() >= 8 && &self.footer[self.footer.len() - 4..] == PARQUET_MAGIC
    }
}

/// Read the Parquet footer for `object` using bounded range GETs.
pub async fn read_parquet_footer<B: StorageBackend>(
    backend: &B,
    object: &ObjectLocation,
) -> Result<(ParquetFooterBytes, ParquetRangeMetrics), StorageError> {
    if !backend.capabilities().range_reads {
        return Err(StorageError::UnsupportedCapability { capability: "range_reads".into() });
    }

    let before = backend.metrics();
    let meta = backend.head(object).await?;
    let size = meta.size;
    if size < FOOTER_TRAILER_LEN {
        return Err(StorageError::InvalidLocation {
            message: format!("object too small for Parquet footer: {size} bytes"),
        });
    }

    let trailer_start = size - FOOTER_TRAILER_LEN;
    let trailer = backend.read_range(object, ByteRange::new(trailer_start, size - 1)?).await?;
    let metadata_len = parse_metadata_length(&trailer)?;

    let footer_start =
        size.checked_sub(u64::from(metadata_len) + FOOTER_TRAILER_LEN).ok_or_else(|| {
            StorageError::InvalidLocation {
                message: format!("invalid Parquet metadata length {metadata_len} for size {size}"),
            }
        })?;

    let footer = if footer_start == trailer_start {
        trailer
    } else {
        backend.read_range(object, ByteRange::new(footer_start, size - 1)?).await?
    };

    if footer.len() < FOOTER_TRAILER_LEN as usize || &footer[footer.len() - 4..] != PARQUET_MAGIC {
        return Err(StorageError::InvalidLocation {
            message: "Parquet footer missing PAR1 magic".into(),
        });
    }

    let after = backend.metrics();
    Ok((
        ParquetFooterBytes { footer, metadata_len, object_size: size },
        metrics_delta(before, after),
    ))
}

fn parse_metadata_length(trailer: &Bytes) -> Result<u32, StorageError> {
    if trailer.len() != FOOTER_TRAILER_LEN as usize {
        return Err(StorageError::InvalidLocation {
            message: format!("expected {FOOTER_TRAILER_LEN}-byte Parquet trailer"),
        });
    }
    if &trailer[4..8] != PARQUET_MAGIC {
        return Err(StorageError::InvalidLocation {
            message: "Parquet trailer missing PAR1 magic".into(),
        });
    }
    Ok(u32::from_le_bytes(trailer[0..4].try_into().unwrap()))
}

fn metrics_delta(before: StorageMetrics, after: StorageMetrics) -> ParquetRangeMetrics {
    ParquetRangeMetrics {
        head_requests: after.head_requests.saturating_sub(before.head_requests),
        range_get_requests: after.range_get_requests.saturating_sub(before.range_get_requests),
        range_bytes_read: after.range_bytes_read.saturating_sub(before.range_bytes_read),
    }
}

/// Build bytes for a minimal synthetic Parquet object (for tests).
#[cfg(test)]
pub(crate) fn synthetic_parquet_bytes(content_len: usize, metadata: &[u8]) -> Vec<u8> {
    let mut data = vec![0u8; content_len];
    data.extend_from_slice(metadata);
    let metadata_len = u32::try_from(metadata.len()).expect("metadata fits in u32");
    data.extend_from_slice(&metadata_len.to_le_bytes());
    data.extend_from_slice(PARQUET_MAGIC);
    data
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::backend::StorageBackend;
    use crate::capabilities::StorageCapabilities;
    use crate::conditional::ConditionalCreate;
    use crate::location::ObjectLocation;
    use crate::memory::MemoryStorageBackend;

    fn memory_backend() -> MemoryStorageBackend {
        MemoryStorageBackend::new(StorageCapabilities::LOCAL)
    }

    async fn put_object(backend: &MemoryStorageBackend, key: &str, data: Vec<u8>) {
        backend
            .conditional_create(
                &ObjectLocation::S3 { bucket: "parquet".into(), key: key.into() },
                ConditionalCreate::must_not_exist(),
                Bytes::from(data),
            )
            .await
            .expect("conditional create");
    }

    #[tokio::test]
    async fn parquet_footer_single_range_when_metadata_empty() {
        let backend = Arc::new(memory_backend());
        let object =
            ObjectLocation::S3 { bucket: "parquet".into(), key: "empty-meta.parquet".into() };
        let bytes = synthetic_parquet_bytes(16, &[]);
        put_object(&backend, "empty-meta.parquet", bytes).await;

        let (footer, metrics) = read_parquet_footer(backend.as_ref(), &object).await.unwrap();
        assert_eq!(footer.metadata_len, 0);
        assert!(footer.magic_valid());
        // One explicit head plus one head inside `read_range` (memory backend).
        assert_eq!(metrics.head_requests, 2);
        assert_eq!(metrics.range_get_requests, 1);
        assert_eq!(metrics.range_bytes_read, FOOTER_TRAILER_LEN);
    }

    #[tokio::test]
    async fn parquet_footer_two_ranges_for_non_empty_metadata() {
        let backend = Arc::new(memory_backend());
        let object =
            ObjectLocation::S3 { bucket: "parquet".into(), key: "with-meta.parquet".into() };
        let metadata = b"not-valid-thrift-but-length-matters";
        let bytes = synthetic_parquet_bytes(32, metadata);
        put_object(&backend, "with-meta.parquet", bytes.clone()).await;

        let (footer, metrics) = read_parquet_footer(backend.as_ref(), &object).await.unwrap();
        assert_eq!(footer.metadata_len, metadata.len() as u32);
        assert_eq!(footer.footer.len(), metadata.len() + 8);
        assert!(footer.magic_valid());
        // One explicit head plus one head per `read_range` call (memory backend).
        assert_eq!(metrics.head_requests, 3);
        assert_eq!(metrics.range_get_requests, 2);
        assert_eq!(
            metrics.range_bytes_read,
            metadata.len() as u64 + FOOTER_TRAILER_LEN + FOOTER_TRAILER_LEN
        );
    }

    #[tokio::test]
    async fn parquet_footer_rejects_invalid_magic() {
        let backend = Arc::new(memory_backend());
        let object = ObjectLocation::S3 { bucket: "parquet".into(), key: "bad.parquet".into() };
        put_object(&backend, "bad.parquet", vec![0u8; 16]).await;
        let err = read_parquet_footer(backend.as_ref(), &object).await.unwrap_err();
        assert!(matches!(err, StorageError::InvalidLocation { .. }));
    }

    #[tokio::test]
    async fn parquet_footer_requires_range_reads_capability() {
        let backend = MemoryStorageBackend::new(StorageCapabilities {
            range_reads: false,
            conditional_create: true,
            ..StorageCapabilities::LOCAL
        });
        let object = ObjectLocation::S3 { bucket: "parquet".into(), key: "x.parquet".into() };
        let err = read_parquet_footer(&backend, &object).await.unwrap_err();
        assert!(matches!(err, StorageError::UnsupportedCapability { .. }));
    }
}
