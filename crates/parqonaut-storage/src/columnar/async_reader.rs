use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::backend::{ByteRange, StorageBackend};
use crate::error::StorageError;
use crate::location::ObjectLocation;
use crate::parquet_range::read_parquet_footer;

/// Range-based Parquet reader over [`StorageBackend`] (no full-object download).
pub struct StorageAsyncFileReader<B: StorageBackend + ?Sized> {
    backend: Arc<B>,
    object: ObjectLocation,
    metadata: Arc<ParquetMetaData>,
}

impl<B: StorageBackend + ?Sized> StorageAsyncFileReader<B> {
    pub async fn open(backend: Arc<B>, object: ObjectLocation) -> Result<Self, StorageError> {
        if !backend.capabilities().range_reads {
            return Err(StorageError::UnsupportedCapability { capability: "range_reads".into() });
        }
        let _head = backend.head(&object).await?;
        let metadata = load_metadata(backend.as_ref(), &object).await?;
        Ok(Self { backend, object, metadata })
    }

    pub fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }
}

async fn load_metadata<B: StorageBackend + ?Sized>(
    backend: &B,
    object: &ObjectLocation,
) -> Result<Arc<ParquetMetaData>, StorageError> {
    let (footer, _) = read_parquet_footer(backend, object).await?;
    let trailer: [u8; 8] = footer.footer[footer.footer.len() - 8..].try_into().map_err(|_| {
        StorageError::InvalidLocation { message: "invalid Parquet footer trailer".into() }
    })?;
    let metadata_len = ParquetMetaDataReader::decode_footer_tail(&trailer)
        .map_err(|e| StorageError::InvalidLocation {
            message: format!("Parquet footer decode: {e}"),
        })?
        .metadata_length();
    if metadata_len + 8 != footer.footer.len() {
        return Err(StorageError::InvalidLocation {
            message: format!(
                "footer buffer length {} does not match metadata length {metadata_len}",
                footer.footer.len()
            ),
        });
    }
    let metadata_bytes = &footer.footer[..metadata_len];
    let meta = ParquetMetaDataReader::decode_metadata(metadata_bytes).map_err(|e| {
        StorageError::InvalidLocation { message: format!("Parquet metadata decode: {e}") }
    })?;
    Ok(Arc::new(meta))
}

impl<B: StorageBackend + ?Sized> AsyncFileReader for StorageAsyncFileReader<B> {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> futures::future::BoxFuture<'_, ParquetResult<Bytes>> {
        if range.end <= range.start {
            return async move { Ok(Bytes::new()) }.boxed();
        }
        let start = range.start as u64;
        let end = range.end.saturating_sub(1) as u64;
        let backend = Arc::clone(&self.backend);
        let object = self.object.clone();
        async move {
            let bytes = backend
                .read_range(&object, ByteRange::new(start, end).map_err(map_storage_err)?)
                .await
                .map_err(map_storage_err)?;
            Ok(bytes)
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> futures::future::BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}

fn map_storage_err(err: StorageError) -> ParquetError {
    ParquetError::General(err.to_string())
}
