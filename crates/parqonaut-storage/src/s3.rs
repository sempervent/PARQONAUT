//! S3-compatible object storage backend (AWS S3, MinIO, etc.).

mod config;
mod error;
mod metadata;
mod read;
mod write;

pub use config::S3Config;

use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::backend::{ByteRange, ListOptions, ListPage, StorageBackend};
use crate::capabilities::StorageCapabilities;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation, S3Location};
use crate::metadata::ObjectMetadata;
use crate::metrics::{StorageMetrics, StorageMetricsCollector};
use crate::stream::{ObjectReadStream, ObjectWriteStream};

use self::error::{map_delete_error, map_get_error, map_head_error, map_list_error, map_put_error};
use self::metadata::{head_to_metadata, list_object_to_metadata};
use self::read::body_reader;
use self::write::open_write_stream;

/// S3-compatible storage backend using the AWS SDK credential provider chain.
pub struct S3StorageBackend {
    client: Client,
    metrics: Arc<StorageMetricsCollector>,
}

impl S3StorageBackend {
    pub async fn new(config: S3Config) -> Self {
        let client = config.build_client().await;
        Self { client, metrics: Arc::new(StorageMetricsCollector::default()) }
    }

    pub fn from_client(client: Client) -> Self {
        Self { client, metrics: Arc::new(StorageMetricsCollector::default()) }
    }

    pub fn with_metrics(client: Client, metrics: Arc<StorageMetricsCollector>) -> Self {
        Self { client, metrics }
    }

    fn s3_dataset(dataset: &DatasetLocation) -> Result<&S3Location, StorageError> {
        match dataset {
            DatasetLocation::S3(loc) => Ok(loc),
            _ => Err(StorageError::InvalidLocation {
                message: format!("expected S3 dataset, got {}", dataset.backend_name()),
            }),
        }
    }

    fn s3_object(object: &ObjectLocation) -> Result<(&str, &str), StorageError> {
        match object {
            ObjectLocation::S3 { bucket, key } => Ok((bucket.as_str(), key.as_str())),
            _ => Err(StorageError::InvalidLocation {
                message: format!("expected S3 object, got {}", object.display_uri()),
            }),
        }
    }
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::S3
    }

    fn metrics(&self) -> StorageMetrics {
        self.metrics.snapshot()
    }

    async fn list(
        &self,
        dataset: &DatasetLocation,
        options: ListOptions,
        continuation_token: Option<&str>,
    ) -> Result<ListPage, StorageError> {
        self.metrics.record_list();
        let s3 = Self::s3_dataset(dataset)?;
        let location = s3.display_uri();

        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&s3.bucket)
            .set_prefix(if s3.prefix.is_empty() { None } else { Some(s3.prefix.clone()) });

        if !options.recursive {
            request = request.delimiter("/");
        }
        if let Some(max) = options.max_keys {
            request = request.max_keys(max as i32);
        }
        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let output = request.send().await.map_err(|e| map_list_error(&location, e))?;

        let mut objects = output
            .contents()
            .iter()
            .filter(|obj| obj.key().is_some())
            .map(|obj| list_object_to_metadata(&s3.bucket, obj))
            .collect::<Vec<_>>();

        objects.sort_by_key(|a| a.location.display_uri());

        let mut prefixes: Vec<String> = output
            .common_prefixes()
            .iter()
            .filter_map(|p| p.prefix().map(str::to_string))
            .collect();
        prefixes.sort();

        Ok(ListPage {
            objects,
            prefixes,
            truncated: output.is_truncated().unwrap_or(false),
            continuation_token: output.next_continuation_token().map(str::to_string),
        })
    }

    async fn head(&self, object: &ObjectLocation) -> Result<ObjectMetadata, StorageError> {
        self.metrics.record_head();
        let (bucket, key) = Self::s3_object(object)?;
        let location = object.display_uri();

        let output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_head_error(&location, e))?;

        Ok(head_to_metadata(
            bucket,
            key,
            output.content_length().unwrap_or(0),
            output.e_tag().map(str::to_string),
            output.version_id().map(str::to_string),
            output.last_modified().cloned(),
        ))
    }

    async fn read_range(
        &self,
        object: &ObjectLocation,
        range: ByteRange,
    ) -> Result<Bytes, StorageError> {
        let (bucket, key) = Self::s3_object(object)?;
        let location = object.display_uri();
        let range_header = format!("bytes={}-{}", range.start, range.end);

        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range_header)
            .send()
            .await
            .map_err(|e| map_get_error(&location, e))?;

        let data = output
            .body
            .collect()
            .await
            .map_err(|e| StorageError::Other { message: e.to_string() })?
            .into_bytes();

        self.metrics.record_range_get(data.len() as u64);
        Ok(data)
    }

    async fn read_stream(
        &self,
        object: &ObjectLocation,
        range: Option<ByteRange>,
    ) -> Result<ObjectReadStream, StorageError> {
        let (bucket, key) = Self::s3_object(object)?;
        let location = object.display_uri();

        let mut request = self.client.get_object().bucket(bucket).key(key);
        if let Some(r) = range {
            request = request.range(format!("bytes={}-{}", r.start, r.end));
        }

        let output = request.send().await.map_err(|e| map_get_error(&location, e))?;

        if range.is_some() {
            self.metrics.record_range_get(0);
        } else {
            self.metrics.record_full_get(0);
        }

        Ok(ObjectReadStream::new(body_reader(output.body)))
    }

    async fn write_stream(
        &self,
        object: &ObjectLocation,
        _content_length: Option<u64>,
    ) -> Result<ObjectWriteStream, StorageError> {
        let (bucket, key) = Self::s3_object(object)?;
        open_write_stream(
            self.client.clone(),
            bucket.to_string(),
            key.to_string(),
            self.metrics.clone(),
        )
        .await
    }

    async fn conditional_create(
        &self,
        object: &ObjectLocation,
        condition: ConditionalCreate,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        if !condition.if_absent {
            return Err(StorageError::UnsupportedCapability {
                capability: "conditional_create without if_absent".into(),
            });
        }

        let (bucket, key) = Self::s3_object(object)?;
        let location = object.display_uri();
        let len = data.len() as u64;

        let output = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.into())
            .if_none_match("*")
            .send()
            .await
            .map_err(|e| map_put_error(&location, e, true))?;

        self.metrics.record_put(len);

        Ok(head_to_metadata(
            bucket,
            key,
            len as i64,
            output.e_tag().map(str::to_string),
            output.version_id().map(str::to_string),
            None,
        ))
    }

    async fn conditional_replace(
        &self,
        object: &ObjectLocation,
        condition: ConditionalReplace,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        let (bucket, key) = Self::s3_object(object)?;
        let location = object.display_uri();
        let len = data.len() as u64;

        let mut request = self.client.put_object().bucket(bucket).key(key).body(data.into());

        if let Some(etag) = &condition.expected_etag {
            request = request.if_match(etag);
        }
        let _ = &condition.expected_version_id;

        let output = request.send().await.map_err(|e| map_put_error(&location, e, false))?;

        self.metrics.record_put(len);

        Ok(head_to_metadata(
            bucket,
            key,
            len as i64,
            output.e_tag().map(str::to_string),
            output.version_id().map(str::to_string),
            None,
        ))
    }

    async fn delete_owned_object(&self, object: &ObjectLocation) -> Result<(), StorageError> {
        let (bucket, key) = Self::s3_object(object)?;
        let location = object.display_uri();

        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_delete_error(&location, e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_config_minio_defaults() {
        let cfg = S3Config::minio("http://127.0.0.1:9000");
        assert!(cfg.path_style);
        assert_eq!(cfg.region.as_deref(), Some("us-east-1"));
    }

    #[tokio::test]
    async fn rejects_non_s3_dataset() {
        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::new()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .build(),
        );
        let backend = S3StorageBackend::from_client(client);
        let dataset = DatasetLocation::parse("file:///tmp").unwrap();
        let err = backend.list(&dataset, ListOptions::default(), None).await.unwrap_err();
        assert!(matches!(err, StorageError::InvalidLocation { .. }));
    }
}
