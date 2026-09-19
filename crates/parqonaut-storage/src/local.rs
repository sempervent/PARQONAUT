//! Local filesystem storage backend.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, ReadBuf, SeekFrom};
use tokio::sync::Mutex;

use crate::backend::{ByteRange, ListOptions, ListPage, StorageBackend};
use crate::capabilities::StorageCapabilities;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, LocalLocation, ObjectLocation, S3Location};
use crate::metadata::ObjectMetadata;
use crate::metrics::{StorageMetrics, StorageMetricsCollector};
use crate::stream::{ObjectReadStream, ObjectWriteStream};

/// Filesystem-backed storage rooted at an optional base directory.
///
/// `ObjectLocation::Local` paths are used as-is when absolute, otherwise joined
/// with `base`. `ObjectLocation::S3` and `DatasetLocation::S3` map to
/// `{base}/{bucket}/{key}` so shared contract tests can run against a temp dir.
pub struct LocalStorageBackend {
    base: Option<PathBuf>,
    metrics: Arc<StorageMetricsCollector>,
}

impl LocalStorageBackend {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: Some(base.as_ref().to_path_buf()),
            metrics: Arc::new(StorageMetricsCollector::default()),
        }
    }

    /// Use absolute local paths directly without a sandbox base.
    pub fn direct() -> Self {
        Self { base: None, metrics: Arc::new(StorageMetricsCollector::default()) }
    }

    fn dataset_root(&self, dataset: &DatasetLocation) -> Result<PathBuf, StorageError> {
        match dataset {
            DatasetLocation::Local(LocalLocation { path }) => {
                self.resolve_local_path(Utf8Path::new(path.as_str()))
            }
            DatasetLocation::S3(s3) => self.resolve_s3_prefix(s3),
        }
    }

    fn object_path(&self, object: &ObjectLocation) -> Result<PathBuf, StorageError> {
        match object {
            ObjectLocation::Local { path } => self.resolve_local_path(path),
            ObjectLocation::S3 { bucket, key } => self.resolve_s3_object(bucket, key),
        }
    }

    fn resolve_local_path(&self, path: &Utf8Path) -> Result<PathBuf, StorageError> {
        let path = PathBuf::from(path.as_str());
        if path.is_absolute() {
            return Ok(path);
        }
        if let Some(base) = &self.base {
            return Ok(base.join(path));
        }
        Ok(path)
    }

    fn resolve_s3_prefix(&self, s3: &S3Location) -> Result<PathBuf, StorageError> {
        let base = self.base.as_ref().ok_or_else(|| StorageError::InvalidLocation {
            message: "S3 dataset locations require a local base directory".into(),
        })?;
        let mut path = base.join(&s3.bucket);
        if !s3.prefix.is_empty() {
            path.push(s3.prefix.trim_end_matches('/'));
        }
        Ok(path)
    }

    fn resolve_s3_object(&self, bucket: &str, key: &str) -> Result<PathBuf, StorageError> {
        let base = self.base.as_ref().ok_or_else(|| StorageError::InvalidLocation {
            message: "S3 object locations require a local base directory".into(),
        })?;
        if key.contains("..") {
            return Err(StorageError::InvalidLocation {
                message: "object key must not contain '..'".into(),
            });
        }
        Ok(base.join(bucket).join(key))
    }

    async fn metadata_for_path(
        &self,
        location: ObjectLocation,
        path: &Path,
    ) -> Result<ObjectMetadata, StorageError> {
        let meta = fs::metadata(path).await.map_err(|e| map_io_error(e, location.display_uri()))?;
        if !meta.is_file() {
            return Err(StorageError::NotFound { location: location.display_uri() });
        }
        Ok(build_metadata(location, &meta))
    }

    async fn ensure_parent(path: &Path) -> Result<(), StorageError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::LOCAL
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
        let root = self.dataset_root(dataset)?;
        if !root.exists() {
            return Ok(ListPage {
                objects: Vec::new(),
                prefixes: Vec::new(),
                truncated: false,
                continuation_token: None,
            });
        }

        let mut entries = collect_list_entries(dataset, &root, options.recursive).await?;
        entries.sort_by_key(|entry| entry.metadata.location.display_uri());

        let start = continuation_token
            .map(|token| {
                entries
                    .iter()
                    .position(|entry| entry.metadata.location.display_uri() == token)
                    .map(|idx| idx + 1)
                    .unwrap_or(entries.len())
            })
            .unwrap_or(0);

        let limit = options.max_keys.unwrap_or(entries.len());
        let total = entries.len();
        let page_entries = entries.into_iter().skip(start).take(limit).collect::<Vec<_>>();
        let truncated = start + page_entries.len() < total;
        let continuation_token = if truncated {
            page_entries.last().map(|entry| entry.metadata.location.display_uri())
        } else {
            None
        };

        let mut objects = Vec::new();
        let mut prefix_set = BTreeSet::new();
        for entry in page_entries {
            if options.recursive || entry.is_file {
                objects.push(entry.metadata);
            } else if let Some(prefix) = entry.prefix {
                prefix_set.insert(prefix);
            }
        }

        Ok(ListPage {
            objects,
            prefixes: prefix_set.into_iter().collect(),
            truncated,
            continuation_token,
        })
    }

    async fn head(&self, object: &ObjectLocation) -> Result<ObjectMetadata, StorageError> {
        self.metrics.record_head();
        let path = self.object_path(object)?;
        self.metadata_for_path(object.clone(), &path).await
    }

    async fn read_range(
        &self,
        object: &ObjectLocation,
        range: ByteRange,
    ) -> Result<Bytes, StorageError> {
        let path = self.object_path(object)?;
        let meta = fs::metadata(&path).await.map_err(|e| map_io_error(e, object.display_uri()))?;
        if !meta.is_file() {
            return Err(StorageError::NotFound { location: object.display_uri() });
        }

        let file_len = meta.len();
        if range.start >= file_len {
            return Err(StorageError::InvalidLocation {
                message: "range start beyond object size".into(),
            });
        }
        let end = range.end.min(file_len.saturating_sub(1));

        let mut file =
            File::open(&path).await.map_err(|e| map_io_error(e, object.display_uri()))?;
        file.seek(SeekFrom::Start(range.start)).await?;
        let len = (end - range.start + 1) as usize;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;
        self.metrics.record_range_get(len as u64);
        Ok(Bytes::from(buf))
    }

    async fn read_stream(
        &self,
        object: &ObjectLocation,
        range: Option<ByteRange>,
    ) -> Result<ObjectReadStream, StorageError> {
        let path = self.object_path(object)?;
        let meta = fs::metadata(&path).await.map_err(|e| map_io_error(e, object.display_uri()))?;
        if !meta.is_file() {
            return Err(StorageError::NotFound { location: object.display_uri() });
        }

        let mut file =
            File::open(&path).await.map_err(|e| map_io_error(e, object.display_uri()))?;
        let bounded_len = if let Some(r) = range {
            file.seek(SeekFrom::Start(r.start)).await?;
            let end = r.end.min(meta.len().saturating_sub(1));
            Some(end.saturating_sub(r.start) + 1)
        } else {
            self.metrics.record_full_get(meta.len());
            None
        };
        Ok(ObjectReadStream::new(RangeLimitedReader { file, bounded_len, read: 0 }))
    }

    async fn write_stream(
        &self,
        object: &ObjectLocation,
        content_length: Option<u64>,
    ) -> Result<ObjectWriteStream, StorageError> {
        let path = self.object_path(object)?;
        Self::ensure_parent(&path).await?;
        let temp_path = temp_path_for(&path);
        let file =
            OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path).await?;
        Ok(ObjectWriteStream::new(LocalWriteStream {
            temp_path,
            final_path: path,
            file: Mutex::new(file),
            content_length,
            bytes_written: 0,
            finalized: false,
        }))
    }

    async fn conditional_create(
        &self,
        object: &ObjectLocation,
        condition: ConditionalCreate,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        let path = self.object_path(object)?;
        Self::ensure_parent(&path).await?;

        if condition.if_absent {
            match OpenOptions::new().write(true).create_new(true).open(&path).await {
                Ok(mut file) => {
                    file.write_all(&data).await?;
                    file.sync_all().await?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(StorageError::Conflict {
                        message: format!("object already exists: {}", object.display_uri()),
                    });
                }
                Err(e) => return Err(map_io_error(e, object.display_uri())),
            }
        } else {
            let mut file =
                OpenOptions::new().write(true).create(true).truncate(true).open(&path).await?;
            file.write_all(&data).await?;
            file.sync_all().await?;
        }

        self.metrics.record_put(data.len() as u64);
        self.metadata_for_path(object.clone(), &path).await
    }

    async fn conditional_replace(
        &self,
        object: &ObjectLocation,
        condition: ConditionalReplace,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        let path = self.object_path(object)?;
        let current = self.metadata_for_path(object.clone(), &path).await?;

        if let Some(expected) = &condition.expected_etag {
            if current.etag.as_deref() != Some(expected.as_str()) {
                return Err(StorageError::PreconditionFailed { message: "etag mismatch".into() });
            }
        }
        if let Some(expected_version) = &condition.expected_version_id {
            if current.version_id.as_deref() != Some(expected_version.as_str()) {
                return Err(StorageError::PreconditionFailed {
                    message: "version_id mismatch".into(),
                });
            }
        }

        let temp_path = temp_path_for(&path);
        {
            let mut file =
                OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path).await?;
            file.write_all(&data).await?;
            file.sync_all().await?;
        }
        fs::rename(&temp_path, &path).await?;
        self.metrics.record_put(data.len() as u64);
        self.metadata_for_path(object.clone(), &path).await
    }

    async fn delete_owned_object(&self, object: &ObjectLocation) -> Result<(), StorageError> {
        let path = self.object_path(object)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(map_io_error(e, object.display_uri())),
        }
    }
}

struct ListEntry {
    metadata: ObjectMetadata,
    is_file: bool,
    prefix: Option<String>,
}

async fn collect_list_entries(
    dataset: &DatasetLocation,
    root: &Path,
    recursive: bool,
) -> Result<Vec<ListEntry>, StorageError> {
    let mut entries = Vec::new();
    if recursive {
        collect_recursive(dataset, root, root, &mut entries).await?;
    } else {
        collect_shallow(dataset, root, root, &mut entries).await?;
    }
    Ok(entries)
}

async fn collect_recursive(
    dataset: &DatasetLocation,
    root: &Path,
    dir: &Path,
    entries: &mut Vec<ListEntry>,
) -> Result<(), StorageError> {
    let mut read_dir = fs::read_dir(dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            Box::pin(collect_recursive(dataset, root, &path, entries)).await?;
        } else if file_type.is_file() {
            if let Some(location) = object_location_for_path(dataset, root, &path) {
                let meta = fs::metadata(&path).await?;
                entries.push(ListEntry {
                    metadata: build_metadata(location, &meta),
                    is_file: true,
                    prefix: None,
                });
            }
        }
    }
    Ok(())
}

async fn collect_shallow(
    dataset: &DatasetLocation,
    root: &Path,
    dir: &Path,
    entries: &mut Vec<ListEntry>,
) -> Result<(), StorageError> {
    let mut read_dir = fs::read_dir(dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            if let Some(relative) = path.strip_prefix(root).ok().and_then(to_utf8_path) {
                let prefix = format!("{}/", relative.as_str().trim_end_matches('/'));
                entries.push(ListEntry {
                    metadata: ObjectMetadata {
                        location: object_location_for_relative(dataset, relative.as_str()),
                        size: 0,
                        etag: None,
                        version_id: None,
                        last_modified: None,
                    },
                    is_file: false,
                    prefix: Some(prefix),
                });
            }
        } else if file_type.is_file() {
            if let Some(location) = object_location_for_path(dataset, root, &path) {
                let meta = fs::metadata(&path).await?;
                entries.push(ListEntry {
                    metadata: build_metadata(location, &meta),
                    is_file: true,
                    prefix: None,
                });
            }
        }
    }
    Ok(())
}

fn object_location_for_path(
    dataset: &DatasetLocation,
    root: &Path,
    path: &Path,
) -> Option<ObjectLocation> {
    let relative = path.strip_prefix(root).ok()?;
    let relative = normalize_path(relative);
    let relative = Utf8Path::from_path(&relative)?;
    Some(object_location_for_relative(dataset, relative.as_str()))
}

fn object_location_for_relative(dataset: &DatasetLocation, relative: &str) -> ObjectLocation {
    match dataset {
        DatasetLocation::Local(_) => ObjectLocation::Local { path: Utf8PathBuf::from(relative) },
        DatasetLocation::S3(s3) => {
            let key = if s3.prefix.is_empty() {
                relative.to_string()
            } else {
                format!("{}/{}", s3.prefix.trim_end_matches('/'), relative)
            };
            ObjectLocation::S3 { bucket: s3.bucket.clone(), key }
        }
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(c) => out.push(c),
            Component::RootDir | Component::Prefix(_) => out.push(component.as_os_str()),
        }
    }
    out
}

fn to_utf8_path(path: &Path) -> Option<&Utf8Path> {
    Utf8Path::from_path(path)
}

fn build_metadata(location: ObjectLocation, meta: &std::fs::Metadata) -> ObjectMetadata {
    ObjectMetadata {
        location,
        size: meta.len(),
        etag: Some(compute_etag(meta)),
        version_id: Some(compute_version_id(meta)),
        last_modified: modified_time(meta),
    }
}

fn compute_etag(meta: &std::fs::Metadata) -> String {
    let modified = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok());
    match modified {
        Some(ts) => format!("local-{}-{}", meta.len(), ts.as_nanos()),
        None => format!("local-{}-unknown", meta.len()),
    }
}

fn compute_version_id(meta: &std::fs::Metadata) -> String {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|ts| ts.as_nanos().to_string())
        .unwrap_or_else(|| "0".into())
}

fn modified_time(meta: &std::fs::Metadata) -> Option<DateTime<Utc>> {
    meta.modified().ok().and_then(|t| {
        DateTime::<Utc>::from_timestamp(t.duration_since(UNIX_EPOCH).ok()?.as_secs() as i64, 0)
    })
}

fn temp_path_for(path: &Path) -> PathBuf {
    path.with_extension(format!("parqonaut-tmp-{}", uuid::Uuid::new_v4()))
}

fn map_io_error(err: std::io::Error, location: String) -> StorageError {
    match err.kind() {
        std::io::ErrorKind::NotFound => StorageError::NotFound { location },
        std::io::ErrorKind::PermissionDenied => StorageError::PermissionDenied { location },
        _ => StorageError::Io(err),
    }
}

struct RangeLimitedReader {
    file: File,
    bounded_len: Option<u64>,
    read: u64,
}

impl AsyncRead for RangeLimitedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(max) = self.bounded_len {
            if self.read >= max {
                return Poll::Ready(Ok(()));
            }
            let allowed = (max - self.read) as usize;
            if buf.remaining() > allowed {
                let mut tmp = ReadBuf::new(&mut buf.initialize_unfilled()[..allowed]);
                let poll = Pin::new(&mut self.file).poll_read(cx, &mut tmp);
                if let Poll::Ready(Ok(())) = poll {
                    let filled = tmp.filled().len();
                    buf.advance(filled);
                    self.read += filled as u64;
                }
                return poll;
            }
        }
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.file).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = poll {
            self.read += (buf.filled().len() - before) as u64;
        }
        poll
    }
}

struct LocalWriteStream {
    temp_path: PathBuf,
    final_path: PathBuf,
    file: Mutex<File>,
    content_length: Option<u64>,
    bytes_written: u64,
    finalized: bool,
}

impl tokio::io::AsyncWrite for LocalWriteStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if let Some(max) = this.content_length {
            if this.bytes_written >= max {
                return Poll::Ready(Ok(0));
            }
            let allowed = (max - this.bytes_written) as usize;
            let to_write = buf.len().min(allowed);
            let mut file = this.file.try_lock().expect("single writer");
            match Pin::new(&mut *file).poll_write(cx, &buf[..to_write]) {
                Poll::Ready(Ok(n)) => {
                    this.bytes_written += n as u64;
                    Poll::Ready(Ok(n))
                }
                other => other,
            }
        } else {
            let mut file = this.file.try_lock().expect("single writer");
            let poll = Pin::new(&mut *file).poll_write(cx, buf);
            if let Poll::Ready(Ok(n)) = poll {
                this.bytes_written += n as u64;
            }
            poll
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let mut file = this.file.try_lock().expect("single writer");
        Pin::new(&mut *file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.finalized {
            return Poll::Ready(Ok(()));
        }
        let mut file = this.file.try_lock().expect("single writer");
        match Pin::new(&mut *file).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                drop(file);
                if let Err(err) = std::fs::rename(&this.temp_path, &this.final_path) {
                    return Poll::Ready(Err(err));
                }
                this.finalized = true;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StorageBackend;
    use crate::conditional::{ConditionalCreate, ConditionalReplace};

    async fn temp_backend() -> (tempfile::TempDir, LocalStorageBackend) {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalStorageBackend::new(dir.path());
        (dir, backend)
    }

    #[tokio::test]
    async fn list_is_deterministically_sorted() {
        let (_dir, backend) = temp_backend().await;
        let dataset = DatasetLocation::parse("s3://bucket/prefix/").unwrap();
        for name in ["c.bin", "a.bin", "b.bin"] {
            let object =
                ObjectLocation::S3 { bucket: "bucket".into(), key: format!("prefix/{name}") };
            backend
                .conditional_create(&object, ConditionalCreate::must_not_exist(), Bytes::from(name))
                .await
                .unwrap();
        }

        let page = backend
            .list(&dataset, ListOptions { recursive: true, max_keys: None }, None)
            .await
            .unwrap();
        let names: Vec<_> = page.objects.iter().map(|m| m.location.display_uri()).collect();
        assert_eq!(
            names,
            vec![
                "s3://bucket/prefix/a.bin",
                "s3://bucket/prefix/b.bin",
                "s3://bucket/prefix/c.bin",
            ]
        );
    }

    #[tokio::test]
    async fn non_recursive_list_returns_immediate_objects() {
        let (_dir, backend) = temp_backend().await;
        let dataset = DatasetLocation::parse("s3://bucket/prefix/").unwrap();
        let nested =
            ObjectLocation::S3 { bucket: "bucket".into(), key: "prefix/nested/deep.bin".into() };
        backend
            .conditional_create(
                &nested,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"x"),
            )
            .await
            .unwrap();

        let page = backend
            .list(&dataset, ListOptions { recursive: false, max_keys: None }, None)
            .await
            .unwrap();
        assert!(page.objects.is_empty());
        assert_eq!(page.prefixes, vec!["nested/"]);
    }

    #[tokio::test]
    async fn ranged_read_and_stream() {
        let (_dir, backend) = temp_backend().await;
        let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "obj.bin".into() };
        backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"hello-world"),
            )
            .await
            .unwrap();

        let range = backend.read_range(&object, ByteRange::new(1, 3).unwrap()).await.unwrap();
        assert_eq!(&range[..], b"ell");

        let mut stream =
            backend.read_stream(&object, Some(ByteRange::new(6, 10).unwrap())).await.unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"world");
    }

    #[tokio::test]
    async fn write_stream_persists_object() {
        let (_dir, backend) = temp_backend().await;
        let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "stream.bin".into() };
        let mut writer = backend.write_stream(&object, Some(5)).await.unwrap();
        writer.write_all(b"hello").await.unwrap();
        assert_eq!(writer.finish().await.unwrap(), 5);

        let head = backend.head(&object).await.unwrap();
        assert_eq!(head.size, 5);
    }

    #[tokio::test]
    async fn conditional_replace_checks_etag() {
        let (_dir, backend) = temp_backend().await;
        let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "obj.bin".into() };
        let created = backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"one"),
            )
            .await
            .unwrap();

        let replaced = backend
            .conditional_replace(
                &object,
                ConditionalReplace::matching(&created),
                Bytes::from_static(b"two"),
            )
            .await
            .unwrap();
        assert_eq!(replaced.size, 3);

        let err = backend
            .conditional_replace(
                &object,
                ConditionalReplace {
                    expected_etag: Some("stale".into()),
                    expected_version_id: None,
                },
                Bytes::from_static(b"nope"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn delete_removes_object() {
        let (_dir, backend) = temp_backend().await;
        let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "gone.bin".into() };
        backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
        backend.delete_owned_object(&object).await.unwrap();
        assert!(matches!(backend.head(&object).await, Err(StorageError::NotFound { .. })));
    }
}
