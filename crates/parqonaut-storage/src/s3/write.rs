use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use bytes::Bytes;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;

use crate::error::StorageError;
use crate::metrics::StorageMetricsCollector;
use crate::stream::ObjectWriteStream;

use super::error::{map_multipart_create_error, map_sdk_error};

/// Minimum part size for intermediate multipart segments (S3 requires >= 5 MiB except last).
fn part_size_bytes() -> usize {
    std::env::var("PARQONAUT_S3_PART_SIZE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n >= 64 * 1024)
        .unwrap_or(5 * 1024 * 1024)
}

struct MultipartState {
    client: Client,
    bucket: String,
    key: String,
    upload_id: Option<String>,
    parts: Vec<CompletedPart>,
    next_part_number: i32,
    buffer: Vec<u8>,
    completed: bool,
    aborted: bool,
    metrics: Arc<StorageMetricsCollector>,
}

impl MultipartState {
    fn location(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }

    async fn ensure_multipart(&mut self) -> Result<(), StorageError> {
        if self.upload_id.is_some() {
            return Ok(());
        }
        let location = self.location();
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await
            .map_err(|e| map_multipart_create_error(&location, e))?;
        let upload_id = response
            .upload_id()
            .ok_or_else(|| StorageError::Other {
                message: "multipart upload missing upload_id".into(),
            })?
            .to_string();
        self.upload_id = Some(upload_id);
        Ok(())
    }

    async fn abort(&mut self) {
        if self.completed || self.aborted {
            return;
        }
        let Some(upload_id) = self.upload_id.take() else {
            return;
        };
        self.aborted = true;
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .send()
            .await;
    }

    async fn upload_part(&mut self, data: Bytes) -> Result<(), StorageError> {
        self.ensure_multipart().await?;
        let upload_id = self.upload_id.as_ref().expect("multipart started");
        let part_number = self.next_part_number;
        self.next_part_number += 1;
        let len = data.len();
        let response = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(data.into())
            .send()
            .await
            .map_err(|e| map_sdk_error(&self.location(), e))?;

        self.metrics.record_multipart_part(len as u64);
        self.parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(response.e_tag().unwrap_or_default())
                .build(),
        );
        Ok(())
    }

    async fn flush_buffer(&mut self, force: bool) -> Result<(), StorageError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if !force && self.buffer.len() < part_size_bytes() {
            return Ok(());
        }
        let data = Bytes::from(std::mem::take(&mut self.buffer));
        self.upload_part(data).await
    }

    async fn complete(&mut self) -> Result<(), StorageError> {
        let part_size = part_size_bytes();
        if self.parts.is_empty() && !self.buffer.is_empty() && self.buffer.len() < part_size {
            let data = Bytes::from(std::mem::take(&mut self.buffer));
            let location = self.location();
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .body(data.into())
                .send()
                .await
                .map_err(|e| map_sdk_error(&location, e))?;
            self.completed = true;
            self.metrics.record_put(0);
            return Ok(());
        }

        self.flush_buffer(true).await?;
        let upload_id = self.upload_id.as_ref().ok_or_else(|| StorageError::Other {
            message: "multipart complete without upload_id".into(),
        })?;
        let upload =
            CompletedMultipartUpload::builder().set_parts(Some(self.parts.clone())).build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|e| map_sdk_error(&self.location(), e))?;
        self.completed = true;
        self.metrics.record_put(0);
        Ok(())
    }
}

fn io_error(err: StorageError) -> io::Error {
    io::Error::other(err.to_string())
}

type PendingUpload = Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send>>;

/// AsyncWrite sink that performs multipart upload and aborts on drop if not completed.
pub(crate) struct MultipartAsyncWrite {
    state: Arc<Mutex<MultipartState>>,
    pending: Option<PendingUpload>,
}

impl MultipartAsyncWrite {
    pub(crate) async fn create(
        client: Client,
        bucket: String,
        key: String,
        metrics: Arc<StorageMetricsCollector>,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            state: Arc::new(Mutex::new(MultipartState {
                client,
                bucket,
                key,
                upload_id: None,
                parts: Vec::new(),
                next_part_number: 1,
                buffer: Vec::new(),
                completed: false,
                aborted: false,
                metrics,
            })),
            pending: None,
        })
    }

    fn poll_pending(
        pending: &mut Option<PendingUpload>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if let Some(fut) = pending.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    pending.take();
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    pending.take();
                    Poll::Ready(Err(io_error(e)))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl AsyncWrite for MultipartAsyncWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if let Poll::Ready(result) = Self::poll_pending(&mut self.pending, cx) {
            result?;
        }
        if self.pending.is_some() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let should_flush = {
            let mut guard = match self.state.try_lock() {
                Ok(g) => g,
                Err(_) => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            };
            guard.buffer.extend_from_slice(buf);
            guard.buffer.len() >= part_size_bytes()
        };
        if should_flush {
            let state = self.state.clone();
            self.pending = Some(Box::pin(async move {
                let mut guard = state.lock().await;
                guard.flush_buffer(false).await
            }));
        }

        if self.pending.is_some() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Self::poll_pending(&mut self.get_mut().pending, cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if self.pending.is_none() {
            let state = self.state.clone();
            self.pending = Some(Box::pin(async move {
                let mut guard = state.lock().await;
                guard.complete().await
            }));
        }
        Self::poll_pending(&mut self.pending, cx)
    }
}

impl Drop for MultipartAsyncWrite {
    fn drop(&mut self) {
        // Do not spawn async abort here: it races with successful shutdown/complete and
        // can remove a just-committed object on S3-compatible backends. Incomplete
        // uploads are aborted on explicit error paths via `abort_if_incomplete`.
    }
}

pub(crate) async fn open_write_stream(
    client: Client,
    bucket: String,
    key: String,
    metrics: Arc<StorageMetricsCollector>,
) -> Result<ObjectWriteStream, StorageError> {
    let writer = MultipartAsyncWrite::create(client, bucket, key, metrics).await?;
    Ok(ObjectWriteStream::new(writer))
}
