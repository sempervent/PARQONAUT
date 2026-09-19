use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncRead, ReadBuf};

use crate::error::StorageError;

/// Bounded object read stream.
///
/// Backpressure: consumers drive reads via `AsyncRead`; producers should not
/// prefetch more than one in-flight buffer per stream. Implementations must
/// not buffer entire large objects in memory.
pub struct ObjectReadStream {
    inner: Pin<Box<dyn AsyncRead + Send + Unpin>>,
    bytes_read: u64,
}

impl ObjectReadStream {
    pub fn new(inner: impl AsyncRead + Send + Unpin + 'static) -> Self {
        Self { inner: Box::pin(inner), bytes_read: 0 }
    }

    pub fn from_bytes(data: Bytes) -> Self {
        Self::new(BytesReader { data, pos: 0 })
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Wrap the underlying reader (test-only fault injection).
    #[cfg(any(test, feature = "test-util"))]
    pub fn inject_reader(
        self,
        wrap: impl FnOnce(Pin<Box<dyn AsyncRead + Send + Unpin>>) -> Pin<Box<dyn AsyncRead + Send + Unpin>>
            + Send
            + 'static,
    ) -> Self {
        Self { inner: wrap(self.inner), bytes_read: self.bytes_read }
    }
}

impl AsyncRead for ObjectReadStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = self.inner.as_mut().poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = poll {
            let read = (buf.filled().len() - before) as u64;
            self.bytes_read += read;
        }
        poll
    }
}

struct BytesReader {
    data: Bytes,
    pos: usize,
}

impl AsyncRead for BytesReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos >= self.data.len() {
            return Poll::Ready(Ok(()));
        }
        let remaining = &self.data[self.pos..];
        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        self.pos += to_copy;
        Poll::Ready(Ok(()))
    }
}

/// Bounded write sink for streaming uploads.
pub struct ObjectWriteStream {
    inner: Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>,
    bytes_written: u64,
    #[cfg(any(test, feature = "test-util"))]
    before_write: Option<std::sync::Arc<dyn Fn() -> Result<(), StorageError> + Send + Sync>>,
}

impl ObjectWriteStream {
    pub fn new(inner: impl tokio::io::AsyncWrite + Send + Unpin + 'static) -> Self {
        Self {
            inner: Box::pin(inner),
            bytes_written: 0,
            #[cfg(any(test, feature = "test-util"))]
            before_write: None,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), StorageError> {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(hook) = &self.before_write {
            hook()?;
        }
        use tokio::io::AsyncWriteExt;
        self.inner.write_all(buf).await?;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<u64, StorageError> {
        use tokio::io::AsyncWriteExt;
        self.inner.shutdown().await?;
        Ok(self.bytes_written)
    }

    /// Wrap the underlying writer (test-only fault injection).
    #[cfg(any(test, feature = "test-util"))]
    pub fn inject_writer(
        self,
        wrap: impl FnOnce(
                Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>,
            ) -> Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>
            + Send
            + 'static,
    ) -> Self {
        Self {
            inner: wrap(self.inner),
            bytes_written: self.bytes_written,
            before_write: self.before_write,
        }
    }

    /// Invoke before each [`Self::write_all`] (test-only fault injection).
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_before_write(
        &mut self,
        hook: std::sync::Arc<dyn Fn() -> Result<(), StorageError> + Send + Sync>,
    ) {
        self.before_write = Some(hook);
    }
}
