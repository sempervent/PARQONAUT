use aws_sdk_s3::primitives::ByteStream;
use tokio::io::AsyncRead;

/// AsyncRead adapter over an S3 GetObject body without buffering the entire object.
pub(crate) fn body_reader(body: ByteStream) -> impl AsyncRead + Send + Unpin + 'static {
    body.into_async_read()
}
