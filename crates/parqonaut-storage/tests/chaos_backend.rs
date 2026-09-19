#![cfg(feature = "test-util")]

use std::future::Future;

use bytes::Bytes;
use parqonaut_storage::backend::{ByteRange, ListOptions, StorageBackend};
use parqonaut_storage::capabilities::StorageCapabilities;
use parqonaut_storage::chaos::{
    faults, ExhaustedBehavior, FaultInjectingBackend, FaultRule, FaultStep, FaultTarget,
};
use parqonaut_storage::conditional::{ConditionalCreate, ConditionalReplace};
use parqonaut_storage::error::{RetryClass, StorageError};
use parqonaut_storage::location::{DatasetLocation, ObjectLocation};
use parqonaut_storage::{LocalStorageBackend, MemoryStorageBackend};
use tokio::io::AsyncReadExt;

fn memory_backend() -> MemoryStorageBackend {
    MemoryStorageBackend::new(StorageCapabilities {
        range_reads: true,
        stream_reads: true,
        stream_writes: false,
        multipart_upload: false,
        conditional_create: true,
        conditional_replace: true,
        server_side_copy: false,
        version_ids: true,
    })
}

async fn temp_local() -> (tempfile::TempDir, LocalStorageBackend) {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalStorageBackend::new(dir.path());
    (dir, backend)
}

async fn with_retry<T, F, Fut>(mut op: F, max_attempts: usize) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    for attempt in 0..max_attempts {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err)
                if err.retry_class() == RetryClass::Retryable && attempt + 1 < max_attempts =>
            {
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop must return")
}

#[tokio::test]
async fn chaos_list_transient_maps_to_retryable() {
    let inner = memory_backend();
    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::List)
            .steps(vec![FaultStep::Inject(faults::transient_timeout())])],
    );
    let dataset = DatasetLocation::parse("s3://bucket/prefix/").unwrap();
    let err = backend.list(&dataset, ListOptions::default(), None).await.unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::Retryable);
}

#[tokio::test]
async fn chaos_head_unavailable_then_ok_with_retry() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "obj.bin".into() };
    inner
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::from_static(b"data"),
        )
        .await
        .unwrap();

    let backend =
        FaultInjectingBackend::new(inner, vec![faults::unavailable_then_ok(FaultTarget::Head)]);

    let meta = with_retry(|| backend.head(&object), 3).await.unwrap();
    assert_eq!(meta.size, 4);
}

#[tokio::test]
async fn chaos_read_range_failure_is_retryable() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "range.bin".into() };
    inner
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::from_static(b"0123456789"),
        )
        .await
        .unwrap();

    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::ReadRange)
            .steps(vec![FaultStep::Inject(faults::service_unavailable())])],
    );

    let err = backend.read_range(&object, ByteRange::new(0, 3).unwrap()).await.unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::Retryable);
}

#[tokio::test]
async fn chaos_read_stream_open_failure_is_retryable() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "stream.bin".into() };
    inner
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::from_static(b"payload"),
        )
        .await
        .unwrap();

    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::ReadStreamOpen)
            .steps(vec![FaultStep::Inject(faults::service_unavailable())])],
    );

    let err = match backend.read_stream(&object, None).await {
        Err(err) => err,
        Ok(_) => panic!("expected read_stream open failure"),
    };
    assert_eq!(err.retry_class(), RetryClass::Retryable);
}

#[tokio::test]
async fn chaos_retry_exhaustion_stays_retryable() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "obj.bin".into() };
    inner
        .conditional_create(&object, ConditionalCreate::must_not_exist(), Bytes::from_static(b"x"))
        .await
        .unwrap();

    let backend =
        FaultInjectingBackend::new(inner, vec![faults::retry_exhaustion(FaultTarget::Head)]);

    let err = with_retry(|| backend.head(&object), 3).await.unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::Retryable);
}

#[tokio::test]
async fn chaos_write_stream_multipart_part_failure() {
    let (_dir, inner) = temp_local().await;
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "upload.bin".into() };

    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::WriteStreamPart)
            .write_part_index(2)
            .steps(vec![FaultStep::Inject(faults::multipart_part_failed(2))])],
    );

    let mut writer = backend.write_stream(&object, None).await.unwrap();
    writer.write_all(b"part-one").await.unwrap();
    let err = writer.write_all(b"part-two").await.unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::Retryable);
}

#[tokio::test]
async fn chaos_conditional_create_conflict_is_never_retry() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "new.bin".into() };

    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::ConditionalCreate)
            .steps(vec![FaultStep::Inject(faults::conflict_exists("s3://bucket/new.bin"))])],
    );

    let err = backend
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::from_static(b"body"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::NeverRetry);
}

#[tokio::test]
async fn chaos_conditional_replace_precondition_is_never_retry() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "replace.bin".into() };
    inner
        .conditional_create(&object, ConditionalCreate::must_not_exist(), Bytes::from_static(b"v1"))
        .await
        .unwrap();

    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::ConditionalReplace)
            .steps(vec![FaultStep::Inject(faults::precondition_etag())])],
    );

    let head = backend.head(&object).await.unwrap();
    let err = backend
        .conditional_replace(
            &object,
            ConditionalReplace::matching(&head),
            Bytes::from_static(b"v2"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::NeverRetry);
}

#[tokio::test]
async fn chaos_permission_denied_is_never_retry() {
    let inner = memory_backend();
    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::Head)
            .steps(vec![FaultStep::Inject(faults::permission_denied("s3://bucket/denied"))])],
    );
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "denied".into() };
    let err = backend.head(&object).await.unwrap_err();
    assert_eq!(err.retry_class(), RetryClass::NeverRetry);
}

#[tokio::test]
async fn chaos_read_stream_body_fault_surfaces_on_first_read() {
    let inner = memory_backend();
    let object = ObjectLocation::S3 { bucket: "bucket".into(), key: "body.bin".into() };
    inner
        .conditional_create(
            &object,
            ConditionalCreate::must_not_exist(),
            Bytes::from_static(b"abcdef"),
        )
        .await
        .unwrap();

    let backend = FaultInjectingBackend::new(
        inner,
        vec![FaultRule::new(FaultTarget::ReadStreamRead)
            .steps(vec![FaultStep::Inject(faults::transient_timeout())])
            .exhausted(ExhaustedBehavior::Delegate)],
    );

    let mut stream = backend.read_stream(&object, None).await.unwrap();
    let mut buf = [0u8; 4];
    let io_err = stream.read(&mut buf).await.unwrap_err();
    assert!(io_err.to_string().contains("upstream timeout"));
}
