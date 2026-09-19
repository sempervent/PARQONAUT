use bytes::Bytes;

use crate::backend::{ByteRange, ListOptions, StorageBackend};
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};

/// Shared behavioral contract for storage backends.
pub async fn storage_backend_contract<B: StorageBackend>(backend: &B) {
    let caps = backend.capabilities();
    let dataset = DatasetLocation::parse("s3://contract-test/prefix/").unwrap();
    let object =
        ObjectLocation::S3 { bucket: "contract-test".into(), key: "prefix/object.bin".into() };

    if caps.conditional_create {
        let meta = backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"hello"),
            )
            .await
            .expect("conditional create");
        assert_eq!(meta.size, 5);
        assert!(meta.etag.is_some());

        let conflict = backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::from_static(b"again"),
            )
            .await;
        assert!(matches!(conflict, Err(StorageError::Conflict { .. })));

        let head = backend.head(&object).await.expect("head");
        assert_eq!(head.size, 5);

        if caps.range_reads {
            let range = backend
                .read_range(&object, ByteRange::new(1, 3).unwrap())
                .await
                .expect("read_range");
            assert_eq!(&range[..], b"ell");
        }

        if caps.conditional_replace {
            let replaced = backend
                .conditional_replace(
                    &object,
                    ConditionalReplace::matching(&head),
                    Bytes::from_static(b"hello-world"),
                )
                .await
                .expect("conditional replace");
            assert_eq!(replaced.size, 11);

            let precond = backend
                .conditional_replace(
                    &object,
                    ConditionalReplace {
                        expected_etag: Some("stale".into()),
                        expected_version_id: None,
                    },
                    Bytes::from_static(b"nope"),
                )
                .await;
            assert!(matches!(precond, Err(StorageError::PreconditionFailed { .. })));
        }

        backend.delete_owned_object(&object).await.expect("delete");
        assert!(matches!(backend.head(&object).await, Err(StorageError::NotFound { .. })));
    }

    if caps.stream_reads {
        let _ = backend.list(&dataset, ListOptions { recursive: true, max_keys: None }, None).await;
    } else {
        let list = backend.list(&dataset, ListOptions::default(), None).await;
        assert!(list.is_err() || matches!(list, Err(StorageError::UnsupportedCapability { .. })));
    }
}

/// Assert capability-specific unsupported errors are explicit.
pub async fn assert_unsupported_when_missing_capability(backend: &dyn StorageBackend) {
    let caps = backend.capabilities();
    let object = ObjectLocation::S3 { bucket: "b".into(), key: "k".into() };
    if !caps.range_reads {
        let err = backend.read_range(&object, ByteRange::new(0, 0).unwrap()).await.unwrap_err();
        assert!(matches!(
            err,
            StorageError::UnsupportedCapability { .. } | StorageError::NotFound { .. }
        ));
    }
}
