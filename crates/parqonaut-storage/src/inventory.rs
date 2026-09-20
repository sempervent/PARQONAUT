//! Deterministic remote dataset inventory via [`StorageBackend::list`].
//!
//! Lists all objects under a dataset prefix recursively, merges paginated results,
//! excludes PARQONAUT-owned metadata/staging paths, and sorts lexically by relative key
//! so ordering is independent of backend page order.

use crate::backend::{ListOptions, StorageBackend};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};
use crate::metadata::ObjectMetadata;

/// Stable inventory of user-owned objects under a dataset prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteInventory {
    pub dataset: DatasetLocation,
    pub objects: Vec<ObjectMetadata>,
}

impl RemoteInventory {
    pub fn relative_key(&self, object: &ObjectMetadata) -> Option<String> {
        dataset_object_relative_key(&self.dataset, &object.location)
    }
}

/// Relative key for an object within a dataset, including logical keys for committed
/// publication data under `.parqonaut/versions/{version}/data/`.
pub fn dataset_object_relative_key(
    dataset: &DatasetLocation,
    object: &ObjectLocation,
) -> Option<String> {
    let key = relative_object_key(dataset, object)?;
    Some(logical_inventory_key(&key))
}

fn logical_inventory_key(relative_key: &str) -> String {
    strip_published_data_relative_key(relative_key).unwrap_or_else(|| relative_key.to_string())
}

/// Maps `.parqonaut/versions/{id}/data/{name}` → `{name}` when present.
pub fn strip_published_data_relative_key(relative_key: &str) -> Option<String> {
    let rest = relative_key.strip_prefix(".parqonaut/versions/")?;
    let (_, after_version) = rest.split_once('/')?;
    after_version.strip_prefix("data/").map(str::to_string)
}

/// Recursively list a dataset and return a sorted, de-duplicated inventory.
pub async fn list_remote_inventory<B: StorageBackend + ?Sized>(
    backend: &B,
    dataset: &DatasetLocation,
) -> Result<RemoteInventory, StorageError> {
    let mut objects = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let page = backend
            .list(dataset, ListOptions { recursive: true, max_keys: None }, continuation.as_deref())
            .await?;
        objects.extend(page.objects);
        if !page.truncated {
            break;
        }
        continuation = page.continuation_token;
    }

    let raw_objects = objects.clone();
    objects.retain(|object| {
        dataset_object_relative_key(dataset, &object.location)
            .map(|key| !is_excluded_inventory_path(&key))
            .unwrap_or(false)
    });

    let has_parquet = objects.iter().any(|object| {
        dataset_object_relative_key(dataset, &object.location)
            .is_some_and(|key| key.ends_with(".parquet"))
    });
    if !has_parquet {
        if let Ok(published) = published_data_inventory(backend, dataset, &raw_objects).await {
            objects.extend(published);
        }
    }

    objects.sort_by(inventory_sort_key);
    objects.dedup_by(|a, b| object_sort_key(&a.location) == object_sort_key(&b.location));

    Ok(RemoteInventory { dataset: dataset.clone(), objects })
}

async fn published_data_inventory<B: StorageBackend + ?Sized>(
    backend: &B,
    dataset: &DatasetLocation,
    raw_objects: &[ObjectMetadata],
) -> Result<Vec<ObjectMetadata>, StorageError> {
    let DatasetLocation::S3(_) = dataset else {
        return Ok(Vec::new());
    };

    use crate::publication::{is_version_committed, read_current_version, PublicationVersionId};

    let Some(version_id) = read_current_version(backend, dataset)
        .await
        .map_err(|e| StorageError::Other { message: e.to_string() })?
    else {
        return Ok(Vec::new());
    };
    let version = PublicationVersionId(version_id);
    if !is_version_committed(backend, dataset, &version)
        .await
        .map_err(|e| StorageError::Other { message: e.to_string() })?
    {
        return Ok(Vec::new());
    }

    let data_prefix = format!(".parqonaut/versions/{}/data/", version.as_str());
    let mut published = Vec::new();
    for object in raw_objects {
        let Some(relative) = relative_object_key(dataset, &object.location) else {
            continue;
        };
        if !relative.starts_with(&data_prefix) {
            continue;
        }
        let logical = relative.strip_prefix(&data_prefix).unwrap_or(&relative);
        if is_excluded_inventory_path(logical) || !logical.ends_with(".parquet") {
            continue;
        }
        published.push(object.clone());
    }
    Ok(published)
}

/// Returns true when `relative_key` is PARQONAUT-owned metadata or staging and must
/// not appear in dataset inventory.
pub fn is_excluded_inventory_path(relative_key: &str) -> bool {
    let key = relative_key.trim_start_matches('/');
    if key.is_empty() {
        return true;
    }
    if key.ends_with(".parqonaut.lock") || key.ends_with(".parqonaut-manifest.json") {
        return true;
    }
    for segment in key.split('/') {
        if segment == ".parqonaut" || segment == "_staging" {
            return true;
        }
        if segment.starts_with(".parqonaut-staging-") {
            return true;
        }
    }
    false
}

/// Stable lexical ordering for inventory entries.
pub fn inventory_sort_key(a: &ObjectMetadata, b: &ObjectMetadata) -> std::cmp::Ordering {
    let key_a = object_sort_key(&a.location);
    let key_b = object_sort_key(&b.location);
    key_a.cmp(&key_b)
}

fn object_sort_key(location: &ObjectLocation) -> String {
    match location {
        ObjectLocation::Local { path } => path.as_str().to_string(),
        ObjectLocation::S3 { key, .. } => key.clone(),
    }
}

/// Relative object key within a dataset prefix (no leading slash).
pub fn relative_object_key(dataset: &DatasetLocation, object: &ObjectLocation) -> Option<String> {
    match (dataset, object) {
        (DatasetLocation::Local(root), ObjectLocation::Local { path }) => {
            if path.starts_with(&root.path) {
                let rel = path.strip_prefix(&root.path).unwrap_or(path);
                Some(rel.as_str().trim_start_matches('/').to_string())
            } else {
                None
            }
        }
        (DatasetLocation::S3(root), ObjectLocation::S3 { bucket, key })
            if root.bucket == *bucket =>
        {
            let prefix = root.prefix.trim_end_matches('/');
            if prefix.is_empty() {
                Some(key.trim_start_matches('/').to_string())
            } else if key == prefix {
                Some(String::new())
            } else if key.starts_with(&format!("{prefix}/")) {
                Some(key[prefix.len() + 1..].to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use camino::Utf8PathBuf;

    use super::*;
    use crate::backend::StorageBackend;
    use crate::capabilities::StorageCapabilities;
    use crate::conditional::ConditionalCreate;
    use crate::memory::MemoryStorageBackend;

    fn memory_backend() -> MemoryStorageBackend {
        MemoryStorageBackend::new(StorageCapabilities {
            conditional_create: true,
            ..StorageCapabilities::LOCAL
        })
    }

    async fn put_s3(
        backend: &MemoryStorageBackend,
        bucket: &str,
        key: &str,
        data: &[u8],
    ) -> ObjectMetadata {
        backend
            .conditional_create(
                &ObjectLocation::S3 { bucket: bucket.into(), key: key.into() },
                ConditionalCreate::must_not_exist(),
                Bytes::copy_from_slice(data),
            )
            .await
            .expect("conditional create")
    }

    #[test]
    fn published_data_maps_to_logical_keys() {
        assert_eq!(
            strip_published_data_relative_key(
                ".parqonaut/versions/run-abc/data/nested/part.parquet"
            )
            .as_deref(),
            Some("nested/part.parquet")
        );
        assert!(strip_published_data_relative_key("plain/part.parquet").is_none());
    }

    #[test]
    fn excludes_parqonaut_and_staging_paths() {
        assert!(is_excluded_inventory_path(".parqonaut/runs/x/journal.sqlite"));
        assert!(is_excluded_inventory_path("data/.parqonaut/manifest.json"));
        assert!(is_excluded_inventory_path("_staging/part.tmp"));
        assert!(is_excluded_inventory_path("out/.parqonaut-staging-abc/part.parquet"));
        assert!(is_excluded_inventory_path("output.parqonaut.lock"));
        assert!(is_excluded_inventory_path(".parqonaut-manifest.json"));
        assert!(!is_excluded_inventory_path("part-000.parquet"));
        assert!(!is_excluded_inventory_path("nested/part-001.parquet"));
    }

    #[test]
    fn inventory_sort_is_lexical() {
        let a = ObjectMetadata {
            location: ObjectLocation::S3 { bucket: "b".into(), key: "p/z.parquet".into() },
            size: 1,
            etag: None,
            version_id: None,
            last_modified: None,
        };
        let b = ObjectMetadata {
            location: ObjectLocation::S3 { bucket: "b".into(), key: "p/a.parquet".into() },
            size: 1,
            etag: None,
            version_id: None,
            last_modified: None,
        };
        let mut objects = [a, b];
        objects.sort_by(inventory_sort_key);
        match &objects[0].location {
            ObjectLocation::S3 { key, .. } => assert_eq!(key, "p/a.parquet"),
            _ => panic!("expected s3"),
        }
    }

    #[tokio::test]
    async fn inventory_lists_and_sorts_recursively() {
        let backend = Arc::new(memory_backend());
        let dataset = DatasetLocation::parse("s3://inventory-bucket/data/").unwrap();

        put_s3(&backend, "inventory-bucket", "data/z.parquet", b"z").await;
        put_s3(&backend, "inventory-bucket", "data/nested/a.parquet", b"a").await;
        put_s3(&backend, "inventory-bucket", "data/.parqonaut/runs/1/journal.sqlite", b"j").await;
        put_s3(&backend, "inventory-bucket", "data/_staging/tmp.bin", b"t").await;
        put_s3(&backend, "inventory-bucket", "data/m.parquet", b"m").await;

        let inventory = list_remote_inventory(backend.as_ref(), &dataset).await.unwrap();
        assert_eq!(inventory.objects.len(), 3);
        let keys: Vec<_> =
            inventory.objects.iter().map(|o| inventory.relative_key(o).unwrap()).collect();
        assert_eq!(
            keys,
            vec!["m.parquet".to_string(), "nested/a.parquet".to_string(), "z.parquet".to_string(),]
        );
    }

    #[tokio::test]
    async fn inventory_sort_independent_of_page_order() {
        let backend = Arc::new(memory_backend());
        let dataset = DatasetLocation::parse("s3://page-order/prefix/").unwrap();

        put_s3(&backend, "page-order", "prefix/c.parquet", b"c").await;
        put_s3(&backend, "page-order", "prefix/a.parquet", b"a").await;
        put_s3(&backend, "page-order", "prefix/b.parquet", b"b").await;

        let inventory = list_remote_inventory(backend.as_ref(), &dataset).await.unwrap();
        let keys: Vec<_> =
            inventory.objects.iter().map(|o| inventory.relative_key(o).unwrap()).collect();
        assert_eq!(
            keys,
            vec!["a.parquet".to_string(), "b.parquet".to_string(), "c.parquet".to_string(),]
        );
    }

    #[test]
    fn relative_key_local() {
        let dataset = DatasetLocation::Local(crate::location::LocalLocation {
            path: Utf8PathBuf::from("/data/set"),
        });
        let object = ObjectLocation::Local { path: Utf8PathBuf::from("/data/set/part.parquet") };
        assert_eq!(relative_object_key(&dataset, &object).as_deref(), Some("part.parquet"));
    }
}
