//! Storage-aware repair execution for local ↔ remote and remote ↔ remote flows.
//!
//! Transforms still run against a bounded local work directory. Remote inputs are
//! streamed in one object at a time; remote outputs publish via
//! [`RemotePublicationSession`] without loading the full dataset into memory.

use std::collections::BTreeMap;
use std::fs;

use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use parqonaut_storage::backend::StorageBackend;
use parqonaut_storage::inventory::{list_remote_inventory, RemoteInventory};
use parqonaut_storage::location::{DatasetLocation, ObjectLocation, S3Location};
use parqonaut_storage::publication::{
    is_version_committed, read_current_version, PublicationManifest, PublicationObjectRecord,
    PublicationVersionId, RemotePublicationSession, PUBLICATION_CONTRACT_VERSION,
};
use tokio::io::AsyncReadExt;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::authorization::OperationAuditContext;
use crate::error::RepairError;
use crate::execute::{
    copy_work_to_output, run_repair_operations, seed_work_dir, ExecutionReport, RepairExecutor,
    RepairOperationOutcome,
};
use crate::fingerprint::{compute_fingerprint_from_scan, paths_overlap};
use crate::manifest::{ExecutionManifest, MANIFEST_VERSION};
use crate::plan::{RepairPlan, PARQONAUT_VERSION};
use crate::storage_scan::location_root;
use crate::verify::{scan_directory, verify_repair};

/// Returns `true` when repair must use async storage execution instead of local-only `execute`.
pub fn requires_storage_execution(source: &DatasetLocation, output: &DatasetLocation) -> bool {
    !matches!((source, output), (DatasetLocation::Local(_), DatasetLocation::Local(_)))
}

impl RepairExecutor {
    /// Execute a repair plan with optional remote source and/or remote output.
    ///
    /// `scan` is required for local sources (plan-time evidence). Remote sources seed from
    /// inventory and derive a before-scan from the staged work directory.
    #[instrument(skip(self, plan, scan, source_backend, output_backend), fields(plan_id = %plan.plan_id))]
    pub async fn execute_storage<SB: StorageBackend, OB: StorageBackend>(
        &self,
        plan: &RepairPlan,
        output: &DatasetLocation,
        scan: &paraclete_types::ScanReport,
        source_backend: &SB,
        output_backend: &OB,
        run_id: &str,
    ) -> Result<ExecutionReport, RepairError> {
        plan.validate_version()?;
        let source = parse_dataset_location(&plan.dataset_root)?;
        let started = Utc::now();
        let execution_id = Uuid::new_v4();

        ensure_output_available(output_backend, output).await?;
        if locations_overlap(&source, output) {
            return Err(RepairError::SourceDestinationOverlap(format!(
                "{} overlaps {}",
                source.display_uri(),
                output.display_uri()
            )));
        }

        verify_source_fingerprint(&source, plan, scan).await?;

        let staging_parent = staging_parent_path(&source, output)?;
        let staging = staging_parent.join(format!(".parqonaut-staging-{execution_id}"));
        let work = staging.join("work");
        fs::create_dir_all(&work)?;

        let audit_ctx = OperationAuditContext {
            plan_id: plan.plan_id.as_str(),
            dataset_fingerprint: plan.dataset_fingerprint.digest.as_str(),
            policy_fingerprint: plan.policy_fingerprint.as_str(),
            executor_version: PARQONAUT_VERSION,
        };

        let mut working = seed_working_set(&source, source_backend, &work, scan).await?;
        let op_root = operation_root(&source, &work);

        let RepairOperationOutcome { executed, skipped, audit } =
            match run_repair_operations(self, plan, &op_root, &work, &mut working, &audit_ctx) {
                Ok(outcome) => outcome,
                Err(err) => {
                    info!(staging = %staging, "storage repair failed; staging retained");
                    return Err(RepairError::PartialExecution {
                        staging: format!("{} ({err})", staging),
                    });
                }
            };

        let before_scan = before_scan_for_source(&source, scan, &work)?;
        let after_scan = scan_directory(&work)?;
        let before_root = before_scan_root(&source, &work);
        let verification = if self.verify_before_publish {
            Some(verify_repair(
                &before_scan,
                &after_scan,
                &before_root,
                &work,
                &plan.policy.repair,
                None,
            )?)
        } else {
            None
        };

        let output_fp = compute_fingerprint_from_scan(&work, &after_scan)?;
        let manifest = ExecutionManifest {
            parqonaut_manifest_version: MANIFEST_VERSION,
            plan_id: plan.plan_id.clone(),
            execution_id: execution_id.to_string(),
            parqonaut_version: PARQONAUT_VERSION.to_string(),
            source_fingerprint: plan.dataset_fingerprint.clone(),
            output_fingerprint: output_fp,
            policy_fingerprint: plan.policy_fingerprint.clone(),
            started_at: started,
            completed_at: Utc::now(),
            operations: audit,
            verification: verification.clone(),
        };

        let (output_path, manifest_path) = match output {
            DatasetLocation::Local(local) => {
                let out = local.path.clone();
                fs::create_dir_all(&out)?;
                copy_work_to_output(&work, &out)?;
                let manifest_path = out.join(".parqonaut-manifest.json");
                manifest.write_json(&manifest_path)?;
                (out.as_str().to_string(), manifest_path.as_str().to_string())
            }
            DatasetLocation::S3(_) => {
                publish_work_dir(output_backend, output, &work, run_id, &manifest).await?
            }
        };

        let _ = fs::remove_dir_all(&staging);

        Ok(ExecutionReport {
            execution_id,
            plan_id: plan.plan_id.clone(),
            started_at: started,
            completed_at: Some(Utc::now()),
            operations_executed: executed,
            operations_skipped: skipped,
            staging_path: staging.as_str().to_string(),
            output_path,
            manifest_path,
            success: true,
        })
    }
}

fn parse_dataset_location(raw: &str) -> Result<DatasetLocation, RepairError> {
    DatasetLocation::parse(raw).map_err(|e| RepairError::Storage(e.to_string()))
}

fn staging_parent_path(
    source: &DatasetLocation,
    output: &DatasetLocation,
) -> Result<Utf8PathBuf, RepairError> {
    match output {
        DatasetLocation::Local(local) => {
            Ok(local.path.parent().unwrap_or_else(|| Utf8Path::new(".")).to_path_buf())
        }
        DatasetLocation::S3(_) => match source {
            DatasetLocation::Local(local) => {
                Ok(local.path.parent().unwrap_or_else(|| Utf8Path::new(".")).to_path_buf())
            }
            DatasetLocation::S3(_) => {
                let mut dir = std::env::temp_dir();
                dir.push("parqonaut-repair-staging");
                Ok(Utf8PathBuf::from_path_buf(dir).map_err(|_| {
                    RepairError::Storage("temp staging path is not valid UTF-8".into())
                })?)
            }
        },
    }
}

fn operation_root(source: &DatasetLocation, work: &Utf8Path) -> Utf8PathBuf {
    match source {
        DatasetLocation::Local(local) => local.path.clone(),
        DatasetLocation::S3(_) => work.to_path_buf(),
    }
}

fn before_scan_root(source: &DatasetLocation, work: &Utf8Path) -> Utf8PathBuf {
    operation_root(source, work)
}

fn before_scan_for_source(
    source: &DatasetLocation,
    scan: &paraclete_types::ScanReport,
    work: &Utf8Path,
) -> Result<paraclete_types::ScanReport, RepairError> {
    match source {
        DatasetLocation::Local(_) => Ok(scan.clone()),
        DatasetLocation::S3(_) => scan_directory(work),
    }
}

pub fn locations_overlap(source: &DatasetLocation, dest: &DatasetLocation) -> bool {
    match (source, dest) {
        (DatasetLocation::Local(a), DatasetLocation::Local(b)) => paths_overlap(&a.path, &b.path),
        (DatasetLocation::S3(a), DatasetLocation::S3(b)) => s3_prefixes_overlap(a, b),
        _ => false,
    }
}

fn s3_prefixes_overlap(a: &S3Location, b: &S3Location) -> bool {
    if a.bucket != b.bucket {
        return false;
    }
    let a_prefix = normalize_s3_prefix(&a.prefix);
    let b_prefix = normalize_s3_prefix(&b.prefix);
    a_prefix.starts_with(&b_prefix) || b_prefix.starts_with(&a_prefix)
}

fn normalize_s3_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

async fn ensure_output_available<B: StorageBackend + ?Sized>(
    backend: &B,
    output: &DatasetLocation,
) -> Result<(), RepairError> {
    match output {
        DatasetLocation::Local(local) => {
            if local.path.exists() {
                return Err(RepairError::OutputExists(local.path.as_str().to_string()));
            }
        }
        DatasetLocation::S3(_) => {
            if read_current_version(backend, output).await.map_err(publication_err)?.is_some() {
                return Err(RepairError::OutputExists(output.display_uri()));
            }
        }
    }
    Ok(())
}

async fn verify_source_fingerprint(
    source: &DatasetLocation,
    plan: &RepairPlan,
    scan: &paraclete_types::ScanReport,
) -> Result<(), RepairError> {
    let root = location_root(source);
    let current = compute_fingerprint_from_scan(&root, scan)?;
    plan.dataset_fingerprint.verify_against(&current)
}

async fn seed_working_set<B: StorageBackend + ?Sized>(
    source: &DatasetLocation,
    backend: &B,
    work: &Utf8Path,
    scan: &paraclete_types::ScanReport,
) -> Result<BTreeMap<String, Utf8PathBuf>, RepairError> {
    match source {
        DatasetLocation::Local(root) => seed_work_dir(&root.path, work, scan),
        DatasetLocation::S3(_) => {
            let inventory = list_remote_inventory(backend, source).await.map_err(storage_err)?;
            seed_work_dir_from_remote(backend, &inventory, work).await
        }
    }
}

async fn seed_work_dir_from_remote<B: StorageBackend + ?Sized>(
    backend: &B,
    inventory: &RemoteInventory,
    work: &Utf8Path,
) -> Result<BTreeMap<String, Utf8PathBuf>, RepairError> {
    let mut map = BTreeMap::new();
    for object in &inventory.objects {
        let Some(rel) = inventory.relative_key(object) else {
            continue;
        };
        if !rel.ends_with(".parquet") {
            continue;
        }
        let dest = work.join(&rel);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        stream_object_to_file(backend, &object.location, &dest).await?;
        map.insert(rel, dest);
    }
    if map.is_empty() {
        return Err(RepairError::DatasetUnreadable(
            "remote inventory contains no parquet objects".into(),
        ));
    }
    Ok(map)
}

async fn stream_object_to_file<B: StorageBackend + ?Sized>(
    backend: &B,
    object: &ObjectLocation,
    dest: &Utf8Path,
) -> Result<(), RepairError> {
    let mut stream = backend.read_stream(object, None).await.map_err(storage_err)?;
    let mut file = tokio::fs::File::create(dest.as_std_path()).await.map_err(RepairError::Io)?;
    tokio::io::copy(&mut stream, &mut file)
        .await
        .map_err(|e| RepairError::Storage(e.to_string()))?;
    Ok(())
}

async fn publish_work_dir<B: StorageBackend + ?Sized>(
    backend: &B,
    output: &DatasetLocation,
    work: &Utf8Path,
    run_id: &str,
    manifest: &ExecutionManifest,
) -> Result<(String, String), RepairError> {
    let mut session = RemotePublicationSession::begin(backend, output.clone(), run_id)
        .await
        .map_err(publication_err)?;

    let mut object_records = Vec::new();
    for entry in walkdir::WalkDir::new(work.as_std_path()).sort_by_file_name() {
        let entry = entry.map_err(|e| RepairError::Io(std::io::Error::other(e)))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(work.as_std_path())
            .map_err(|e| RepairError::Io(std::io::Error::other(e)))?
            .to_str()
            .ok_or_else(|| RepairError::Storage("non-UTF8 work path".into()))?
            .replace('\\', "/");
        let data = tokio::fs::read(entry.path()).await.map_err(RepairError::Io)?;
        let meta =
            session.put_version_object(&rel, Bytes::from(data)).await.map_err(publication_err)?;
        object_records.push(PublicationObjectRecord {
            key: rel,
            size: meta.size,
            etag: meta.etag.clone(),
            version_id: meta.version_id.clone(),
        });
    }

    let manifest_json = serde_json::to_vec(manifest).map_err(RepairError::Json)?;
    let manifest_meta = session
        .put_version_object(".parqonaut-manifest.json", Bytes::from(manifest_json))
        .await
        .map_err(publication_err)?;
    object_records.push(PublicationObjectRecord {
        key: ".parqonaut-manifest.json".into(),
        size: manifest_meta.size,
        etag: manifest_meta.etag.clone(),
        version_id: manifest_meta.version_id.clone(),
    });

    session.mark_uploaded().await.map_err(publication_err)?;
    session.mark_verified().await.map_err(publication_err)?;

    let publication_manifest = PublicationManifest {
        parqonaut_publication_version: PUBLICATION_CONTRACT_VERSION,
        version_id: run_id.to_string(),
        run_id: run_id.to_string(),
        objects: object_records,
    };
    session.write_manifest(&publication_manifest).await.map_err(publication_err)?;

    let manifest_uri = match output {
        DatasetLocation::S3(s3) => format!(
            "s3://{}/{}",
            s3.bucket,
            s3.object_key(&format!(".parqonaut/versions/{run_id}/data/.parqonaut-manifest.json"))
                .map_err(storage_err)?
        ),
        DatasetLocation::Local(local) => local
            .path
            .join(session.version_object_key(".parqonaut-manifest.json"))
            .as_str()
            .to_string(),
    };

    session.finish(true).await.map_err(publication_err)?;

    let version = PublicationVersionId::from_run_id(run_id);
    debug_assert!(is_version_committed(backend, output, &version).await.unwrap_or(false));

    Ok((output.display_uri(), manifest_uri))
}

fn storage_err(err: parqonaut_storage::StorageError) -> RepairError {
    RepairError::Storage(err.to_string())
}

fn publication_err(err: parqonaut_storage::publication::PublicationError) -> RepairError {
    RepairError::Publication(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parqonaut_storage::backend::StorageBackend;
    use parqonaut_storage::capabilities::StorageCapabilities;
    use parqonaut_storage::conditional::ConditionalCreate;
    use parqonaut_storage::memory::MemoryStorageBackend;

    fn memory_backend() -> MemoryStorageBackend {
        MemoryStorageBackend::new(StorageCapabilities {
            range_reads: true,
            stream_reads: true,
            conditional_create: true,
            conditional_replace: true,
            ..StorageCapabilities::S3
        })
    }

    async fn put_object(backend: &MemoryStorageBackend, bucket: &str, key: &str, data: &[u8]) {
        let object = ObjectLocation::S3 { bucket: bucket.into(), key: key.into() };
        backend
            .conditional_create(
                &object,
                ConditionalCreate::must_not_exist(),
                Bytes::copy_from_slice(data),
            )
            .await
            .unwrap();
    }

    #[test]
    fn overlap_detects_nested_s3_prefixes() {
        let a = DatasetLocation::S3(S3Location { bucket: "b".into(), prefix: "datasets/a".into() });
        let b =
            DatasetLocation::S3(S3Location { bucket: "b".into(), prefix: "datasets/a/out".into() });
        assert!(locations_overlap(&a, &b));
        let c =
            DatasetLocation::S3(S3Location { bucket: "other".into(), prefix: "datasets/a".into() });
        assert!(!locations_overlap(&a, &c));
    }

    #[tokio::test]
    async fn streams_remote_seed_one_object_at_a_time() {
        let backend = memory_backend();
        put_object(&backend, "src", "data/part-000.parquet", b"PAR1-demo").await;
        let dataset = DatasetLocation::parse("s3://src/data/").unwrap();
        let inventory = list_remote_inventory(&backend, &dataset).await.unwrap();
        let work = tempfile::tempdir().unwrap();
        let map = seed_work_dir_from_remote(
            &backend,
            &inventory,
            Utf8Path::new(work.path().to_str().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(map.len(), 1);
        let bytes = fs::read(map.values().next().unwrap()).unwrap();
        assert_eq!(bytes, b"PAR1-demo");
    }
}
