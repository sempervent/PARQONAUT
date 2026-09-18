use std::collections::BTreeMap;
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use parqonaut_transform::{
    merge_parquet_files, rewrite_parquet_file, rewrite_parquet_with_cast,
    rewrite_parquet_with_rename, split_parquet_file,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::action::RepairAction;
use crate::authorization::{
    AuthorizationSource, OperationAuditContext, OperationAuditRecord, RepairAuthorization,
};
use crate::deps::sort_by_dependencies;
use crate::error::RepairError;
use crate::fingerprint::{compute_fingerprint_from_scan, paths_overlap, relativize};
use crate::manifest::{ExecutionManifest, MANIFEST_VERSION};
use crate::plan::{RepairOperation, RepairPlan, PARQONAUT_VERSION};
use crate::safety::RepairSafety;
use crate::verify::{scan_directory, verify_repair};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub execution_id: Uuid,
    pub plan_id: String,
    pub started_at: chrono::DateTime<Utc>,
    pub completed_at: Option<chrono::DateTime<Utc>>,
    pub operations_executed: Vec<String>,
    pub operations_skipped: Vec<String>,
    pub staging_path: String,
    pub output_path: String,
    pub manifest_path: String,
    pub success: bool,
}

pub struct RepairExecutor {
    pub authorization: RepairAuthorization,
    pub verify_before_publish: bool,
}

impl Default for RepairExecutor {
    fn default() -> Self {
        Self { authorization: RepairAuthorization::default(), verify_before_publish: true }
    }
}

impl RepairExecutor {
    #[instrument(skip(self, plan, scan), fields(plan_id = %plan.plan_id))]
    pub fn execute(
        &self,
        plan: &RepairPlan,
        output: &Utf8Path,
        scan: &paraclete_types::ScanReport,
    ) -> Result<ExecutionReport, RepairError> {
        plan.validate_version()?;
        let root = Utf8Path::new(&plan.dataset_root);
        let started = Utc::now();
        let execution_id = Uuid::new_v4();

        if output.exists() {
            return Err(RepairError::OutputExists(output.as_str().to_string()));
        }
        if paths_overlap(root, output) {
            return Err(RepairError::SourceDestinationOverlap(format!("{} ⊆ {}", root, output)));
        }

        let current_fp = compute_fingerprint_from_scan(root, scan)?;
        plan.dataset_fingerprint.verify_against(&current_fp)?;

        let staging = output
            .parent()
            .unwrap_or_else(|| Utf8Path::new("."))
            .join(format!(".parqonaut-staging-{execution_id}"));
        let work = staging.join("work");
        fs::create_dir_all(&work)?;

        let mut executed = Vec::new();
        let mut skipped = Vec::new();
        let mut audit = Vec::new();
        let audit_ctx = OperationAuditContext {
            plan_id: plan.plan_id.as_str(),
            dataset_fingerprint: plan.dataset_fingerprint.digest.as_str(),
            policy_fingerprint: plan.policy_fingerprint.as_str(),
            executor_version: PARQONAUT_VERSION,
        };

        let mut working = seed_work_dir(root, &work, scan)?;

        let sorted = sort_by_dependencies(&plan.operations)?;
        let run_result = (|| {
            for op in sorted {
                let now = Utc::now();
                match op.safety {
                    RepairSafety::Safe => {
                        apply_operation(op, plan, root, &work, &mut working)?;
                        executed.push(op.operation_id.clone());
                        audit.push(OperationAuditRecord::new(
                            audit_ctx,
                            op.operation_id.clone(),
                            op.safety,
                            AuthorizationSource::AutomaticSafePolicy,
                            now,
                            true,
                            None,
                        ));
                    }
                    RepairSafety::ReviewRequired => {
                        if self.authorization.is_authorized(&op.operation_id) {
                            apply_operation(op, plan, root, &work, &mut working)?;
                            executed.push(op.operation_id.clone());
                            audit.push(OperationAuditRecord::new(
                                audit_ctx,
                                op.operation_id.clone(),
                                op.safety,
                                AuthorizationSource::ExplicitUser,
                                now,
                                true,
                                None,
                            ));
                        } else {
                            skipped.push(op.operation_id.clone());
                            audit.push(OperationAuditRecord::new(
                                audit_ctx,
                                op.operation_id.clone(),
                                op.safety,
                                AuthorizationSource::NotAuthorized,
                                now,
                                false,
                                Some(format!(
                                    "review-required operation {} not authorized",
                                    op.operation_id
                                )),
                            ));
                        }
                    }
                    RepairSafety::Blocked | RepairSafety::Destructive => {
                        skipped.push(op.operation_id.clone());
                        audit.push(OperationAuditRecord::new(
                            audit_ctx,
                            op.operation_id.clone(),
                            op.safety,
                            AuthorizationSource::BlockedByPolicy,
                            now,
                            false,
                            Some("operation blocked by safety policy".into()),
                        ));
                    }
                }
            }
            Ok::<(), RepairError>(())
        })();

        if let Err(err) = run_result {
            info!(staging = %staging, "repair failed; staging retained");
            return Err(RepairError::PartialExecution { staging: format!("{} ({err})", staging) });
        }

        fs::create_dir_all(output)?;
        copy_work_to_output(&work, output)?;

        let after_scan = scan_directory(output)?;
        let verification = if self.verify_before_publish {
            Some(verify_repair(scan, &after_scan, root, output, &plan.policy.repair, None)?)
        } else {
            None
        };

        let output_fp = compute_fingerprint_from_scan(output, &after_scan)?;
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
        let manifest_path = output.join(".parqonaut-manifest.json");
        manifest.write_json(&manifest_path)?;

        let _ = fs::remove_dir_all(&staging);

        Ok(ExecutionReport {
            execution_id,
            plan_id: plan.plan_id.clone(),
            started_at: started,
            completed_at: Some(Utc::now()),
            operations_executed: executed,
            operations_skipped: skipped,
            staging_path: staging.as_str().to_string(),
            output_path: output.as_str().to_string(),
            manifest_path: manifest_path.as_str().to_string(),
            success: true,
        })
    }
}

fn seed_work_dir(
    root: &Utf8Path,
    work: &Utf8Path,
    scan: &paraclete_types::ScanReport,
) -> Result<BTreeMap<String, Utf8PathBuf>, RepairError> {
    let mut map = BTreeMap::new();
    for asset in &scan.assets {
        if asset.format != paraclete_types::DataFormat::Parquet {
            continue;
        }
        let rel = relativize(root, &asset.path)?;
        let dest = work.join(&rel);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(asset.path.as_std_path(), dest.as_std_path())?;
        map.insert(rel, dest);
    }
    Ok(map)
}

fn apply_operation(
    op: &RepairOperation,
    plan: &RepairPlan,
    root: &Utf8Path,
    work: &Utf8Path,
    working: &mut BTreeMap<String, Utf8PathBuf>,
) -> Result<(), RepairError> {
    match &op.action {
        RepairAction::MergeSmallFiles { target_bytes, input_paths } => {
            let abs: Vec<String> = input_paths
                .iter()
                .filter(|rel| working.contains_key(*rel))
                .map(|rel| working.get(rel).expect("filtered").as_str().to_string())
                .collect();
            if abs.len() < 2 {
                return Ok(());
            }
            let merge_out = work.join("_merge_out");
            fs::create_dir_all(&merge_out)?;
            let outputs = merge_parquet_files(
                &abs,
                merge_out.as_std_path(),
                "part",
                *target_bytes,
                None,
                None,
                false,
            )
            .map_err(|e| RepairError::TransformFailed(e.to_string()))?;
            for rel in input_paths {
                if let Some(path) = working.remove(rel) {
                    let _ = fs::remove_file(path);
                }
            }
            for (idx, out) in outputs.into_iter().enumerate() {
                let rel = format!("merged-part-{idx:04}.parquet");
                let dest = work.join(&rel);
                fs::copy(&out, &dest)?;
                working.insert(rel, dest);
            }
            let _ = fs::remove_dir_all(&merge_out);
        }
        RepairAction::SplitLargeFile { target_bytes, input_path } => {
            let src = working.get(input_path).cloned().unwrap_or_else(|| root.join(input_path));
            let split_out = work.join("_split_out");
            fs::create_dir_all(&split_out)?;
            let outputs = split_parquet_file(
                src.as_str(),
                split_out.as_std_path(),
                "split",
                *target_bytes,
                None,
                None,
                true,
            )
            .map_err(|e| RepairError::TransformFailed(e.to_string()))?;
            if let Some(old) = working.remove(input_path) {
                let _ = fs::remove_file(old);
            }
            for (idx, out) in outputs.into_iter().enumerate() {
                let rel = format!("{input_path}.split-{idx:04}.parquet");
                let dest = work.join(&rel);
                fs::copy(&out, &dest)?;
                working.insert(rel, dest);
            }
            let _ = fs::remove_dir_all(&split_out);
        }
        RepairAction::ResizeRowGroups { target_bytes, target_paths } => {
            let mb = (*target_bytes / (1024 * 1024)).max(1);
            rewrite_targets(working, target_paths, None, Some(mb), false)?;
        }
        RepairAction::Recompress { codec, target_paths } => {
            rewrite_targets(working, target_paths, Some(codec.as_str()), None, false)?;
        }
        RepairAction::RebuildStatistics { target_paths } => {
            rewrite_targets(working, target_paths, None, None, true)?;
        }
        RepairAction::CastColumn { column, to_type, .. } => {
            cast_targets(working, target_paths_all(working, &[]), column, to_type)?;
        }
        RepairAction::AlignSchema { field, to_type, .. } => {
            cast_targets(working, target_paths_all(working, &[]), field, to_type)?;
        }
        RepairAction::RenameColumn { from, to } => {
            rename_targets(working, target_paths_all(working, &[]), from, to)?;
        }
        other => {
            return Err(RepairError::UnsupportedOperation {
                operation_id: op.operation_id.clone(),
                action: other.kind_label().into(),
            });
        }
    }
    let _ = plan;
    Ok(())
}

fn target_paths_all(
    working: &BTreeMap<String, Utf8PathBuf>,
    target_paths: &[String],
) -> Vec<String> {
    if target_paths.is_empty() {
        working.keys().cloned().collect()
    } else {
        target_paths.to_vec()
    }
}

fn rename_targets(
    working: &mut BTreeMap<String, Utf8PathBuf>,
    paths: Vec<String>,
    from: &str,
    to: &str,
) -> Result<(), RepairError> {
    for rel in paths {
        let src = working.get(&rel).ok_or_else(|| RepairError::PreconditionFailed {
            operation_id: rel.clone(),
            detail: "rename target missing".into(),
        })?;
        let tmp = src.with_extension("parqonaut-rename.parquet");
        rewrite_parquet_with_rename(src.as_str(), tmp.as_str(), from, to, None, None, true)
            .map_err(|e| RepairError::TransformFailed(e.to_string()))?;
        fs::rename(tmp.as_std_path(), src.as_std_path())?;
        working.insert(rel, src.clone());
    }
    Ok(())
}

fn cast_targets(
    working: &mut BTreeMap<String, Utf8PathBuf>,
    paths: Vec<String>,
    column: &str,
    to_type: &str,
) -> Result<(), RepairError> {
    for rel in paths {
        let src = working.get(&rel).ok_or_else(|| RepairError::PreconditionFailed {
            operation_id: rel.clone(),
            detail: "cast target missing".into(),
        })?;
        let tmp = src.with_extension("parqonaut-cast.parquet");
        rewrite_parquet_with_cast(src.as_str(), tmp.as_str(), column, to_type, None, None, true)
            .map_err(|e| RepairError::TransformFailed(e.to_string()))?;
        fs::rename(tmp.as_std_path(), src.as_std_path())?;
        working.insert(rel, src.clone());
    }
    Ok(())
}

fn rewrite_targets(
    working: &mut BTreeMap<String, Utf8PathBuf>,
    target_paths: &[String],
    compression: Option<&str>,
    row_group_mb: Option<u64>,
    rebuild_stats: bool,
) -> Result<(), RepairError> {
    let paths: Vec<String> = target_paths_all(working, target_paths)
        .into_iter()
        .filter(|p| working.contains_key(p))
        .collect();
    for rel in paths {
        let src = working.get(&rel).ok_or_else(|| RepairError::PreconditionFailed {
            operation_id: rel.clone(),
            detail: "target path not in working set".into(),
        })?;
        let tmp = src.with_extension("parqonaut-tmp.parquet");
        rewrite_parquet_file(src.as_str(), tmp.as_str(), compression, row_group_mb, rebuild_stats)
            .map_err(|e| RepairError::TransformFailed(e.to_string()))?;
        fs::rename(tmp.as_std_path(), src.as_std_path())?;
        working.insert(rel, src.clone());
    }
    Ok(())
}

fn copy_work_to_output(work: &Utf8Path, output: &Utf8Path) -> Result<(), RepairError> {
    for entry in walkdir::WalkDir::new(work.as_std_path()).sort_by_file_name() {
        let entry = entry.map_err(|e| RepairError::Io(std::io::Error::other(e)))?;
        if entry.file_type().is_file() {
            let rel = entry
                .path()
                .strip_prefix(work.as_std_path())
                .map_err(|e| RepairError::Io(std::io::Error::other(e)))?;
            let dest = output.join(rel.to_str().unwrap_or("file.parquet"));
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), dest.as_std_path())?;
        }
    }
    Ok(())
}
