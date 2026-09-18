use std::collections::BTreeMap;
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use parqonaut_transform::{merge_parquet_files, rewrite_parquet_file};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};
use uuid::Uuid;

use crate::action::RepairAction;
use crate::error::RepairError;
use crate::fingerprint::{compute_fingerprint_from_scan, paths_overlap, relativize};
use crate::plan::{RepairOperation, RepairPlan};
use crate::safety::RepairSafety;

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
    pub success: bool,
}

#[derive(Default)]
pub struct RepairExecutor {
    pub authorize_review: bool,
}

impl RepairExecutor {
    #[instrument(skip(self, plan, scan), fields(plan_id = %plan.plan_id))]
    pub fn execute(
        &self,
        plan: &RepairPlan,
        output: &Utf8Path,
        scan: &paraclete_types::ScanReport,
    ) -> Result<ExecutionReport, RepairError> {
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

        let mut working = seed_work_dir(root, &work, scan)?;

        let run_result = (|| {
            for op in &plan.operations {
                match op.safety {
                    RepairSafety::Safe => {
                        apply_operation(op, plan, root, &work, &mut working)?;
                        executed.push(op.operation_id.clone());
                    }
                    RepairSafety::ReviewRequired => {
                        if self.authorize_review {
                            return Err(RepairError::ReviewRequiredNotAuthorized {
                                operation_id: op.operation_id.clone(),
                            });
                        }
                        skipped.push(op.operation_id.clone());
                    }
                    RepairSafety::Destructive => {
                        skipped.push(op.operation_id.clone());
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
                .map(|rel| working.get(rel).unwrap_or(&root.join(rel)).as_str().to_string())
                .collect();
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
        RepairAction::ResizeRowGroups { target_bytes, target_paths } => {
            let mb = (*target_bytes / (1024 * 1024)).max(1);
            rewrite_targets(working, target_paths, None, Some(mb))?;
        }
        RepairAction::Recompress { codec, target_paths } => {
            rewrite_targets(working, target_paths, Some(codec.as_str()), None)?;
        }
        RepairAction::RebuildStatistics { target_paths } => {
            rewrite_targets(working, target_paths, None, None)?;
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

fn rewrite_targets(
    working: &mut BTreeMap<String, Utf8PathBuf>,
    target_paths: &[String],
    compression: Option<&str>,
    row_group_mb: Option<u64>,
) -> Result<(), RepairError> {
    let paths: Vec<String> = if target_paths.iter().all(|p| working.contains_key(p)) {
        target_paths.to_vec()
    } else {
        working.keys().cloned().collect()
    };
    for rel in paths {
        let src = working.get(&rel).ok_or_else(|| RepairError::PreconditionFailed {
            operation_id: rel.clone(),
            detail: "target path not in working set".into(),
        })?;
        let tmp = src.with_extension("parqonaut-tmp.parquet");
        rewrite_parquet_file(src.as_str(), tmp.as_str(), compression, row_group_mb, true)
            .map_err(|e| RepairError::TransformFailed(e.to_string()))?;
        fs::rename(tmp.as_std_path(), src.as_std_path())?;
        working.insert(rel.clone(), src.clone());
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
