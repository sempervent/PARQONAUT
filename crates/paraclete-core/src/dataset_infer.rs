//! Logical dataset inference, Hive-style partition parsing, and multi-dataset bucketing.

use std::collections::{BTreeMap, BTreeSet};

use camino::{Utf8Path, Utf8PathBuf};
use paraclete_types::{
    DataFormat, Dataset, DatasetFile, GroupingKind, PartitionLayout, PartitionSegment,
    ResolvedAsset, ScanPlan,
};

use crate::parquet_inspect::ParquetInspection;

/// Notes from anchor-level inference (for Phase 3 findings).
#[derive(Debug, Clone)]
pub enum AnchorInferenceNote {
    /// Multiple schema signatures under one anchor were split into separate datasets.
    SchemaSplit { anchor: String, dataset_ids: Vec<String> },
    /// Mixed schemas were co-located under `part-*.parquet`-style names; kept in one dataset.
    MixedSchemaPartPattern { anchor: String, dataset_id: String },
}

/// Returns true when a single path component looks like a Hive-style `key=value` directory.
pub fn hive_dir_segment(comp: &str) -> bool {
    comp.split_once('=').is_some_and(|(k, v)| {
        !k.is_empty() && !v.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

/// Hive-style `key=value` segments discovered on a path relative to a scan root.
pub fn hive_partition_segments(root: &Utf8Path, file: &Utf8Path) -> Vec<PartitionSegment> {
    let rel = if file.starts_with(root) { file.strip_prefix(root).unwrap_or(file) } else { file };
    let mut out = Vec::new();
    for comp in rel.components().flat_map(|c| c.as_str().split('/')) {
        if comp.is_empty() {
            continue;
        }
        if let Some((k, v)) = comp.split_once('=') {
            if !k.is_empty()
                && !v.is_empty()
                && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                out.push(PartitionSegment { key: k.to_string(), value: v.to_string() });
            }
        }
    }
    out
}

/// Stable schema signature for grouping / splitting (sorted field name + logical type).
pub fn parquet_schema_signature(insp: &ParquetInspection) -> String {
    let mut pairs: Vec<String> =
        insp.schema.fields.iter().map(|f| format!("{}={}", f.name, f.logical_type)).collect();
    pairs.sort();
    pairs.join("\x1f")
}

fn stable_fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

fn short_sig(sig: &str) -> String {
    format!("{:016x}", stable_fnv1a64(sig.as_bytes()))
}

/// Dataset anchor: parent directory of the file relative to the scan root, with trailing
/// Hive-style directory components stripped.
pub fn dataset_anchor_key(root: &Utf8Path, file: &Utf8Path) -> String {
    let rel = if file.starts_with(root) { file.strip_prefix(root).unwrap_or(file) } else { file };
    let parent = rel.parent().unwrap_or_else(|| Utf8Path::new(""));
    let mut comps: Vec<&str> =
        parent.components().map(|c| c.as_str()).filter(|s| !s.is_empty() && *s != ".").collect();
    while let Some(last) = comps.last() {
        if hive_dir_segment(last) {
            comps.pop();
        } else {
            break;
        }
    }
    comps.join("/")
}

fn dataset_id_for_anchor(root: &Utf8Path, anchor: &str) -> String {
    if anchor.is_empty() {
        format!("{}::parquet::default", root)
    } else {
        format!("{}::parquet::{}", root, anchor)
    }
}

fn basename(path: &Utf8Path) -> String {
    path.file_name().map(str::to_string).unwrap_or_default()
}

fn looks_like_part_parquet_file(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    (lower.starts_with("part-") || lower.starts_with("part_"))
        && (lower.ends_with(".parquet") || lower.ends_with(".pq"))
}

/// Builds logical Parquet datasets using path anchors plus schema/part-file heuristics.
pub fn infer_parquet_datasets(
    plan: &ScanPlan,
    parquet_paths: &[Utf8PathBuf],
    inspections: &BTreeMap<Utf8PathBuf, ParquetInspection>,
) -> (Vec<Dataset>, Vec<AnchorInferenceNote>) {
    let mut buckets: BTreeMap<String, Vec<Utf8PathBuf>> = BTreeMap::new();
    for p in parquet_paths {
        let anchor = dataset_anchor_key(&plan.root, p);
        buckets.entry(anchor).or_default().push(p.clone());
    }
    let mut datasets = Vec::new();
    let mut notes = Vec::new();
    for (anchor, mut paths) in buckets {
        paths.sort();
        let base_id = dataset_id_for_anchor(&plan.root, &anchor);
        let sigs: BTreeMap<String, Vec<Utf8PathBuf>> = {
            let mut m: BTreeMap<String, Vec<Utf8PathBuf>> = BTreeMap::new();
            for p in &paths {
                if let Some(insp) = inspections.get(p) {
                    let sig = parquet_schema_signature(insp);
                    m.entry(sig).or_default().push(p.clone());
                }
            }
            m
        };
        let distinct = sigs.len();
        let all_part_named = paths.iter().all(|p| looks_like_part_parquet_file(&basename(p)));
        let split_by_schema =
            distinct > 1 && !(all_part_named && paths.iter().all(|p| inspections.contains_key(p)));

        if split_by_schema {
            let mut ids = Vec::new();
            for (sig, mut group_paths) in sigs {
                group_paths.sort();
                let sid = format!("{}::sig{}", base_id, short_sig(&sig));
                ids.push(sid.clone());
                datasets.push(build_dataset(
                    plan,
                    &sid,
                    &group_paths,
                    inspections,
                    GroupingKind::PathAnchorSchemaSplit,
                ));
            }
            ids.sort();
            notes.push(AnchorInferenceNote::SchemaSplit {
                anchor: anchor.clone(),
                dataset_ids: ids,
            });
        } else {
            let gk = if paths.len() <= 1 {
                GroupingKind::SingleFile
            } else if distinct > 1 && all_part_named {
                notes.push(AnchorInferenceNote::MixedSchemaPartPattern {
                    anchor: anchor.clone(),
                    dataset_id: base_id.clone(),
                });
                GroupingKind::PathAnchorPartFilePattern
            } else {
                GroupingKind::PathAnchor
            };
            datasets.push(build_dataset(plan, &base_id, &paths, inspections, gk));
        }
    }
    datasets.sort_by(|a, b| a.dataset_id.cmp(&b.dataset_id));
    (datasets, notes)
}

fn build_dataset(
    plan: &ScanPlan,
    dataset_id: &str,
    paths: &[Utf8PathBuf],
    inspections: &BTreeMap<Utf8PathBuf, ParquetInspection>,
    grouping_kind: GroupingKind,
) -> Dataset {
    let any_hive = paths.iter().any(|p| !hive_partition_segments(&plan.root, p).is_empty());
    let partition_layout =
        if any_hive { PartitionLayout::HiveDirectoryKeys } else { PartitionLayout::Unpartitioned };
    let schema = paths.iter().find_map(|p| inspections.get(p).map(|i| i.schema.clone()));
    let files: Vec<DatasetFile> = paths
        .iter()
        .map(|p| {
            let parts = hive_partition_segments(&plan.root, p);
            let len = std::fs::metadata(p.as_std_path()).ok().map(|m| m.len());
            DatasetFile {
                path: p.clone(),
                format: DataFormat::Parquet,
                partitions: parts,
                byte_length: len,
            }
        })
        .collect();
    Dataset {
        dataset_id: dataset_id.to_string(),
        grouping_kind,
        partition_layout,
        files,
        schema,
        column_profiles: Vec::new(),
    }
}

/// Partition key sets per file for Hive consistency checks within one dataset.
pub fn partition_key_sets_for_dataset(dataset: &Dataset) -> Vec<(Utf8PathBuf, BTreeSet<String>)> {
    dataset
        .files
        .iter()
        .map(|f| {
            let keys: BTreeSet<String> = f.partitions.iter().map(|s| s.key.clone()).collect();
            (f.path.clone(), keys)
        })
        .collect()
}

/// Dominant tabular format among resolved assets (ignores `Unknown` for dominance).
pub fn dominant_format(assets: &[ResolvedAsset]) -> DataFormat {
    let mut counts: Vec<(DataFormat, usize)> = Vec::new();
    for a in assets {
        if a.format == DataFormat::Unknown {
            continue;
        }
        if let Some((_, c)) = counts.iter_mut().find(|(f, _)| *f == a.format) {
            *c += 1;
        } else {
            counts.push((a.format, 1));
        }
    }
    counts.into_iter().max_by_key(|(_, c)| *c).map(|(f, _)| f).unwrap_or(DataFormat::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_strips_trailing_hive_dirs() {
        let root = Utf8Path::new("/lake");
        let f = Utf8Path::new("/lake/dt=1/p.parquet");
        assert_eq!(dataset_anchor_key(root, f), "");
        let f2 = Utf8Path::new("/lake/region=eu/p.parquet");
        assert_eq!(dataset_anchor_key(root, f2), "");
    }

    #[test]
    fn anchor_keeps_product_prefixes() {
        let root = Utf8Path::new("/data");
        let a = Utf8Path::new("/data/sales/a.parquet");
        let b = Utf8Path::new("/data/logs/b.parquet");
        assert_eq!(dataset_anchor_key(root, a), "sales");
        assert_eq!(dataset_anchor_key(root, b), "logs");
    }
}
