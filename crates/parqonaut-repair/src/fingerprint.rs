//! Dataset fingerprint for stale-plan rejection.
//!
//! ## Integrity model
//!
//! The fingerprint binds a repair plan to a specific dataset snapshot using **metadata
//! only** (paths, sizes, row counts, schema signatures, row-group counts, compression
//! labels). It deliberately does **not** hash column data — scanning a 50 GB dataset
//! for planning must remain bounded.
//!
//! If any member file is added, removed, resized, or its Parquet footer metadata changes
//! after plan generation, repair execution fails until the plan is regenerated.

use camino::Utf8Path;
use paraclete_types::ScanReport;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::RepairError;
use crate::inventory::DatasetInventory;

pub const FINGERPRINT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintEntry {
    pub relative_path: String,
    pub size_bytes: u64,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub schema_signature: String,
    pub compression_codecs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetFingerprint {
    pub version: u32,
    pub root: String,
    pub entries: Vec<FingerprintEntry>,
    pub digest: String,
}

impl DatasetFingerprint {
    pub fn verify_against(&self, current: &DatasetFingerprint) -> Result<(), RepairError> {
        if self.digest != current.digest {
            return Err(RepairError::DatasetChanged {
                expected: self.digest.clone(),
                actual: current.digest.clone(),
            });
        }
        Ok(())
    }
}

pub fn compute_dataset_fingerprint(
    root: &Utf8Path,
    inventory: &DatasetInventory,
) -> Result<DatasetFingerprint, RepairError> {
    let mut entries = Vec::new();
    for file in &inventory.parquet_files {
        let rel = relativize(root, &file.path)?;
        entries.push(FingerprintEntry {
            relative_path: rel,
            size_bytes: file.size_bytes,
            num_rows: file.num_rows,
            num_row_groups: file.num_row_groups,
            schema_signature: file.schema_signature.clone(),
            compression_codecs: file.compression_codecs.clone(),
        });
    }
    entries.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    let digest = digest_entries(&entries);
    Ok(DatasetFingerprint {
        version: FINGERPRINT_VERSION,
        root: root.as_str().to_string(),
        entries,
        digest,
    })
}

pub fn compute_fingerprint_from_scan(
    root: &Utf8Path,
    report: &ScanReport,
) -> Result<DatasetFingerprint, RepairError> {
    let inventory = DatasetInventory::from_scan_report(root, report)?;
    compute_dataset_fingerprint(root, &inventory)
}

fn digest_entries(entries: &[FingerprintEntry]) -> String {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    let payload = serde_json::to_string(&sorted).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(b"parqonaut-dataset-fp-v1\0");
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn relativize(root: &Utf8Path, path: &Utf8Path) -> Result<String, RepairError> {
    if path.starts_with(root) {
        Ok(path.strip_prefix(root).unwrap_or(path).as_str().trim_start_matches('/').to_string())
    } else {
        Ok(path.as_str().to_string())
    }
}

/// Detect source/destination overlap (repair must not write into source tree).
pub fn paths_overlap(source: &Utf8Path, dest: &Utf8Path) -> bool {
    source.starts_with(dest) || dest.starts_with(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_order_independent() {
        let e1 = FingerprintEntry {
            relative_path: "a.parquet".into(),
            size_bytes: 100,
            num_rows: 10,
            num_row_groups: 1,
            schema_signature: "x".into(),
            compression_codecs: vec!["ZSTD".into()],
        };
        let e2 = FingerprintEntry {
            relative_path: "b.parquet".into(),
            size_bytes: 200,
            num_rows: 20,
            num_row_groups: 2,
            schema_signature: "y".into(),
            compression_codecs: vec!["SNAPPY".into()],
        };
        let d1 = digest_entries(&[e1.clone(), e2.clone()]);
        let d2 = digest_entries(&[e2, e1]);
        assert_eq!(d1, d2);
    }
}
