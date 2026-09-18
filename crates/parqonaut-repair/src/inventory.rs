use std::collections::BTreeMap;

use camino::{Utf8Path, Utf8PathBuf};
use paraclete_core::inspect_parquet_file;
use paraclete_types::{DataFormat, FieldDefinition, ScanReport};

use crate::error::RepairError;

#[derive(Debug, Clone)]
pub struct ParquetFileMeta {
    pub path: Utf8PathBuf,
    pub size_bytes: u64,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub median_row_group_bytes: u64,
    pub compression_codecs: Vec<String>,
    pub schema_signature: String,
    pub fields: Vec<FieldDefinition>,
    pub has_statistics: bool,
}

#[derive(Debug, Clone)]
pub struct DatasetInventory {
    pub root: Utf8PathBuf,
    pub parquet_files: Vec<ParquetFileMeta>,
    pub total_rows: i64,
    pub schema_signatures: BTreeMap<String, Vec<Utf8PathBuf>>,
}

impl DatasetInventory {
    pub fn from_scan_report(root: &Utf8Path, report: &ScanReport) -> Result<Self, RepairError> {
        let mut parquet_files = Vec::new();
        let mut total_rows: i64 = 0;
        let mut schema_signatures: BTreeMap<String, Vec<Utf8PathBuf>> = BTreeMap::new();

        let mut assets: Vec<_> =
            report.assets.iter().filter(|a| a.format == DataFormat::Parquet).collect();
        assets.sort_by_key(|a| &a.path);

        for asset in assets {
            let insp = inspect_parquet_file(&asset.path)
                .map_err(|e| RepairError::DatasetUnreadable(format!("{}: {e}", asset.path)))?;
            total_rows += insp.num_rows;
            let sig = paraclete_core::parquet_schema_signature(&insp);
            schema_signatures.entry(sig.clone()).or_default().push(asset.path.clone());

            let rg_sizes: Vec<u64> =
                insp.row_groups.iter().map(|rg| rg.compressed_size.max(0) as u64).collect();
            let median_rg = median_u64(&rg_sizes);

            let has_statistics = insp.statistics_present;

            parquet_files.push(ParquetFileMeta {
                path: asset.path.clone(),
                size_bytes: asset.size_bytes,
                num_rows: insp.num_rows,
                num_row_groups: insp.num_row_groups,
                median_row_group_bytes: median_rg,
                compression_codecs: insp.compression_codecs.iter().cloned().collect(),
                schema_signature: sig,
                fields: insp.schema.fields.clone(),
                has_statistics,
            });
        }

        Ok(Self { root: root.to_path_buf(), parquet_files, total_rows, schema_signatures })
    }

    pub fn median_file_size(&self) -> u64 {
        median_u64(&self.parquet_files.iter().map(|f| f.size_bytes).collect::<Vec<_>>())
    }

    pub fn all_compression_codecs(&self) -> Vec<String> {
        let mut codecs: BTreeMap<String, ()> = BTreeMap::new();
        for f in &self.parquet_files {
            for c in &f.compression_codecs {
                codecs.insert(c.clone(), ());
            }
        }
        codecs.keys().cloned().collect()
    }

    pub fn small_files(&self, threshold_bytes: u64) -> Vec<&ParquetFileMeta> {
        self.parquet_files.iter().filter(|f| f.size_bytes < threshold_bytes).collect()
    }

    /// Structural schema key including nullability (for merge compatibility).
    pub fn structural_schema_key(fields: &[FieldDefinition]) -> String {
        let mut parts: Vec<String> = fields
            .iter()
            .map(|f| {
                let ty = f.logical_type.split_whitespace().next().unwrap_or("UNKNOWN");
                format!("{}:{}:{}", f.name, ty, f.nullable)
            })
            .collect();
        parts.sort();
        parts.join("\x1f")
    }
}

pub fn median_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}
