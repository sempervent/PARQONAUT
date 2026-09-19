//! Parquet footer inspection (metadata only; no row reads in Phase 1).

use std::collections::BTreeSet;
use std::fs::File;

use camino::Utf8Path;
use paraclete_types::{FieldDefinition, SchemaSnapshot};
use parquet::basic::Repetition;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::{ChunkReader, FileReader, SerializedFileReader};

use crate::CoreError;

/// Per-row-group summary derived from the Parquet footer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RowGroupSummary {
    pub index: usize,
    pub num_rows: i64,
    pub compressed_size: i64,
    pub total_byte_size: i64,
}

/// Parquet-specific inspection result for one file.
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetInspection {
    pub path: String,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub row_groups: Vec<RowGroupSummary>,
    pub schema: SchemaSnapshot,
    pub created_by: Option<String>,
    /// Distinct compression codecs observed on column chunks (uppercase labels).
    pub compression_codecs: BTreeSet<String>,
    /// True when every column chunk in every row group exposes statistics metadata.
    pub statistics_present: bool,
}

/// Reads Parquet footer metadata for a UTF-8 path.
pub fn inspect_parquet_file(path: &Utf8Path) -> Result<ParquetInspection, CoreError> {
    let file = File::open(path.as_std_path())?;
    inspect_parquet_chunk_reader(path.as_str().to_string(), file)
}

/// Reads Parquet footer metadata from any [`ChunkReader`] source.
pub fn inspect_parquet_chunk_reader<R: ChunkReader + 'static>(
    path: String,
    reader: R,
) -> Result<ParquetInspection, CoreError> {
    let reader =
        SerializedFileReader::new(reader).map_err(|e| CoreError::Parquet(e.to_string()))?;
    inspection_from_metadata(path, reader.metadata())
}

/// Back-compat alias for [`inspect_parquet_chunk_reader`].
pub fn inspect_parquet_reader<R: ChunkReader + 'static>(
    path: String,
    reader: R,
) -> Result<ParquetInspection, CoreError> {
    inspect_parquet_chunk_reader(path, reader)
}

/// Reads Parquet footer metadata from a bounded footer buffer (metadata + 8-byte trailer).
///
/// `object_size` is the full object length; `footer` is the tail slice ending in `PAR1`.
pub fn inspect_parquet_footer_buffer(
    path: String,
    object_size: u64,
    footer: &[u8],
) -> Result<ParquetInspection, CoreError> {
    if footer.len() < 8 || footer.len() as u64 > object_size {
        return Err(CoreError::Parquet("invalid Parquet footer buffer".into()));
    }
    let trailer: [u8; 8] = footer[footer.len() - 8..]
        .try_into()
        .map_err(|_| CoreError::Parquet("invalid Parquet footer trailer".into()))?;
    let metadata_len = ParquetMetaDataReader::decode_footer(&trailer)
        .map_err(|e| CoreError::Parquet(e.to_string()))?;
    if metadata_len + 8 != footer.len() {
        return Err(CoreError::Parquet(format!(
            "footer buffer length {} does not match metadata length {metadata_len}",
            footer.len(),
        )));
    }
    let metadata_bytes = &footer[..metadata_len];
    let meta = ParquetMetaDataReader::decode_metadata(metadata_bytes)
        .map_err(|e| CoreError::Parquet(e.to_string()))?;
    inspection_from_metadata(path, &meta)
}

/// Upper bound on heap used by [`inspect_parquet_footer_buffer`] (footer bytes only).
pub fn footer_inspection_heap_bound(footer_len: usize) -> usize {
    footer_len
}

fn inspection_from_metadata(
    path: String,
    meta: &parquet::file::metadata::ParquetMetaData,
) -> Result<ParquetInspection, CoreError> {
    let fm = meta.file_metadata();
    let nrg = meta.num_row_groups();
    let mut row_groups = Vec::with_capacity(nrg);
    for i in 0..nrg {
        let rg = meta.row_group(i);
        row_groups.push(RowGroupSummary {
            index: i,
            num_rows: rg.num_rows(),
            compressed_size: rg.compressed_size(),
            total_byte_size: rg.total_byte_size(),
        });
    }
    let schema = schema_from_parquet_meta(fm.schema_descr());
    let created_by = fm.created_by().map(str::to_string);
    let mut compression_codecs = BTreeSet::new();
    let mut statistics_present = nrg > 0;
    for i in 0..nrg {
        let rg = meta.row_group(i);
        for col in rg.columns() {
            compression_codecs.insert(format!("{:?}", col.compression()).to_uppercase());
            if col.statistics().is_none() {
                statistics_present = false;
            }
        }
    }
    Ok(ParquetInspection {
        path,
        num_rows: fm.num_rows(),
        num_row_groups: nrg,
        row_groups,
        schema,
        created_by,
        compression_codecs,
        statistics_present,
    })
}

fn schema_from_parquet_meta(desc: &parquet::schema::types::SchemaDescriptor) -> SchemaSnapshot {
    let mut fields = Vec::with_capacity(desc.num_columns());
    for i in 0..desc.num_columns() {
        let col = desc.column(i);
        let t = col.self_type();
        let nullable = t.get_basic_info().repetition() == Repetition::OPTIONAL;
        fields.push(FieldDefinition {
            name: col.name().to_string(),
            logical_type: format!("{:?} {:?}", col.physical_type(), col.logical_type()),
            nullable,
        });
    }
    SchemaSnapshot { fields }
}
