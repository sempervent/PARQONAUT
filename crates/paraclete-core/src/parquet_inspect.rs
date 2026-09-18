//! Parquet footer inspection (metadata only; no row reads in Phase 1).

use std::fs::File;

use camino::Utf8Path;
use paraclete_types::{FieldDefinition, SchemaSnapshot};
use parquet::basic::Repetition;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::CoreError;

/// Per-row-group summary derived from the Parquet footer.
#[derive(Debug, Clone, PartialEq)]
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
}

/// Reads Parquet footer metadata for a UTF-8 path.
pub fn inspect_parquet_file(path: &Utf8Path) -> Result<ParquetInspection, CoreError> {
    let file = File::open(path.as_std_path())?;
    let reader = SerializedFileReader::new(file).map_err(|e| CoreError::Parquet(e.to_string()))?;
    let meta = reader.metadata();
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
    Ok(ParquetInspection {
        path: path.as_str().to_string(),
        num_rows: fm.num_rows(),
        num_row_groups: nrg,
        row_groups,
        schema,
        created_by,
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
