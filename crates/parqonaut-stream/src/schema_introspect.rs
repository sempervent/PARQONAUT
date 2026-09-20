use crate::csv_in::{CsvConfig, CsvReader};
use crate::discover::{FileFormat, InputFile};
use crate::error::Result;
use crate::parquet_in::ParquetReader;
use arrow2::datatypes::Schema;

pub fn schema_for_input(file: &InputFile) -> Result<Schema> {
    match file.format {
        FileFormat::Parquet => {
            let reader = ParquetReader::new(&file.path, 1024)?;
            Ok(reader.get_schema().clone())
        }
        FileFormat::Csv => {
            let config = CsvConfig { batch_size: 1024, ..CsvConfig::default() };
            let mut reader = CsvReader::new(&file.path, &config)?;
            reader.infer_schema()
        }
    }
}

pub fn schemas_for_inputs(files: &[InputFile]) -> Result<Vec<Schema>> {
    files.iter().map(schema_for_input).collect()
}
