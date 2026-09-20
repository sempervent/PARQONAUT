use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

pub struct ParquetReader {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    schema: SchemaRef,
}

impl ParquetReader {
    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> Result<Self> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let reader = builder.with_batch_size(batch_size).build()?;
        Ok(Self { reader, schema })
    }

    pub fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        use std::iter::Iterator;
        match Iterator::next(&mut self.reader) {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(e.into()),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn get_schema(&self) -> &arrow::datatypes::Schema {
        self.schema.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer_parquet::{ParquetWriter, ParquetWriterConfig};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::basic::Compression;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_parquet_reader_roundtrip() {
        let temp_dir = tempdir().unwrap();
        let parquet_file = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let a = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let b = Arc::new(StringArray::from(vec!["x", "y", "z"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a, b]).unwrap();

        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(&parquet_file, schema, &config).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let mut reader = ParquetReader::new(&parquet_file, 1000).unwrap();
        assert_eq!(reader.get_schema().fields().len(), 2);
        assert!(reader.read_batch().unwrap().is_some());
    }
}
