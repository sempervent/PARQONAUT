use crate::error::{MawError, Result};
use arrow2::{array::Array, chunk::Chunk, io::parquet::read::FileReader};
use parquet2::read::read_metadata;
use std::{fs::File, path::Path};

pub struct ParquetReader {
    reader: FileReader<File>,
    #[allow(dead_code)]
    batch_size: usize,
}

impl ParquetReader {
    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> Result<Self> {
        let mut file = File::open(path)?;
        let metadata = read_metadata(&mut file).map_err(MawError::Parquet2)?;

        let schema = arrow2::io::parquet::read::infer_schema(&metadata)
            .map_err(|e| MawError::Schema(e.to_string()))?;

        let reader =
            FileReader::new(file, metadata.row_groups, schema, Some(batch_size), None, None);

        Ok(Self { reader, batch_size })
    }

    pub fn read_batch(&mut self) -> Result<Option<Chunk<Box<dyn Array>>>> {
        match self.reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(MawError::Parquet2(e.into())),
            None => Ok(None),
        }
    }

    pub fn get_schema(&self) -> &arrow2::datatypes::Schema {
        self.reader.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer_parquet::{ParquetWriter, ParquetWriterConfig};
    use arrow2::{
        array::{Int64Array, Utf8Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_parquet_reader_roundtrip() {
        let temp_dir = tempdir().unwrap();
        let parquet_file = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let a = Int64Array::from_slice([1, 2, 3]);
        let b = Utf8Array::<i32>::from_slice(["x", "y", "z"]);
        let batch = Chunk::new(vec![a.boxed(), b.boxed()]);

        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(&parquet_file, schema, &config).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let mut reader = ParquetReader::new(&parquet_file, 1000).unwrap();
        assert_eq!(reader.get_schema().fields.len(), 2);
        assert!(reader.read_batch().unwrap().is_some());
    }
}
