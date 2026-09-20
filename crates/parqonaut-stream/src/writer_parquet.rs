use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

pub struct ParquetWriter {
    writer: ArrowWriter<File>,
}

pub struct ParquetWriterConfig {
    pub compression: Compression,
    pub zstd_level: i32,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self { compression: Compression::UNCOMPRESSED, zstd_level: 3 }
    }
}

impl ParquetWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        schema: SchemaRef,
        config: &ParquetWriterConfig,
    ) -> Result<Self> {
        let compression = match config.compression {
            Compression::ZSTD(_) => {
                Compression::ZSTD(ZstdLevel::try_new(config.zstd_level).unwrap_or_default())
            }
            Compression::GZIP(_) => Compression::GZIP(GzipLevel::default()),
            other => other,
        };

        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(128 * 1024 * 1024)
            .build();

        let file = File::create(path)?;
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(Self { writer })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        self.writer.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_parquet_writer_roundtrip_metadata() {
        let temp_dir = tempdir().unwrap();
        let parquet_file = temp_dir.path().join("output.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(&parquet_file, schema, &config).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        assert!(parquet_file.exists());
        let file = File::open(&parquet_file).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }
}
