use crate::error::{MawError, Result};
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use parquet2::compression::Compression;
use std::{fs::File, path::Path, sync::Arc};

pub struct ParquetWriter {
    writer: FileWriter<File>,
    schema: Arc<Schema>,
    options: WriteOptions,
    encodings: Vec<Vec<Encoding>>,
}

pub struct ParquetWriterConfig {
    pub row_group_size: usize,
    pub compression: Compression,
    pub zstd_level: u32,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            row_group_size: 128 * 1024 * 1024,
            compression: Compression::Uncompressed,
            zstd_level: 3,
        }
    }
}

impl ParquetWriterConfig {
    fn compression_options(&self) -> CompressionOptions {
        match self.compression {
            Compression::Zstd => CompressionOptions::Zstd(Some(
                parquet2::compression::ZstdLevel::try_new(self.zstd_level as i32)
                    .unwrap_or_default(),
            )),
            Compression::Snappy => CompressionOptions::Snappy,
            Compression::Gzip => CompressionOptions::Gzip(None),
            _ => CompressionOptions::Uncompressed,
        }
    }
}

impl ParquetWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        schema: Arc<Schema>,
        config: &ParquetWriterConfig,
    ) -> Result<Self> {
        let options = WriteOptions {
            write_statistics: true,
            compression: config.compression_options(),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let encodings =
            schema.fields.iter().map(|f| transverse(&f.data_type, |_| Encoding::Plain)).collect();

        let file = File::create(path)?;
        let writer = FileWriter::try_new(file, (*schema).clone(), options)
            .map_err(|e| MawError::Parquet2(e.into()))?;

        Ok(Self { writer, schema, options, encodings })
    }

    pub fn write_batch(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<()> {
        let iter = std::iter::once(Ok(batch.clone()));
        let row_groups =
            RowGroupIterator::try_new(iter, &self.schema, self.options, self.encodings.clone())
                .map_err(|e| MawError::Parquet2(e.into()))?;

        for group in row_groups {
            self.writer
                .write(group.map_err(|e| MawError::Parquet2(e.into()))?)
                .map_err(|e| MawError::Parquet2(e.into()))?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.writer.end(None).map_err(|e| MawError::Parquet2(e.into()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::{
        array::{Int64Array, Utf8Array},
        datatypes::{DataType, Field},
    };
    use parquet2::read::read_metadata;
    use tempfile::tempdir;

    #[test]
    fn test_parquet_writer_roundtrip_metadata() {
        let temp_dir = tempdir().unwrap();
        let parquet_file = temp_dir.path().join("output.parquet");

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

        assert!(parquet_file.exists());
        let mut file = File::open(&parquet_file).unwrap();
        let metadata = read_metadata(&mut file).unwrap();
        assert_eq!(metadata.row_groups.len(), 1);
        assert_eq!(metadata.row_groups[0].num_rows(), 3);
    }
}
