use crate::error::Result;
use arrow2::{array::*, chunk::Chunk, datatypes::DataType};
use csv::{Writer, WriterBuilder};
use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::Path,
};

pub struct CsvWriter {
    writer: Writer<BufWriter<File>>,
    headers_written: bool,
    #[allow(dead_code)]
    delimiter: u8,
    #[allow(dead_code)]
    quote: u8,
    na_string: String,
}

pub struct CsvWriterConfig {
    pub delimiter: u8,
    pub quote: u8,
    pub na_string: String,
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self { delimiter: b',', quote: b'"', na_string: "".to_string() }
    }
}

impl CsvWriter {
    pub fn new<P: AsRef<Path>>(path: P, config: &CsvWriterConfig) -> Result<Self> {
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;

        let writer = WriterBuilder::new()
            .delimiter(config.delimiter)
            .quote(config.quote)
            .from_writer(BufWriter::new(file));

        Ok(Self {
            writer,
            headers_written: false,
            delimiter: config.delimiter,
            quote: config.quote,
            na_string: config.na_string.clone(),
        })
    }

    pub fn write_batch(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<()> {
        // Write headers if not already written
        if !self.headers_written {
            self.write_headers(batch)?;
            self.headers_written = true;
        }

        // Write data rows
        for row_idx in 0..batch.len() {
            let mut record = Vec::new();

            for col_idx in 0..batch.arrays().len() {
                let array = &*batch.arrays()[col_idx];
                let value = self.array_value_to_string(array, row_idx)?;
                record.push(value);
            }

            self.writer.write_record(&record)?;
        }

        self.writer.flush()?;
        Ok(())
    }

    fn write_headers(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<()> {
        // For now, use generic column names
        let headers: Vec<String> =
            (0..batch.arrays().len()).map(|i| format!("col_{}", i + 1)).collect();

        self.writer.write_record(&headers)?;
        Ok(())
    }

    fn array_value_to_string(&self, array: &dyn Array, row_idx: usize) -> Result<String> {
        if array.is_null(row_idx) {
            return Ok(self.na_string.clone());
        }

        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                Ok(string_array.value(row_idx).to_string())
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(float_array.value(row_idx).to_string())
            }
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(bool_array.value(row_idx).to_string())
            }
            _ => {
                // Default to string representation
                Ok("unknown".to_string())
            }
        }
    }

    pub fn finish(self) -> Result<()> {
        // Writer is automatically closed when dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::{
        array::{Int64Array, Utf8Array},
        chunk::Chunk,
    };
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_csv_writer() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("output.csv");

        let a = Int64Array::from_slice([1, 2, 3]);
        let b = Utf8Array::<i32>::from_slice(["x", "y", "z"]);
        let batch = Chunk::new(vec![a.boxed(), b.boxed()]);

        let config = CsvWriterConfig::default();
        let mut writer = CsvWriter::new(&csv_file, &config).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let content = fs::read_to_string(&csv_file).unwrap();
        assert!(content.contains("col_1,col_2"));
        assert!(content.contains("1,x"));
        assert!(content.contains("2,y"));
        assert!(content.contains("3,z"));
    }
}
