use crate::error::Result;
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use csv::{Writer, WriterBuilder};
use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::Path,
};

pub struct CsvWriter {
    writer: Writer<BufWriter<File>>,
    headers_written: bool,
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

        Ok(Self { writer, headers_written: false, na_string: config.na_string.clone() })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if !self.headers_written {
            self.write_headers(batch)?;
            self.headers_written = true;
        }

        for row_idx in 0..batch.num_rows() {
            let mut record = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = self.array_value_to_string(array.as_ref(), row_idx)?;
                record.push(value);
            }
            self.writer.write_record(&record)?;
        }

        self.writer.flush()?;
        Ok(())
    }

    fn write_headers(&mut self, batch: &RecordBatch) -> Result<()> {
        let headers: Vec<String> =
            batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        self.writer.write_record(&headers)?;
        Ok(())
    }

    fn array_value_to_string(&self, array: &dyn Array, row_idx: usize) -> Result<String> {
        if array.is_null(row_idx) {
            return Ok(self.na_string.clone());
        }

        match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
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
            _ => Ok("unknown".to_string()),
        }
    }

    pub fn finish(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::fs;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_csv_writer() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("output.csv");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let config = CsvWriterConfig::default();
        let mut writer = CsvWriter::new(&csv_file, &config).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let content = fs::read_to_string(&csv_file).unwrap();
        assert!(content.contains("a,b"));
        assert!(content.contains("1,x"));
        assert!(content.contains("2,y"));
        assert!(content.contains("3,z"));
    }
}
