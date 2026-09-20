use crate::error::Result;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use csv::{ByteRecord, ReaderBuilder};
use encoding_rs::{Encoding, UTF_8};
use std::sync::Arc;
use std::{fs::File, io::Read, path::Path};

pub struct CsvReader {
    reader: csv::Reader<Box<dyn Read + Send>>,
    headers: Vec<String>,
    batch_size: usize,
    na_values: Vec<String>,
    encoding: &'static Encoding,
    pending_record: Option<ByteRecord>,
}

pub struct CsvConfig {
    pub delimiter: Option<u8>,
    pub quote: Option<u8>,
    pub has_headers: bool,
    pub encoding: String,
    pub na_values: Vec<String>,
    pub batch_size: usize,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            delimiter: None,
            quote: None,
            has_headers: true,
            encoding: "utf8".to_string(),
            na_values: vec!["NA".to_string(), "null".to_string(), "\\N".to_string()],
            batch_size: 64_000,
        }
    }
}

impl CsvReader {
    pub fn new<P: AsRef<Path>>(path: P, config: &CsvConfig) -> Result<Self> {
        let path = path.as_ref();

        let reader: Box<dyn Read + Send> = if path.to_string_lossy() == "-" {
            Box::new(std::io::stdin())
        } else {
            Box::new(File::open(path)?)
        };

        let mut builder = ReaderBuilder::new();

        if let Some(delimiter) = config.delimiter {
            builder.delimiter(delimiter);
        }

        if let Some(quote) = config.quote {
            builder.quote(quote);
        }

        builder.has_headers(config.has_headers);

        let mut reader = builder.from_reader(reader);

        let (headers, pending_record) = if config.has_headers {
            let headers = reader.headers()?.iter().map(|h| h.to_string()).collect();
            (headers, None)
        } else {
            let mut first = ByteRecord::new();
            let col_count = if reader.read_byte_record(&mut first)? { first.len() } else { 0 };
            let headers = (0..col_count).map(|i| format!("col_{}", i + 1)).collect();
            (headers, Some(first))
        };

        let encoding = match config.encoding.to_lowercase().as_str() {
            "utf8" | "utf-8" => UTF_8,
            "latin1" | "iso-8859-1" => encoding_rs::WINDOWS_1252,
            _ => UTF_8,
        };

        Ok(Self {
            reader,
            headers,
            batch_size: config.batch_size,
            na_values: config.na_values.clone(),
            encoding,
            pending_record,
        })
    }

    pub fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut records = Vec::with_capacity(self.batch_size);

        if let Some(record) = self.pending_record.take() {
            records.push(record);
        }

        for _ in 0..self.batch_size {
            let mut record = ByteRecord::new();
            if !self.reader.read_byte_record(&mut record)? {
                break;
            }
            records.push(record);
        }

        if records.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.records_to_batch(&records)?))
    }

    fn records_to_batch(&self, records: &[ByteRecord]) -> Result<RecordBatch> {
        let num_columns = self.headers.len();
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let mut values = Vec::with_capacity(records.len());
            let mut nulls = Vec::with_capacity(records.len());

            for record in records {
                if col_idx < record.len() {
                    let field = &record[col_idx];
                    let field_str = self.decode_field(field)?;

                    if self.na_values.contains(&field_str) {
                        values.push(None);
                        nulls.push(true);
                    } else {
                        values.push(Some(field_str));
                        nulls.push(false);
                    }
                } else {
                    values.push(None);
                    nulls.push(true);
                }
            }

            let array = self.create_column_array(&values, &nulls)?;
            columns.push(array);
        }

        let fields: Vec<Field> = self
            .headers
            .iter()
            .enumerate()
            .map(|(idx, name)| Field::new(name, columns[idx].data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).map_err(|e| e.into())
    }

    fn decode_field(&self, field: &[u8]) -> Result<String> {
        let field = if field.starts_with(&[0xEF, 0xBB, 0xBF]) { &field[3..] } else { field };

        let (decoded, _, had_errors) = self.encoding.decode(field);
        if had_errors {
            tracing::warn!("Encoding errors detected in field, using lossy conversion");
        }
        Ok(decoded.to_string())
    }

    fn create_column_array(
        &self,
        values: &[Option<String>],
        nulls: &[bool],
    ) -> Result<Arc<dyn Array>> {
        let mut has_strings = false;
        let mut has_ints = false;
        let mut has_floats = false;
        let mut has_bools = false;

        for (value, is_null) in values.iter().zip(nulls.iter()) {
            if *is_null {
                continue;
            }
            if let Some(val) = value {
                if val.parse::<i64>().is_ok() {
                    has_ints = true;
                } else if val.parse::<f64>().is_ok() {
                    has_floats = true;
                } else if val.parse::<bool>().is_ok() {
                    has_bools = true;
                } else {
                    has_strings = true;
                }
            }
        }

        if has_strings || (!has_ints && !has_floats && !has_bools) {
            let string_values: Vec<Option<&str>> =
                values.iter().map(|v| v.as_ref().map(|s| s.as_str())).collect();
            Ok(Arc::new(StringArray::from(string_values)))
        } else if has_floats {
            let float_values: Vec<Option<f64>> =
                values.iter().map(|v| v.as_ref().and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Float64Array::from(float_values)))
        } else if has_ints {
            let int_values: Vec<Option<i64>> =
                values.iter().map(|v| v.as_ref().and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(Int64Array::from(int_values)))
        } else if has_bools {
            let bool_values: Vec<Option<bool>> =
                values.iter().map(|v| v.as_ref().and_then(|s| s.parse().ok())).collect();
            Ok(Arc::new(BooleanArray::from(bool_values)))
        } else {
            let string_values: Vec<Option<&str>> =
                values.iter().map(|v| v.as_ref().map(|s| s.as_str())).collect();
            Ok(Arc::new(StringArray::from(string_values)))
        }
    }

    pub fn get_headers(&self) -> &[String] {
        &self.headers
    }

    pub fn infer_schema(&mut self) -> Result<Schema> {
        if let Some(batch) = self.read_batch()? {
            Ok(batch.schema().as_ref().clone())
        } else {
            Ok(Schema::new(
                self.headers
                    .iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_csv_reader() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("test.csv");
        fs::write(&csv_file, "a,b,c\n1,2,3\n4,5,6\n").unwrap();

        let config = CsvConfig::default();
        let mut reader = CsvReader::new(&csv_file, &config).unwrap();

        let batch = reader.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_csv_without_headers() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("test.csv");
        fs::write(&csv_file, "1,2,3\n4,5,6\n").unwrap();

        let config = CsvConfig { has_headers: false, ..CsvConfig::default() };
        let mut reader = CsvReader::new(&csv_file, &config).unwrap();

        let batch = reader.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let headers = reader.get_headers();
        assert_eq!(headers[0], "col_1");
        assert_eq!(headers[1], "col_2");
        assert_eq!(headers[2], "col_3");
    }
}
