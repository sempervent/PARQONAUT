use crate::error::Result;
use arrow2::{
    array::{Array, BooleanArray, Float64Array, Int64Array, Utf8Array},
    chunk::Chunk,
    datatypes::DataType,
};
use csv::{ByteRecord, ReaderBuilder};
use encoding_rs::{Encoding, UTF_8};
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

    pub fn read_batch(&mut self) -> Result<Option<Chunk<Box<dyn Array>>>> {
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

        // Convert to Chunk
        let batch = self.records_to_batch(&records)?;
        Ok(Some(batch))
    }

    fn records_to_batch(&self, records: &[ByteRecord]) -> Result<Chunk<Box<dyn Array>>> {
        let num_columns = self.headers.len();
        let mut columns: Vec<Box<dyn Array>> = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let _column_name = &self.headers[col_idx];
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

            // Infer column type and create array
            let array = self.create_column_array(&values, &nulls)?;
            columns.push(array);
        }

        let _schema = arrow2::datatypes::Schema::from(
            self.headers
                .iter()
                .map(|name| arrow2::datatypes::Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );

        Ok(Chunk::new(columns))
    }

    fn decode_field(&self, field: &[u8]) -> Result<String> {
        // Handle BOM
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
    ) -> Result<Box<dyn Array>> {
        // Try to infer the best type for this column
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

        // Create the appropriate array type
        if has_strings || (!has_ints && !has_floats && !has_bools) {
            // String array
            let string_values: Vec<Option<&str>> =
                values.iter().map(|v| v.as_ref().map(|s| s.as_str())).collect();
            Ok(Box::new(Utf8Array::<i32>::from(string_values)))
        } else if has_floats {
            // Float array
            let float_values: Vec<Option<f64>> =
                values.iter().map(|v| v.as_ref().and_then(|s| s.parse().ok())).collect();
            Ok(Box::new(Float64Array::from(float_values)))
        } else if has_ints {
            // Integer array
            let int_values: Vec<Option<i64>> =
                values.iter().map(|v| v.as_ref().and_then(|s| s.parse().ok())).collect();
            Ok(Box::new(Int64Array::from(int_values)))
        } else if has_bools {
            // Boolean array
            let bool_values: Vec<Option<bool>> =
                values.iter().map(|v| v.as_ref().and_then(|s| s.parse().ok())).collect();
            Ok(Box::new(BooleanArray::from(bool_values)))
        } else {
            // Default to string
            let string_values: Vec<Option<&str>> =
                values.iter().map(|v| v.as_ref().map(|s| s.as_str())).collect();
            Ok(Box::new(Utf8Array::<i32>::from(string_values)))
        }
    }

    pub fn get_headers(&self) -> &[String] {
        &self.headers
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
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.arrays().len(), 3);
    }

    #[test]
    fn test_csv_without_headers() {
        let temp_dir = tempdir().unwrap();
        let csv_file = temp_dir.path().join("test.csv");
        fs::write(&csv_file, "1,2,3\n4,5,6\n").unwrap();

        let config = CsvConfig { has_headers: false, ..CsvConfig::default() };
        let mut reader = CsvReader::new(&csv_file, &config).unwrap();

        let batch = reader.read_batch().unwrap().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.arrays().len(), 3);

        let headers = reader.get_headers();
        assert_eq!(headers[0], "col_1");
        assert_eq!(headers[1], "col_2");
        assert_eq!(headers[2], "col_3");
    }
}
