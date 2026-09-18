use crate::error::{MawError, Result};
use crate::schema::UnifiedSchema;
use arrow2::{
    array::*,
    chunk::Chunk,
    datatypes::{DataType, Schema},
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct BatchAligner {
    unified_schema: Arc<UnifiedSchema>,
    column_mapping: HashMap<String, String>, // original -> unified
    include_columns: Option<Vec<String>>,
    exclude_columns: Option<Vec<String>>,
    stringify_conflicts: bool,
}

impl BatchAligner {
    pub fn new(
        unified_schema: Arc<UnifiedSchema>,
        column_mapping: HashMap<String, String>,
        include_columns: Option<Vec<String>>,
        exclude_columns: Option<Vec<String>>,
        stringify_conflicts: bool,
    ) -> Self {
        Self {
            unified_schema,
            column_mapping,
            include_columns,
            exclude_columns,
            stringify_conflicts,
        }
    }

    pub fn align_batch(&self, batch: Chunk<Box<dyn Array>>) -> Result<Chunk<Box<dyn Array>>> {
        let mut aligned_columns = Vec::new();
        let mut aligned_fields = Vec::new();

        for field in &self.unified_schema.schema.fields {
            let column_name = &field.name;
            let target_type = field.data_type();

            // Check if column should be included
            if let Some(include) = &self.include_columns {
                if !include.contains(column_name) {
                    continue;
                }
            }

            // Check if column should be excluded
            if let Some(exclude) = &self.exclude_columns {
                if exclude.contains(column_name) {
                    continue;
                }
            }

            // Find the source column (handle renames)
            let source_column = self.find_source_column(column_name);

            let aligned_array = if let Some(source_idx) = source_column {
                if source_idx < batch.arrays().len() {
                    let source_array = &*batch.arrays()[source_idx];
                    self.coerce_column(
                        source_array,
                        source_array.data_type(),
                        target_type,
                        batch.len(),
                    )?
                } else {
                    self.create_null_column(target_type, batch.len())?
                }
            } else {
                // Column doesn't exist in source - create null column
                self.create_null_column(target_type, batch.len())?
            };

            aligned_columns.push(aligned_array);
            aligned_fields.push(field.clone());
        }

        let _aligned_schema = Schema::from(aligned_fields);
        Ok(Chunk::new(aligned_columns))
    }

    fn find_source_column(&self, unified_name: &str) -> Option<usize> {
        if let Some(original) = self.column_mapping.get(unified_name) {
            return self.unified_schema.schema.fields.iter().position(|f| f.name == *original);
        }

        for (original, mapped) in &self.column_mapping {
            if mapped == unified_name {
                return self.unified_schema.schema.fields.iter().position(|f| f.name == *original);
            }
        }

        self.unified_schema.schema.fields.iter().position(|f| f.name == unified_name)
    }

    fn coerce_column(
        &self,
        array: &dyn Array,
        source_type: &DataType,
        target_type: &DataType,
        num_rows: usize,
    ) -> Result<Box<dyn Array>> {
        if source_type == target_type {
            return match target_type {
                DataType::Int64 => {
                    let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    Ok(Box::new(values.clone()))
                }
                DataType::Utf8 => {
                    let values = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                    Ok(Box::new(values.clone()))
                }
                _ => self.create_null_column(target_type, num_rows),
            };
        }

        match (source_type, target_type) {
            // String to other types
            (DataType::Utf8, DataType::Int64) => {
                let string_array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let int_values: Vec<Option<i64>> = (0..num_rows)
                    .map(|i| {
                        if string_array.is_null(i) {
                            None
                        } else {
                            string_array.value(i).parse().ok()
                        }
                    })
                    .collect();
                Ok(Box::new(Int64Array::from(int_values)))
            }
            (DataType::Utf8, DataType::Float64) => {
                let string_array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let float_values: Vec<Option<f64>> = (0..num_rows)
                    .map(|i| {
                        if string_array.is_null(i) {
                            None
                        } else {
                            string_array.value(i).parse().ok()
                        }
                    })
                    .collect();
                Ok(Box::new(Float64Array::from(float_values)))
            }
            (DataType::Utf8, DataType::Boolean) => {
                let string_array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let bool_values: Vec<Option<bool>> = (0..num_rows)
                    .map(|i| {
                        if string_array.is_null(i) {
                            None
                        } else {
                            string_array.value(i).parse().ok()
                        }
                    })
                    .collect();
                Ok(Box::new(BooleanArray::from(bool_values)))
            }

            // Integer to float
            (DataType::Int64, DataType::Float64) => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let float_values: Vec<Option<f64>> =
                    (0..num_rows)
                        .map(|i| {
                            if int_array.is_null(i) {
                                None
                            } else {
                                Some(int_array.value(i) as f64)
                            }
                        })
                        .collect();
                Ok(Box::new(Float64Array::from(float_values)))
            }

            // Any type to string
            (_, DataType::Utf8) => {
                let string_values: Vec<Option<&str>> = (0..num_rows)
                    .map(|i| {
                        if array.is_null(i) {
                            None
                        } else {
                            Some("converted") // Simplified - would need proper string conversion
                        }
                    })
                    .collect();
                Ok(Box::new(Utf8Array::<i32>::from(string_values)))
            }

            // Default: return as string if stringify_conflicts is enabled
            _ if self.stringify_conflicts => {
                let string_values: Vec<Option<&str>> = (0..num_rows)
                    .map(|i| {
                        if array.is_null(i) {
                            None
                        } else {
                            Some("converted") // Simplified - would need proper string conversion
                        }
                    })
                    .collect();
                Ok(Box::new(Utf8Array::<i32>::from(string_values)))
            }

            _ => Err(MawError::Schema(format!(
                "Cannot coerce {:?} to {:?}",
                source_type, target_type
            ))),
        }
    }

    fn create_null_column(&self, data_type: &DataType, num_rows: usize) -> Result<Box<dyn Array>> {
        match data_type {
            DataType::Utf8 => {
                let nulls: Vec<Option<&str>> = vec![None; num_rows];
                Ok(Box::new(Utf8Array::<i32>::from(nulls)))
            }
            DataType::Int64 => {
                let nulls: Vec<Option<i64>> = vec![None; num_rows];
                Ok(Box::new(Int64Array::from(nulls)))
            }
            DataType::Float64 => {
                let nulls: Vec<Option<f64>> = vec![None; num_rows];
                Ok(Box::new(Float64Array::from(nulls)))
            }
            DataType::Boolean => {
                let nulls: Vec<Option<bool>> = vec![None; num_rows];
                Ok(Box::new(BooleanArray::from(nulls)))
            }
            _ => {
                // Default to string for unknown types
                let nulls: Vec<Option<&str>> = vec![None; num_rows];
                Ok(Box::new(Utf8Array::<i32>::from(nulls)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::{
        array::{Int64Array, Utf8Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Schema},
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_batch_alignment() {
        let a = Int64Array::from_slice([1, 2, 3]);
        let b = Utf8Array::<i32>::from_slice(["x", "y", "z"]);
        let batch = Chunk::new(vec![a.boxed(), b.boxed()]);

        let unified_schema = Arc::new(UnifiedSchema {
            schema: Schema::from(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]),
            ..UnifiedSchema::default()
        });
        let column_mapping = HashMap::new();
        let aligner = BatchAligner::new(unified_schema, column_mapping, None, None, false);

        let aligned = aligner.align_batch(batch).unwrap();
        assert_eq!(aligned.len(), 3);
    }
}
