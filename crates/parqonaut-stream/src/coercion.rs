use crate::error::{MawError, Result};
use crate::schema::UnifiedSchema;
use arrow2::{array::*, chunk::Chunk, datatypes::DataType};
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
    pub fn unified_schema(&self) -> &Arc<UnifiedSchema> {
        &self.unified_schema
    }

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

    pub fn align_batch_with_source(
        &self,
        batch: Chunk<Box<dyn Array>>,
        source_fields: &[String],
    ) -> Result<Chunk<Box<dyn Array>>> {
        let mut aligned_columns = Vec::new();

        for field in &self.unified_schema.schema.fields {
            let column_name = &field.name;
            let target_type = field.data_type();

            if let Some(include) = &self.include_columns {
                if !include.contains(column_name) {
                    continue;
                }
            }

            if let Some(exclude) = &self.exclude_columns {
                if exclude.contains(column_name) {
                    continue;
                }
            }

            let mapped_name = self
                .column_mapping
                .get(column_name)
                .map(String::as_str)
                .unwrap_or(column_name.as_str());

            let source_idx = source_fields.iter().position(|n| n == mapped_name);

            let aligned_array = if let Some(source_idx) = source_idx {
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
                self.create_null_column(target_type, batch.len())?
            };

            aligned_columns.push(aligned_array);
        }

        Ok(Chunk::new(aligned_columns))
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
                DataType::Int32 => {
                    let v = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    Ok(Box::new(v.clone()) as Box<dyn Array>)
                }
                DataType::Int64 => {
                    let v = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    Ok(Box::new(v.clone()) as Box<dyn Array>)
                }
                DataType::Float64 => {
                    let v = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    Ok(Box::new(v.clone()) as Box<dyn Array>)
                }
                DataType::Utf8 => {
                    let v = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                    Ok(Box::new(v.clone()) as Box<dyn Array>)
                }
                DataType::Boolean => {
                    let v = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    Ok(Box::new(v.clone()) as Box<dyn Array>)
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

            (_, DataType::Utf8) => self.stringify_column(array, num_rows),
            _ if self.stringify_conflicts && matches!(target_type, DataType::Utf8) => {
                self.stringify_column(array, num_rows)
            }

            _ => Err(MawError::Schema(format!(
                "Cannot coerce {:?} to {:?}",
                source_type, target_type
            ))),
        }
    }
}

fn array_value_display(array: &dyn Array, index: usize) -> String {
    use arrow2::array::*;
    if array.is_null(index) {
        return String::new();
    }
    match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            a.value(index).to_string()
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(index).to_string()
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(index).to_string()
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            a.value(index).to_string()
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(index).to_string()
        }
        other => format!("{other:?}"),
    }
}

impl BatchAligner {
    fn stringify_column(&self, array: &dyn Array, num_rows: usize) -> Result<Box<dyn Array>> {
        let owned: Vec<Option<String>> = (0..num_rows)
            .map(|i| if array.is_null(i) { None } else { Some(array_value_display(array, i)) })
            .collect();
        let refs: Vec<Option<&str>> = owned.iter().map(|o| o.as_deref()).collect();
        Ok(Box::new(Utf8Array::<i32>::from(refs)))
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

        let aligned =
            aligner.align_batch_with_source(batch, &["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(aligned.len(), 3);
    }
}
