use crate::error::{MawError, Result};
use crate::schema::UnifiedSchema;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BatchAligner {
    unified_schema: Arc<UnifiedSchema>,
    column_mapping: HashMap<String, String>,
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
        batch: RecordBatch,
        source_fields: &[String],
    ) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut aligned_columns: Vec<Arc<dyn Array>> = Vec::new();

        for field in self.unified_schema.schema.fields() {
            let column_name = field.name();
            let target_type = field.data_type();

            if let Some(include) = &self.include_columns {
                if !include.iter().any(|c| c == column_name) {
                    continue;
                }
            }

            if let Some(exclude) = &self.exclude_columns {
                if exclude.iter().any(|c| c == column_name) {
                    continue;
                }
            }

            let mapped_name =
                self.column_mapping.get(column_name).map(String::as_str).unwrap_or(column_name);

            let source_idx = source_fields.iter().position(|n| n == mapped_name);

            let aligned_array = if let Some(source_idx) = source_idx {
                if source_idx < batch.num_columns() {
                    let source_array = batch.column(source_idx);
                    self.coerce_column(
                        source_array,
                        source_array.data_type(),
                        target_type,
                        num_rows,
                    )?
                } else {
                    self.create_null_column(target_type, num_rows)?
                }
            } else {
                self.create_null_column(target_type, num_rows)?
            };

            aligned_columns.push(aligned_array);
        }

        let schema = Arc::new(self.unified_schema.schema.clone());
        RecordBatch::try_new(schema, aligned_columns).map_err(|e| e.into())
    }

    fn coerce_column(
        &self,
        array: &dyn Array,
        source_type: &DataType,
        target_type: &DataType,
        num_rows: usize,
    ) -> Result<Arc<dyn Array>> {
        if source_type == target_type {
            return match target_type {
                DataType::Int32 => {
                    let v = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    Ok(Arc::new(v.clone()))
                }
                DataType::Int64 => {
                    let v = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    Ok(Arc::new(v.clone()))
                }
                DataType::Float64 => {
                    let v = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    Ok(Arc::new(v.clone()))
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let v = array.as_any().downcast_ref::<StringArray>().unwrap();
                    Ok(Arc::new(v.clone()))
                }
                DataType::Boolean => {
                    let v = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    Ok(Arc::new(v.clone()))
                }
                _ => self.create_null_column(target_type, num_rows),
            };
        }

        match (source_type, target_type) {
            (DataType::Utf8, DataType::Int64) | (DataType::LargeUtf8, DataType::Int64) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let int_values: Vec<Option<i64>> = (0..num_rows)
                    .map(|i| {
                        if string_array.is_null(i) {
                            None
                        } else {
                            string_array.value(i).parse().ok()
                        }
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(int_values)))
            }
            (DataType::Utf8, DataType::Float64) | (DataType::LargeUtf8, DataType::Float64) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let float_values: Vec<Option<f64>> = (0..num_rows)
                    .map(|i| {
                        if string_array.is_null(i) {
                            None
                        } else {
                            string_array.value(i).parse().ok()
                        }
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(float_values)))
            }
            (DataType::Utf8, DataType::Boolean) | (DataType::LargeUtf8, DataType::Boolean) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let bool_values: Vec<Option<bool>> = (0..num_rows)
                    .map(|i| {
                        if string_array.is_null(i) {
                            None
                        } else {
                            string_array.value(i).parse().ok()
                        }
                    })
                    .collect();
                Ok(Arc::new(BooleanArray::from(bool_values)))
            }
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
                Ok(Arc::new(Float64Array::from(float_values)))
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

    fn stringify_column(&self, array: &dyn Array, num_rows: usize) -> Result<Arc<dyn Array>> {
        let owned: Vec<Option<String>> = (0..num_rows)
            .map(|i| if array.is_null(i) { None } else { Some(array_value_display(array, i)) })
            .collect();
        let refs: Vec<Option<&str>> = owned.iter().map(|o| o.as_deref()).collect();
        Ok(Arc::new(StringArray::from(refs)))
    }

    fn create_null_column(&self, data_type: &DataType, num_rows: usize) -> Result<Arc<dyn Array>> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => {
                Ok(Arc::new(StringArray::from(vec![None::<&str>; num_rows])))
            }
            DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![None::<i64>; num_rows]))),
            DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![None::<f64>; num_rows]))),
            DataType::Boolean => Ok(Arc::new(BooleanArray::from(vec![None::<bool>; num_rows]))),
            _ => Ok(Arc::new(StringArray::from(vec![None::<&str>; num_rows]))),
        }
    }
}

fn array_value_display(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return String::new();
    }
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::collections::HashMap;

    #[test]
    fn test_batch_alignment() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let unified_schema = Arc::new(UnifiedSchema {
            schema: Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]),
            ..UnifiedSchema::default()
        });
        let aligner = BatchAligner::new(unified_schema, HashMap::new(), None, None, false);

        let aligned =
            aligner.align_batch_with_source(batch, &["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(aligned.num_rows(), 3);
    }
}
