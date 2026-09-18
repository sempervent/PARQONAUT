// Compatibility layer for Arrow API differences across versions
// Manual implementations that work across Arrow versions
use crate::error::{ParqknifeError, Result};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;

pub fn eq_arrays(left: &Int64Array, right: &Int64Array) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParqknifeError::FilterError("Array length mismatch".to_string()));
    }
    let mut result = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            result.push(false);
        } else {
            result.push(left.value(i) == right.value(i));
        }
    }
    Ok(BooleanArray::from(result))
}

pub fn lt_arrays(left: &Int64Array, right: &Int64Array) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParqknifeError::FilterError("Array length mismatch".to_string()));
    }
    let mut result = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            result.push(false);
        } else {
            result.push(left.value(i) < right.value(i));
        }
    }
    Ok(BooleanArray::from(result))
}

pub fn le_arrays(left: &Int64Array, right: &Int64Array) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParqknifeError::FilterError("Array length mismatch".to_string()));
    }
    let mut result = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            result.push(false);
        } else {
            result.push(left.value(i) <= right.value(i));
        }
    }
    Ok(BooleanArray::from(result))
}

pub fn gt_arrays(left: &Int64Array, right: &Int64Array) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParqknifeError::FilterError("Array length mismatch".to_string()));
    }
    let mut result = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            result.push(false);
        } else {
            result.push(left.value(i) > right.value(i));
        }
    }
    Ok(BooleanArray::from(result))
}

pub fn ge_arrays(left: &Int64Array, right: &Int64Array) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParqknifeError::FilterError("Array length mismatch".to_string()));
    }
    let mut result = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            result.push(false);
        } else {
            result.push(left.value(i) >= right.value(i));
        }
    }
    Ok(BooleanArray::from(result))
}

// String comparisons
pub fn eq_string_arrays(left: &StringArray, right: &StringArray) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(ParqknifeError::FilterError("Array length mismatch".to_string()));
    }
    let mut result = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            result.push(false);
        } else {
            result.push(left.value(i) == right.value(i));
        }
    }
    Ok(BooleanArray::from(result))
}

pub fn and_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    compute::and_kleene(left, right).map_err(ParqknifeError::Arrow)
}

pub fn or_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray> {
    compute::or_kleene(left, right).map_err(ParqknifeError::Arrow)
}

pub fn not_bool(arr: &BooleanArray) -> Result<BooleanArray> {
    compute::not(arr).map_err(ParqknifeError::Arrow)
}

pub fn is_null_array(arr: &dyn arrow::array::Array) -> Result<BooleanArray> {
    compute::is_null(arr).map_err(ParqknifeError::Arrow)
}

pub fn is_not_null_array(arr: &dyn arrow::array::Array) -> Result<BooleanArray> {
    compute::is_not_null(arr).map_err(ParqknifeError::Arrow)
}
