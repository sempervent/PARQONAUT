use crate::engine::compat::*;
use crate::error::{Result, TransformError};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum FilterExpr {
    Eq(String, Value),
    Ne(String, Value),
    Lt(String, Value),
    Le(String, Value),
    Gt(String, Value),
    Ge(String, Value),
    In(String, Vec<Value>),
    IsNull(String),
    IsNotNull(String),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

#[derive(Debug, Clone)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

pub fn parse_filter(expr: &str) -> Result<FilterExpr> {
    filter_expr(expr)
        .map(|(_, expr)| expr)
        .map_err(|e| TransformError::FilterError(format!("Parse error: {:?}", e)))
}

fn filter_expr(input: &str) -> IResult<&str, FilterExpr> {
    alt((map(or_expr, |e| e), map(and_expr, |e| e), map(comparison, |e| e)))(input)
}

fn or_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (input, first) = and_expr(input)?;
    let (input, rest) = nom::multi::many0(preceded(
        delimited(multispace0, tag("OR"), multispace0),
        and_expr,
    ))(input)?;

    Ok((input, rest.into_iter().fold(first, |acc, e| FilterExpr::Or(Box::new(acc), Box::new(e)))))
}

fn and_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (input, first) = comparison(input)?;
    let (input, rest) = nom::multi::many0(preceded(
        delimited(multispace0, tag("AND"), multispace0),
        comparison,
    ))(input)?;

    Ok((input, rest.into_iter().fold(first, |acc, e| FilterExpr::And(Box::new(acc), Box::new(e)))))
}

fn comparison(input: &str) -> IResult<&str, FilterExpr> {
    alt((is_null_expr, is_not_null_expr, in_expr, binary_comparison, not_expr, paren_expr))(input)
}

fn not_expr(input: &str) -> IResult<&str, FilterExpr> {
    map(preceded(delimited(multispace0, tag("NOT"), multispace1), comparison), |e| {
        FilterExpr::Not(Box::new(e))
    })(input)
}

fn paren_expr(input: &str) -> IResult<&str, FilterExpr> {
    delimited(
        delimited(multispace0, char('('), multispace0),
        filter_expr,
        delimited(multispace0, char(')'), multispace0),
    )(input)
}

fn binary_comparison(input: &str) -> IResult<&str, FilterExpr> {
    let (input, column) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, op) = comparison_op(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = value(input)?;

    let expr = match op {
        "=" => FilterExpr::Eq(column, value),
        "!=" => FilterExpr::Ne(column, value),
        "<" => FilterExpr::Lt(column, value),
        "<=" => FilterExpr::Le(column, value),
        ">" => FilterExpr::Gt(column, value),
        ">=" => FilterExpr::Ge(column, value),
        _ => unreachable!(),
    };

    Ok((input, expr))
}

fn comparison_op(input: &str) -> IResult<&str, &str> {
    alt((tag("!="), tag("<="), tag(">="), tag("="), tag("<"), tag(">")))(input)
}

fn is_null_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (input, column) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("IS NULL")(input)?;
    Ok((input, FilterExpr::IsNull(column)))
}

fn is_not_null_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (input, column) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("IS NOT NULL")(input)?;
    Ok((input, FilterExpr::IsNotNull(column)))
}

fn in_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (input, column) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("IN")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, values) = delimited(
        char('('),
        nom::multi::separated_list1(delimited(multispace0, char(','), multispace0), value),
        char(')'),
    )(input)?;
    Ok((input, FilterExpr::In(column, values)))
}

fn identifier(input: &str) -> IResult<&str, String> {
    map(
        recognize(pair(
            nom::character::complete::satisfy(|c: char| c == '_' || c.is_alphabetic()),
            take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        )),
        |s: &str| s.to_string(),
    )(input)
}

fn value(input: &str) -> IResult<&str, Value> {
    alt((
        map(nom::character::complete::i64, Value::Int64),
        map(
            recognize(tuple((
                opt(char('-')),
                nom::character::complete::digit1,
                opt(preceded(char('.'), nom::character::complete::digit1)),
            ))),
            |s: &str| Value::Float64(s.parse().unwrap_or(0.0)),
        ),
        map(delimited(char('"'), take_while1(|c: char| c != '"'), char('"')), |s: &str| {
            Value::String(s.to_string())
        }),
        map(tag("true"), |_| Value::Bool(true)),
        map(tag("false"), |_| Value::Bool(false)),
        map(tag("NULL"), |_| Value::Null),
    ))(input)
}

pub fn evaluate_filter(batch: &RecordBatch, expr: &FilterExpr) -> Result<BooleanArray> {
    match expr {
        FilterExpr::Eq(col, val) => eval_eq(batch, col, val),
        FilterExpr::Ne(col, val) => eval_ne(batch, col, val),
        FilterExpr::Lt(col, val) => eval_lt(batch, col, val),
        FilterExpr::Le(col, val) => eval_le(batch, col, val),
        FilterExpr::Gt(col, val) => eval_gt(batch, col, val),
        FilterExpr::Ge(col, val) => eval_ge(batch, col, val),
        FilterExpr::In(col, vals) => eval_in(batch, col, vals),
        FilterExpr::IsNull(col) => eval_is_null(batch, col),
        FilterExpr::IsNotNull(col) => eval_is_not_null(batch, col),
        FilterExpr::And(l, r) => {
            let left = evaluate_filter(batch, l)?;
            let right = evaluate_filter(batch, r)?;
            and_kleene(&left, &right)
        }
        FilterExpr::Or(l, r) => {
            let left = evaluate_filter(batch, l)?;
            let right = evaluate_filter(batch, r)?;
            or_kleene(&left, &right)
        }
        FilterExpr::Not(e) => {
            let result = evaluate_filter(batch, e)?;
            not_bool(&result)
        }
    }
}

fn eval_eq(batch: &RecordBatch, col: &str, val: &Value) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;

    match val {
        Value::Int64(v) => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| TransformError::FilterError("Type mismatch".to_string()))?;
            // Create a constant array and compare
            let constant = Int64Array::from(vec![*v; arr.len()]);
            eq_arrays(arr, &constant)
        }
        Value::String(v) => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| TransformError::FilterError("Type mismatch".to_string()))?;
            let constant = StringArray::from(vec![v.as_str(); arr.len()]);
            eq_string_arrays(arr, &constant)
        }
        _ => Err(TransformError::FilterError("Unsupported comparison type".to_string())),
    }
}

fn eval_ne(batch: &RecordBatch, col: &str, val: &Value) -> Result<BooleanArray> {
    let result = eval_eq(batch, col, val)?;
    not_bool(&result)
}

fn eval_lt(batch: &RecordBatch, col: &str, val: &Value) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;

    match val {
        Value::Int64(v) => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| TransformError::FilterError("Type mismatch".to_string()))?;
            let constant = Int64Array::from(vec![*v; arr.len()]);
            lt_arrays(arr, &constant)
        }
        _ => Err(TransformError::FilterError("Unsupported comparison type".to_string())),
    }
}

fn eval_le(batch: &RecordBatch, col: &str, val: &Value) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;

    match val {
        Value::Int64(v) => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| TransformError::FilterError("Type mismatch".to_string()))?;
            let constant = Int64Array::from(vec![*v; arr.len()]);
            le_arrays(arr, &constant)
        }
        _ => Err(TransformError::FilterError("Unsupported comparison type".to_string())),
    }
}

fn eval_gt(batch: &RecordBatch, col: &str, val: &Value) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;

    match val {
        Value::Int64(v) => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| TransformError::FilterError("Type mismatch".to_string()))?;
            let constant = Int64Array::from(vec![*v; arr.len()]);
            gt_arrays(arr, &constant)
        }
        _ => Err(TransformError::FilterError("Unsupported comparison type".to_string())),
    }
}

fn eval_ge(batch: &RecordBatch, col: &str, val: &Value) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;

    match val {
        Value::Int64(v) => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| TransformError::FilterError("Type mismatch".to_string()))?;
            let constant = Int64Array::from(vec![*v; arr.len()]);
            ge_arrays(arr, &constant)
        }
        _ => Err(TransformError::FilterError("Unsupported comparison type".to_string())),
    }
}

fn eval_in(batch: &RecordBatch, col: &str, vals: &[Value]) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;

    // Build a Vec for membership testing (HashSet doesn't work with f64)
    // For small lists, linear scan is fine
    let value_vec: Vec<Value> = vals.to_vec();

    // Determine the type from the first value (all should be same type)
    if vals.is_empty() {
        return Ok(BooleanArray::from(vec![false; array.len()]));
    }

    match &vals[0] {
        Value::Int64(_) => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                TransformError::FilterError("Type mismatch for IN: expected Int64".to_string())
            })?;
            let mut result = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(false); // NULL values don't match
                } else {
                    let val = arr.value(i);
                    result.push(value_vec.contains(&Value::Int64(val)));
                }
            }
            Ok(BooleanArray::from(result))
        }
        Value::String(_) => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                TransformError::FilterError("Type mismatch for IN: expected String".to_string())
            })?;
            let mut result = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(false); // NULL values don't match
                } else {
                    let val = arr.value(i).to_string();
                    result.push(value_vec.contains(&Value::String(val)));
                }
            }
            Ok(BooleanArray::from(result))
        }
        Value::Bool(_) => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                TransformError::FilterError("Type mismatch for IN: expected Bool".to_string())
            })?;
            let mut result = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(false); // NULL values don't match
                } else {
                    let val = arr.value(i);
                    result.push(value_vec.contains(&Value::Bool(val)));
                }
            }
            Ok(BooleanArray::from(result))
        }
        Value::Float64(_) => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                TransformError::FilterError("Type mismatch for IN: expected Float64".to_string())
            })?;
            let mut result = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    result.push(false); // NULL values don't match
                } else {
                    let val = arr.value(i);
                    // For floats, we need to handle NaN carefully
                    // Use linear search with special NaN handling
                    let mut found = false;
                    for v in &value_vec {
                        if let Value::Float64(f) = v {
                            // Handle NaN: NaN != NaN, so check explicitly
                            if (val.is_nan() && f.is_nan()) || val == *f {
                                found = true;
                                break;
                            }
                        }
                    }
                    result.push(found);
                }
            }
            Ok(BooleanArray::from(result))
        }
        Value::Null => {
            Err(TransformError::FilterError("IN operator cannot use NULL as a value".to_string()))
        }
    }
}

fn eval_is_null(batch: &RecordBatch, col: &str) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;
    is_null_array(array)
}

fn eval_is_not_null(batch: &RecordBatch, col: &str) -> Result<BooleanArray> {
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| TransformError::InvalidInput(format!("Column not found: {}", col)))?;
    is_not_null_array(array)
}

pub struct FilterTransform {
    expr: FilterExpr,
}

impl FilterTransform {
    pub fn new(expr: FilterExpr) -> Self {
        Self { expr }
    }
}

impl crate::engine::pipeline::Transform for FilterTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mask = evaluate_filter(&batch, &self.expr)?;
        let filtered = compute::filter_record_batch(&batch, &mask)?;
        Ok(filtered)
    }

    fn name(&self) -> &str {
        "filter"
    }
}
