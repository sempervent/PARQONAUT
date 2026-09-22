use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parqonaut_transform::engine::filter::{evaluate_filter, parse_filter, FilterExpr};
use parqonaut_transform::error::Result;
use std::sync::Arc;

fn create_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
        ],
    ).unwrap()
}

#[test]
fn test_parse_filter_eq() -> Result<()> {
    let expr = parse_filter("id = 3")?;
    match expr {
        FilterExpr::Eq(col, val) => {
            assert_eq!(col, "id");
            match val {
                parqonaut_transform::engine::filter::Value::Int64(v) => assert_eq!(v, 3),
                _ => panic!("Expected Int64"),
            }
        }
        _ => panic!("Expected Eq"),
    }
    Ok(())
}

#[test]
fn test_parse_filter_and() -> Result<()> {
    let expr = parse_filter("id > 2 AND id < 5")?;
    match expr {
        FilterExpr::And(_, _) => {}
        _ => panic!("Expected And"),
    }
    Ok(())
}

#[test]
fn test_evaluate_filter_eq() -> Result<()> {
    let batch = create_test_batch();
    let expr = parse_filter("id = 3")?;
    let result = evaluate_filter(&batch, &expr)?;
    
    // Should have 5 elements, only index 2 should be true
    assert_eq!(result.len(), 5);
    assert!(!result.value(0));
    assert!(!result.value(1));
    assert!(result.value(2));
    assert!(!result.value(3));
    assert!(!result.value(4));
    
    Ok(())
}

#[test]
fn test_evaluate_filter_gt() -> Result<()> {
    let batch = create_test_batch();
    let expr = parse_filter("id > 3")?;
    let result = evaluate_filter(&batch, &expr)?;
    
    assert_eq!(result.len(), 5);
    assert!(!result.value(0));
    assert!(!result.value(1));
    assert!(!result.value(2));
    assert!(result.value(3));
    assert!(result.value(4));
    
    Ok(())
}
