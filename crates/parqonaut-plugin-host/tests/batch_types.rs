use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parqonaut_plugin_host::{
    validate_schema_supported, BatchPluginSession, PluginCatalog, PluginHostError,
    PluginRuntimeConfig,
};
use std::collections::HashMap;

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

fn plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn passthrough_roundtrip(batch: RecordBatch) {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("batch-passthrough").unwrap();
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    validate_schema_supported(batch.schema().as_ref()).expect("supported schema");
    let mut session =
        BatchPluginSession::start(entry, serde_json::json!({}), "types-test", &runtime, None, None)
            .unwrap();
    let out = session.transform(batch.clone()).expect("roundtrip");
    assert_eq!(out.schema(), batch.schema());
    assert_eq!(out.num_rows(), batch.num_rows());
    session.finish().unwrap();
}

#[test]
fn arrow_ipc_int32_utf8_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn arrow_ipc_int64_float64_bool() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
        Field::new("b", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn arrow_ipc_timestamp_micros() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000), None]))],
    )
    .unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn arrow_ipc_decimal128() {
    let schema =
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Decimal128(10, 2), true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(
            arrow::array::Decimal128Array::from(vec![Some(12345), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        )],
    )
    .unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn arrow_ipc_dictionary_utf8() {
    let values: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec!["a", "b"]));
    let keys = Int32Array::from(vec![0, 1, 0]);
    let dict = DictionaryArray::try_new(keys, values).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("cat", dict.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn arrow_ipc_timestamp_with_timezone() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(
            TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000)])
                .with_timezone("America/New_York"),
        )],
    )
    .unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn arrow_ipc_field_metadata_preserved() {
    let mut meta = HashMap::new();
    meta.insert("unit".into(), "widgets".into());
    let field = Field::new("id", DataType::Int32, false).with_metadata(meta);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn nested_list_rejected_before_spawn() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let err = validate_schema_supported(&schema).unwrap_err();
    assert!(matches!(err, PluginHostError::PluginUnsupportedArrowType(_)));
}
