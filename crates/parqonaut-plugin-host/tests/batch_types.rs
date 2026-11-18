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

#[test]
fn unsupported_struct_map_fixed_binary_rejected_before_spawn() {
    let struct_schema = Schema::new(vec![Field::new(
        "nested",
        DataType::Struct(vec![Field::new("a", DataType::Int32, false)].into()),
        true,
    )]);
    assert!(matches!(
        validate_schema_supported(&struct_schema).unwrap_err(),
        PluginHostError::PluginUnsupportedArrowType(_)
    ));

    let fixed_bin_schema =
        Schema::new(vec![Field::new("bin", DataType::FixedSizeBinary(16), false)]);
    assert!(matches!(
        validate_schema_supported(&fixed_bin_schema).unwrap_err(),
        PluginHostError::PluginUnsupportedArrowType(_)
    ));
}

#[test]
fn arrow_ipc_zero_row_batch_and_multiple_consecutive_batches() {
    let catalog = PluginCatalog::discover_from_roots(&[plugins_root()]).unwrap();
    let entry = catalog.get("batch-passthrough").unwrap();
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    let mut session = BatchPluginSession::start(
        entry,
        serde_json::json!({}),
        "multi-batch-test",
        &runtime,
        None,
        None,
    )
    .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let zero_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ],
    )
    .unwrap();
    let out_zero = session.transform(zero_batch).unwrap();
    assert_eq!(out_zero.num_rows(), 0);

    for i in 0..4 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(i), None])),
                Arc::new(StringArray::from(vec![Some("row"), None])),
            ],
        )
        .unwrap();
        let out = session.transform(batch.clone()).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema(), batch.schema());
    }
    session.finish().unwrap();
}

#[test]
fn arrow_ipc_large_payload_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let n = 8_000usize;
    let ids: Vec<i32> = (0..n as i32).collect();
    let payload_str = "x".repeat(256);
    let payloads: Vec<&str> = vec![payload_str.as_str(); n];
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(payloads))],
    )
    .unwrap();
    passthrough_roundtrip(batch);
}

#[test]
fn schema_metadata_change_and_missing_column_rejected() {
    let work = tempfile::tempdir().unwrap();
    let p_meta = work.path().join("meta-mut");
    std::fs::create_dir_all(&p_meta).unwrap();
    std::fs::write(
        p_meta.join("parqonaut-plugin.json"),
        r#"{
          "protocol_version": 1,
          "name": "meta-mut",
          "version": "0.1.0",
          "entrypoint": "meta_mut:transform",
          "capabilities": { "batch_transform": {} }
        }"#,
    )
    .unwrap();
    std::fs::write(
        p_meta.join("meta_mut.py"),
        "def transform(batch, _cfg):\n    return batch.replace_schema_metadata({'injected': 'yes'})\n",
    )
    .unwrap();
    let entry_meta = PluginCatalog::validate_path(&p_meta).unwrap();
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    let mut sess = BatchPluginSession::start(
        &entry_meta,
        serde_json::json!({}),
        "meta-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let err = sess.transform(batch).unwrap_err();
    assert!(matches!(err, PluginHostError::PluginSchemaMismatch(_)));
    drop(sess);

    let p_drop = work.path().join("drop-col");
    std::fs::create_dir_all(&p_drop).unwrap();
    std::fs::write(
        p_drop.join("parqonaut-plugin.json"),
        r#"{
          "protocol_version": 1,
          "name": "drop-col",
          "version": "0.1.0",
          "entrypoint": "drop_col:transform",
          "capabilities": { "batch_transform": {} }
        }"#,
    )
    .unwrap();
    std::fs::write(
        p_drop.join("drop_col.py"),
        "import pyarrow as pa\ndef transform(batch, _cfg):\n    return pa.RecordBatch.from_arrays([batch.column(0)], names=['id'])\n",
    )
    .unwrap();
    let entry_drop = PluginCatalog::validate_path(&p_drop).unwrap();
    let mut sess2 = BatchPluginSession::start(
        &entry_drop,
        serde_json::json!({}),
        "drop-test",
        &runtime,
        None,
        None,
    )
    .unwrap();
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("extra", DataType::Int32, false),
    ]));
    let batch2 = RecordBatch::try_new(
        schema2,
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(Int32Array::from(vec![2]))],
    )
    .unwrap();
    let err2 = sess2.transform(batch2).unwrap_err();
    assert!(matches!(err2, PluginHostError::PluginSchemaMismatch(_)));
}
