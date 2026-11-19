#![no_main]

use libfuzzer_sys::fuzz_target;
use parqonaut_repair::{
    resolve_canonical_schema, FieldDescriptor, FileSchemaView, SchemaPolicy,
};

fuzz_target!(|data: &[u8]| {
    if data.len() > 512 * 1024 {
        return;
    }
    if let Ok(raw) = serde_json::from_slice::<Vec<(String, Vec<FieldDescriptor>)>>(data) {
        let views: Vec<FileSchemaView> =
            raw.into_iter().map(|(path, fields)| FileSchemaView { path, fields }).collect();
        let _ = resolve_canonical_schema(&views, None, &SchemaPolicy::default());
    }
    if let Ok(pol) = serde_json::from_slice::<SchemaPolicy>(data) {
        let _ = resolve_canonical_schema(&[], None, &pol);
    }
});
