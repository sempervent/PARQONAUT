use paraclete_plugin_protocol::PluginManifest;

#[test]
fn plugin_manifest_schema_serializes() {
    let schema = schemars::schema_for!(PluginManifest);
    serde_json::to_value(schema).expect("schema should serialize to JSON");
}
