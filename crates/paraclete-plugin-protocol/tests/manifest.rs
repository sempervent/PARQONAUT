use paraclete_plugin_protocol::{PluginCapabilities, PluginExecutionPhase, PluginManifest};
use paraclete_types::DataFormat;

#[test]
fn manifest_validation_rejects_empty_name() {
    let manifest = PluginManifest {
        name: "   ".into(),
        version: "0.1.0".into(),
        entrypoint: "plugin:run".into(),
        capabilities: PluginCapabilities {
            supported_formats: vec![DataFormat::Parquet],
            supported_phases: vec![PluginExecutionPhase::PostRules],
            tags: Vec::new(),
        },
        metadata: Default::default(),
    };
    assert!(manifest.validate().is_err());
}

#[test]
fn manifest_roundtrips_json() {
    let manifest = PluginManifest {
        name: "demo".into(),
        version: "0.1.0".into(),
        entrypoint: "paraclete_plugins.demo:run".into(),
        capabilities: PluginCapabilities {
            supported_formats: vec![DataFormat::Csv, DataFormat::Parquet],
            supported_phases: vec![
                PluginExecutionPhase::PreScan,
                PluginExecutionPhase::PostInventory,
            ],
            tags: vec!["demo".into()],
        },
        metadata: Default::default(),
    };
    manifest.validate().unwrap();
    let json = serde_json::to_string(&manifest).unwrap();
    let back: PluginManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(manifest, back);
}
