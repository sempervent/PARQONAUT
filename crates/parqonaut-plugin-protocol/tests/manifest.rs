use parqonaut_plugin_protocol::{
    BatchTransformCapabilities, PluginCapabilities, PluginExecutionPhase, PluginManifest,
    ScanAnalyzerCapabilities, PLUGIN_PROTOCOL_VERSION,
};
use parqonaut_types::DataFormat;

fn scan_caps() -> PluginCapabilities {
    PluginCapabilities {
        scan: Some(ScanAnalyzerCapabilities {
            supported_formats: vec![DataFormat::Parquet],
            supported_phases: vec![PluginExecutionPhase::PostRules],
            tags: Vec::new(),
        }),
        batch_transform: None,
    }
}

#[test]
fn manifest_validation_rejects_empty_name() {
    let manifest = PluginManifest {
        protocol_version: PLUGIN_PROTOCOL_VERSION,
        name: "   ".into(),
        version: "0.1.0".into(),
        entrypoint: "plugin:run".into(),
        capabilities: scan_caps(),
        requires_parqonaut: None,
        metadata: Default::default(),
    };
    assert!(manifest.validate().is_err());
}

#[test]
fn manifest_roundtrips_json() {
    let manifest = PluginManifest {
        protocol_version: PLUGIN_PROTOCOL_VERSION,
        name: "demo".into(),
        version: "0.1.0".into(),
        entrypoint: "parqonaut_plugins.demo:run".into(),
        capabilities: PluginCapabilities {
            scan: Some(ScanAnalyzerCapabilities {
                supported_formats: vec![DataFormat::Csv, DataFormat::Parquet],
                supported_phases: vec![
                    PluginExecutionPhase::PreScan,
                    PluginExecutionPhase::PostInventory,
                ],
                tags: vec!["demo".into()],
            }),
            batch_transform: Some(BatchTransformCapabilities { tags: vec![] }),
        },
        requires_parqonaut: Some(">=0.10,<1.0".into()),
        metadata: Default::default(),
    };
    manifest.validate().unwrap();
    let json = serde_json::to_string(&manifest).unwrap();
    let back: PluginManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(manifest, back);
}
