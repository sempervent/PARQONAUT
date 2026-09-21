use std::path::PathBuf;

use parqonaut_plugin_host::PluginCatalog;
use tempfile::TempDir;

fn workspace_fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

#[test]
fn discovers_example_rules_fixture() {
    let roots = vec![workspace_fixture_root()];
    let catalog = PluginCatalog::discover_from_roots(&roots).expect("discover");
    let entry = catalog.get("example-rules").expect("example-rules");
    assert!(!entry.digest.is_empty());
    assert_ne!(entry.digest, "pending");
}

#[test]
fn duplicate_name_is_deterministic() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("roots");
    std::fs::create_dir_all(&root).unwrap();
    let a = root.join("a-plugin");
    let b = root.join("b-plugin");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    let manifest = r#"{
      "protocol_version": 1,
      "name": "dupe",
      "version": "0.1.0",
      "entrypoint": "x:run",
      "capabilities": { "scan": { "supported_formats": ["parquet"], "supported_phases": ["post_rules"] } }
    }"#;
    std::fs::write(a.join("parqonaut-plugin.json"), manifest).unwrap();
    std::fs::write(b.join("parqonaut-plugin.json"), manifest).unwrap();
    std::fs::write(a.join("x.py"), "def run(c): pass\n").unwrap();
    std::fs::write(b.join("x.py"), "def run(c): pass\n").unwrap();
    let err = PluginCatalog::discover_from_roots(&[root]).unwrap_err();
    assert!(err.to_string().contains("duplicate"));
}
