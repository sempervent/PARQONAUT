use std::fs;
use std::path::PathBuf;

use parqonaut_plugin_host::{
    batch_plugin_spawn_count, BatchPluginSession, PluginCatalog, PluginHostError,
    PluginRuntimeConfig,
};

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

#[test]
fn realistic_stale_digest_rejected_before_spawn() {
    let src =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins/batch-passthrough");
    let work = tempfile::tempdir().expect("tempdir");
    let plugin_dir = work.path().join("batch-passthrough");
    copy_dir_all(&src, &plugin_dir).expect("copy plugin");

    let entry_a = PluginCatalog::validate_path(&plugin_dir).expect("validate A");
    let digest_a = entry_a.digest.clone();

    let py = plugin_dir.join("batch_passthrough.py");
    let mut code = fs::read_to_string(&py).expect("read plugin");
    code.push_str("\n# stale bump\n");
    fs::write(&py, code).expect("write plugin");

    let entry_b = PluginCatalog::validate_path(&plugin_dir).expect("validate B");
    let digest_b = entry_b.digest.clone();
    assert_ne!(digest_a, digest_b, "digests must differ after edit");

    let spawns_before = batch_plugin_spawn_count().load(std::sync::atomic::Ordering::SeqCst);
    let runtime =
        PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() };
    let err = match BatchPluginSession::start(
        &entry_b,
        serde_json::json!({}),
        "stale-ab",
        &runtime,
        Some(digest_a.as_str()),
        None,
    ) {
        Ok(_) => panic!("stale plan must fail before spawn"),
        Err(e) => e,
    };
    assert!(matches!(err, PluginHostError::StalePlugin { .. }));
    assert_eq!(
        batch_plugin_spawn_count().load(std::sync::atomic::Ordering::SeqCst),
        spawns_before,
        "stale rejection must not spawn Python"
    );
}

fn copy_dir_all(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dest = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&entry.path(), &dest)?;
        } else {
            fs::copy(entry.path(), dest)?;
        }
    }
    Ok(())
}
