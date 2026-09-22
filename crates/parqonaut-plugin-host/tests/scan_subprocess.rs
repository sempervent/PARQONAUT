use std::path::PathBuf;

use parqonaut_plugin_host::{CancelToken, PluginCatalog, PluginHost, PluginRuntimeConfig};
use parqonaut_plugin_protocol::{PluginExecutionPhase, PluginScanContext};
use parqonaut_types::{ScanProfile, ScanRequest, ScanTarget};

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

fn fixture_plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn runtime() -> PluginRuntimeConfig {
    PluginRuntimeConfig { sdk_src_root: Some(sdk_path()), ..PluginRuntimeConfig::default() }
}

#[test]
fn example_rules_executes_post_rules() {
    let catalog = PluginCatalog::discover_from_roots(&[fixture_plugins_root()]).unwrap();
    let host = PluginHost::new(catalog, runtime());
    let resolved = host.resolve_plugins(&["example-rules".into()]).unwrap();
    let target = ScanTarget::LocalDirectory { path: camino::Utf8PathBuf::from("fixtures/csv") };
    let req = ScanRequest::new(target, ScanProfile::Quick);
    let ctx = PluginScanContext {
        request: req,
        discovered_files: vec!["a.csv".into()],
        inventory_summary: Default::default(),
        builtin_finding_summaries: vec![],
        hints: Default::default(),
    };
    let (findings, runs) = host
        .run_phase(
            &resolved,
            PluginExecutionPhase::PostRules,
            ctx,
            &Default::default(),
            &CancelToken::new(),
        )
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert!(runs[0].status == "succeeded");
    assert_eq!(findings.len(), 1);
    assert!(findings[0].code.as_str().contains("plugin.example_rules"));
}

#[test]
fn secret_canaries_not_visible_to_plugin() {
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "CANARY_AWS_SECRET");
    std::env::set_var("PRQNT_BOOTSTRAP_ADMIN_TOKEN", "CANARY_PRQNT");
    std::env::set_var("GITHUB_TOKEN", "CANARY_GITHUB");
    std::env::set_var("DATABASE_URL", "postgres://user:CANARY_DB@example/db");

    let catalog = PluginCatalog::discover_from_roots(&[fixture_plugins_root()]).unwrap();
    let host = PluginHost::new(catalog, runtime());
    let resolved = host.resolve_plugins(&["leak-env".into()]).unwrap();
    let req = ScanRequest::new(
        ScanTarget::LocalDirectory { path: camino::Utf8PathBuf::from(".") },
        ScanProfile::Quick,
    );
    let ctx = PluginScanContext {
        request: req,
        discovered_files: vec![],
        inventory_summary: Default::default(),
        builtin_finding_summaries: vec![],
        hints: Default::default(),
    };
    let (findings, _) = host
        .run_phase(
            &resolved,
            PluginExecutionPhase::PostRules,
            ctx,
            &Default::default(),
            &CancelToken::new(),
        )
        .unwrap();
    let blob = serde_json::to_string(&findings).unwrap();
    assert!(!blob.contains("CANARY_AWS_SECRET"));
    assert!(!blob.contains("CANARY_PRQNT"));
    assert!(!blob.contains("CANARY_GITHUB"));
    assert!(!blob.contains("CANARY_DB"));
}

#[test]
fn bad_json_stdout_maps_to_protocol_violation() {
    let catalog = PluginCatalog::discover_from_roots(&[fixture_plugins_root()]).unwrap();
    let host = PluginHost::new(catalog, runtime());
    let resolved = host.resolve_plugins(&["bad-json".into()]).unwrap();
    let req = ScanRequest::new(
        ScanTarget::LocalDirectory { path: camino::Utf8PathBuf::from(".") },
        ScanProfile::Quick,
    );
    let ctx = PluginScanContext {
        request: req,
        discovered_files: vec![],
        inventory_summary: Default::default(),
        builtin_finding_summaries: vec![],
        hints: Default::default(),
    };
    let err = host
        .run_phase(
            &resolved,
            PluginExecutionPhase::PostRules,
            ctx,
            &Default::default(),
            &CancelToken::new(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        parqonaut_plugin_host::PluginHostError::ProtocolViolation(_)
            | parqonaut_plugin_host::PluginHostError::ProcessFailed { .. }
    ));
}
