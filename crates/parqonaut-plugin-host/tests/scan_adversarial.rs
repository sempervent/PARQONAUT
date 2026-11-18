//! Subprocess adversarial coverage for scan analyzer plugins.

use std::path::PathBuf;

use parqonaut_plugin_host::{
    CancelToken, PluginCatalog, PluginHost, PluginHostError, PluginResourcePolicy,
    PluginRuntimeConfig, ScanPluginExecutor,
};
use parqonaut_plugin_protocol::{PluginExecutionPhase, PluginScanContext};
use parqonaut_types::{ScanProfile, ScanRequest, ScanTarget};

fn sdk_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/parqonaut_plugins/src")
}

fn fixture_plugins_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/plugins")
}

fn ctx() -> PluginScanContext {
    let req = ScanRequest::new(
        ScanTarget::LocalDirectory { path: camino::Utf8PathBuf::from("fixtures/csv") },
        ScanProfile::Quick,
    );
    PluginScanContext {
        request: req,
        discovered_files: vec![],
        inventory_summary: Default::default(),
        builtin_finding_summaries: vec![],
        hints: Default::default(),
    }
}

fn host_with_policy(policy: PluginResourcePolicy) -> PluginHost {
    let catalog = PluginCatalog::discover_from_roots(&[fixture_plugins_root()]).unwrap();
    let runtime = PluginRuntimeConfig {
        sdk_src_root: Some(sdk_path()),
        policy,
        ..PluginRuntimeConfig::default()
    };
    PluginHost::new(catalog, runtime)
}

fn run_plugin(host: &PluginHost, name: &str) -> Result<(), PluginHostError> {
    let resolved = host.resolve_plugins(&[name.into()])?;
    host.run_phase(
        &resolved,
        PluginExecutionPhase::PostRules,
        ctx(),
        &Default::default(),
        &CancelToken::new(),
    )
    .map(|_| ())
}

#[test]
fn timeout_terminates_and_reaps_child() {
    let policy =
        PluginResourcePolicy { default_timeout_ms: 400, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let catalog = host.catalog();
    let entry = catalog.get("adv-sleep").unwrap();
    let executor = ScanPluginExecutor::new(PluginRuntimeConfig {
        sdk_src_root: Some(sdk_path()),
        policy: PluginResourcePolicy { default_timeout_ms: 400, ..Default::default() },
        ..Default::default()
    });
    let err = executor
        .execute_scan(entry, PluginExecutionPhase::PostRules, ctx(), &CancelToken::new(), Some(400))
        .unwrap_err();
    assert!(matches!(err, PluginHostError::Timeout { .. }));
}

#[test]
fn nonzero_exit_includes_identity_and_bounded_stderr() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-exit-one").unwrap_err();
    match err {
        PluginHostError::ProcessFailed { detail, .. } => {
            assert!(detail.contains("adv-exit-one"));
            assert!(detail.contains("diagnostic-from-plugin"));
            assert!(!detail.contains("AWS_SECRET"));
        }
        other => panic!("expected ProcessFailed, got {other:?}"),
    }
}

#[test]
fn huge_stderr_is_capped() {
    let policy =
        PluginResourcePolicy { max_stderr_bytes: 8 * 1024, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    // Should succeed; stderr is capped internally.
    run_plugin(&host, "adv-huge-stderr").expect("huge stderr plugin should complete");
}

#[test]
fn oversized_stdout_returns_response_too_large() {
    let policy =
        PluginResourcePolicy { max_response_bytes: 4096, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let err = run_plugin(&host, "adv-huge-stdout").unwrap_err();
    assert!(matches!(err, PluginHostError::ResponseTooLarge { .. }));
}

#[test]
fn too_many_findings_rejected() {
    let policy = PluginResourcePolicy { max_findings: 4, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let err = run_plugin(&host, "adv-many-findings").unwrap_err();
    assert!(matches!(err, PluginHostError::ResultPolicyViolation(_)));
}

#[test]
fn long_summary_rejected() {
    let policy =
        PluginResourcePolicy { max_finding_summary_len: 128, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let err = run_plugin(&host, "adv-long-summary").unwrap_err();
    assert!(matches!(err, PluginHostError::ResultPolicyViolation(_)));
}

#[test]
fn long_detail_rejected() {
    let policy =
        PluginResourcePolicy { max_finding_detail_len: 128, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let err = run_plugin(&host, "adv-long-detail").unwrap_err();
    assert!(matches!(err, PluginHostError::ResultPolicyViolation(_)));
}

#[test]
fn too_many_evidence_ids_rejected() {
    let policy =
        PluginResourcePolicy { max_evidence_ids_per_finding: 2, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let err = run_plugin(&host, "adv-many-evidence").unwrap_err();
    assert!(matches!(err, PluginHostError::ResultPolicyViolation(_)));
}

#[test]
fn oversized_annotations_rejected() {
    let policy =
        PluginResourcePolicy { max_annotation_bytes: 1024, ..PluginResourcePolicy::default() };
    let host = host_with_policy(policy);
    let err = run_plugin(&host, "adv-big-annotations").unwrap_err();
    assert!(matches!(err, PluginHostError::ResultPolicyViolation(_)));
}

#[test]
fn invalid_evidence_id_rejected() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-bad-evidence").unwrap_err();
    assert!(matches!(err, PluginHostError::InvalidEvidence(_)));
}

#[test]
fn wrong_protocol_version_rejected() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-wrong-protocol").unwrap_err();
    assert!(matches!(err, PluginHostError::ProtocolVersionMismatch { .. }));
}

#[test]
fn malformed_json_rejected() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-malformed-json").unwrap_err();
    assert!(matches!(err, PluginHostError::ProtocolViolation(_)));
}

#[test]
fn missing_module_entrypoint() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-missing-module").unwrap_err();
    assert!(matches!(
        err,
        PluginHostError::EntrypointInvalid { .. } | PluginHostError::ProcessFailed { .. }
    ));
}

#[test]
fn missing_callable_entrypoint() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-missing-callable").unwrap_err();
    assert!(matches!(
        err,
        PluginHostError::EntrypointInvalid { .. } | PluginHostError::ProcessFailed { .. }
    ));
}

#[test]
fn non_callable_entrypoint() {
    let host = host_with_policy(PluginResourcePolicy::default());
    let err = run_plugin(&host, "adv-non-callable").unwrap_err();
    assert!(matches!(
        err,
        PluginHostError::EntrypointInvalid { .. } | PluginHostError::ProcessFailed { .. }
    ));
}

#[test]
fn slow_scan_cancellable_via_token() {
    std::env::set_var("PARQONAUT_TEST_SLOW_SCAN_SEC", "8");
    let catalog = PluginCatalog::discover_from_roots(&[fixture_plugins_root()]).unwrap();
    let entry = catalog.get("adv-sleep").unwrap();
    let executor = ScanPluginExecutor::new(PluginRuntimeConfig {
        sdk_src_root: Some(sdk_path()),
        ..PluginRuntimeConfig::default()
    });
    let cancel = CancelToken::new();
    let cancel_worker = cancel.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(300));
        cancel_worker.cancel();
    });
    let err = executor
        .execute_scan(entry, PluginExecutionPhase::PostRules, ctx(), &cancel, None)
        .unwrap_err();
    assert!(matches!(err, PluginHostError::Cancelled));
}

#[test]
fn scan_large_stdin_never_consumed_cancels_and_times_out_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let plugin_dir = dir.path().join("hang-stdin");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("parqonaut-plugin.json"),
        r#"{
          "protocol_version": 1,
          "name": "hang-stdin",
          "version": "0.1.0",
          "entrypoint": "hang_stdin:analyze",
          "capabilities": { "scan": { "supported_formats": ["parquet"], "supported_phases": ["post_rules"] } }
        }"#,
    )
    .unwrap();
    // Note: runner reads stdin before importing plugin module, so test with a wrapper python script
    // or a slow-reading scenario; here we test a Python script passed as python_executable that sleeps without reading stdin.
    let sleeper = dir.path().join("sleep_ignore_stdin.py");
    std::fs::write(&sleeper, "#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&sleeper).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&sleeper, perms).unwrap();
    }
    std::fs::write(plugin_dir.join("hang_stdin.py"), "def analyze(c): pass\n").unwrap();
    let entry = PluginCatalog::validate_path(&plugin_dir).unwrap();

    let mut big_ctx = ctx();
    // > 256 KiB exceeds OS pipe buffer so blocking stdin write would deadlock without background stdin writer.
    big_ctx.discovered_files = vec!["a".repeat(2048); 200];

    let executor = ScanPluginExecutor::new(PluginRuntimeConfig {
        python_executable: Some(sleeper),
        sdk_src_root: Some(sdk_path()),
        policy: PluginResourcePolicy { default_timeout_ms: 250, ..Default::default() },
        ..Default::default()
    });
    let start = std::time::Instant::now();
    let err = executor
        .execute_scan(
            &entry,
            PluginExecutionPhase::PostRules,
            big_ctx.clone(),
            &CancelToken::new(),
            Some(250),
        )
        .unwrap_err();
    assert!(matches!(err, PluginHostError::Timeout { .. }), "expected Timeout, got {err:?}");
    assert!(start.elapsed() < std::time::Duration::from_secs(3));

    let cancel = CancelToken::new();
    let cancel_bg = cancel.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(120));
        cancel_bg.cancel();
    });
    let start_cancel = std::time::Instant::now();
    let err_cancel = executor
        .execute_scan(&entry, PluginExecutionPhase::PostRules, big_ctx, &cancel, Some(30_000))
        .unwrap_err();
    assert!(matches!(err_cancel, PluginHostError::Cancelled), "got {err_cancel:?}");
    assert!(
        start_cancel.elapsed() < std::time::Duration::from_secs(3),
        "cancellation waited for timeout: {:?}",
        start_cancel.elapsed()
    );
}
