//! Scan analyzer subprocess transport (JSON stdin/stdout, bounded I/O).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parqonaut_plugin_protocol::{
    PluginExecutionPhase, PluginRequest, PluginResponse, PluginResult, PLUGIN_PROTOCOL_VERSION,
};
use serde::Deserialize;

use crate::catalog::CatalogEntry;
use crate::env::plugin_child_env;
use crate::error::PluginHostError;
use crate::policy::PluginResourcePolicy;

const RUNNER_MODULE: &str = "parqonaut_plugins.runner";

/// Optional cooperative cancellation (set before/during execute).
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct PluginRuntimeConfig {
    pub python_executable: Option<PathBuf>,
    /// Directory containing the `parqonaut_plugins` package (typically `.../src`).
    pub sdk_src_root: Option<PathBuf>,
    pub policy: PluginResourcePolicy,
}

impl Default for PluginRuntimeConfig {
    fn default() -> Self {
        Self {
            python_executable: std::env::var("PARQONAUT_PLUGIN_PYTHON").ok().map(PathBuf::from),
            sdk_src_root: std::env::var("PARQONAUT_PLUGIN_SDK_PATH").ok().map(PathBuf::from),
            policy: PluginResourcePolicy::default(),
        }
    }
}

pub struct ScanPluginExecutor {
    pub runtime: PluginRuntimeConfig,
}

impl ScanPluginExecutor {
    pub fn new(runtime: PluginRuntimeConfig) -> Self {
        Self { runtime }
    }

    pub fn execute_scan(
        &self,
        entry: &CatalogEntry,
        phase: PluginExecutionPhase,
        context: parqonaut_plugin_protocol::PluginScanContext,
        cancel: &CancelToken,
        timeout_ms: Option<u64>,
    ) -> Result<(PluginResult, String), PluginHostError> {
        if entry.compatibility != crate::catalog::PluginCompatibility::Compatible {
            let req = entry.manifest.requires_parqonaut.clone().unwrap_or_default();
            return Err(PluginHostError::HostVersionMismatch {
                name: entry.manifest.name.clone(),
                requirement: req,
            });
        }
        let scan = entry.manifest.capabilities.scan.as_ref().ok_or_else(|| {
            PluginHostError::InvalidResponse("plugin has no scan capability".into())
        })?;
        if !scan.supported_phases.contains(&phase) {
            return Err(PluginHostError::UnsupportedPhase {
                name: entry.manifest.name.clone(),
                phase: format!("{phase:?}"),
            });
        }

        let request = PluginRequest {
            protocol_version: PLUGIN_PROTOCOL_VERSION,
            manifest: entry.manifest.clone(),
            phase,
            context,
            config: Default::default(),
        };
        request.validate()?;
        let request_json = serde_json::to_vec(&request)?;
        if request_json.len() > self.runtime.policy.max_request_bytes {
            return Err(PluginHostError::ProtocolViolation("request JSON too large".into()));
        }

        let python = resolve_python(&entry.root, &self.runtime)?;
        let pythonpath = build_pythonpath(&entry.root, self.runtime.sdk_src_root.as_deref())?;
        let env = plugin_child_env(&pythonpath);

        let mut child = Command::new(&python)
            .arg("-m")
            .arg(RUNNER_MODULE)
            .current_dir(&entry.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(env)
            .spawn()
            .map_err(|e| PluginHostError::SpawnFailed(e.to_string()))?;

        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(&request_json).map_err(|e| {
            let _ = terminate_child(&mut child);
            PluginHostError::SpawnFailed(e.to_string())
        })?;
        drop(stdin);

        let timeout =
            Duration::from_millis(timeout_ms.unwrap_or(self.runtime.policy.default_timeout_ms));
        let stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");

        let policy = self.runtime.policy.clone();
        let stdout_handle = thread::spawn(move || read_bounded(stdout, policy.max_response_bytes));
        let stderr_handle = thread::spawn(move || read_bounded(stderr, policy.max_stderr_bytes));

        let start = Instant::now();
        loop {
            if cancel.is_cancelled() {
                let _ = terminate_child(&mut child);
                return Err(PluginHostError::ProtocolViolation("cancelled".into()));
            }
            if let Some(status) = child.try_wait().map_err(PluginHostError::Io)? {
                let stdout_bytes = stdout_handle.join().expect("stdout thread").map_err(|e| {
                    let _ = terminate_child(&mut child);
                    e
                })?;
                let stderr_bytes = stderr_handle.join().expect("stderr thread").map_err(|e| {
                    let _ = terminate_child(&mut child);
                    e
                })?;
                let stderr_tail = tail_str(&stderr_bytes, 8 * 1024);
                if !status.success() {
                    return Err(PluginHostError::ProcessFailed {
                        code: status.code(),
                        detail: stderr_tail,
                    });
                }
                let (response, _trailing) = parse_stdout_json(&stdout_bytes)?;
                response.validate()?;
                if response.plugin != entry.manifest.name {
                    return Err(PluginHostError::InvalidResponse(format!(
                        "response plugin name mismatch: {}",
                        response.plugin
                    )));
                }
                validate_result_bounds(&response.result, &self.runtime.policy)?;
                return Ok((response.result, stderr_tail));
            }
            if start.elapsed() >= timeout {
                let _ = terminate_child(&mut child);
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

fn resolve_python(
    plugin_root: &Path,
    runtime: &PluginRuntimeConfig,
) -> Result<PathBuf, PluginHostError> {
    if let Some(p) = &runtime.python_executable {
        if p.is_file() {
            return Ok(p.clone());
        }
        return Err(PluginHostError::RuntimeUnavailable(format!(
            "configured python not found: {}",
            p.display()
        )));
    }
    let venv = plugin_root.join(".venv").join("bin").join("python");
    if venv.is_file() {
        return Ok(venv);
    }
    for candidate in ["python3", "python"] {
        if let Ok(p) = which::which(candidate) {
            return Ok(p);
        }
    }
    Err(PluginHostError::RuntimeUnavailable(
        "no python interpreter found (set PARQONAUT_PLUGIN_PYTHON)".into(),
    ))
}

fn build_pythonpath(plugin_root: &Path, sdk: Option<&Path>) -> Result<String, PluginHostError> {
    let mut parts = vec![plugin_root.to_string_lossy().into_owned()];
    if let Some(s) = sdk {
        parts.push(s.to_string_lossy().into_owned());
    }
    Ok(parts.join(":"))
}

fn read_bounded<R: Read>(mut reader: R, max: usize) -> Result<Vec<u8>, PluginHostError> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if buf.len() >= max {
            return Err(PluginHostError::ResponseTooLarge { size: buf.len(), max });
        }
        let to_read = (max - buf.len()).min(chunk.len());
        let n = reader.read(&mut chunk[..to_read]).map_err(PluginHostError::Io)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(buf)
}

fn parse_stdout_json(bytes: &[u8]) -> Result<(PluginResponse, String), PluginHostError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| PluginHostError::ProtocolViolation("stdout not utf-8".into()))?;
    let trimmed = text.trim();
    let mut de = serde_json::Deserializer::from_str(trimmed);
    let response = PluginResponse::deserialize(&mut de).map_err(|e| {
        PluginHostError::ProtocolViolation(format!("stdout is not valid JSON: {e}"))
    })?;
    if let Err(e) = de.end() {
        return Err(PluginHostError::ProtocolViolation(format!(
            "stdout trailing content after JSON: {e}"
        )));
    }
    Ok((response, String::new()))
}

fn validate_result_bounds(
    result: &PluginResult,
    policy: &PluginResourcePolicy,
) -> Result<(), PluginHostError> {
    if result.findings.len() > policy.max_findings {
        return Err(PluginHostError::ResultPolicyViolation(format!(
            "too many findings: {}",
            result.findings.len()
        )));
    }
    for f in &result.findings {
        if f.summary.len() > policy.max_finding_summary_len {
            return Err(PluginHostError::ResultPolicyViolation("summary too long".into()));
        }
        if f.detail.len() > policy.max_finding_detail_len {
            return Err(PluginHostError::ResultPolicyViolation("detail too long".into()));
        }
        if f.evidence_ids.len() > policy.max_evidence_ids_per_finding {
            return Err(PluginHostError::ResultPolicyViolation("too many evidence ids".into()));
        }
    }
    if result.annotations.len() > policy.max_annotations {
        return Err(PluginHostError::ResultPolicyViolation("too many annotations".into()));
    }
    let ann_size = serde_json::to_vec(&result.annotations).unwrap_or_default().len();
    if ann_size > policy.max_annotation_bytes {
        return Err(PluginHostError::ResultPolicyViolation("annotations too large".into()));
    }
    Ok(())
}

fn tail_str(bytes: &[u8], max: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.len() <= max {
        s.into_owned()
    } else {
        s[s.len() - max..].to_string()
    }
}

fn terminate_child(child: &mut Child) -> Result<(), PluginHostError> {
    let _ = child.kill();
    let grace = Duration::from_millis(500);
    let start = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        if start.elapsed() >= grace {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
}
