//! Scan analyzer subprocess transport (JSON stdin/stdout, bounded I/O).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use parqonaut_plugin_protocol::{
    PluginExecutionPhase, PluginRequest, PluginResponse, PluginResult, PLUGIN_PROTOCOL_VERSION,
};
use serde::Deserialize;

use crate::catalog::CatalogEntry;
use crate::env::plugin_child_env;
use crate::error::PluginHostError;
use crate::policy::{BatchResourcePolicy, PluginResourcePolicy};

pub(crate) const RUNNER_MODULE: &str = "parqonaut_plugins.runner";

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
    pub batch_policy: BatchResourcePolicy,
}

impl Default for PluginRuntimeConfig {
    fn default() -> Self {
        Self {
            python_executable: std::env::var("PARQONAUT_PLUGIN_PYTHON").ok().map(PathBuf::from),
            sdk_src_root: std::env::var("PARQONAUT_PLUGIN_SDK_PATH").ok().map(PathBuf::from),
            policy: PluginResourcePolicy::default(),
            batch_policy: BatchResourcePolicy::default(),
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
            .arg("scan")
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
        let max_stdout = policy.max_response_bytes;
        let max_stderr = policy.max_stderr_bytes;
        let stdout_out: Arc<Mutex<Option<Result<Vec<u8>, PluginHostError>>>> =
            Arc::new(Mutex::new(None));
        let stderr_out: Arc<Mutex<Option<Result<Vec<u8>, PluginHostError>>>> =
            Arc::new(Mutex::new(None));
        let stdout_slot = Arc::clone(&stdout_out);
        let stderr_slot = Arc::clone(&stderr_out);
        let stdout_handle = thread::spawn(move || {
            *stdout_slot.lock().expect("lock") = Some(read_bounded(stdout, max_stdout));
        });
        let stderr_handle = thread::spawn(move || {
            *stderr_slot.lock().expect("lock") =
                Some(read_stderr_capped(stderr, max_stderr).map_err(PluginHostError::Io));
        });

        let start = Instant::now();
        loop {
            if cancel.is_cancelled() {
                let _ = terminate_child(&mut child);
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return Err(PluginHostError::ProtocolViolation("cancelled".into()));
            }
            if matches!(stdout_out.lock().expect("lock").as_ref(), Some(Err(_))) {
                let err = stdout_out.lock().expect("lock").take().unwrap().unwrap_err();
                let _ = terminate_child(&mut child);
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return Err(err);
            }
            if let Some(status) = child.try_wait().map_err(PluginHostError::Io)? {
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                let stdout_bytes = stdout_out.lock().expect("lock").take().unwrap_or(Err(
                    PluginHostError::ProtocolViolation("stdout reader missing".into()),
                ))?;
                let stderr_bytes = match stderr_out.lock().expect("lock").take() {
                    Some(Ok(v)) => v,
                    Some(Err(e)) => return Err(e),
                    None => Vec::new(),
                };
                let stderr_tail = tail_str(&stderr_bytes, 8 * 1024);
                if !status.success() {
                    let detail = format!(
                        "plugin={} version={} digest={}: {stderr_tail}",
                        entry.manifest.name, entry.manifest.version, entry.digest
                    );
                    if stderr_tail.contains("No module named")
                        || stderr_tail.contains("entrypoint not callable")
                        || stderr_tail.contains("entrypoint must be module:callable")
                    {
                        return Err(PluginHostError::EntrypointInvalid {
                            name: entry.manifest.name.clone(),
                            detail: stderr_tail,
                        });
                    }
                    return Err(PluginHostError::ProcessFailed { code: status.code(), detail });
                }
                let (response, _trailing) = parse_stdout_json(&stdout_bytes)?;
                if let Err(e) = response.validate() {
                    return Err(map_protocol_validate_err(e));
                }
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
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                let _ = child.wait();
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

fn map_protocol_validate_err(e: parqonaut_plugin_protocol::PluginProtocolError) -> PluginHostError {
    match e {
        parqonaut_plugin_protocol::PluginProtocolError::ProtocolVersionMismatch {
            expected,
            found,
        } => PluginHostError::ProtocolVersionMismatch {
            detail: format!("expected protocol {expected}, found {found}"),
        },
        other => PluginHostError::Protocol(other),
    }
}

pub(crate) fn resolve_python(
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
    if let Some(sdk) = &runtime.sdk_src_root {
        let sdk_venv = sdk.parent().map(|p| p.join(".venv").join("bin").join("python"));
        if let Some(p) = sdk_venv {
            if p.is_file() {
                return Ok(p);
            }
        }
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

pub(crate) fn build_pythonpath(
    plugin_root: &Path,
    sdk: Option<&Path>,
) -> Result<String, PluginHostError> {
    let mut parts = vec![plugin_root.to_string_lossy().into_owned()];
    if let Some(s) = sdk {
        parts.push(s.to_string_lossy().into_owned());
    }
    Ok(parts.join(":"))
}

/// Reads stderr up to `max` bytes, then drains the remainder so the child cannot block on a full pipe.
fn read_stderr_capped<R: Read>(mut reader: R, max: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if buf.len() < max {
            let to_read = (max - buf.len()).min(chunk.len());
            let n = reader.read(&mut chunk[..to_read])?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        } else {
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                break;
            }
        }
    }
    Ok(buf)
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

pub(crate) fn terminate_child(child: &mut Child) -> Result<(), PluginHostError> {
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
