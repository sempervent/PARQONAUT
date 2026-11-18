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
            python_executable: std::env::var("PARQONAUT_PLUGIN_PYTHON")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(PathBuf::from),
            sdk_src_root: std::env::var("PARQONAUT_PLUGIN_SDK_PATH")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(PathBuf::from),
            policy: PluginResourcePolicy::default(),
            batch_policy: BatchResourcePolicy::default(),
        }
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn as_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("child guard active")
    }

    fn terminate_and_reap(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = terminate_child(&mut child);
        }
    }

    #[allow(clippy::zombie_processes)] // Caller already observed exit via `try_wait()` -> `Some(status)`.
    fn mark_reaped(&mut self) {
        self.0 = None;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.terminate_and_reap();
    }
}

pub struct ScanPluginExecutor {
    pub runtime: PluginRuntimeConfig,
}

impl ScanPluginExecutor {
    pub fn new(runtime: PluginRuntimeConfig) -> Self {
        Self { runtime }
    }

    #[allow(clippy::type_complexity)]
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

        let mut cmd = Command::new(&python);
        cmd.arg("-m")
            .arg(RUNNER_MODULE)
            .arg("scan")
            .current_dir(&entry.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(env);
        configure_plugin_child(&mut cmd);
        let mut child =
            ChildGuard::new(cmd.spawn().map_err(|e| PluginHostError::SpawnFailed(e.to_string()))?);

        let mut stdin = child.as_mut().stdin.take().expect("stdin");
        let stdout = child.as_mut().stdout.take().expect("stdout");
        let stderr = child.as_mut().stderr.take().expect("stderr");

        let timeout =
            Duration::from_millis(timeout_ms.unwrap_or(self.runtime.policy.default_timeout_ms));
        let policy = self.runtime.policy.clone();
        let max_stdout = policy.max_response_bytes;
        let max_stderr = policy.max_stderr_bytes;

        let stdin_out: Arc<Mutex<Option<std::io::Result<()>>>> = Arc::new(Mutex::new(None));
        let stdout_out: Arc<Mutex<Option<Result<Vec<u8>, PluginHostError>>>> =
            Arc::new(Mutex::new(None));
        let stderr_out: Arc<Mutex<Option<Result<Vec<u8>, PluginHostError>>>> =
            Arc::new(Mutex::new(None));

        let stdin_slot = Arc::clone(&stdin_out);
        let stdin_handle = thread::spawn(move || {
            let res = stdin.write_all(&request_json).and_then(|()| stdin.flush());
            drop(stdin);
            *stdin_slot.lock().expect("lock") = Some(res);
        });

        let stdout_slot = Arc::clone(&stdout_out);
        let stdout_handle = thread::spawn(move || {
            *stdout_slot.lock().expect("lock") = Some(read_bounded(stdout, max_stdout));
        });
        let stderr_slot = Arc::clone(&stderr_out);
        let stderr_handle = thread::spawn(move || {
            *stderr_slot.lock().expect("lock") =
                Some(read_stderr_capped(stderr, max_stderr).map_err(PluginHostError::Io));
        });

        let start = Instant::now();
        loop {
            if cancel.is_cancelled() {
                child.terminate_and_reap();
                let _ = stdin_handle.join();
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return Err(PluginHostError::Cancelled);
            }
            if matches!(stdout_out.lock().expect("lock").as_ref(), Some(Err(_))) {
                let err = stdout_out.lock().expect("lock").take().unwrap().unwrap_err();
                child.terminate_and_reap();
                let _ = stdin_handle.join();
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return Err(err);
            }
            if let Some(status) = child.as_mut().try_wait().map_err(PluginHostError::Io)? {
                child.mark_reaped();
                let _ = stdin_handle.join();
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
                    return Err(classify_process_failure(
                        &entry.manifest.name,
                        &entry.manifest.version,
                        &entry.digest,
                        status.code(),
                        &stderr_tail,
                    ));
                }
                if let Some(Err(e)) = stdin_out.lock().expect("lock").take() {
                    if e.kind() != std::io::ErrorKind::BrokenPipe {
                        return Err(PluginHostError::Io(e));
                    }
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
                child.terminate_and_reap();
                let _ = stdin_handle.join();
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            thread::sleep(Duration::from_millis(10));
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

fn is_bare_executable_name(p: &Path) -> bool {
    let s = p.to_string_lossy();
    !p.is_absolute()
        && !s.contains('/')
        && !s.contains('\\')
        && matches!(
            p.components().collect::<Vec<_>>().as_slice(),
            [std::path::Component::Normal(_)]
        )
}

fn is_executable_file(p: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(p) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn absolutize_preserving_symlink(p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(p)
    } else {
        p.to_path_buf()
    }
}

pub(crate) fn resolve_python(
    plugin_root: &Path,
    runtime: &PluginRuntimeConfig,
) -> Result<PathBuf, PluginHostError> {
    if let Some(p) = &runtime.python_executable {
        if p.as_os_str().is_empty() {
            return Err(PluginHostError::RuntimeUnavailable(
                "configured python path is empty".into(),
            ));
        }
        if is_bare_executable_name(p) {
            return which::which(p).map_err(|_| {
                PluginHostError::RuntimeUnavailable(format!(
                    "configured python not found in PATH: {}",
                    p.display()
                ))
            });
        }
        let abs = absolutize_preserving_symlink(p);
        if is_executable_file(&abs) {
            return Ok(abs);
        }
        if abs.exists() {
            return Err(PluginHostError::RuntimeUnavailable(format!(
                "configured python is not an executable file: {}",
                p.display()
            )));
        }
        return Err(PluginHostError::RuntimeUnavailable(format!(
            "configured python not found: {}",
            p.display()
        )));
    }
    if let Some(sdk) = &runtime.sdk_src_root {
        let sdk_venv = sdk.parent().map(|p| p.join(".venv").join("bin").join("python"));
        if let Some(p) = sdk_venv {
            if is_executable_file(&p) {
                return Ok(absolutize_preserving_symlink(&p));
            }
        }
    }
    let venv = plugin_root.join(".venv").join("bin").join("python");
    if is_executable_file(&venv) {
        return Ok(absolutize_preserving_symlink(&venv));
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

pub(crate) fn classify_process_failure(
    name: &str,
    version: &str,
    digest: &str,
    code: Option<i32>,
    stderr_tail: &str,
) -> PluginHostError {
    if stderr_tail.contains("No module named 'pyarrow'")
        || stderr_tail.contains("No module named \"pyarrow\"")
        || stderr_tail.contains("No module named 'pydantic'")
        || stderr_tail.contains("No module named \"pydantic\"")
        || stderr_tail.contains("No module named 'parqonaut_plugins'")
        || stderr_tail.contains("No module named \"parqonaut_plugins\"")
    {
        return PluginHostError::RuntimeUnavailable(format!(
            "python runtime missing required SDK dependencies for plugin={name} version={version}: {stderr_tail}"
        ));
    }
    if stderr_tail.contains("No module named")
        || stderr_tail.contains("entrypoint not callable")
        || stderr_tail.contains("entrypoint must be module:callable")
    {
        return PluginHostError::EntrypointInvalid {
            name: name.to_string(),
            detail: stderr_tail.to_string(),
        };
    }
    let detail = format!("plugin={name} version={version} digest={digest}: {stderr_tail}");
    PluginHostError::ProcessFailed { code, detail }
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
pub(crate) fn read_stderr_capped<R: Read>(mut reader: R, max: usize) -> std::io::Result<Vec<u8>> {
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

pub(crate) fn tail_str(bytes: &[u8], max: usize) -> String {
    if bytes.len() <= max {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let mut start = bytes.len() - max;
    // Advance past up to 3 UTF-8 continuation bytes so valid UTF-8 isn't split mid-codepoint.
    for _ in 0..3 {
        if start < bytes.len() && (bytes[start] & 0xC0) == 0x80 {
            start += 1;
        } else {
            break;
        }
    }
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}

#[cfg(unix)]
pub(crate) fn configure_plugin_child(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;
    cmd.process_group(0);
}

#[cfg(not(unix))]
pub(crate) fn configure_plugin_child(_cmd: &mut Command) {}

pub(crate) fn terminate_child(child: &mut Child) -> Result<(), PluginHostError> {
    #[cfg(unix)]
    {
        let pid = child.id() as libc::pid_t;
        // SAFETY: kill(-pgid) targets the plugin process group we create with process_group(0).
        #[allow(clippy::undocumented_unsafe_blocks)]
        unsafe {
            let _ = libc::kill(-pid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    fn make_executable(path: &Path) {
        std::fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(path, perms).unwrap();
        }
    }

    #[test]
    fn resolve_python_accepts_absolute_executable() {
        let dir = TempDir::new().unwrap();
        let py = dir.path().join("my-python");
        make_executable(&py);
        let runtime = PluginRuntimeConfig {
            python_executable: Some(py.clone()),
            ..PluginRuntimeConfig::default()
        };
        let resolved = resolve_python(dir.path(), &runtime).unwrap();
        assert_eq!(resolved, py);
    }

    #[test]
    fn resolve_python_resolves_bare_name_from_path() {
        let dir = TempDir::new().unwrap();
        let runtime = PluginRuntimeConfig {
            python_executable: Some(PathBuf::from("python3")),
            ..PluginRuntimeConfig::default()
        };
        let resolved = resolve_python(dir.path(), &runtime).unwrap();
        assert!(
            resolved.is_absolute(),
            "expected resolved PATH entry to be absolute: {resolved:?}"
        );
        assert!(resolved.is_file());
    }

    #[test]
    fn resolve_python_rejects_missing_bare_name_with_actionable_error() {
        let dir = TempDir::new().unwrap();
        let runtime = PluginRuntimeConfig {
            python_executable: Some(PathBuf::from("parqonaut-definitely-missing-python-bin")),
            ..PluginRuntimeConfig::default()
        };
        let err = resolve_python(dir.path(), &runtime).unwrap_err();
        match err {
            PluginHostError::RuntimeUnavailable(msg) => {
                assert!(msg.contains("PATH"), "expected PATH hint in {msg}");
                assert!(msg.contains("parqonaut-definitely-missing-python-bin"));
            }
            other => panic!("expected RuntimeUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn resolve_python_rejects_missing_absolute_and_non_executable() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing-python");
        let runtime = PluginRuntimeConfig {
            python_executable: Some(missing),
            ..PluginRuntimeConfig::default()
        };
        let err = resolve_python(dir.path(), &runtime).unwrap_err();
        assert!(matches!(err, PluginHostError::RuntimeUnavailable(_)));

        let non_exec = dir.path().join("plain.txt");
        std::fs::write(&non_exec, "not executable").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&non_exec).unwrap().permissions();
            perms.set_mode(0o644);
            std::fs::set_permissions(&non_exec, perms).unwrap();
            let runtime2 = PluginRuntimeConfig {
                python_executable: Some(non_exec),
                ..PluginRuntimeConfig::default()
            };
            let err2 = resolve_python(dir.path(), &runtime2).unwrap_err();
            match err2 {
                PluginHostError::RuntimeUnavailable(msg) => {
                    assert!(msg.contains("not an executable file"), "got {msg}");
                }
                other => panic!("expected RuntimeUnavailable, got {other:?}"),
            }
        }
    }

    #[test]
    fn resolve_python_handles_relative_path_predictably() {
        let cwd = std::env::current_dir().unwrap();
        let dir = tempfile::Builder::new().prefix("rel-py-").tempdir_in(&cwd).unwrap();
        let rel_dir = dir.path().strip_prefix(&cwd).unwrap();
        let py_abs = dir.path().join("py");
        make_executable(&py_abs);
        let py_rel = rel_dir.join("py");

        let runtime = PluginRuntimeConfig {
            python_executable: Some(py_rel),
            ..PluginRuntimeConfig::default()
        };
        let resolved = resolve_python(dir.path(), &runtime).unwrap();
        assert!(resolved.is_absolute());
        assert_eq!(resolved, py_abs);

        let missing_rel = rel_dir.join("missing-py");
        let runtime_missing = PluginRuntimeConfig {
            python_executable: Some(missing_rel),
            ..PluginRuntimeConfig::default()
        };
        let err = resolve_python(dir.path(), &runtime_missing).unwrap_err();
        assert!(matches!(err, PluginHostError::RuntimeUnavailable(_)));
    }

    #[test]
    fn tail_str_handles_multibyte_utf8_boundaries() {
        let s = "hello-🦀🦀🦀-world";
        let bytes = s.as_bytes();
        for max in 0..=bytes.len() + 2 {
            let out = tail_str(bytes, max);
            assert!(out.len() <= max, "max={max}, out_len={}, out={out:?}", out.len());
        }
    }

    #[test]
    fn read_stderr_capped_terminates_and_caps_beyond_limit() {
        let data = vec![b'x'; 32 * 1024];
        let out = read_stderr_capped(Cursor::new(data), 1024).unwrap();
        assert_eq!(out.len(), 1024);
    }
}
