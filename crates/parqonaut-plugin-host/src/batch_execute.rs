//! Batch-transform subprocess (length-framed Arrow IPC).

use std::io::Write;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use parqonaut_plugin_protocol::PLUGIN_PROTOCOL_VERSION;
use tempfile::TempDir;

use crate::batch_context::BatchPluginContextFile;
use crate::batch_ipc::{encode_batch_frame, read_batch_frame};
use crate::batch_schema::{enforce_row_policy, require_schema_equal};
use crate::catalog::{CatalogEntry, PluginCompatibility};
use crate::env::plugin_child_env;
use crate::error::PluginHostError;
use crate::policy::BatchResourcePolicy;
use crate::scan_execute::{
    build_pythonpath, classify_process_failure, configure_plugin_child, read_stderr_capped,
    resolve_python, tail_str, terminate_child, CancelToken, PluginRuntimeConfig, RUNNER_MODULE,
};

static BATCH_PLUGIN_SPAWNS: AtomicUsize = AtomicUsize::new(0);

/// Test/diagnostic counter incremented on each batch plugin child spawn.
pub fn batch_plugin_spawn_count() -> &'static AtomicUsize {
    &BATCH_PLUGIN_SPAWNS
}

enum StdinCommand {
    Frame { bytes: Vec<u8>, ack: SyncSender<std::io::Result<()>> },
    Finish { ack: SyncSender<std::io::Result<()>> },
}

/// Active batch plugin child (one process per pipeline stage).
pub struct BatchPluginSession {
    plugin_name: String,
    plugin_version: String,
    plugin_digest: String,
    child: Child,
    reaped: bool,
    cleaned_up: bool,
    stdin_tx: Option<SyncSender<StdinCommand>>,
    stdout_rx: Receiver<Result<Option<RecordBatch>, PluginHostError>>,
    stdin_handle: Option<JoinHandle<()>>,
    stdout_handle: Option<JoinHandle<()>>,
    stderr_handle: Option<JoinHandle<Vec<u8>>>,
    policy: BatchResourcePolicy,
    cancel: Option<CancelToken>,
    _temp: TempDir,
}

impl BatchPluginSession {
    pub fn start(
        entry: &CatalogEntry,
        config: serde_json::Value,
        execution_id: &str,
        runtime: &PluginRuntimeConfig,
        expected_digest: Option<&str>,
        cancel: Option<CancelToken>,
    ) -> Result<Self, PluginHostError> {
        if cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            return Err(PluginHostError::Cancelled);
        }
        if entry.compatibility != PluginCompatibility::Compatible {
            let req = entry.manifest.requires_parqonaut.clone().unwrap_or_default();
            return Err(PluginHostError::HostVersionMismatch {
                name: entry.manifest.name.clone(),
                requirement: req,
            });
        }
        if let Some(expected) = expected_digest {
            if expected != entry.digest {
                return Err(PluginHostError::StalePlugin {
                    expected: expected.to_string(),
                    actual: entry.digest.clone(),
                });
            }
        }
        entry.manifest.capabilities.batch_transform.as_ref().ok_or_else(|| {
            PluginHostError::InvalidResponse("plugin has no batch_transform capability".into())
        })?;

        let batch_policy = runtime.batch_policy.clone();
        let config_bytes = serde_json::to_vec(&config)?;
        if config_bytes.len() > batch_policy.max_config_bytes {
            return Err(PluginHostError::ResultPolicyViolation(format!(
                "batch plugin config exceeds max size {} bytes",
                batch_policy.max_config_bytes
            )));
        }

        let temp = TempDir::new().map_err(PluginHostError::Io)?;
        let ctx_path = temp.path().join("context.json");
        let ctx = BatchPluginContextFile {
            protocol_version: PLUGIN_PROTOCOL_VERSION,
            plugin: entry.manifest.name.clone(),
            plugin_version: entry.manifest.version.clone(),
            plugin_digest: entry.digest.clone(),
            entrypoint: entry.manifest.entrypoint.clone(),
            execution_id: execution_id.to_string(),
            config,
            max_output_rows_per_input_batch: batch_policy.max_output_rows_per_input_batch,
            max_output_expansion_factor: batch_policy.max_output_expansion_factor,
        };
        std::fs::write(&ctx_path, serde_json::to_vec(&ctx)?)?;

        let python = resolve_python(&entry.root, runtime)?;
        let pythonpath = build_pythonpath(&entry.root, runtime.sdk_src_root.as_deref())?;
        let mut env = plugin_child_env(&pythonpath);
        env.insert("PARQONAUT_BATCH_CONTEXT_PATH".into(), ctx_path.to_string_lossy().into_owned());

        let mut cmd = Command::new(&python);
        cmd.arg("-m")
            .arg(RUNNER_MODULE)
            .arg("batch")
            .arg("--context")
            .arg(&ctx_path)
            .current_dir(&entry.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(env);
        configure_plugin_child(&mut cmd);
        let mut child = cmd.spawn().map_err(|e| PluginHostError::SpawnFailed(e.to_string()))?;
        BATCH_PLUGIN_SPAWNS.fetch_add(1, Ordering::SeqCst);

        let mut stdin = child.stdin.take().expect("stdin");
        let mut stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");

        let stderr_cap = batch_policy.max_stderr_bytes;
        let stderr_handle =
            thread::spawn(move || read_stderr_capped(stderr, stderr_cap).unwrap_or_default());

        let (stdin_tx, stdin_rx) = mpsc::sync_channel::<StdinCommand>(1);
        let stdin_handle = thread::spawn(move || {
            while let Ok(cmd) = stdin_rx.recv() {
                match cmd {
                    StdinCommand::Frame { bytes, ack } => {
                        let res = stdin.write_all(&bytes).and_then(|()| stdin.flush());
                        let is_err = res.is_err();
                        let _ = ack.send(res);
                        if is_err {
                            break;
                        }
                    }
                    StdinCommand::Finish { ack } => {
                        let res = stdin.write_all(&0u32.to_le_bytes()).and_then(|()| stdin.flush());
                        let _ = ack.send(res);
                        break;
                    }
                }
            }
            drop(stdin);
        });

        let (stdout_tx, stdout_rx) =
            mpsc::sync_channel::<Result<Option<RecordBatch>, PluginHostError>>(1);
        let stdout_handle = thread::spawn(move || loop {
            let res = read_batch_frame(&mut stdout);
            let done = matches!(res, Ok(None) | Err(_));
            if stdout_tx.send(res).is_err() || done {
                break;
            }
        });

        Ok(Self {
            plugin_name: entry.manifest.name.clone(),
            plugin_version: entry.manifest.version.clone(),
            plugin_digest: entry.digest.clone(),
            child,
            reaped: false,
            cleaned_up: false,
            stdin_tx: Some(stdin_tx),
            stdout_rx,
            stdin_handle: Some(stdin_handle),
            stdout_handle: Some(stdout_handle),
            stderr_handle: Some(stderr_handle),
            policy: batch_policy,
            cancel,
            _temp: temp,
        })
    }

    pub fn abort(&mut self) -> Result<(), PluginHostError> {
        self.abort_internal();
        Ok(())
    }

    pub fn context_dir_gone(&self) -> bool {
        !self._temp.path().exists()
    }

    fn is_cancelled(&self) -> bool {
        self.cancel.as_ref().is_some_and(CancelToken::is_cancelled)
    }

    fn abort_internal(&mut self) {
        if self.cleaned_up {
            return;
        }
        drop(self.stdin_tx.take());
        if !self.reaped {
            let _ = terminate_child(&mut self.child);
            self.reaped = true;
        }
        if let Some(h) = self.stdin_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stdout_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stderr_handle.take() {
            let _ = h.join();
        }
        self.cleaned_up = true;
    }

    fn wait_exit_and_stderr(&mut self, wait_limit: Duration) -> (Option<ExitStatus>, String) {
        drop(self.stdin_tx.take());
        let deadline = Instant::now() + wait_limit;
        let status = if self.reaped {
            None
        } else {
            loop {
                match self.child.try_wait() {
                    Ok(Some(s)) => {
                        self.reaped = true;
                        break Some(s);
                    }
                    Ok(None) => {
                        if Instant::now() >= deadline {
                            let _ = terminate_child(&mut self.child);
                            self.reaped = true;
                            break None;
                        }
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => {
                        let _ = terminate_child(&mut self.child);
                        self.reaped = true;
                        break None;
                    }
                }
            }
        };
        if let Some(h) = self.stdin_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stdout_handle.take() {
            let _ = h.join();
        }
        let stderr_bytes =
            self.stderr_handle.take().and_then(|h| h.join().ok()).unwrap_or_default();
        self.cleaned_up = true;
        let stderr_tail = tail_str(&stderr_bytes, 8 * 1024);
        (status, stderr_tail)
    }

    fn classify_exited_error(&mut self, fallback: PluginHostError) -> PluginHostError {
        let (status, stderr_tail) = self.wait_exit_and_stderr(Duration::from_millis(500));
        if let Some(s) = status {
            if !s.success() {
                return classify_process_failure(
                    &self.plugin_name,
                    &self.plugin_version,
                    &self.plugin_digest,
                    s.code(),
                    &stderr_tail,
                );
            }
        }
        fallback
    }

    pub fn transform(&mut self, batch: RecordBatch) -> Result<RecordBatch, PluginHostError> {
        if self.is_cancelled() {
            self.abort_internal();
            return Err(PluginHostError::Cancelled);
        }
        let frame = encode_batch_frame(&batch)?;
        let timeout = Duration::from_millis(self.policy.default_timeout_ms);
        let deadline = Instant::now() + timeout;

        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        let Some(stdin_tx) = self.stdin_tx.as_ref() else {
            return Err(PluginHostError::ProtocolViolation("plugin stdin already closed".into()));
        };
        if stdin_tx.send(StdinCommand::Frame { bytes: frame, ack: ack_tx }).is_err() {
            return Err(self.classify_exited_error(PluginHostError::ProtocolViolation(
                "plugin stdin writer stopped".into(),
            )));
        }

        loop {
            if self.is_cancelled() {
                self.abort_internal();
                return Err(PluginHostError::Cancelled);
            }
            let now = Instant::now();
            if now >= deadline {
                self.abort_internal();
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            let step = (deadline - now).min(Duration::from_millis(10));
            match ack_rx.recv_timeout(step) {
                Ok(Ok(())) => break,
                Ok(Err(e)) => {
                    return Err(self.classify_exited_error(PluginHostError::Io(e)));
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(self.classify_exited_error(PluginHostError::ProtocolViolation(
                        "plugin stdin writer disconnected".into(),
                    )));
                }
            }
        }

        let out = loop {
            if self.is_cancelled() {
                self.abort_internal();
                return Err(PluginHostError::Cancelled);
            }
            let now = Instant::now();
            if now >= deadline {
                self.abort_internal();
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            let step = (deadline - now).min(Duration::from_millis(10));
            match self.stdout_rx.recv_timeout(step) {
                Ok(Ok(Some(b))) => break b,
                Ok(Ok(None)) | Err(RecvTimeoutError::Disconnected) => {
                    return Err(self.classify_exited_error(PluginHostError::ProtocolViolation(
                        "plugin closed stdout before response batch".into(),
                    )));
                }
                Ok(Err(e)) => {
                    let (status, stderr_tail) =
                        self.wait_exit_and_stderr(Duration::from_millis(250));
                    if let Some(s) = status {
                        if !s.success() && matches!(e, PluginHostError::Io(_)) {
                            return Err(classify_process_failure(
                                &self.plugin_name,
                                &self.plugin_version,
                                &self.plugin_digest,
                                s.code(),
                                &stderr_tail,
                            ));
                        }
                    }
                    return Err(e);
                }
                Err(RecvTimeoutError::Timeout) => continue,
            }
        };

        require_schema_equal(batch.schema().as_ref(), out.schema().as_ref())?;
        enforce_row_policy(
            &batch,
            &out,
            self.policy.max_output_rows_per_input_batch,
            self.policy.max_output_expansion_factor,
        )?;
        Ok(out)
    }

    pub fn finish(mut self) -> Result<(), PluginHostError> {
        if self.is_cancelled() {
            self.abort_internal();
            return Err(PluginHostError::Cancelled);
        }
        let timeout = Duration::from_millis(self.policy.default_timeout_ms);
        let deadline = Instant::now() + timeout;

        if let Some(stdin_tx) = self.stdin_tx.take() {
            let (ack_tx, ack_rx) = mpsc::sync_channel(1);
            if stdin_tx.send(StdinCommand::Finish { ack: ack_tx }).is_ok() {
                loop {
                    if self.is_cancelled() {
                        self.abort_internal();
                        return Err(PluginHostError::Cancelled);
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        self.abort_internal();
                        return Err(PluginHostError::Timeout {
                            timeout_ms: timeout.as_millis() as u64,
                        });
                    }
                    let step = (deadline - now).min(Duration::from_millis(10));
                    match ack_rx.recv_timeout(step) {
                        Ok(Ok(())) => break,
                        Ok(Err(e)) => {
                            if e.kind() == std::io::ErrorKind::BrokenPipe {
                                break;
                            }
                            self.abort_internal();
                            return Err(PluginHostError::Io(e));
                        }
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
            }
        }

        loop {
            if self.is_cancelled() {
                self.abort_internal();
                return Err(PluginHostError::Cancelled);
            }
            if let Some(status) = self.child.try_wait().map_err(PluginHostError::Io)? {
                self.reaped = true;
                if let Some(h) = self.stdin_handle.take() {
                    let _ = h.join();
                }
                if let Some(h) = self.stdout_handle.take() {
                    let _ = h.join();
                }
                let stderr_bytes =
                    self.stderr_handle.take().and_then(|h| h.join().ok()).unwrap_or_default();
                self.cleaned_up = true;
                if !status.success() {
                    let stderr_tail = tail_str(&stderr_bytes, 8 * 1024);
                    return Err(classify_process_failure(
                        &self.plugin_name,
                        &self.plugin_version,
                        &self.plugin_digest,
                        status.code(),
                        &stderr_tail,
                    ));
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.abort_internal();
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for BatchPluginSession {
    fn drop(&mut self) {
        self.abort_internal();
    }
}
