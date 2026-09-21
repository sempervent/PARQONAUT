//! Batch-transform subprocess (length-framed Arrow IPC).

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use parqonaut_plugin_protocol::PLUGIN_PROTOCOL_VERSION;
use tempfile::TempDir;

use crate::batch_context::BatchPluginContextFile;
use crate::batch_ipc::{read_batch_frame, write_batch_frame};
use crate::batch_schema::{enforce_row_policy, require_schema_equal};
use crate::catalog::CatalogEntry;
use crate::env::plugin_child_env;
use crate::error::PluginHostError;
use crate::policy::BatchResourcePolicy;
use crate::scan_execute::{
    build_pythonpath, resolve_python, terminate_child, PluginRuntimeConfig, RUNNER_MODULE,
};

/// Active batch plugin child (one process per pipeline stage).
pub struct BatchPluginSession {
    child: Child,
    stdin: Box<dyn Write + Send>,
    stdout: Arc<Mutex<Box<dyn Read + Send>>>,
    policy: BatchResourcePolicy,
    _temp: TempDir,
}

use std::io::Read;

impl BatchPluginSession {
    pub fn start(
        entry: &CatalogEntry,
        config: serde_json::Value,
        execution_id: &str,
        runtime: &PluginRuntimeConfig,
        expected_digest: Option<&str>,
    ) -> Result<Self, PluginHostError> {
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

        let temp = TempDir::new().map_err(PluginHostError::Io)?;
        let ctx_path = temp.path().join("context.json");
        let batch_policy = runtime.batch_policy.clone();
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
        let env = plugin_child_env(&pythonpath);

        let mut child = Command::new(&python)
            .arg("-m")
            .arg(RUNNER_MODULE)
            .arg("batch")
            .arg("--context")
            .arg(&ctx_path)
            .current_dir(&entry.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(env)
            .spawn()
            .map_err(|e| PluginHostError::SpawnFailed(e.to_string()))?;

        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");

        let stderr_cap = batch_policy.max_stderr_bytes;
        thread::spawn(move || drain_stderr(stderr, stderr_cap));

        Ok(Self {
            child,
            stdin: Box::new(stdin),
            stdout: Arc::new(Mutex::new(Box::new(stdout))),
            policy: batch_policy,
            _temp: temp,
        })
    }

    pub fn transform(&mut self, batch: RecordBatch) -> Result<RecordBatch, PluginHostError> {
        write_batch_frame(&mut self.stdin, &batch)?;
        let mut guard = self.stdout.lock().expect("stdout lock");
        let out = read_batch_frame(guard.as_mut())?.ok_or_else(|| {
            PluginHostError::ProtocolViolation("plugin closed stdout before response batch".into())
        })?;
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
        self.stdin.write_all(&0u32.to_le_bytes()).map_err(PluginHostError::Io)?;
        self.stdin.flush().map_err(PluginHostError::Io)?;
        drop(self.stdin);
        let start = Instant::now();
        let timeout = Duration::from_millis(self.policy.default_timeout_ms);
        loop {
            if let Some(status) = self.child.try_wait()? {
                if !status.success() {
                    return Err(PluginHostError::ProcessFailed {
                        code: status.code(),
                        detail: format!("batch plugin exit {:?}", status.code()),
                    });
                }
                return Ok(());
            }
            if start.elapsed() >= timeout {
                let _ = terminate_child(&mut self.child);
                return Err(PluginHostError::Timeout { timeout_ms: timeout.as_millis() as u64 });
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

fn drain_stderr(mut stderr: impl Read + Send, cap: usize) {
    let mut buf = [0u8; 8192];
    let mut total = 0usize;
    loop {
        if total >= cap {
            let _ = stderr.read(&mut buf);
            continue;
        }
        let n = stderr.read(&mut buf).unwrap_or(0);
        if n == 0 {
            break;
        }
        total += n;
    }
}
