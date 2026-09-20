use crate::progress::{ProgressEvent, ProgressObserver};
use std::io::{self, IsTerminal, Write};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct NoOpProgressObserver;

impl ProgressObserver for NoOpProgressObserver {
    fn emit(&self, _event: ProgressEvent) {}
}

#[derive(Debug, Default)]
pub struct CollectingProgressObserver {
    events: Arc<Mutex<Vec<ProgressEvent>>>,
}

impl CollectingProgressObserver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn take_events(&self) -> Vec<ProgressEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl ProgressObserver for CollectingProgressObserver {
    fn emit(&self, event: ProgressEvent) {
        self.events.lock().unwrap().push(event);
    }
}

pub struct TracingProgressObserver;

impl ProgressObserver for TracingProgressObserver {
    fn emit(&self, event: ProgressEvent) {
        tracing::info!(target: "parqonaut.progress", ?event, "progress");
    }
}

pub struct JsonLinesProgressObserver<W: Write + Send + Sync> {
    writer: Mutex<W>,
}

impl<W: Write + Send + Sync> JsonLinesProgressObserver<W> {
    pub fn new(writer: W) -> Self {
        Self { writer: Mutex::new(writer) }
    }
}

impl<W: Write + Send + Sync> ProgressObserver for JsonLinesProgressObserver<W> {
    fn emit(&self, event: ProgressEvent) {
        if let Ok(line) = serde_json::to_string(&event) {
            let mut w = self.writer.lock().unwrap();
            let _ = writeln!(w, "{line}");
            let _ = w.flush();
        }
    }
}

pub struct TerminalProgressObserver {
    tty: bool,
}

impl Default for TerminalProgressObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl TerminalProgressObserver {
    pub fn new() -> Self {
        Self { tty: io::stderr().is_terminal() }
    }

    pub fn is_tty(&self) -> bool {
        self.tty
    }
}

impl ProgressObserver for TerminalProgressObserver {
    fn emit(&self, event: ProgressEvent) {
        if !self.tty {
            tracing::info!(
                target: "parqonaut.progress",
                kind = ?event.kind,
                path = ?event.path,
                "progress"
            );
            return;
        }
        eprintln!("[{:?}] {}", event.kind, event.message.as_deref().unwrap_or(""));
    }
}
