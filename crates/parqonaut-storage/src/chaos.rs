//! Deterministic fault injection for storage integration and retry tests.
//!
//! Enabled only with the `test-util` feature.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, ReadBuf};

use crate::backend::{ByteRange, ListOptions, ListPage, StorageBackend};
use crate::capabilities::StorageCapabilities;
use crate::conditional::{ConditionalCreate, ConditionalReplace};
use crate::error::StorageError;
use crate::location::{DatasetLocation, ObjectLocation};
use crate::metadata::ObjectMetadata;
use crate::metrics::StorageMetrics;
use crate::stream::{ObjectReadStream, ObjectWriteStream};

/// Storage operation targeted by a fault rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultTarget {
    List,
    Head,
    ReadRange,
    ReadStreamOpen,
    ReadStreamRead,
    WriteStreamOpen,
    /// Nth `write_all` on a write stream (1-based), simulating multipart part uploads.
    WriteStreamPart,
    ConditionalCreate,
    ConditionalReplace,
    DeleteOwned,
}

/// One step in a per-invocation fault sequence.
#[derive(Debug)]
pub enum FaultStep {
    /// Return the given error without calling the inner backend.
    Inject(StorageError),
    /// Call the inner backend for this invocation.
    Delegate,
}

/// Behavior after the configured sequence is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExhaustedBehavior {
    #[default]
    Delegate,
    /// Keep returning the last injected error from the sequence.
    RepeatLast,
}

/// Deterministic fault rule matched in declaration order (first match wins).
#[derive(Debug)]
pub struct FaultRule {
    pub target: FaultTarget,
    /// When set, the object or dataset URI must contain this substring.
    pub location_substr: Option<String>,
    /// For [`FaultTarget::WriteStreamPart`], only match this 1-based part index.
    pub write_part_index: Option<u64>,
    pub steps: Vec<FaultStep>,
    pub exhausted: ExhaustedBehavior,
}

impl FaultRule {
    pub fn new(target: FaultTarget) -> Self {
        Self {
            target,
            location_substr: None,
            write_part_index: None,
            steps: Vec::new(),
            exhausted: ExhaustedBehavior::Delegate,
        }
    }

    pub fn location_substr(mut self, needle: impl Into<String>) -> Self {
        self.location_substr = Some(needle.into());
        self
    }

    pub fn write_part_index(mut self, index: u64) -> Self {
        self.write_part_index = Some(index);
        self
    }

    pub fn steps(mut self, steps: Vec<FaultStep>) -> Self {
        self.steps = steps;
        self
    }

    pub fn exhausted(mut self, behavior: ExhaustedBehavior) -> Self {
        self.exhausted = behavior;
        self
    }

    fn matches(&self, ctx: &FaultContext<'_>) -> bool {
        if self.target != ctx.target {
            return false;
        }
        if let Some(part) = self.write_part_index {
            if ctx.write_part_index != Some(part) {
                return false;
            }
        }
        if let Some(needle) = &self.location_substr {
            let haystack = ctx.location_uri();
            if !haystack.contains(needle) {
                return false;
            }
        }
        true
    }

    fn outcome_at(&self, invocation: u64) -> Option<StorageError> {
        if self.steps.is_empty() {
            return None;
        }
        let last_inject = self
            .steps
            .iter()
            .enumerate()
            .filter_map(|(i, s)| match s {
                FaultStep::Inject(_) => Some(i),
                FaultStep::Delegate => None,
            })
            .last();

        if invocation < self.steps.len() as u64 {
            return match &self.steps[invocation as usize] {
                FaultStep::Inject(err) => Some(replay_error(err)),
                FaultStep::Delegate => None,
            };
        }

        match self.exhausted {
            ExhaustedBehavior::Delegate => None,
            ExhaustedBehavior::RepeatLast => last_inject.map(|idx| match &self.steps[idx] {
                FaultStep::Inject(err) => replay_error(err),
                FaultStep::Delegate => unreachable!("last inject index points at Inject"),
            }),
        }
    }
}

fn clone_rules(rules: &[FaultRule]) -> Vec<FaultRule> {
    rules
        .iter()
        .map(|rule| FaultRule {
            target: rule.target,
            location_substr: rule.location_substr.clone(),
            write_part_index: rule.write_part_index,
            steps: rule
                .steps
                .iter()
                .map(|step| match step {
                    FaultStep::Inject(err) => FaultStep::Inject(replay_error(err)),
                    FaultStep::Delegate => FaultStep::Delegate,
                })
                .collect(),
            exhausted: rule.exhausted,
        })
        .collect()
}

fn replay_error(err: &StorageError) -> StorageError {
    match err {
        StorageError::NotFound { location } => StorageError::NotFound { location: location.clone() },
        StorageError::PermissionDenied { location } => {
            StorageError::PermissionDenied { location: location.clone() }
        }
        StorageError::Authentication => StorageError::Authentication,
        StorageError::Conflict { message } => StorageError::Conflict { message: message.clone() },
        StorageError::PreconditionFailed { message } => {
            StorageError::PreconditionFailed { message: message.clone() }
        }
        StorageError::Transient { message } => StorageError::Transient { message: message.clone() },
        StorageError::Unavailable { message } => {
            StorageError::Unavailable { message: message.clone() }
        }
        StorageError::InvalidLocation { message } => {
            StorageError::InvalidLocation { message: message.clone() }
        }
        StorageError::UnsupportedCapability { capability } => StorageError::UnsupportedCapability {
            capability: capability.clone(),
        },
        StorageError::Io(e) => StorageError::Other { message: e.to_string() },
        StorageError::Other { message } => StorageError::Other { message: message.clone() },
    }
}

struct FaultContext<'a> {
    target: FaultTarget,
    object: Option<&'a ObjectLocation>,
    dataset: Option<&'a DatasetLocation>,
    write_part_index: Option<u64>,
}

impl FaultContext<'_> {
    fn location_uri(&self) -> String {
        if let Some(object) = self.object {
            return object.display_uri();
        }
        if let Some(dataset) = self.dataset {
            return dataset.display_uri();
        }
        String::new()
    }
}

/// Wraps any [`StorageBackend`] with deterministic, rule-driven failures.
pub struct FaultInjectingBackend<B> {
    inner: B,
    rules: Vec<FaultRule>,
    counters: Arc<Vec<AtomicU64>>,
    write_part_counters: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
}

impl<B> FaultInjectingBackend<B> {
    pub fn new(inner: B, rules: Vec<FaultRule>) -> Self {
        let counters = rules.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            inner,
            rules,
            counters: Arc::new(counters),
            write_part_counters: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    fn evaluate(&self, ctx: &FaultContext<'_>) -> Option<StorageError> {
        for (idx, rule) in self.rules.iter().enumerate() {
            if !rule.matches(ctx) {
                continue;
            }
            let n = self.counters[idx].fetch_add(1, Ordering::SeqCst);
            if let Some(err) = rule.outcome_at(n) {
                return Some(err);
            }
            return None;
        }
        None
    }

}

#[async_trait]
impl<B: StorageBackend> StorageBackend for FaultInjectingBackend<B> {
    fn capabilities(&self) -> StorageCapabilities {
        self.inner.capabilities()
    }

    fn metrics(&self) -> StorageMetrics {
        self.inner.metrics()
    }

    async fn list(
        &self,
        dataset: &DatasetLocation,
        options: ListOptions,
        continuation_token: Option<&str>,
    ) -> Result<ListPage, StorageError> {
        let ctx = FaultContext {
            target: FaultTarget::List,
            object: None,
            dataset: Some(dataset),
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&ctx) {
            return Err(err);
        }
        self.inner.list(dataset, options, continuation_token).await
    }

    async fn head(&self, object: &ObjectLocation) -> Result<ObjectMetadata, StorageError> {
        let ctx = FaultContext {
            target: FaultTarget::Head,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&ctx) {
            return Err(err);
        }
        self.inner.head(object).await
    }

    async fn read_range(
        &self,
        object: &ObjectLocation,
        range: ByteRange,
    ) -> Result<Bytes, StorageError> {
        let ctx = FaultContext {
            target: FaultTarget::ReadRange,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&ctx) {
            return Err(err);
        }
        self.inner.read_range(object, range).await
    }

    async fn read_stream(
        &self,
        object: &ObjectLocation,
        range: Option<ByteRange>,
    ) -> Result<ObjectReadStream, StorageError> {
        let open_ctx = FaultContext {
            target: FaultTarget::ReadStreamOpen,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&open_ctx) {
            return Err(err);
        }

        let stream = self.inner.read_stream(object, range).await?;
        let backend = self.clone_shim();
        let object = object.clone();
        Ok(stream.inject_reader(move |inner| {
            Box::pin(FaultInjectingReader {
                inner,
                backend,
                object,
                first_read: false,
            })
        }))
    }

    async fn write_stream(
        &self,
        object: &ObjectLocation,
        content_length: Option<u64>,
    ) -> Result<ObjectWriteStream, StorageError> {
        let open_ctx = FaultContext {
            target: FaultTarget::WriteStreamOpen,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&open_ctx) {
            return Err(err);
        }

        let mut stream = self.inner.write_stream(object, content_length).await?;
        let backend = self.clone_shim();
        let object_for_hook = object.clone();
        stream.set_before_write(Arc::new(move || {
            let part = backend.next_write_part_index(&object_for_hook);
            let ctx = FaultContext {
                target: FaultTarget::WriteStreamPart,
                object: Some(&object_for_hook),
                dataset: None,
                write_part_index: Some(part),
            };
            backend.evaluate(&ctx).map_or(Ok(()), Err)
        }));
        Ok(stream)
    }

    async fn conditional_create(
        &self,
        object: &ObjectLocation,
        condition: ConditionalCreate,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        let ctx = FaultContext {
            target: FaultTarget::ConditionalCreate,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&ctx) {
            return Err(err);
        }
        self.inner.conditional_create(object, condition, data).await
    }

    async fn conditional_replace(
        &self,
        object: &ObjectLocation,
        condition: ConditionalReplace,
        data: Bytes,
    ) -> Result<ObjectMetadata, StorageError> {
        let ctx = FaultContext {
            target: FaultTarget::ConditionalReplace,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&ctx) {
            return Err(err);
        }
        self.inner.conditional_replace(object, condition, data).await
    }

    async fn delete_owned_object(&self, object: &ObjectLocation) -> Result<(), StorageError> {
        let ctx = FaultContext {
            target: FaultTarget::DeleteOwned,
            object: Some(object),
            dataset: None,
            write_part_index: None,
        };
        if let Some(err) = self.evaluate(&ctx) {
            return Err(err);
        }
        self.inner.delete_owned_object(object).await
    }
}

impl<B: StorageBackend> FaultInjectingBackend<B> {
    fn clone_shim(&self) -> FaultInjectingBackendShim {
        FaultInjectingBackendShim {
            rules: clone_rules(&self.rules),
            counters: Arc::clone(&self.counters),
            write_part_counters: Arc::clone(&self.write_part_counters),
        }
    }
}

struct FaultInjectingBackendShim {
    rules: Vec<FaultRule>,
    counters: Arc<Vec<AtomicU64>>,
    write_part_counters: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
}

impl Clone for FaultInjectingBackendShim {
    fn clone(&self) -> Self {
        Self {
            rules: clone_rules(&self.rules),
            counters: Arc::clone(&self.counters),
            write_part_counters: Arc::clone(&self.write_part_counters),
        }
    }
}

impl FaultInjectingBackendShim {
    fn evaluate(&self, ctx: &FaultContext<'_>) -> Option<StorageError> {
        for (idx, rule) in self.rules.iter().enumerate() {
            if !rule.matches(ctx) {
                continue;
            }
            let n = self.counters[idx].fetch_add(1, Ordering::SeqCst);
            if let Some(err) = rule.outcome_at(n) {
                return Some(err);
            }
            return None;
        }
        None
    }

    fn next_write_part_index(&self, object: &ObjectLocation) -> u64 {
        let key = object.display_uri();
        let mut map = self.write_part_counters.lock().expect("write part lock");
        let entry = map.entry(key).or_insert(0);
        *entry += 1;
        *entry
    }
}

struct FaultInjectingReader {
    inner: Pin<Box<dyn AsyncRead + Send + Unpin>>,
    backend: FaultInjectingBackendShim,
    object: ObjectLocation,
    first_read: bool,
}

impl AsyncRead for FaultInjectingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.first_read {
            self.first_read = true;
            let ctx = FaultContext {
                target: FaultTarget::ReadStreamRead,
                object: Some(&self.object),
                dataset: None,
                write_part_index: None,
            };
            if let Some(err) = self.backend.evaluate(&ctx) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    err.to_string(),
                )));
            }
        }
        self.inner.as_mut().poll_read(cx, buf)
    }
}

/// Helpers for constructing common HTTP-shaped storage faults in tests.
pub mod faults {
    use super::*;

    pub fn service_unavailable() -> StorageError {
        StorageError::Unavailable { message: "503 Service Unavailable".into() }
    }

    pub fn transient_timeout() -> StorageError {
        StorageError::Transient { message: "upstream timeout".into() }
    }

    pub fn permission_denied(location: &str) -> StorageError {
        StorageError::PermissionDenied { location: location.into() }
    }

    pub fn conflict_exists(location: &str) -> StorageError {
        StorageError::Conflict { message: format!("object already exists: {location}") }
    }

    pub fn precondition_etag() -> StorageError {
        StorageError::PreconditionFailed { message: "etag mismatch".into() }
    }

    pub fn multipart_part_failed(part: u32) -> StorageError {
        StorageError::Transient { message: format!("multipart upload part {part} failed") }
    }

    /// 503 once, then delegate (retry success path).
    pub fn unavailable_then_ok(target: FaultTarget) -> FaultRule {
        FaultRule::new(target).steps(vec![
            FaultStep::Inject(service_unavailable()),
            FaultStep::Delegate,
        ])
    }

    /// Always return 503 (retry exhaustion when caller cap is lower).
    pub fn retry_exhaustion(target: FaultTarget) -> FaultRule {
        FaultRule::new(target)
            .steps(vec![FaultStep::Inject(service_unavailable())])
            .exhausted(ExhaustedBehavior::RepeatLast)
    }
}
