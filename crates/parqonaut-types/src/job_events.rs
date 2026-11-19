//! Durable job operational events and bounded retention policies (v0.11+).

use chrono::{DateTime, Utc};
use parqonaut_workflow::{ProgressEventKind, ProgressMetrics};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::JobId;

pub const JOB_EVENT_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_MAX_EVENTS_PER_JOB: usize = 500;
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 64;
pub const DEFAULT_IDEMPOTENCY_TTL_SECS: i64 = 86_400;

/// Coarse execution stage for durable job events.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum JobStage {
    Queue,
    Worker,
    Scan,
    Diagnose,
    Plan,
    Repair,
    Batch,
    Plugin,
    Publish,
    Verify,
    Cancel,
    Recovery,
}

impl JobStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Worker => "worker",
            Self::Scan => "scan",
            Self::Diagnose => "diagnose",
            Self::Plan => "plan",
            Self::Repair => "repair",
            Self::Batch => "batch",
            Self::Plugin => "plugin",
            Self::Publish => "publish",
            Self::Verify => "verify",
            Self::Cancel => "cancel",
            Self::Recovery => "recovery",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queue" => Some(Self::Queue),
            "worker" => Some(Self::Worker),
            "scan" => Some(Self::Scan),
            "diagnose" => Some(Self::Diagnose),
            "plan" => Some(Self::Plan),
            "repair" => Some(Self::Repair),
            "batch" => Some(Self::Batch),
            "plugin" => Some(Self::Plugin),
            "publish" => Some(Self::Publish),
            "verify" => Some(Self::Verify),
            "cancel" => Some(Self::Cancel),
            "recovery" => Some(Self::Recovery),
            _ => None,
        }
    }
}

/// Ordered, durable operational event recorded for an application job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema, utoipa::ToSchema)]
pub struct JobEvent {
    pub event_id: Uuid,
    pub job_id: JobId,
    /// Monotonic 1-based sequence cursor scoped to `job_id`.
    pub seq: i64,
    pub schema_version: u32,
    pub occurred_at: DateTime<Utc>,
    pub kind: ProgressEventKind,
    pub stage: JobStage,
    pub is_terminal: bool,
    #[serde(default)]
    pub metrics: ProgressMetrics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_digest: Option<String>,
}

/// Retention and backpressure bounds for the durable event journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobEventRetentionPolicy {
    pub max_events_per_job: usize,
    pub channel_capacity: usize,
    pub idempotency_ttl_secs: i64,
}

impl Default for JobEventRetentionPolicy {
    fn default() -> Self {
        Self {
            max_events_per_job: DEFAULT_MAX_EVENTS_PER_JOB,
            channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
            idempotency_ttl_secs: DEFAULT_IDEMPOTENCY_TTL_SECS,
        }
    }
}

impl JobEventRetentionPolicy {
    pub fn from_env() -> Self {
        let mut p = Self::default();
        if let Ok(v) = std::env::var("PARQONAUT_MAX_EVENTS_PER_JOB") {
            if let Ok(n) = v.trim().parse::<usize>() {
                p.max_events_per_job = n.max(10);
            }
        }
        if let Ok(v) = std::env::var("PARQONAUT_EVENT_CHANNEL_CAPACITY") {
            if let Ok(n) = v.trim().parse::<usize>() {
                p.channel_capacity = n.max(4);
            }
        }
        if let Ok(v) = std::env::var("PARQONAUT_IDEMPOTENCY_TTL_SECS") {
            if let Ok(n) = v.trim().parse::<i64>() {
                p.idempotency_ttl_secs = n.max(60);
            }
        }
        p
    }
}

/// Sanitizes an event message to a bounded byte length without secrets or raw credentialed URLs.
pub fn sanitize_event_message(raw: &str, max_bytes: usize) -> String {
    let mut out = String::with_capacity(raw.len().min(max_bytes));
    let mut redact_next = false;
    for token in raw.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        if redact_next {
            out.push_str("<redacted>");
            redact_next = false;
            continue;
        }
        if token.eq_ignore_ascii_case("bearer") {
            out.push_str("Bearer");
            redact_next = true;
        } else if token.contains("://") && token.contains('@') {
            out.push_str("<redacted-url>");
        } else if let Some((k, _v)) = token.split_once('=') {
            let kl = k.to_ascii_lowercase();
            if kl.contains("password")
                || kl.contains("passwd")
                || kl.contains("secret")
                || kl.contains("token")
                || kl.contains("api_key")
                || kl.contains("access_key")
                || kl.contains("credential")
            {
                out.push_str(k);
                out.push_str("=<redacted>");
            } else {
                out.push_str(token);
            }
        } else {
            out.push_str(token);
        }
    }
    if out.len() <= max_bytes {
        return out;
    }
    if max_bytes <= 3 {
        let mut end = max_bytes;
        while end > 0 && !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        return out;
    }
    let mut end = max_bytes - 3;
    while end > 0 && !out.is_char_boundary(end) {
        end -= 1;
    }
    out.truncate(end);
    out.push_str("...");
    out
}
