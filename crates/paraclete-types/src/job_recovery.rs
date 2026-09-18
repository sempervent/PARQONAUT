//! Bounded retry and lease timing for async scan jobs (Phase 8).

/// Default policy for in-process worker: lease renewal, heartbeat cadence, and max execution attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobRecoveryPolicy {
    /// How far ahead `leased_until` is set on each heartbeat (wall clock).
    pub lease_duration_secs: u64,
    /// How often the worker renews the lease while a scan runs.
    pub heartbeat_interval_secs: u64,
    /// Maximum claim attempts (including retries after stale recovery). When exceeded, stale jobs fail.
    pub max_attempts: u32,
}

impl Default for JobRecoveryPolicy {
    fn default() -> Self {
        Self { lease_duration_secs: 30, heartbeat_interval_secs: 10, max_attempts: 3 }
    }
}
