//! Resource bounds for untrusted plugin subprocess I/O.

#[derive(Debug, Clone)]
pub struct PluginResourcePolicy {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_stderr_bytes: usize,
    pub max_findings: usize,
    pub max_finding_summary_len: usize,
    pub max_finding_detail_len: usize,
    pub max_evidence_ids_per_finding: usize,
    pub max_annotations: usize,
    pub max_annotation_bytes: usize,
    pub default_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct BatchResourcePolicy {
    pub max_stderr_bytes: usize,
    pub max_output_rows_per_input_batch: u64,
    pub max_output_expansion_factor: f64,
    pub default_timeout_ms: u64,
    pub max_inflight_batches: usize,
    pub max_config_bytes: usize,
}

impl Default for BatchResourcePolicy {
    fn default() -> Self {
        Self {
            max_stderr_bytes: 256 * 1024,
            max_output_rows_per_input_batch: 1_000_000,
            max_output_expansion_factor: 4.0,
            default_timeout_ms: 300_000,
            max_inflight_batches: 4,
            max_config_bytes: 64 * 1024,
        }
    }
}

impl Default for PluginResourcePolicy {
    fn default() -> Self {
        Self {
            max_request_bytes: 4 * 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_stderr_bytes: 256 * 1024,
            max_findings: 256,
            max_finding_summary_len: 4_096,
            max_finding_detail_len: 32_768,
            max_evidence_ids_per_finding: 32,
            max_annotations: 64,
            max_annotation_bytes: 256 * 1024,
            default_timeout_ms: 30_000,
        }
    }
}
