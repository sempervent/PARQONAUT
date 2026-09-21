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
