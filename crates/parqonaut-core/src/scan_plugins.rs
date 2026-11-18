//! Optional scan analyzer plugin hooks (implemented by `parqonaut-plugin-host`).

use std::collections::BTreeSet;

use parqonaut_plugin_protocol::{PluginExecutionPhase, PluginScanContext};
use parqonaut_types::{Finding, PluginExecutionRecord};

use crate::CoreError;

/// Host-provided scan plugin runner invoked at declared phases.
pub trait ScanPluginHost: Send + Sync {
    fn run_phase(
        &self,
        phase: PluginExecutionPhase,
        context: PluginScanContext,
        known_evidence: &BTreeSet<String>,
    ) -> Result<(Vec<Finding>, Vec<PluginExecutionRecord>), CoreError>;

    fn resolved_plugin_order(&self) -> &[String] {
        &[]
    }
}
