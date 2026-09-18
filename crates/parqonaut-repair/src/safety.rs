use serde::{Deserialize, Serialize};

/// Safety classification for repair operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RepairSafety {
    /// Preserves logical row content; mechanically reversible or reproducible.
    Safe,
    /// May alter representation or require assumptions; needs explicit authorization.
    ReviewRequired,
    /// Alters logical content; never auto-executed.
    Destructive,
    /// Diagnosed conflict that cannot be repaired under policy; never executed.
    Blocked,
}

impl RepairSafety {
    pub fn is_auto_executable(self) -> bool {
        matches!(self, Self::Safe)
    }

    pub fn requires_explicit_authorization(self) -> bool {
        matches!(self, Self::ReviewRequired)
    }
}
