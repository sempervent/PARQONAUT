//! Evidence-driven dataset repair: diagnose → plan → execute → verify.
//!
//! Every repair operation traces to concrete scan/diagnosis findings. Safe operations
//! may be executed automatically; ReviewRequired and Destructive operations are
//! represented in plans but never auto-executed in Phase 2.

#![forbid(unsafe_code)]

mod action;
mod diagnose;
mod error;
mod execute;
mod fingerprint;
mod inventory;
mod plan;
mod policy;
mod rules;
mod safety;
mod stable_id;
mod verify;

pub mod fmt;

pub use action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
pub use diagnose::{diagnose, DiagnosisReport};
pub use error::RepairError;
pub use execute::{ExecutionReport, RepairExecutor};
pub use fingerprint::{compute_dataset_fingerprint, DatasetFingerprint, FingerprintEntry};
pub use inventory::DatasetInventory;
pub use plan::{generate_plan, RepairOperation, RepairPlan, PLAN_SCHEMA_VERSION};
pub use policy::RepairPolicy;
pub use rules::apply_repair_rules;
pub use safety::RepairSafety;
pub use verify::{scan_directory, verify_repair, VerificationOutcome, VerificationReport};
