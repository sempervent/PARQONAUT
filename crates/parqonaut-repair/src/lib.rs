//! Evidence-driven dataset repair: diagnose → plan → execute → verify.
//!
//! Phase 3 adds policy-governed schema reconciliation with explicit authorization,
//! durable plan contracts, execution manifests, and CI-oriented checks.

#![forbid(unsafe_code)]

mod action;
mod authorization;
mod check;
mod deps;
mod diagnose;
mod error;
mod execute;
mod fingerprint;
mod inventory;
mod manifest;
mod plan;
mod plan_canonical;
mod plan_diff;
mod policy;
mod rules;
mod safety;
mod schema;
mod schema_policy;
mod schema_rules;
pub mod stable_id;
mod verify;

pub mod fmt;

pub use action::{EvidenceRef, ExpectedOutcome, Precondition, RepairAction};
pub use authorization::{
    AuthorizationSource, OperationAuditContext, OperationAuditRecord, RepairAuthorization,
};
pub use check::{evaluate_check, CheckExitCode, CheckReport};
pub use diagnose::{diagnose, DiagnosisReport};
pub use error::RepairError;
pub use execute::{ExecutionReport, RepairExecutor};
pub use fingerprint::{compute_dataset_fingerprint, DatasetFingerprint, FingerprintEntry};
pub use inventory::DatasetInventory;
pub use manifest::{ExecutionManifest, MANIFEST_VERSION};
pub use plan::{
    generate_plan, RepairOperation, RepairPlan, PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
};
pub use plan_canonical::canonical_plan_json;
pub use plan_diff::{diff_plans, PlanDiff};
pub use policy::RepairPolicy;
pub use rules::apply_repair_rules;
pub use safety::RepairSafety;
pub use schema::{
    classify_conversion, compute_schema_diff, explain_schema_conflict, resolve_canonical_schema,
    Compatibility, FieldDescriptor, FieldDifference, FieldPath, FileSchemaView, PhysicalType,
    SchemaDiff, SchemaDifferenceKind, SchemaResolution, UnresolvableSchemaConflict,
};
pub use schema_policy::{CiPolicy, EffectivePolicy, FilePolicy, SchemaPolicy};
pub use stable_id::{canonical_json, stable_hex_id};
pub use verify::{scan_directory, verify_repair, VerificationOutcome, VerificationReport};
