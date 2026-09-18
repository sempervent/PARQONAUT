use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::RepairError;
use crate::safety::RepairSafety;

/// How an operation was authorized for execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationSource {
    AutomaticSafePolicy,
    ExplicitUser,
    NotAuthorized,
    BlockedByPolicy,
}

/// Per-operation audit record written to execution manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationAuditRecord {
    pub operation_id: String,
    pub plan_id: String,
    pub dataset_fingerprint: String,
    pub policy_fingerprint: String,
    pub executor_version: String,
    pub safety: RepairSafety,
    pub authorization_source: AuthorizationSource,
    pub authorization_time: DateTime<Utc>,
    pub executed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
}

/// Shared audit linkage duplicated on each operation record for manifest portability.
#[derive(Debug, Clone, Copy)]
pub struct OperationAuditContext<'a> {
    pub plan_id: &'a str,
    pub dataset_fingerprint: &'a str,
    pub policy_fingerprint: &'a str,
    pub executor_version: &'a str,
}

impl OperationAuditRecord {
    pub fn new(
        ctx: OperationAuditContext<'_>,
        operation_id: String,
        safety: RepairSafety,
        authorization_source: AuthorizationSource,
        authorization_time: DateTime<Utc>,
        executed: bool,
        skip_reason: Option<String>,
    ) -> Self {
        Self {
            operation_id,
            plan_id: ctx.plan_id.to_string(),
            dataset_fingerprint: ctx.dataset_fingerprint.to_string(),
            policy_fingerprint: ctx.policy_fingerprint.to_string(),
            executor_version: ctx.executor_version.to_string(),
            safety,
            authorization_source,
            authorization_time,
            executed,
            skip_reason,
        }
    }
}

/// Explicit operation-level authorization supplied by the user.
#[derive(Debug, Clone, Default)]
pub struct RepairAuthorization {
    pub authorized_operation_ids: BTreeSet<String>,
}

impl RepairAuthorization {
    pub fn from_ids<I: IntoIterator<Item = String>>(ids: I) -> Self {
        Self { authorized_operation_ids: ids.into_iter().collect() }
    }

    pub fn is_authorized(&self, operation_id: &str) -> bool {
        self.authorized_operation_ids.contains(operation_id)
    }

    pub fn resolve(
        &self,
        operation_id: &str,
        safety: RepairSafety,
    ) -> Result<AuthorizationSource, RepairError> {
        match safety {
            RepairSafety::Safe => Ok(AuthorizationSource::AutomaticSafePolicy),
            RepairSafety::ReviewRequired => {
                if self.is_authorized(operation_id) {
                    Ok(AuthorizationSource::ExplicitUser)
                } else {
                    Err(RepairError::ReviewRequiredNotAuthorized {
                        operation_id: operation_id.to_string(),
                    })
                }
            }
            RepairSafety::Destructive => {
                Err(RepairError::DestructiveNotAllowed { operation_id: operation_id.to_string() })
            }
            RepairSafety::Blocked => Err(RepairError::UnsupportedOperation {
                operation_id: operation_id.to_string(),
                action: "blocked".into(),
            }),
        }
    }
}
