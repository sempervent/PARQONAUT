//! HTTP bearer-token identity (Phase 9). Roles are ordered for authorization checks.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable row id for a stored bearer token (not the secret).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct AuthTokenId(pub Uuid);

impl AuthTokenId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AuthTokenId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AuthTokenId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Authorization role for API requests (least privilege first).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthRole {
    /// Read-only: runs, jobs, reports, diff, lists.
    Reader,
    /// Submit scans and async jobs.
    Operator,
    /// Full access (reserved for future admin-only routes).
    Admin,
}

impl AuthRole {
    /// Whether this role may perform actions requiring `required`.
    #[must_use]
    pub fn satisfies(self, required: Self) -> bool {
        self >= required
    }
}

impl std::str::FromStr for AuthRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "reader" => Ok(Self::Reader),
            "operator" => Ok(Self::Operator),
            "admin" => Ok(Self::Admin),
            _ => Err(format!("unknown auth role `{s}`")),
        }
    }
}

/// Authenticated caller after bearer validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthPrincipal {
    pub token_id: AuthTokenId,
    pub label: String,
    pub role: AuthRole,
}

/// Whether a stored token row is still usable for authentication.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthTokenStatus {
    Active,
    Disabled,
}
