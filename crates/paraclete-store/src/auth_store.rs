//! Bearer token storage: SHA-256 hashes only; optional note and display prefix (Phase 11).

use chrono::{DateTime, Utc};
use hex::encode as hex_encode;
use paraclete_types::{AuthPrincipal, AuthRole, AuthTokenId};
use rand::Rng;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::StoreError;
use crate::postgres_store::PostgresScanStore;
use crate::sqlite_store::SqliteScanStore;

/// Safe metadata for a token row (no secret, no hash).
#[derive(Debug, Clone)]
pub struct AuthTokenSummary {
    pub token_id: AuthTokenId,
    pub label: String,
    pub role: AuthRole,
    pub created_at: DateTime<Utc>,
    pub disabled_at: Option<DateTime<Utc>>,
    pub note: Option<String>,
    pub token_prefix: Option<String>,
    /// Set when this token was last used successfully for authentication (`verify_bearer_token`).
    pub last_used_at: Option<DateTime<Utc>>,
    /// If this row was disabled during rotation, the new token id (audit trail).
    pub replaced_by_token_id: Option<AuthTokenId>,
}

fn hash_bearer_token(token: &str) -> String {
    let mut h = Sha256::new();
    h.update(token.as_bytes());
    hex_encode(h.finalize())
}

fn parse_role(s: &str) -> Result<AuthRole, StoreError> {
    s.parse().map_err(|e: String| StoreError::AuthToken(e))
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StoreError::AuthToken(format!("bad timestamp: {e}")))
}

fn generate_bearer_secret() -> String {
    let mut b = [0u8; 32];
    rand::thread_rng().fill(&mut b[..]);
    format!("plc_{}", hex_encode(b))
}

impl SqliteScanStore {
    /// Inserts a token row; `plaintext` is hashed and not stored (tests / bootstrap helpers).
    pub async fn insert_auth_token(
        &self,
        label: &str,
        plaintext: &str,
        role: AuthRole,
    ) -> Result<AuthTokenId, StoreError> {
        let id = AuthTokenId::new();
        let hash = hash_bearer_token(plaintext);
        let now = Utc::now().to_rfc3339();
        let role_s = auth_role_db(role);
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix)
               VALUES (?, ?, ?, ?, ?, NULL, NULL)"#,
        )
        .bind(id.0.to_string())
        .bind(&hash)
        .bind(label)
        .bind(role_s)
        .bind(&now)
        .execute(self.pool())
        .await?;
        Ok(id)
    }

    /// Replaces the bootstrap token row (label `bootstrap`) when env-based bootstrap is used.
    pub async fn upsert_bootstrap_token(
        &self,
        plaintext: &str,
        role: AuthRole,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("DELETE FROM auth_tokens WHERE label = 'bootstrap'").execute(&mut *tx).await?;
        let id = AuthTokenId::new();
        let hash = hash_bearer_token(plaintext);
        let now = Utc::now().to_rfc3339();
        let role_s = auth_role_db(role);
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix)
               VALUES (?, ?, ?, ?, ?, NULL, NULL)"#,
        )
        .bind(id.0.to_string())
        .bind(&hash)
        .bind("bootstrap")
        .bind(role_s)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Creates a new token: generates a one-time secret, stores only its hash, returns `(id, plaintext)`.
    pub async fn create_auth_token(
        &self,
        label: &str,
        role: AuthRole,
        note: Option<&str>,
    ) -> Result<(AuthTokenId, String), StoreError> {
        let secret = generate_bearer_secret();
        let prefix: String = secret.chars().take(12).collect();
        let id = AuthTokenId::new();
        let hash = hash_bearer_token(&secret);
        let now = Utc::now().to_rfc3339();
        let role_s = auth_role_db(role);
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix)
               VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(id.0.to_string())
        .bind(&hash)
        .bind(label)
        .bind(role_s)
        .bind(&now)
        .bind(note)
        .bind(&prefix)
        .execute(self.pool())
        .await?;
        Ok((id, secret))
    }

    /// Validates a bearer secret and returns the principal, or `None` if invalid or disabled.
    ///
    /// On success, sets **`last_used_at`** to the current time (Phase 16: one row update per successful auth).
    pub async fn verify_bearer_token(
        &self,
        plaintext: &str,
    ) -> Result<Option<AuthPrincipal>, StoreError> {
        let hash = hash_bearer_token(plaintext);
        let row = sqlx::query_as::<_, AuthTokenRow>(
            r#"SELECT token_id, label, role FROM auth_tokens
               WHERE token_hash = ? AND disabled_at IS NULL"#,
        )
        .bind(&hash)
        .fetch_optional(self.pool())
        .await?;
        let Some(r) = row else {
            return Ok(None);
        };
        let token_id = Uuid::parse_str(&r.token_id)
            .map(AuthTokenId)
            .map_err(|_| StoreError::AuthToken("invalid token_id in auth_tokens".into()))?;
        let role = parse_role(&r.role)?;
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"UPDATE auth_tokens SET last_used_at = ? WHERE token_id = ? AND disabled_at IS NULL"#,
        )
        .bind(&now)
        .bind(&r.token_id)
        .execute(self.pool())
        .await?;
        Ok(Some(AuthPrincipal { token_id, label: r.label, role }))
    }

    /// Atomically creates a new token (same label, role, note) and disables the old row, recording **`replaced_by_token_id`** on the old row.
    ///
    /// Returns the new **`AuthTokenId`** and one-time plaintext secret (hash stored only).
    pub async fn rotate_auth_token(
        &self,
        old_id: AuthTokenId,
    ) -> Result<(AuthTokenId, String), StoreError> {
        let mut tx = self.pool().begin().await?;
        let old = sqlx::query_as::<_, AuthTokenListRow>(
            r#"SELECT token_id, label, role, created_at, disabled_at, note, token_prefix, last_used_at, replaced_by_token_id
               FROM auth_tokens WHERE token_id = ? AND disabled_at IS NULL"#,
        )
        .bind(old_id.0.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(old_row) = old else {
            return Err(StoreError::AuthTokenNotFound(old_id.0));
        };

        let secret = generate_bearer_secret();
        let prefix: String = secret.chars().take(12).collect();
        let new_id = AuthTokenId::new();
        let new_hash = hash_bearer_token(&secret);
        let now = Utc::now().to_rfc3339();
        let role_s = old_row.role.clone();
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix, last_used_at, replaced_by_token_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)"#,
        )
        .bind(new_id.0.to_string())
        .bind(&new_hash)
        .bind(&old_row.label)
        .bind(&role_s)
        .bind(&now)
        .bind(&old_row.note)
        .bind(&prefix)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"UPDATE auth_tokens SET disabled_at = ?, replaced_by_token_id = ? WHERE token_id = ?"#,
        )
        .bind(&now)
        .bind(new_id.0.to_string())
        .bind(old_id.0.to_string())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok((new_id, secret))
    }

    /// Returns number of rows in `auth_tokens`.
    pub async fn count_auth_tokens(&self) -> Result<i64, StoreError> {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM auth_tokens").fetch_one(self.pool()).await?;
        Ok(n)
    }

    /// Lists all tokens (safe columns only).
    pub async fn list_auth_tokens(&self) -> Result<Vec<AuthTokenSummary>, StoreError> {
        let rows = sqlx::query_as::<_, AuthTokenListRow>(
            r#"SELECT token_id, label, role, created_at, disabled_at, note, token_prefix, last_used_at, replaced_by_token_id
               FROM auth_tokens ORDER BY created_at DESC"#,
        )
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(|r| r.into_summary()).collect::<Result<Vec<_>, _>>()
    }

    /// Returns metadata for one token, if present.
    pub async fn get_auth_token_summary(
        &self,
        token_id: AuthTokenId,
    ) -> Result<Option<AuthTokenSummary>, StoreError> {
        let row = sqlx::query_as::<_, AuthTokenListRow>(
            r#"SELECT token_id, label, role, created_at, disabled_at, note, token_prefix, last_used_at, replaced_by_token_id
               FROM auth_tokens WHERE token_id = ?"#,
        )
        .bind(token_id.0.to_string())
        .fetch_optional(self.pool())
        .await?;
        match row {
            None => Ok(None),
            Some(r) => Ok(Some(r.into_summary()?)),
        }
    }

    /// Disables a token. Returns `Err` if the id does not exist.
    pub async fn disable_auth_token(
        &self,
        token_id: AuthTokenId,
    ) -> Result<AuthTokenSummary, StoreError> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(r#"UPDATE auth_tokens SET disabled_at = ? WHERE token_id = ?"#)
            .bind(&now)
            .bind(token_id.0.to_string())
            .execute(self.pool())
            .await?;
        if res.rows_affected() == 0 {
            return Err(StoreError::AuthTokenNotFound(token_id.0));
        }
        self.get_auth_token_summary(token_id)
            .await?
            .ok_or_else(|| StoreError::AuthToken("token row missing after disable".into()))
    }

    /// If `PARACLETE_BOOTSTRAP_TOKEN` is set, upserts label `bootstrap` (optional `PARACLETE_BOOTSTRAP_ROLE`, default `admin`).
    pub async fn bootstrap_auth_from_env(&self) -> Result<(), StoreError> {
        let Ok(raw) = std::env::var("PARACLETE_BOOTSTRAP_TOKEN") else {
            return Ok(());
        };
        if raw.is_empty() {
            return Ok(());
        }
        let role = match std::env::var("PARACLETE_BOOTSTRAP_ROLE") {
            Ok(s) => s.parse().map_err(|e: String| StoreError::AuthToken(e))?,
            Err(_) => AuthRole::Admin,
        };
        self.upsert_bootstrap_token(&raw, role).await
    }
}

impl PostgresScanStore {
    pub async fn insert_auth_token(
        &self,
        label: &str,
        plaintext: &str,
        role: AuthRole,
    ) -> Result<AuthTokenId, StoreError> {
        let id = AuthTokenId::new();
        let hash = hash_bearer_token(plaintext);
        let now = Utc::now().to_rfc3339();
        let role_s = auth_role_db(role);
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix)
               VALUES ($1, $2, $3, $4, $5, NULL, NULL)"#,
        )
        .bind(id.0.to_string())
        .bind(&hash)
        .bind(label)
        .bind(role_s)
        .bind(&now)
        .execute(self.pool())
        .await?;
        Ok(id)
    }

    pub async fn upsert_bootstrap_token(
        &self,
        plaintext: &str,
        role: AuthRole,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query("DELETE FROM auth_tokens WHERE label = 'bootstrap'").execute(&mut *tx).await?;
        let id = AuthTokenId::new();
        let hash = hash_bearer_token(plaintext);
        let now = Utc::now().to_rfc3339();
        let role_s = auth_role_db(role);
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix)
               VALUES ($1, $2, $3, $4, $5, NULL, NULL)"#,
        )
        .bind(id.0.to_string())
        .bind(&hash)
        .bind("bootstrap")
        .bind(role_s)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_auth_token(
        &self,
        label: &str,
        role: AuthRole,
        note: Option<&str>,
    ) -> Result<(AuthTokenId, String), StoreError> {
        let secret = generate_bearer_secret();
        let prefix: String = secret.chars().take(12).collect();
        let id = AuthTokenId::new();
        let hash = hash_bearer_token(&secret);
        let now = Utc::now().to_rfc3339();
        let role_s = auth_role_db(role);
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix)
               VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
        )
        .bind(id.0.to_string())
        .bind(&hash)
        .bind(label)
        .bind(role_s)
        .bind(&now)
        .bind(note)
        .bind(&prefix)
        .execute(self.pool())
        .await?;
        Ok((id, secret))
    }

    pub async fn verify_bearer_token(
        &self,
        plaintext: &str,
    ) -> Result<Option<AuthPrincipal>, StoreError> {
        let hash = hash_bearer_token(plaintext);
        let row = sqlx::query_as::<_, AuthTokenRow>(
            r#"SELECT token_id, label, role FROM auth_tokens
               WHERE token_hash = $1 AND disabled_at IS NULL"#,
        )
        .bind(&hash)
        .fetch_optional(self.pool())
        .await?;
        let Some(r) = row else {
            return Ok(None);
        };
        let token_id = Uuid::parse_str(&r.token_id)
            .map(AuthTokenId)
            .map_err(|_| StoreError::AuthToken("invalid token_id in auth_tokens".into()))?;
        let role = parse_role(&r.role)?;
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"UPDATE auth_tokens SET last_used_at = $1 WHERE token_id = $2 AND disabled_at IS NULL"#,
        )
        .bind(&now)
        .bind(&r.token_id)
        .execute(self.pool())
        .await?;
        Ok(Some(AuthPrincipal { token_id, label: r.label, role }))
    }

    pub async fn rotate_auth_token(
        &self,
        old_id: AuthTokenId,
    ) -> Result<(AuthTokenId, String), StoreError> {
        let mut tx = self.pool().begin().await?;
        let old = sqlx::query_as::<_, AuthTokenListRow>(
            r#"SELECT token_id, label, role, created_at, disabled_at, note, token_prefix, last_used_at, replaced_by_token_id
               FROM auth_tokens WHERE token_id = $1 AND disabled_at IS NULL"#,
        )
        .bind(old_id.0.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(old_row) = old else {
            return Err(StoreError::AuthTokenNotFound(old_id.0));
        };

        let secret = generate_bearer_secret();
        let prefix: String = secret.chars().take(12).collect();
        let new_id = AuthTokenId::new();
        let new_hash = hash_bearer_token(&secret);
        let now = Utc::now().to_rfc3339();
        let role_s = old_row.role.clone();
        sqlx::query(
            r#"INSERT INTO auth_tokens (token_id, token_hash, label, role, created_at, note, token_prefix, last_used_at, replaced_by_token_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL)"#,
        )
        .bind(new_id.0.to_string())
        .bind(&new_hash)
        .bind(&old_row.label)
        .bind(&role_s)
        .bind(&now)
        .bind(&old_row.note)
        .bind(&prefix)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"UPDATE auth_tokens SET disabled_at = $1, replaced_by_token_id = $2 WHERE token_id = $3"#,
        )
        .bind(&now)
        .bind(new_id.0.to_string())
        .bind(old_id.0.to_string())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok((new_id, secret))
    }

    pub async fn count_auth_tokens(&self) -> Result<i64, StoreError> {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM auth_tokens").fetch_one(self.pool()).await?;
        Ok(n)
    }

    pub async fn list_auth_tokens(&self) -> Result<Vec<AuthTokenSummary>, StoreError> {
        let rows = sqlx::query_as::<_, AuthTokenListRow>(
            r#"SELECT token_id, label, role, created_at, disabled_at, note, token_prefix, last_used_at, replaced_by_token_id
               FROM auth_tokens ORDER BY created_at DESC"#,
        )
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(|r| r.into_summary()).collect::<Result<Vec<_>, _>>()
    }

    pub async fn get_auth_token_summary(
        &self,
        token_id: AuthTokenId,
    ) -> Result<Option<AuthTokenSummary>, StoreError> {
        let row = sqlx::query_as::<_, AuthTokenListRow>(
            r#"SELECT token_id, label, role, created_at, disabled_at, note, token_prefix, last_used_at, replaced_by_token_id
               FROM auth_tokens WHERE token_id = $1"#,
        )
        .bind(token_id.0.to_string())
        .fetch_optional(self.pool())
        .await?;
        match row {
            None => Ok(None),
            Some(r) => Ok(Some(r.into_summary()?)),
        }
    }

    pub async fn disable_auth_token(
        &self,
        token_id: AuthTokenId,
    ) -> Result<AuthTokenSummary, StoreError> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(r#"UPDATE auth_tokens SET disabled_at = $1 WHERE token_id = $2"#)
            .bind(&now)
            .bind(token_id.0.to_string())
            .execute(self.pool())
            .await?;
        if res.rows_affected() == 0 {
            return Err(StoreError::AuthTokenNotFound(token_id.0));
        }
        self.get_auth_token_summary(token_id)
            .await?
            .ok_or_else(|| StoreError::AuthToken("token row missing after disable".into()))
    }

    pub async fn bootstrap_auth_from_env(&self) -> Result<(), StoreError> {
        let Ok(raw) = std::env::var("PARACLETE_BOOTSTRAP_TOKEN") else {
            return Ok(());
        };
        if raw.is_empty() {
            return Ok(());
        }
        let role = match std::env::var("PARACLETE_BOOTSTRAP_ROLE") {
            Ok(s) => s.parse().map_err(|e: String| StoreError::AuthToken(e))?,
            Err(_) => AuthRole::Admin,
        };
        self.upsert_bootstrap_token(&raw, role).await
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AuthTokenRow {
    token_id: String,
    label: String,
    role: String,
}

#[derive(Debug, sqlx::FromRow)]
struct AuthTokenListRow {
    token_id: String,
    label: String,
    role: String,
    created_at: String,
    disabled_at: Option<String>,
    note: Option<String>,
    token_prefix: Option<String>,
    last_used_at: Option<String>,
    replaced_by_token_id: Option<String>,
}

impl AuthTokenListRow {
    fn into_summary(self) -> Result<AuthTokenSummary, StoreError> {
        let token_id = Uuid::parse_str(&self.token_id)
            .map(AuthTokenId)
            .map_err(|_| StoreError::AuthToken("invalid token_id".into()))?;
        let role = parse_role(&self.role)?;
        let replaced_by_token_id = match &self.replaced_by_token_id {
            None => None,
            Some(s) => Some(
                Uuid::parse_str(s)
                    .map(AuthTokenId)
                    .map_err(|_| StoreError::AuthToken("invalid replaced_by_token_id".into()))?,
            ),
        };
        Ok(AuthTokenSummary {
            token_id,
            label: self.label,
            role,
            created_at: parse_dt(&self.created_at)?,
            disabled_at: self.disabled_at.map(|s| parse_dt(&s)).transpose()?,
            note: self.note,
            token_prefix: self.token_prefix,
            last_used_at: self.last_used_at.map(|s| parse_dt(&s)).transpose()?,
            replaced_by_token_id,
        })
    }
}

fn auth_role_db(r: AuthRole) -> &'static str {
    match r {
        AuthRole::Reader => "reader",
        AuthRole::Operator => "operator",
        AuthRole::Admin => "admin",
    }
}
