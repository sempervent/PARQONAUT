-- Phase 16: last successful auth timestamp; optional linkage when a token is rotated.

ALTER TABLE auth_tokens ADD COLUMN last_used_at TEXT;
ALTER TABLE auth_tokens ADD COLUMN replaced_by_token_id TEXT;
