-- Optional operator note and non-secret prefix for token identification (Phase 11).

ALTER TABLE auth_tokens ADD COLUMN note TEXT;
ALTER TABLE auth_tokens ADD COLUMN token_prefix TEXT;
