-- Bearer API tokens (hashed at rest) for Phase 9 authentication.

CREATE TABLE auth_tokens (
  token_id TEXT PRIMARY KEY NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  role TEXT NOT NULL,
  created_at TEXT NOT NULL,
  disabled_at TEXT
);

CREATE INDEX idx_auth_tokens_hash ON auth_tokens(token_hash);
