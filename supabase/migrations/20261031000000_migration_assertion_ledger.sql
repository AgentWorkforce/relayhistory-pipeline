-- Single-use ledger for RelayAuth migration assertions (relayauth#110,
-- `POST /v1/tokens/migration-assertion`; O2 of the Neon-to-S3 export).
--
-- RelayAuth records neither `jti` nor `meta.nonce`, so the ingest makes each
-- assertion single-use: the manifest commit (`POST /v1/pipeline/manifests`)
-- inserts one row here, insert-if-absent on both keys, in the same transaction
-- as its `sessions.pipeline_jobs` row. A conflict on either key is a replay
-- (409 `migration_assertion_replayed`), and a failed enqueue rolls the
-- consumption back with it. See relayhistory-cloud
-- `packages/relayhistory/src/lib/pipeline/migration-assertions.ts`.
--
-- Ids and a plan digest only: no token, no signature, no session content.
-- Service-role only: RLS on with no policy, and no grant to tenants. Code only:
-- applying it to a hosted project needs Khaliq's go.
--
-- Plan binding: `plan_entry_digest` is the signed `meta.planEntryDigest`, the
-- digest of the one canonical export plan entry this assertion uploaded
-- (96BullSessions docs/contracts/relayhistory-export-plan-entry-v1.md); the
-- ingest derived it from the upload itself before inserting.
--
-- Chunk uses: `migration_assertion_chunks` holds at most one chunk per
-- (jti, seq), and the ingest admits only seq < the entry's chunk_count, so one
-- assertion writes at most chunk_count chunks within its <= 65 s validity.
--
-- Retention (no cron, nothing scheduled): every ledger insert first deletes at
-- most 100 rows (per table) whose `expires_at` passed more than 5 minutes ago.
-- `expires_at` is the token's exp plus the ingest's 5 second migration skew.
-- The verifier refuses an expired token before it touches the ledger (401
-- `invalid_claims`), so deleting a row can never make that `jti` or nonce
-- acceptable again.

CREATE TABLE sessions.migration_assertion_uses (
  jti text PRIMARY KEY CHECK (length(jti) BETWEEN 1 AND 128),
  nonce text NOT NULL UNIQUE CHECK (nonce ~ '^[A-Za-z0-9_-]{16,128}$'),
  grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  plan_entry_digest text NOT NULL CHECK (plan_entry_digest ~ '^[0-9a-f]{64}$'),
  -- The token's exp plus skew: the earliest moment the row may be purged.
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz NOT NULL DEFAULT now()
);
-- Audit: every upload a grant committed, in order.
CREATE INDEX migration_assertion_uses_grant_idx
  ON sessions.migration_assertion_uses (grant_id, consumed_at);

ALTER TABLE sessions.migration_assertion_uses ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_assertion_uses FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.migration_assertion_uses TO service_role;

-- Retention scans.
CREATE INDEX migration_assertion_uses_expires_idx
  ON sessions.migration_assertion_uses (expires_at);

-- One chunk per (assertion, seq) while the assertion is live.
CREATE TABLE sessions.migration_assertion_chunks (
  jti text NOT NULL CHECK (length(jti) BETWEEN 1 AND 128),
  seq bigint NOT NULL CHECK (seq >= 0),
  start_offset bigint NOT NULL CHECK (start_offset >= 0),
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  expires_at timestamptz NOT NULL,
  reserved_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (jti, seq)
);
CREATE INDEX migration_assertion_chunks_expires_idx
  ON sessions.migration_assertion_chunks (expires_at);

ALTER TABLE sessions.migration_assertion_chunks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_assertion_chunks FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.migration_assertion_chunks TO service_role;
