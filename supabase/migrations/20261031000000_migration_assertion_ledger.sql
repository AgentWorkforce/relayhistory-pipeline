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

CREATE TABLE sessions.migration_assertion_uses (
  jti text PRIMARY KEY CHECK (length(jti) BETWEEN 1 AND 128),
  nonce text NOT NULL UNIQUE CHECK (nonce ~ '^[A-Za-z0-9_-]{16,128}$'),
  grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  consumed_at timestamptz NOT NULL DEFAULT now()
);
-- Audit: every upload a grant committed, in order.
CREATE INDEX migration_assertion_uses_grant_idx
  ON sessions.migration_assertion_uses (grant_id, consumed_at);

ALTER TABLE sessions.migration_assertion_uses ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.migration_assertion_uses FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.migration_assertion_uses TO service_role;
