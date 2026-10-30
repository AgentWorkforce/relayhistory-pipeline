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
-- Retention (no cron, nothing scheduled; a purge is a deliberate operator call):
--   * Keep every row until its grant is revoked or its export is complete,
--     whichever comes first. Rows are never removed during an export window.
--   * Never remove a row before `expires_at`: the token's `exp` plus the
--     ingest's 5 second migration clock skew. After `expires_at` no verifier
--     accepts the token (it is refused as expired, 401 `invalid_claims`), so
--     removing the row cannot make that `jti` or nonce acceptable again.
--   * `sessions.purge_migration_assertion_uses(grant_id)` is the only purge.
--     The caller asserts that the grant is revoked or its export complete; the
--     function itself still keeps every row whose `expires_at` has not passed
--     by `now()` plus a 5 minute margin for database/ingest clock drift.

CREATE TABLE sessions.migration_assertion_uses (
  jti text PRIMARY KEY CHECK (length(jti) BETWEEN 1 AND 128),
  nonce text NOT NULL UNIQUE CHECK (nonce ~ '^[A-Za-z0-9_-]{16,128}$'),
  grant_id text NOT NULL CHECK (length(grant_id) BETWEEN 1 AND 128),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  plan_digest text NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
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

-- The only purge (see Retention above). Returns the number of rows removed.
CREATE FUNCTION sessions.purge_migration_assertion_uses(p_grant_id text)
RETURNS bigint
LANGUAGE sql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $$
  WITH purged AS (
    DELETE FROM sessions.migration_assertion_uses
     WHERE grant_id = p_grant_id
       AND expires_at < now() - interval '5 minutes'
    RETURNING 1
  )
  SELECT count(*) FROM purged
$$;
REVOKE ALL ON FUNCTION sessions.purge_migration_assertion_uses(text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION sessions.purge_migration_assertion_uses(text)
  TO service_role;
