-- Portable (self-host) search completeness state: the ledgered, guard-compatible home
-- of the content-free projection state that every portable search backend (OpenSearch,
-- or a later-approved Postgres index) needs (PR #280 SQL-owner findings H1, M1).
-- Registry slot 20261105047000 (layered-sessions-pipeline; lands after 040000-046000 in
-- numeric order). The table holds identifiers, epochs, counts and flags only: no
-- transcript content, titles or summaries.
--
-- Deliberately NOT here:
--   * A transcript-content index table. Decision 0002 keeps session content in S3 and
--     says a full-text projection is built from S3, not duplicated into metadata storage;
--     only a search-projection stopgap is allowed. Whether a self-host Postgres content
--     index may live in HoopDb is a separate placement/retention/erasure decision with
--     its own slot. An OpenSearch install therefore gets no content table here.
--   * Any bulls96_read_api grant or policy (on this table or on session_directory), and
--     any change to session_read_api_role_verify.sql: production read-API surface, see
--     below.
--
-- H1. The portable writer runs on the existing hoopdb_url runtime pool, inside the
-- scope-locked transaction that also writes sessions.session_directory, so the recorded
-- epoch is fenced by the directory row lock. The table is added to
-- RUNTIME_APPLICATION_TABLE_PRIVILEGES and RUNTIME_APPLICATION_RLS_TABLES
-- (src/hoopdb/migrate-guards.ts), so the migrator's own installRuntimeApplicationAcl
-- grants SELECT/INSERT/UPDATE/DELETE and the trusted-backend policy. No manual grant, no
-- guard relaxation, no second connection, no owner credentials.
-- supabase/bootstrap/portable_search_storage_verify.sql re-checks the exact admission.
-- M1. Created once in the migrator's transaction under the ledger, inventory and drift
-- checks. The ACL is revoked from PUBLIC and the platform roles and then asserted
-- owner-only, so an owner's ALTER DEFAULT PRIVILEGES cannot leak a grant (55000).
-- No reader surface. Granting bulls96_read_api anything on a sessions table makes the
-- production supabase/bootstrap/session_read_api_role_verify.sql fail ("SELECT on ...",
-- "unapproved policy"), so every reader grant/policy (this table and the
-- session_directory columns) ships together with that verifier's exact extension in a
-- separate, not-yet-allocated slot, reviewed by the migration lead. Until then only the runtime
-- writer can reach this table; the read API's completeness check fails closed.
-- FORCE ROW LEVEL SECURITY: the owner has no policy, so an owner connection sees and
-- writes nothing.
--
-- Effect per mode: in selfhosted, cloudsql, supabase and rds alike this creates one empty
-- table and the runtime grant; no service reads or writes it unless a reviewed portable
-- composition is explicitly selected (default-off). It changes the migration-set
-- generation, so it ships only in a reviewed generation window. Down path:
-- supabase/rollbacks/20261105047000_portable_search_storage.down.sql.

CREATE TABLE sessions.search_projection_state (
  backend_id text NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  canonical_session_id text NOT NULL,
  source_epoch bigint NOT NULL,
  coverage_from_ms bigint NOT NULL,
  revision bigint NOT NULL,
  row_count bigint NOT NULL,
  complete boolean NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT search_projection_state_pkey PRIMARY KEY (backend_id, org_id, workspace_id, source, session_id),
  CONSTRAINT search_projection_state_counts CHECK (revision >= 0 AND row_count >= 0 AND coverage_from_ms >= 0),
  CONSTRAINT search_projection_state_ids CHECK (backend_id ~ '^[a-zA-Z0-9._-]{1,128}$' AND org_id <> '' AND workspace_id <> '')
);

ALTER TABLE sessions.search_projection_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.search_projection_state FORCE ROW LEVEL SECURITY;

-- Owner-only ACL first, whatever default privileges the migration owner carries.
REVOKE ALL ON sessions.search_projection_state
  FROM PUBLIC, anon, authenticated, service_role;
DO $owner_only$
DECLARE
  v_row record;
  v_problems text[] := '{}';
BEGIN
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel, a.grantee, a.privilege_type
      FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
     WHERE c.oid = 'sessions.search_projection_state'::regclass
       AND a.grantee <> c.relowner
  LOOP
    v_problems := array_append(v_problems,
      format('%s: %s granted to %s', v_row.rel, v_row.privilege_type,
             CASE WHEN v_row.grantee = 0 THEN 'PUBLIC' ELSE v_row.grantee::regrole::text END));
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_attribute a
              WHERE a.attrelid = 'sessions.search_projection_state'::regclass
                AND a.attnum > 0 AND a.attacl IS NOT NULL) THEN
    v_problems := array_append(v_problems, 'a column ACL was inherited'::text);
  END IF;
  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search storage: inherited default privileges: ' || array_to_string(v_problems, '; '),
      HINT = 'Remove the migration owner''s ALTER DEFAULT PRIVILEGES for tables, then rerun. Nothing was committed.';
  END IF;
END
$owner_only$;
