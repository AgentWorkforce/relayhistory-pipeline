-- Portable (self-host) search storage: the ledgered, guard-compatible home of the
-- Postgres search index and its content-free completeness state (PR #280 SQL-owner
-- findings H1, M1-M4). Registry slot 20261105047000 (layered-sessions-pipeline).
--
-- Why here and not in an operator-created `search_index` schema:
--   * H1. The portable writer runs on the existing hoopdb_url runtime pool, inside the
--     scope-locked transaction that also writes sessions.session_directory, so the
--     recorded projection epoch is fenced by the directory row lock. The runtime-role
--     guard (src/hoopdb/migrate-guards.ts) accepts schema USAGE only on sessions and
--     public, and only the exact table privileges in RUNTIME_APPLICATION_TABLE_PRIVILEGES.
--     Both tables below are added to that allowlist and to RUNTIME_APPLICATION_RLS_TABLES,
--     so the migrator's own installRuntimeApplicationAcl grants SELECT/INSERT/UPDATE/
--     DELETE and the trusted-backend policy. No manual grant, no guard relaxation, no
--     second connection, no owner credentials.
--   * M1. The objects are created once, in the migrator's transaction, under the ledger,
--     inventory and drift checks, not by a runtime CREATE ... IF NOT EXISTS. ACLs are
--     revoked from PUBLIC and the platform roles and then asserted owner-only, so an
--     owner's global ALTER DEFAULT PRIVILEGES cannot leak a grant (fail closed, 55000).
--     supabase/bootstrap/portable_search_storage_verify.sql re-checks ownership, shape,
--     ACL, RLS/FORCE, triggers and the exact policy set; the Lambda assemblies run it.
--   * M2. The names are fixed. The portable composition refuses SEARCH_INDEX_POSTGRES_SCHEMA.
--   * M3/M4. bulls96_read_api (verified by 20261105034000; never created or altered here)
--     receives column SELECT only, and tenant-bound FOR SELECT policies on both tables and
--     on the session_directory identity/epoch/readiness columns that the completeness
--     query joins. The read API runs both its completeness and its index queries inside
--     the TenantRunner READ ONLY transaction that binds bulls96_read.org_id/workspace_id
--     (session_read_api_role.sql). session_read_api_role_verify.sql accepts exactly these.
--
-- FORCE ROW LEVEL SECURITY: the migration owner has no policy here, so even an owner
-- connection sees and writes nothing (and the Lambda assemblies refuse owner, superuser
-- and BYPASSRLS connections outright, M5).
--
-- Content: transcript_events holds transcript text for the Postgres search backend only.
-- It is a derived, rebuildable index (decision 0002 keeps text out of the directory
-- table itself, which is unchanged). Unused profiles leave it empty.
--
-- Code only; nothing here is applied by this repository. It changes the migration-set
-- generation, so it ships only in a reviewed generation window. Down path:
-- supabase/rollbacks/20261105047000_portable_search_storage.down.sql.

DO $prereq$
BEGIN
  IF to_regprocedure('public.gin_trgm_ops(internal,int2)') IS NULL
     AND NOT EXISTS (SELECT 1 FROM pg_opclass c JOIN pg_namespace n ON n.oid = c.opcnamespace
                      WHERE c.opcname = 'gin_trgm_ops' AND n.nspname = 'public') THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search storage: public.gin_trgm_ops (pg_trgm) is missing';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = 'portable search storage: role bulls96_read_api does not exist; this migration does not create it';
  END IF;
END
$prereq$;

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

CREATE TABLE sessions.search_transcript_events (
  org_id text COLLATE "C" NOT NULL,
  workspace_id text COLLATE "C" NOT NULL,
  ts_ms bigint NOT NULL,
  session_id text COLLATE "C" NOT NULL,
  source text COLLATE "C" NOT NULL,
  machine_id text COLLATE "C" NOT NULL,
  kind text COLLATE "C" NOT NULL,
  event_id text COLLATE "C" NOT NULL,
  type text NOT NULL,
  user_id text NOT NULL,
  actor_name text NOT NULL,
  actor_role text NOT NULL,
  subagent_id text NOT NULL,
  project_id text NOT NULL,
  task_title text NOT NULL,
  task_ref text NOT NULL,
  task_ref_id text NOT NULL,
  tags text[] NOT NULL,
  files_touched text[] NOT NULL,
  tool_name text NOT NULL,
  tool_status text NOT NULL,
  delivery_record_id text NOT NULL,
  content text NOT NULL,
  content_bytes integer NOT NULL,
  content_prefix_folded text COLLATE "C" NOT NULL,
  task_title_folded text COLLATE "C" NOT NULL,
  fingerprint text NOT NULL,
  version bigint NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT search_transcript_events_pkey
    PRIMARY KEY (org_id, workspace_id, ts_ms, session_id, source, machine_id, kind, event_id),
  CONSTRAINT search_transcript_events_tenant CHECK (org_id <> '' AND workspace_id <> '')
);
CREATE INDEX search_transcript_events_session_idx ON sessions.search_transcript_events
  (org_id, workspace_id, session_id, ts_ms, source, machine_id, kind, event_id);
CREATE INDEX search_transcript_events_scope_idx ON sessions.search_transcript_events
  (org_id, workspace_id, source, session_id);
CREATE INDEX search_transcript_events_content_trgm ON sessions.search_transcript_events
  USING gin (content_prefix_folded public.gin_trgm_ops);
CREATE INDEX search_transcript_events_title_trgm ON sessions.search_transcript_events
  USING gin (task_title_folded public.gin_trgm_ops);

ALTER TABLE sessions.search_projection_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.search_projection_state FORCE ROW LEVEL SECURITY;
ALTER TABLE sessions.search_transcript_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.search_transcript_events FORCE ROW LEVEL SECURITY;

-- Owner-only ACL first, whatever default privileges the migration owner carries.
REVOKE ALL ON sessions.search_projection_state, sessions.search_transcript_events
  FROM PUBLIC, anon, authenticated, service_role;
DO $owner_only$
DECLARE
  v_row record;
  v_problems text[] := '{}';
BEGIN
  FOR v_row IN
    SELECT c.oid::regclass::text AS rel, a.grantee, a.privilege_type
      FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
     WHERE c.oid IN ('sessions.search_projection_state'::regclass, 'sessions.search_transcript_events'::regclass)
       AND a.grantee <> c.relowner
  LOOP
    v_problems := array_append(v_problems,
      format('%s: %s granted to %s', v_row.rel, v_row.privilege_type,
             CASE WHEN v_row.grantee = 0 THEN 'PUBLIC' ELSE v_row.grantee::regrole::text END));
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_attribute a
              WHERE a.attrelid IN ('sessions.search_projection_state'::regclass, 'sessions.search_transcript_events'::regclass)
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

-- Reader (bulls96_read_api): exact column SELECT, tenant-bound. No write, no other column.
GRANT SELECT (backend_id, org_id, workspace_id, source, session_id, canonical_session_id,
              source_epoch, coverage_from_ms, complete)
  ON sessions.search_projection_state TO bulls96_read_api;
GRANT SELECT (org_id, workspace_id, ts_ms, session_id, source, machine_id, kind, event_id,
              type, user_id, actor_name, actor_role, subagent_id, project_id, task_title,
              task_ref, task_ref_id, tags, files_touched, tool_name, tool_status,
              delivery_record_id, content, content_prefix_folded, task_title_folded)
  ON sessions.search_transcript_events TO bulls96_read_api;
GRANT SELECT (org_id, workspace_id, harness, session_id, event_projection_epoch, event_projection_ready)
  ON sessions.session_directory TO bulls96_read_api;

CREATE POLICY search_projection_state_read_api ON sessions.search_projection_state
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (org_id = current_setting('bulls96_read.org_id', true)
         AND workspace_id = current_setting('bulls96_read.workspace_id', true));
CREATE POLICY search_transcript_events_read_api ON sessions.search_transcript_events
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (org_id = current_setting('bulls96_read.org_id', true)
         AND workspace_id = current_setting('bulls96_read.workspace_id', true));
CREATE POLICY session_directory_portable_read_api ON sessions.session_directory
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (org_id = current_setting('bulls96_read.org_id', true)
         AND workspace_id = current_setting('bulls96_read.workspace_id', true));
