-- Portable (self-host) search reader contract: the read API's exact, tenant-bound access
-- to the content-free completeness state of 20261105047000 and to the directory
-- identity/epoch/readiness columns that the completeness query joins.
-- Registry slot 20261105048000 (layered-sessions-pipeline; lands after 040000-047000 in
-- numeric order; transactional). PRODUCTION READ-API SURFACE: reviewed by the migration
-- lead (migration-handover-agent) before it lands.
--
-- Grants, all to bulls96_read_api (verified by 20261105034000; never created or altered here):
--   * column SELECT on sessions.search_projection_state: exactly the nine columns the
--     completeness query reads (src/session-pipeline/postgres-search-state.ts partials):
--     backend_id, org_id, workspace_id, source, session_id, canonical_session_id,
--     source_epoch, coverage_from_ms, complete. Not revision, row_count or updated_at.
--   * column SELECT on sessions.session_directory: org_id, workspace_id, harness,
--     session_id, event_projection_epoch, event_projection_ready. No title, summary,
--     search text, user or any other directory column.
-- Policies: two PERMISSIVE FOR SELECT TO bulls96_read_api, tenant-bound to the same
-- transaction-local settings the existing session_index_read_api policy trusts
-- (bulls96_read.org_id / bulls96_read.workspace_id, bound by the read API's TenantRunner
-- from the verified assertion). A setting never set is NULL; after a transaction-local
-- set_config it reads ''; neither matches a row (org_id/workspace_id are never NULL or '').
-- PERMISSIVE policies OR together per role: these name bulls96_read_api only, so they
-- widen nothing for any other role. For bulls96_read_api on session_directory they add
-- direct, column-limited, tenant-bound reads; its existing directory reads stay the
-- definer functions of 20261105034000.
--
-- Verifiers (changed together, both exact and all-or-nothing on this migration's two
-- policies, so they pass identically before it is applied and refuse any partial surface):
--   supabase/bootstrap/session_read_api_role_verify.sql      (production read role)
--   supabase/bootstrap/portable_search_storage_verify.sql    (phase 2 storage admission)
--
-- Code only; nothing here is applied by this repository. It changes the migration-set
-- generation. Down path: supabase/rollbacks/20261105048000_portable_search_reader_contract.down.sql.

DO $prereq$
BEGIN
  IF to_regclass('sessions.search_projection_state') IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search reader contract: 20261105047000 (sessions.search_projection_state) is not applied';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = 'portable search reader contract: role bulls96_read_api does not exist; this migration does not create it';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polname IN ('search_projection_state_read_api', 'session_directory_portable_read_api')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'portable search reader contract: a policy of this migration already exists (unledgered footprint)';
  END IF;
END
$prereq$;

GRANT SELECT (backend_id, org_id, workspace_id, source, session_id, canonical_session_id,
              source_epoch, coverage_from_ms, complete)
  ON sessions.search_projection_state TO bulls96_read_api;
GRANT SELECT (org_id, workspace_id, harness, session_id, event_projection_epoch, event_projection_ready)
  ON sessions.session_directory TO bulls96_read_api;

CREATE POLICY search_projection_state_read_api ON sessions.search_projection_state
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (org_id = current_setting('bulls96_read.org_id', true)
         AND workspace_id = current_setting('bulls96_read.workspace_id', true));
CREATE POLICY session_directory_portable_read_api ON sessions.session_directory
  AS PERMISSIVE FOR SELECT TO bulls96_read_api
  USING (org_id = current_setting('bulls96_read.org_id', true)
         AND workspace_id = current_setting('bulls96_read.workspace_id', true));
