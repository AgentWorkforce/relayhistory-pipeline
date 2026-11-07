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
-- Readiness RPC (decision A, registry-approved in-place 048 scope amendment):
--   sessions.portable_reader_readiness(p_names text[], p_generation text) RETURNS text.
--   The fixed reader may not SELECT the migrator's ledger or readiness marker; instead
--   this owner-owned SECURITY DEFINER function returns ONE bounded status code:
--   'pass' | 'schema_pending' | 'marker_missing' | 'generation_mismatch' |
--   'validation_pending'. No names, rows, generation or tenant data are returned.
--   Identity guard (no tenant requirement: assembly has none): session_user must be
--   bulls96_read_api AND the `role` setting must be unset or bulls96_read_api, so an owner
--   or other login that SET ROLEs to the reader is refused (42501). Inside SECURITY
--   DEFINER current_user is the owner, so it is deliberately not the identity test.
--   Inputs: 1..512 names, each '(upstream|96bulls):<file>', no NULLs, deduplicated; a
--   64-hex generation; otherwise 22023. VOLATILE: each query takes a fresh snapshot under
--   the caller's READ COMMITTED transaction; the reader gate calls it twice (initial and
--   FINAL), so a concurrent flip across the verifier interval is caught; no whole-pass
--   serialization is claimed. search_path is pinned; every relation is schema-qualified;
--   no dynamic SQL. EXECUTE: owner and bulls96_read_api only (PUBLIC revoked at creation).
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
  IF to_regprocedure('sessions.portable_reader_readiness(text[],text)') IS NOT NULL
     OR EXISTS (SELECT 1 FROM pg_policy WHERE polname IN ('search_projection_state_read_api', 'session_directory_portable_read_api')) THEN
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

CREATE FUNCTION sessions.portable_reader_readiness(p_names text[], p_generation text)
RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $readiness$
DECLARE
  v_names text[];
  v_applied int;
  v_status text;
  v_generation text;
BEGIN
  IF session_user::text <> 'bulls96_read_api'
     OR coalesce(nullif(current_setting('role', true), 'none'), session_user::text) <> 'bulls96_read_api' THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'portable reader readiness: the caller is not the fixed reader on its own login';
  END IF;
  IF p_generation IS NULL OR p_generation !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'portable reader readiness: generation must be 64 hex';
  END IF;
  IF p_names IS NULL OR cardinality(p_names) < 1 OR cardinality(p_names) > 512
     OR array_position(p_names, NULL) IS NOT NULL
     OR EXISTS (SELECT 1 FROM unnest(p_names) AS n(name) WHERE n.name !~ '^(upstream|96bulls):[A-Za-z0-9_.-]{1,200}$') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'portable reader readiness: invalid expected schema names';
  END IF;
  SELECT array_agg(DISTINCT n.name) INTO v_names FROM unnest(p_names) AS n(name);
  SELECT count(*)::int INTO v_applied FROM sessions.__96bulls_migrations AS l WHERE l.name = ANY (v_names);
  IF v_applied <> cardinality(v_names) THEN
    RETURN 'schema_pending';
  END IF;
  SELECT m.status, m.generation INTO v_status, v_generation
    FROM sessions.__96bulls_role_validation AS m WHERE m.name = 'session_directory_search_worker';
  IF NOT FOUND THEN
    RETURN 'marker_missing';
  END IF;
  IF v_generation IS DISTINCT FROM p_generation THEN
    RETURN 'generation_mismatch';
  END IF;
  IF v_status IS DISTINCT FROM 'pass' THEN
    RETURN 'validation_pending';
  END IF;
  RETURN 'pass';
END
$readiness$;
REVOKE ALL ON FUNCTION sessions.portable_reader_readiness(text[], text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.portable_reader_readiness(text[], text) TO bulls96_read_api;
