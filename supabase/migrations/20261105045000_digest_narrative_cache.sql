-- The daily digest narrative cache off Neon (lane F4; registry slot 045000, confirmed by
-- the registry owner). Replaces relayhistory's sessions.daily_digests (Neon 0007), which
-- GET /v1/digest/daily reads and, on a cache miss, WRITES.
--
-- The read API stays read-only: it reads the cache through digest_narrative_read as
-- bulls96_read_api. On a miss or a stale stats hash it asks the service-bound narrative
-- Worker, which holds the model key and generates, then writes through
-- digest_narrative_upsert as bulls96_digest_narrative (its only grant).
--
--   1. sessions.daily_digest_narratives: PK (org_id, day, tz, scope_key), as daily_digests.
--      scope_key is relayhistory's workspace form 'workspace:' || JSON [workspaceId, projectId]
--      (every SP digest is workspace-scoped). Bounded inline text (1..16384 characters).
--      RLS on, no policy, no table grant: reachable only through the functions below.
--   2. sessions.digest_narrative_read(workspace, day, tz, scope_key): SECURITY DEFINER,
--      EXECUTE for bulls96_read_api alone. The tenant comes from
--      session_directory_read_api_scope (20261105034000): the role guard and the per-
--      transaction tenant binding. A scope_key naming another workspace is refused 42501.
--   3. sessions.digest_narrative_upsert(org, workspace, day, tz, scope_key, stats_hash, text,
--      model, generated_at): SECURITY DEFINER, EXECUTE for bulls96_digest_narrative alone,
--      which it checks itself (42501 otherwise). INSERT ... ON CONFLICT DO UPDATE, as
--      relayhistory's upsertCachedNarrative.
--   4. sessions.purge_daily_digest_narratives(org, workspace): owner-only EXECUTE. Upload
--      erasure (C3/#289) must call it in the same transaction, as Neon's set_session_uploads
--      deletes daily_digests rows (0074:235).
--
-- Trust: the writer role can upsert for any tenant. The narrative Worker has no public
-- route and is reachable only by the read Worker's service binding, which passes the
-- verified token's tenant. The role is NOINHERIT, has no members and logs in only from
-- that Worker (Hyperdrive; its password lives in 1Password).
--
-- Roles: never created or altered here. bulls96_read_api (session_read_api_roles.sql) and
-- bulls96_digest_narrative (both by session_read_api_roles.sql) are provisioned before migrate and
-- verified here, fail closed (42704 absent, 55000 elevated).
--
-- Transactional (no CONCURRENTLY, no enum change): migrate.ts wraps it with its ledger row.
-- Code only; applying it to a real database needs Khaliq's explicit OK. Down path:
-- supabase/rollbacks/20261105045000_digest_narrative_cache.down.sql.

DO $roles$
DECLARE
  v_name text;
  r pg_roles%ROWTYPE;
BEGIN
  FOREACH v_name IN ARRAY ARRAY['bulls96_read_api', 'bulls96_digest_narrative'] LOOP
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE = '42704',
        MESSAGE = format('role %s does not exist; this migration does not create it', v_name),
        HINT = 'Run supabase/bootstrap/session_read_api_roles.sql as a privileged role first, then rerun.';
    END IF;
    IF r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolinherit THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('role %s exists elevated', v_name);
    END IF;
  END LOOP;
END
$roles$;

CREATE TABLE sessions.daily_digest_narratives (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 255),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 255),
  day date NOT NULL,
  tz text NOT NULL CHECK (char_length(tz) BETWEEN 1 AND 128),
  scope_key text NOT NULL CHECK (char_length(scope_key) BETWEEN 1 AND 1024),
  stats_hash text NOT NULL CHECK (stats_hash ~ '^[0-9a-f]{64}$'),
  narrative_text text NOT NULL CHECK (char_length(narrative_text) BETWEEN 1 AND 16384),
  narrative_model text CHECK (narrative_model IS NULL OR char_length(narrative_model) BETWEEN 1 AND 200),
  narrative_generated_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, day, tz, scope_key)
);
CREATE INDEX daily_digest_narratives_workspace ON sessions.daily_digest_narratives (org_id, workspace_id);
ALTER TABLE sessions.daily_digest_narratives ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.daily_digest_narratives FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api, bulls96_digest_narrative;

-- The workspace a scope_key names ('workspace:' || JSON [workspaceId, projectId]); NULL if malformed.
CREATE FUNCTION sessions.digest_narrative_scope_workspace(p_scope_key text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, pg_temp AS $f$
DECLARE v jsonb;
BEGIN
  IF p_scope_key IS NULL OR left(p_scope_key, 10) <> 'workspace:' THEN RETURN NULL; END IF;
  BEGIN
    v := substr(p_scope_key, 11)::jsonb;
  EXCEPTION WHEN others THEN RETURN NULL;
  END;
  IF jsonb_typeof(v) <> 'array' OR jsonb_array_length(v) <> 2 OR jsonb_typeof(v -> 0) <> 'string'
     OR jsonb_typeof(v -> 1) NOT IN ('string', 'null') THEN RETURN NULL; END IF;
  RETURN v ->> 0;
END
$f$;

CREATE FUNCTION sessions.digest_narrative_read(p_workspace_id text, p_day date, p_tz text, p_scope_key text)
RETURNS TABLE (stats_hash text, narrative_text text, narrative_model text, narrative_generated_at timestamptz)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $read$
DECLARE v_org text;
BEGIN
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF sessions.digest_narrative_scope_workspace(p_scope_key) IS DISTINCT FROM p_workspace_id THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'digest narrative: scope key names another workspace';
  END IF;
  RETURN QUERY
    SELECT n.stats_hash, n.narrative_text, n.narrative_model, n.narrative_generated_at
      FROM sessions.daily_digest_narratives AS n
     WHERE n.org_id = v_org AND n.workspace_id = p_workspace_id
       AND n.day = p_day AND n.tz = p_tz AND n.scope_key = p_scope_key;
END
$read$;

CREATE FUNCTION sessions.digest_narrative_upsert(
  p_org_id text, p_workspace_id text, p_day date, p_tz text, p_scope_key text,
  p_stats_hash text, p_text text, p_model text, p_generated_at timestamptz
)
RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $upsert$
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'bulls96_digest_narrative' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'digest narrative: caller is not the narrative writer role';
  END IF;
  IF p_org_id IS NULL OR p_workspace_id IS NULL
     OR sessions.digest_narrative_scope_workspace(p_scope_key) IS DISTINCT FROM p_workspace_id THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'digest narrative: tenant and scope key must agree';
  END IF;
  INSERT INTO sessions.daily_digest_narratives AS n
    (org_id, workspace_id, day, tz, scope_key, stats_hash, narrative_text, narrative_model, narrative_generated_at, updated_at)
  VALUES (p_org_id, p_workspace_id, p_day, p_tz, p_scope_key, p_stats_hash, p_text, p_model, p_generated_at, p_generated_at)
  ON CONFLICT (org_id, day, tz, scope_key) DO UPDATE
    SET stats_hash = EXCLUDED.stats_hash, narrative_text = EXCLUDED.narrative_text,
        narrative_model = EXCLUDED.narrative_model, narrative_generated_at = EXCLUDED.narrative_generated_at,
        updated_at = EXCLUDED.updated_at
    WHERE n.workspace_id = EXCLUDED.workspace_id;
END
$upsert$;

CREATE FUNCTION sessions.purge_daily_digest_narratives(p_org_id text, p_workspace_id text)
RETURNS bigint
LANGUAGE sql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $purge$
  WITH d AS (DELETE FROM sessions.daily_digest_narratives WHERE org_id = p_org_id AND workspace_id = p_workspace_id RETURNING 1)
  SELECT count(*) FROM d;
$purge$;

REVOKE ALL ON FUNCTION sessions.digest_narrative_scope_workspace(text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.digest_narrative_read(text, date, text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.digest_narrative_upsert(text, text, date, text, text, text, text, text, timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.purge_daily_digest_narratives(text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.digest_narrative_read(text, date, text, text) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.digest_narrative_upsert(text, text, date, text, text, text, text, text, timestamptz)
  TO bulls96_digest_narrative;
