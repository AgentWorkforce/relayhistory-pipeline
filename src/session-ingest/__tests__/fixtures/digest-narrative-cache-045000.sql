-- Test fixture: the daily digest narrative cache table and its purge function, VERBATIM from sessions-pipeline #299
-- (lane F4) at f430980d9bc7e414d17612ce378e534c3d249602, supabase/migrations/20261105045000_digest_narrative_cache.sql.
-- Only these two objects (no roles/read/upsert functions): the C3 authority must purge through this exact function.
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

CREATE FUNCTION sessions.purge_daily_digest_narratives(p_org_id text, p_workspace_id text)
RETURNS bigint
LANGUAGE sql VOLATILE
SET search_path = pg_catalog, sessions, pg_temp AS $purge$
  WITH d AS (DELETE FROM sessions.daily_digest_narratives WHERE org_id = p_org_id AND workspace_id = p_workspace_id RETURNING 1)
  SELECT count(*) FROM d;
$purge$;
REVOKE ALL ON FUNCTION sessions.purge_daily_digest_narratives(text, text) FROM PUBLIC, anon, authenticated, service_role;
