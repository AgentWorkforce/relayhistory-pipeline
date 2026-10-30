-- Fast, CDC-safe lookup of sessions by their delivered root agent name.
--
-- PostgreSQL 17 logical replication does not publish generated-column values.
-- Keep the normalized name and the session's event recency as ordinary columns,
-- maintained on the Neon source. The table remains one row per catalog session
-- key: child-agent labels in session_relationships are deliberately not session
-- identities and do not belong here.
--
-- This migration installs maintenance only. Existing rows are filled in bounded,
-- resumable batches by scripts/rollout-session-agent-lookup.mjs; the migration
-- never scans or rewrites the table in one transaction.

SELECT set_config('relayhistory.migration_saved_lock_timeout', current_setting('lock_timeout'), true),
       set_config('lock_timeout', '5s', true);

ALTER TABLE sessions.session_catalog_agents
  ADD COLUMN agent_name_normalized text,
  ADD COLUMN last_ts timestamptz;

-- Every new/re-delivered catalog row has a replication-safe normalized value.
-- On insert (or an identity move), seed recency from the maintained rollup. A
-- concurrent rollup writer either finishes first and is visible here, or updates
-- this row after waiting for this transaction's row lock.
CREATE FUNCTION sessions.prepare_session_catalog_agent_lookup()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $prepare$
BEGIN
  NEW.agent_name_normalized := lower(NEW.agent_name);
  IF TG_OP = 'INSERT'
     OR (NEW.org_id, NEW.workspace_id, NEW.source, NEW.session_id)
        IS DISTINCT FROM (OLD.org_id, OLD.workspace_id, OLD.source, OLD.session_id) THEN
    SELECT max(r.last_ts) INTO NEW.last_ts
      FROM sessions.session_rollups AS r
     WHERE r.org_id = NEW.org_id AND r.workspace_id = NEW.workspace_id
       AND r.source = NEW.source AND r.session_id = NEW.session_id;
  END IF;
  RETURN NEW;
END
$prepare$;

CREATE TRIGGER session_catalog_agents_lookup_prepare
  BEFORE INSERT OR UPDATE ON sessions.session_catalog_agents
  FOR EACH ROW EXECUTE FUNCTION sessions.prepare_session_catalog_agent_lookup();

-- Refresh exactly the session keys changed by session_rollups. Rollup writes
-- already hold session_rollup_lock_key(org, session), so inserts and invalidating
-- updates/deletes serialize with the existing rebuild/backfill machinery.
CREATE FUNCTION sessions.refresh_session_catalog_agent_recency(p_keys jsonb)
RETURNS void LANGUAGE sql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh$
  UPDATE sessions.session_catalog_agents AS a
     SET last_ts = x.last_ts
    FROM (
      SELECT k.org_id, k.workspace_id, k.source, k.session_id, max(r.last_ts) AS last_ts
        FROM jsonb_to_recordset(p_keys) AS k(
          org_id text, workspace_id text, source text, session_id text)
        LEFT JOIN sessions.session_rollups AS r
          ON r.org_id = k.org_id AND r.workspace_id = k.workspace_id
         AND r.source = k.source AND r.session_id = k.session_id
       GROUP BY k.org_id, k.workspace_id, k.source, k.session_id
    ) AS x
   WHERE a.org_id = x.org_id AND a.workspace_id = x.workspace_id
     AND a.source = x.source AND a.session_id = x.session_id
     AND a.last_ts IS DISTINCT FROM x.last_ts
$refresh$;

CREATE FUNCTION sessions.refresh_inserted_session_catalog_agent_recency()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh_inserted$
BEGIN
  PERFORM sessions.refresh_session_catalog_agent_recency((
    SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
      'org_id', org_id, 'workspace_id', workspace_id,
      'source', source, 'session_id', session_id)), '[]'::jsonb)
      FROM new_rows));
  RETURN NULL;
END
$refresh_inserted$;

CREATE FUNCTION sessions.refresh_deleted_session_catalog_agent_recency()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh_deleted$
BEGIN
  PERFORM sessions.refresh_session_catalog_agent_recency((
    SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
      'org_id', org_id, 'workspace_id', workspace_id,
      'source', source, 'session_id', session_id)), '[]'::jsonb)
      FROM old_rows));
  RETURN NULL;
END
$refresh_deleted$;

CREATE FUNCTION sessions.refresh_updated_session_catalog_agent_recency()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $refresh_updated$
BEGIN
  PERFORM sessions.refresh_session_catalog_agent_recency((
    SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object(
      'org_id', org_id, 'workspace_id', workspace_id,
      'source', source, 'session_id', session_id)), '[]'::jsonb)
      FROM (SELECT org_id, workspace_id, source, session_id FROM old_rows
            UNION
            SELECT org_id, workspace_id, source, session_id FROM new_rows) AS changed));
  RETURN NULL;
END
$refresh_updated$;

CREATE TRIGGER session_rollups_agent_recency_insert
  AFTER INSERT ON sessions.session_rollups REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.refresh_inserted_session_catalog_agent_recency();
CREATE TRIGGER session_rollups_agent_recency_delete
  AFTER DELETE ON sessions.session_rollups REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.refresh_deleted_session_catalog_agent_recency();
CREATE TRIGGER session_rollups_agent_recency_update
  AFTER UPDATE ON sessions.session_rollups REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.refresh_updated_session_catalog_agent_recency();

-- A catalog deletion invalidates the name projection. 0058 deliberately left
-- stale rows and made reads join session_catalog; CDC consumers cannot safely
-- reproduce that implicit join, so remove the source row in the same transaction.
CREATE FUNCTION sessions.delete_removed_session_catalog_agents()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $delete_removed$
BEGIN
  DELETE FROM sessions.session_catalog_agents AS a
   USING old_rows AS o
   WHERE a.org_id = o.org_id AND a.workspace_id = o.workspace_id
     AND a.source = o.source AND a.session_id = o.session_id;
  RETURN NULL;
END
$delete_removed$;

CREATE TRIGGER session_catalog_agent_delete
  AFTER DELETE ON sessions.session_catalog REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.delete_removed_session_catalog_agents();

CREATE TABLE sessions.session_catalog_agent_lookup_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  cursor_org_id text,
  cursor_workspace_id text,
  cursor_source text,
  cursor_session_id text,
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.session_catalog_agent_lookup_rollout (completed_at)
  SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM sessions.session_catalog_agents) THEN now() END;

CREATE FUNCTION sessions.session_catalog_agent_lookup_backfill_step(p_batch integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp
SET plan_cache_mode = force_custom_plan AS $backfill$
DECLARE
  state sessions.session_catalog_agent_lookup_rollout%ROWTYPE;
  orgs text[];
  workspaces text[];
  sources text[];
  ids text[];
  found_count integer;
BEGIN
  IF p_batch < 1 OR p_batch > 1000 THEN
    RAISE EXCEPTION 'session agent lookup batch must be between 1 and 1000'
      USING ERRCODE = '22023';
  END IF;
  SELECT * INTO state FROM sessions.session_catalog_agent_lookup_rollout FOR UPDATE;
  IF state.completed_at IS NOT NULL THEN RETURN 0; END IF;

  SELECT array_agg(a.org_id ORDER BY a.org_id, a.workspace_id, a.source, a.session_id),
         array_agg(a.workspace_id ORDER BY a.org_id, a.workspace_id, a.source, a.session_id),
         array_agg(a.source ORDER BY a.org_id, a.workspace_id, a.source, a.session_id),
         array_agg(a.session_id ORDER BY a.org_id, a.workspace_id, a.source, a.session_id)
    INTO orgs, workspaces, sources, ids
    FROM (SELECT org_id, workspace_id, source, session_id
            FROM sessions.session_catalog_agents
           WHERE state.cursor_org_id IS NULL
              OR (org_id, workspace_id, source, session_id) >
                 (state.cursor_org_id, state.cursor_workspace_id, state.cursor_source, state.cursor_session_id)
           ORDER BY org_id, workspace_id, source, session_id
           LIMIT p_batch) AS a;
  found_count := coalesce(array_length(orgs, 1), 0);

  IF found_count > 0 THEN
    -- Yield the whole ordered batch rather than skipping a busy row behind the
    -- cursor; the operator retries after its catalog delivery commits.
    PERFORM 1
      FROM sessions.session_catalog_agents AS a
      JOIN unnest(orgs, workspaces, sources, ids) AS k(org_id, workspace_id, source, session_id)
        ON a.org_id = k.org_id AND a.workspace_id = k.workspace_id
       AND a.source = k.source AND a.session_id = k.session_id
       FOR UPDATE OF a NOWAIT;

    -- Remove 0058-era orphans before this table becomes independently safe to publish.
    DELETE FROM sessions.session_catalog_agents AS a
     USING unnest(orgs, workspaces, sources, ids) AS k(org_id, workspace_id, source, session_id)
     WHERE a.org_id = k.org_id AND a.workspace_id = k.workspace_id
       AND a.source = k.source AND a.session_id = k.session_id
       AND NOT EXISTS (SELECT 1 FROM sessions.session_catalog AS c
                        WHERE c.org_id = a.org_id AND c.workspace_id = a.workspace_id
                          AND c.source = a.source AND c.session_id = a.session_id);

    UPDATE sessions.session_catalog_agents AS a
       SET agent_name_normalized = lower(a.agent_name),
           last_ts = (SELECT max(r.last_ts) FROM sessions.session_rollups AS r
                       WHERE r.org_id = a.org_id AND r.workspace_id = a.workspace_id
                         AND r.source = a.source AND r.session_id = a.session_id)
      FROM unnest(orgs, workspaces, sources, ids) AS k(org_id, workspace_id, source, session_id)
     WHERE a.org_id = k.org_id AND a.workspace_id = k.workspace_id
       AND a.source = k.source AND a.session_id = k.session_id;
  END IF;

  UPDATE sessions.session_catalog_agent_lookup_rollout SET
    cursor_org_id = coalesce(orgs[found_count], cursor_org_id),
    cursor_workspace_id = coalesce(workspaces[found_count], cursor_workspace_id),
    cursor_source = coalesce(sources[found_count], cursor_source),
    cursor_session_id = coalesce(ids[found_count], cursor_session_id),
    completed_at = CASE WHEN found_count < p_batch THEN now() END,
    updated_at = now();
  RETURN found_count;
END
$backfill$;
REVOKE ALL ON FUNCTION sessions.session_catalog_agent_lookup_backfill_step(integer) FROM PUBLIC;

SELECT set_config('lock_timeout', COALESCE(NULLIF(
  current_setting('relayhistory.migration_saved_lock_timeout', true), ''), current_setting('lock_timeout')), true);
