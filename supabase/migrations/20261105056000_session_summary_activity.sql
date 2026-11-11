-- Daily counts of ready native session summaries, for GET /v1/sessions/summaries/activity.
-- Counts only: no ids, titles or text leave the function. The source is the one
-- sessions.session_brief_summary_for (044100) serves to GET /v1/sessions/:id/summary:
-- sessions.session_briefs rows that are ready, native (never a Neon backfill), carry a
-- description and model, and are fresh against the live physical pointers. A summary
-- counts on the UTC day of its generated_at (the latest generation; a regenerated brief
-- moves to its new day), once per logical session. Logical sessions the per-session read
-- refuses (more than one live physical alias, divergent canonical ids, history scope still
-- needing qualified metadata) are not counted.
--
-- Bounded: the brief scan uses the session_briefs primary key prefix (org_id,
-- workspace_id) and filters generated_at to [p_since, p_until]; every per-brief lookup is
-- a primary-key prefix probe of session_index / ingest_history_scope_current, and the
-- alias check is one pass over the workspace's live pointers. Same
-- tenant-pinned read-role scope as the directory activity; no table grants.
CREATE FUNCTION sessions.session_summary_activity_for(
 p_workspace_id text, p_since timestamptz, p_until timestamptz
) RETURNS TABLE(day date, total bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $summary_activity$
DECLARE v_org text; v_live boolean;
BEGIN
 IF p_workspace_id IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='summary activity: workspace is required';
 END IF;
 v_org := sessions.session_directory_read_api_scope(p_workspace_id);
 IF p_since IS NULL OR p_until IS NULL OR p_until < p_since THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='summary activity: since and until are required, since first';
 END IF;
 IF (p_until AT TIME ZONE 'UTC')::date - (p_since AT TIME ZONE 'UTC')::date + 1 > 366 THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='summary activity: at most 366 UTC days';
 END IF;
 v_live := sessions.ingest_history_live();
 RETURN QUERY
 WITH briefs AS MATERIALIZED (
  SELECT b.harness, b.session_id, b.generated_at, b.brief_source_revisions
  FROM sessions.session_briefs b
  WHERE b.org_id=v_org AND b.workspace_id=p_workspace_id
   AND b.generated_at >= p_since AND b.generated_at <= p_until
   AND b.status='ready' AND b.origin='native'
   AND b.engine_version NOT LIKE 'neon-backfill:%'
   AND b.description IS NOT NULL AND btrim(b.description)<>''
   AND b.model IS NOT NULL AND btrim(b.model)<>''
   AND b.brief_source_revisions IS NOT NULL
 ), fresh AS (
  SELECT b.harness, b.session_id, b.generated_at, l.logical
  FROM briefs b CROSS JOIN LATERAL (
   SELECT min(coalesce(p.canonical_session_id,p.session_id)) AS logical,
    count(DISTINCT coalesce(p.canonical_session_id,p.session_id)) AS logicals,
    jsonb_object_agg(p.machine_id,p.revision) FILTER (WHERE p.manifest_key IS NOT NULL) AS revisions
   FROM sessions.session_index p
   WHERE p.org_id=v_org AND p.workspace_id=p_workspace_id AND p.harness=b.harness
    AND p.session_id=b.session_id AND p.deleted_at IS NULL) l
  WHERE l.logical IS NOT NULL AND l.logicals=1
   AND l.revisions IS NOT DISTINCT FROM b.brief_source_revisions
 ), aliased AS (
  -- A logical session with more than one live physical alias: the per-session read
  -- refuses it (0A000), so it is not a ready summary. One pass over the workspace's
  -- live pointers, semi-joined to the in-range logical ids.
  SELECT q.harness, coalesce(q.canonical_session_id,q.session_id) AS logical
  FROM sessions.session_index q
  WHERE q.org_id=v_org AND q.workspace_id=p_workspace_id AND q.deleted_at IS NULL
   AND (q.canonical_session_id IN (SELECT f.logical FROM fresh f) OR q.session_id IN (SELECT f.logical FROM fresh f))
  GROUP BY 1,2 HAVING count(DISTINCT q.session_id)>1
 )
 SELECT (f.generated_at AT TIME ZONE 'UTC')::date AS day, count(DISTINCT (f.harness, f.logical))::bigint AS total
 FROM fresh f
 WHERE NOT EXISTS(SELECT 1 FROM aliased a WHERE a.harness=f.harness AND a.logical=f.logical)
  AND NOT (v_live AND EXISTS(
  SELECT 1 FROM sessions.ingest_history_scope_current h
  WHERE h.org_id=v_org AND h.workspace_id=p_workspace_id AND h.harness=f.harness
   AND h.session_id IN (f.session_id, f.logical)))
 GROUP BY 1 ORDER BY 1;
END
$summary_activity$;
REVOKE ALL ON FUNCTION sessions.session_summary_activity_for(text,timestamptz,timestamptz)
 FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.session_summary_activity_for(text,timestamptz,timestamptz) TO bulls96_read_api;
DO $revoke$
DECLARE v_grantee text; v_fn regprocedure := 'sessions.session_summary_activity_for(text,timestamptz,timestamptz)'::regprocedure;
BEGIN
 FOR v_grantee IN SELECT r.rolname FROM pg_proc p
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
  JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=v_fn AND a.privilege_type='EXECUTE'
   AND a.grantee<>p.proowner AND r.rolname<>'bulls96_read_api'
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',v_fn,v_grantee); END LOOP;
END
$revoke$;
