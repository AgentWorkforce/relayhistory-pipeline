-- Per-user session counts for GET /v1/sessions/users (the owner-only Usage "People" table).
-- Code only: applying this to a real database needs Khaliq's explicit OK.
--
-- One bounded aggregate over the logical-session directory, so a caller no longer pages the
-- whole list (limit <= 200) to count sessions per member. It selects exactly the rows
-- sessions.list_session_directory_for (034000) lists for p_last_ts_after / p_last_ts_before:
-- the bound tenant's org and workspace, deleted_at IS NULL (erased and retired sessions are
-- tombstoned there and never count), last_activity_at >= after and < before. Each row counts
-- once for every distinct id in its user_ids (the projector writes user_ids and the
-- session_directory_users companion the list's user= filter matches from the same live
-- pointers), so a session with several users counts once for each.
--
-- Returns, per user: the session count, the latest last_activity_at, and up to 3 project ids
-- by count (ties by id, byte order; null or empty projects skipped) as [{"name","sessions"}]. Users are
-- ordered by count desc, then user id (byte order); at most p_limit + 1 rows, so the caller
-- can tell a truncated answer. No title, summary, session id or other content leaves it.
--
-- Bounded: the scan is the (org_id, workspace_id, last_activity_at) range of
-- session_directory_workspace_recent_idx (20261030010000, WHERE deleted_at IS NULL), and the
-- window is at most 366 UTC days (the read API allows 92). No new index or table.
-- Same tenant-pinned read-role scope as the directory list (42501), SECURITY DEFINER,
-- EXECUTE to bulls96_read_api alone. Errors: 42501 scope; 22023 invalid arguments.
CREATE FUNCTION sessions.session_directory_users_for(
 p_workspace_id text, p_last_ts_after timestamptz, p_last_ts_before timestamptz, p_limit integer
) RETURNS TABLE(user_id text, sessions bigint, last_session_at timestamptz, top_projects jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $users$
DECLARE v_org text;
BEGIN
 IF p_workspace_id IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='session users: workspace is required';
 END IF;
 -- Role guard and tenant pin; refuses (42501) before any value is read.
 v_org := sessions.session_directory_read_api_scope(p_workspace_id);
 IF p_last_ts_after IS NULL OR p_last_ts_before IS NULL OR p_last_ts_after > p_last_ts_before THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='session users: both bounds are required, after first';
 END IF;
 IF (p_last_ts_before AT TIME ZONE 'UTC')::date - (p_last_ts_after AT TIME ZONE 'UTC')::date + 1 > 366 THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='session users: at most 366 UTC days';
 END IF;
 IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
  RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='session users: limit must be 1 to 1000';
 END IF;
 RETURN QUERY
 WITH member AS MATERIALIZED (
  SELECT DISTINCT u.id AS uid, d.harness, d.session_id, d.project_id, d.last_activity_at
    FROM sessions.session_directory AS d
    CROSS JOIN LATERAL unnest(d.user_ids) AS u(id)
   WHERE d.org_id = v_org AND d.workspace_id = p_workspace_id AND d.deleted_at IS NULL
     AND d.last_activity_at >= p_last_ts_after AND d.last_activity_at < p_last_ts_before
     AND u.id IS NOT NULL AND u.id <> ''
 ), per_user AS (
  SELECT m.uid, count(*)::bigint AS n, max(m.last_activity_at) AS last
    FROM member AS m GROUP BY m.uid
 ), page AS (
  SELECT p.uid, p.n, p.last FROM per_user AS p
   ORDER BY p.n DESC, p.uid COLLATE "C"
   LIMIT p_limit + 1
 ), projects AS (
  SELECT m.uid, m.project_id, count(*)::bigint AS n,
         row_number() OVER (PARTITION BY m.uid ORDER BY count(*) DESC, m.project_id COLLATE "C") AS rn
    FROM member AS m
   WHERE m.project_id IS NOT NULL AND m.project_id <> '' AND m.uid IN (SELECT g.uid FROM page AS g)
   GROUP BY m.uid, m.project_id
 )
 SELECT g.uid, g.n, g.last,
        coalesce((SELECT jsonb_agg(jsonb_build_object('name', x.project_id, 'sessions', x.n) ORDER BY x.rn)
                    FROM projects AS x WHERE x.uid = g.uid AND x.rn <= 3), '[]'::jsonb)
   FROM page AS g
  ORDER BY g.n DESC, g.uid COLLATE "C";
END
$users$;

REVOKE ALL ON FUNCTION sessions.session_directory_users_for(text, timestamptz, timestamptz, integer)
 FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_directory_users_for(text, timestamptz, timestamptz, integer)
 TO bulls96_read_api;

-- As 034000: strip EXECUTE from every other grantee present at creation (default privileges).
DO $revoke$
DECLARE v_grantee text; v_fn regprocedure := 'sessions.session_directory_users_for(text, timestamptz, timestamptz, integer)'::regprocedure;
BEGIN
 FOR v_grantee IN SELECT r.rolname FROM pg_proc p
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
  JOIN pg_roles r ON r.oid = a.grantee WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE'
   AND a.grantee <> p.proowner AND r.rolname <> 'bulls96_read_api'
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee); END LOOP;
END
$revoke$;
