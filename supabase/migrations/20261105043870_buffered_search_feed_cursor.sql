-- Buffered search feed cursor and RPCs (slot 043870, assigned by the registry keeper).
-- Materialized from docs/proposals/buffered-search-feed.draft.sql (#339); function bodies unchanged
-- except the search_path, which follows 20261105043860.
--
-- The session pipeline's buffered-stage search feed (INGEST_BUFFERED_SEARCH_FEED, default off;
-- src/session-pipeline/buffered-search-feed.ts) feeds each accepted buffered stage's RETAINED lines
-- into ClickHouse transcript_events. It needs, as its own login buffered_search_feed_runtime, and nothing else:
--
--   sessions.buffered_search_feed_cursors   named keyset cursors (one per backfill/tail). RLS on; no
--                                           role but the owner holds any privilege on it.
--   sessions.buffered_search_feed_page(p)   keyset page of acknowledged, live, ACTIVE buffered stages
--                                           after a named cursor. No transcript bytes, no retention
--                                           decision, no locks. Creates the cursor on first use; a
--                                           cursor is bound to its `since` (22023 on a mismatch).
--   sessions.buffered_search_feed_stage(p)  one stage's indexability plus the ordinals retention keeps
--                                           now. The retention decision is sessions.ingest_buffer_gc_ordinals,
--                                           the exact authority the buffered GC and the history publisher
--                                           use: whatever GC would erase is never indexed. One short
--                                           transaction per call (the lazy branch holds the receipt seat
--                                           clock FOR SHARE, as one GC claim does).
--   sessions.buffered_search_feed_advance(p) moves a cursor forward only (monotonic), after ClickHouse writes.
--
-- Fail closed. Status is 'live' only when the stage is its batch's active outbox stage, acknowledged,
-- cleanup_state 'live', the batch is committed + buffered + staged, no GC lease or replacement is in
-- flight, and the 20261105041000 privacy authority is not fenced. A retention refusal (55000 from the
-- GC authority: malformed or missing ACK decisions) yields status 'refused' and retained [].
-- 40001/55P03/57014 propagate so the caller retries. Malformed input is 22023.
--
-- Locks. page and stage take no row lock (stage's GC-authority call may hold the receipt seat clock
-- FOR SHARE for its own short transaction); advance updates one cursor row. Every function carries
-- lock_timeout 2s; the migration itself runs under lock_timeout 5s.
--
-- Schema version. Applying this migration changes the migration-set generation()
-- (src/hoopdb/migrate.ts); see 20261105043700's note on the buffered Worker's captured generation.
--
-- Roles. Never creates or alters a role. The feed's runtime role buffered_search_feed_runtime is
-- provisioned NOLOGIN by supabase/bootstrap/session_ingest_roles.sql (and the cloudsql/rds role
-- bootstraps); LOGIN only by the operator step supabase/bootstrap/buffered_search_feed_runtime_role.sql,
-- which needs Khaliq's explicit OK. Fail closed, as 20261105043850 does for its installer: 42704 if the
-- role is absent, 55000 if it is elevated, a member of another role, or has any member other than
-- Supabase's PG16+ creator edge. Grants no table privilege; USAGE on schema sessions and EXECUTE on the
-- three feed functions to buffered_search_feed_runtime only (src/hoopdb/sessions-function-acl.json
-- +3 entries, lane scripts/sessions-function-boundary/lanes/buffered-search-feed.ts; phase 2 verifier
-- supabase/bootstrap/buffered_search_feed_runtime_verify.sql). service_role holds nothing here.
-- Every function admits only the genuine login: session_user is buffered_search_feed_runtime AND
-- current_setting('role') is 'none' (the 20261105043860 guard), else 42501. current_user cannot be
-- pinned: inside SECURITY DEFINER it is the owner. The backend identity (pg_stat_activity usename,
-- system_user) is not checked here, as the acquisition RPCs do not; the feeder asserts it client-side
-- before arming (src/session-pipeline/buffered-feed-login.ts). The functions run as their
-- owner, which must also own the ledger tables they read and the owner-only helpers they call
-- (ingest_buffer_gc_ordinals, ingest_exact_keys, ingest_bounded).
--
-- Rollout. Must be applied (with Khaliq's explicit apply OK) BEFORE INGEST_BUFFERED_SEARCH_FEED is
-- set to on; without it every feed call fails and the feed indexes nothing. The ClickHouse columns
-- (docs/proposals/buffered-search-feed.clickhouse.draft.sql) and the read gate
-- (READ_API_BUFFERED_SEARCH_GATE, 20261105043860) are separate prerequisites of arming the feed.
-- Code only; nothing here is applied, deployed, enabled or granted by this repository. Down path:
-- supabase/rollbacks/20261105043870_buffered_search_feed_cursor.down.sql.
SET LOCAL lock_timeout='5s';

DO $buffered_search_feed_creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relname IN('ingest_stage_intents','ingest_buffer_outbox','ingest_batches',
    'ingest_privacy_authority')
    AND c.relowner<>me)
  OR (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='sessions' AND c.relkind='r' AND c.relname IN('ingest_stage_intents','ingest_buffer_outbox',
    'ingest_batches','ingest_privacy_authority'))<>4
  OR (SELECT count(*) FROM pg_proc p
   WHERE p.oid IN(to_regprocedure('sessions.ingest_buffer_gc_ordinals(uuid)'),
                  to_regprocedure('sessions.ingest_exact_keys(jsonb, text[])'),
                  to_regprocedure('sessions.ingest_bounded(jsonb, integer, integer)'))
     AND p.proowner=me)<>3
  OR to_regclass('sessions.buffered_search_feed_cursors') IS NOT NULL
  OR to_regprocedure('sessions.buffered_search_feed_page(jsonb)') IS NOT NULL
  OR to_regprocedure('sessions.buffered_search_feed_stage(jsonb)') IS NOT NULL
  OR to_regprocedure('sessions.buffered_search_feed_advance(jsonb)') IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed: requires original creator, the buffered ledger and 20261105041000';END IF;
END $buffered_search_feed_creator$;

-- The runtime role, mirroring 20261105043850's $acquisition_role$ exactly: absent is 42704; elevated, a
-- member of another role, or any inbound member is 55000, before any grant. Another login that is a member
-- OF the role could SET ROLE to it; the functions' session_user guard refuses that too, and this keeps the
-- grant set from ever being reachable through membership. The only tolerated member is Supabase's PG16+
-- creator edge (postgres, granted by superuser supabase_admin, ADMIN TRUE, INHERIT FALSE, SET FALSE: the
-- verifier's predicate). Before PG16 every member is refused.
DO $buffered_search_feed_role$
DECLARE r pg_roles%ROWTYPE; m record;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='buffered_search_feed_runtime';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='42704',MESSAGE='buffered search feed: role buffered_search_feed_runtime absent; run the role bootstrap first';END IF;
 IF r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
  OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed: role buffered_search_feed_runtime exists elevated or is a member';END IF;
 IF current_setting('server_version_num')::int>=160000 THEN
  FOR m IN EXECUTE 'SELECT u.rolname,g.rolname AS grantor,g.rolsuper AS grantor_super,a.admin_option,a.inherit_option,a.set_option FROM pg_auth_members a JOIN pg_roles u ON u.oid=a.member JOIN pg_roles g ON g.oid=a.grantor WHERE a.roleid=$1' USING r.oid LOOP
   IF m.rolname='postgres' AND m.grantor='supabase_admin' AND m.grantor_super AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option THEN NULL;
   ELSE RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed: role buffered_search_feed_runtime has an unexpected member';END IF;
  END LOOP;
 ELSIF EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid=r.oid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed: role buffered_search_feed_runtime has an unexpected member';END IF;
END $buffered_search_feed_role$;

CREATE TABLE sessions.buffered_search_feed_cursors(
 name text PRIMARY KEY CHECK(name~'^[a-z][a-z0-9-]{0,62}$'),
 since timestamptz NOT NULL,
 after_created_at timestamptz,
 after_stage_token uuid,
 stages_indexed bigint NOT NULL DEFAULT 0 CHECK(stages_indexed>=0),
 stages_skipped bigint NOT NULL DEFAULT 0 CHECK(stages_skipped>=0),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK((after_created_at IS NULL)=(after_stage_token IS NULL))
);
ALTER TABLE sessions.buffered_search_feed_cursors ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.buffered_search_feed_cursors
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder, bulls96_read_api,
       buffered_search_feed_runtime;
-- Default privileges may have granted the new table to another role at creation: strip every
-- non-owner grantee. The feed reaches it only through the SECURITY DEFINER functions below.
DO $cursor_acl$
DECLARE v_grantee text;
BEGIN
  FOR v_grantee IN
    SELECT DISTINCT r.rolname
      FROM pg_class AS c
      CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) AS a
      JOIN pg_roles AS r ON r.oid = a.grantee
     WHERE c.oid = 'sessions.buffered_search_feed_cursors'::regclass AND a.grantee <> c.relowner
  LOOP
    EXECUTE format('REVOKE ALL ON sessions.buffered_search_feed_cursors FROM %I', v_grantee);
  END LOOP;
END
$cursor_acl$;

CREATE FUNCTION sessions.buffered_search_feed_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp SET lock_timeout='2s' AS $page$
DECLARE v_name text;v_since timestamptz;v_limit int;c sessions.buffered_search_feed_cursors%ROWTYPE;v_rows jsonb;
BEGIN
 -- Admission (the 20261105043860 guard): only the genuine buffered_search_feed_runtime login, no SET ROLE.
 IF session_user::text IS DISTINCT FROM 'buffered_search_feed_runtime' OR coalesce(current_setting('role', true), 'none') <> 'none' THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='buffered search feed: caller is not the buffered_search_feed_runtime login';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['cursor','since','limit']);
 IF jsonb_typeof(p->'cursor') IS DISTINCT FROM 'string' OR p->>'cursor'!~'^[a-z][a-z0-9-]{0,62}$'
  OR jsonb_typeof(p->'since') IS DISTINCT FROM 'string' OR p->>'since'!~'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: cursor or since malformed';END IF;
 v_name:=p->>'cursor';v_since:=(p->>'since')::timestamptz;v_limit:=sessions.ingest_bounded(p->'limit',1,200);
 INSERT INTO sessions.buffered_search_feed_cursors(name,since) VALUES(v_name,v_since) ON CONFLICT(name) DO NOTHING;
 SELECT * INTO STRICT c FROM sessions.buffered_search_feed_cursors WHERE name=v_name;
 -- A cursor is bound to its lower bound: a different `since` is a different backfill, never a silent rewind.
 IF c.since<>v_since THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: cursor since mismatch';END IF;
 -- ingest_stage_intents_cleanup_idx(cleanup_state,created_at,stage_token) serves this keyset.
 SELECT coalesce(jsonb_agg(jsonb_build_object(
   'stageToken',s.stage_token,
   'createdAt',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'tenant',jsonb_build_object('orgId',s.org_id,'workspaceId',s.workspace_id,'userId',s.user_id),
   'originId',s.origin_id,'batchId',s.batch_id) ORDER BY s.created_at,s.stage_token),'[]')
  INTO v_rows FROM (
   SELECT s.* FROM sessions.ingest_stage_intents s
    WHERE s.cleanup_state='live' AND s.receipt_state='acknowledged' AND s.created_at>=c.since
      AND (c.after_created_at IS NULL OR (s.created_at,s.stage_token)>(c.after_created_at,c.after_stage_token))
      AND EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=s.org_id AND o.workspace_id=s.workspace_id
       AND o.origin_id=s.origin_id AND o.batch_id=s.batch_id AND o.state='buffered' AND o.active_stage_token=s.stage_token)
    ORDER BY s.created_at,s.stage_token LIMIT v_limit) s;
 RETURN jsonb_build_object('cursor',v_name,'since',to_char(c.since AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'after',CASE WHEN c.after_stage_token IS NULL THEN NULL ELSE jsonb_build_object(
   'createdAt',to_char(c.after_created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'stageToken',c.after_stage_token) END,
  'stages',v_rows);
END $page$;

CREATE FUNCTION sessions.buffered_search_feed_stage(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp SET lock_timeout='2s' AS $stage$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;act sessions.ingest_stage_intents%ROWTYPE;o sessions.ingest_buffer_outbox%ROWTYPE;
 v_status text;v_reason text;v_retained jsonb:='[]';v_batch boolean;v_fenced boolean;
BEGIN
 -- Admission (the 20261105043860 guard): only the genuine buffered_search_feed_runtime login, no SET ROLE.
 IF session_user::text IS DISTINCT FROM 'buffered_search_feed_runtime' OR coalesce(current_setting('role', true), 'none') <> 'none' THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='buffered search feed: caller is not the buffered_search_feed_runtime login';END IF;
 IF p IS NULL OR jsonb_typeof(p)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(p))<>1 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: exactly one of stageToken, stageKey';END IF;
 IF p ? 'stageToken' THEN
  IF jsonb_typeof(p->'stageToken') IS DISTINCT FROM 'string' OR p->>'stageToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: stageToken malformed';END IF;
  SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=(p->>'stageToken')::uuid;
 ELSIF p ? 'stageKey' THEN
  IF jsonb_typeof(p->'stageKey') IS DISTINCT FROM 'string' OR octet_length(p->>'stageKey') NOT BETWEEN 1 AND 2048
   OR p->>'stageKey' !~ '/buffered-batches/v1/ws=[^/]+/m=srv_[0-9a-f]{32}/b=[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{32}\.staged\.ndjson\.zst$' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: stageKey malformed';END IF;
  SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_key=p->>'stageKey';
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: exactly one of stageToken, stageKey';END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false,'status','gone','reason','stage_absent','retained','[]'::jsonb);END IF;

 SELECT * INTO o FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
  AND origin_id=s.origin_id AND batch_id=s.batch_id;
 IF FOUND AND o.state='buffered' AND o.user_id=s.user_id AND o.digest=s.digest THEN
  SELECT * INTO act FROM sessions.ingest_stage_intents WHERE stage_token=o.active_stage_token;
 END IF;
 -- What retention keeps NOW for this batch, from its current active object (a GC replacement keeps the
 -- original claim ordinals). The recheck after a ClickHouse write deletes rows of anything outside it.
 IF act.stage_token IS NOT NULL AND act.receipt_state='acknowledged' AND act.cleanup_state='live' THEN
  BEGIN
   v_retained:=sessions.ingest_buffer_gc_ordinals(act.stage_token);
  EXCEPTION WHEN SQLSTATE '55000' THEN
   v_retained:='[]';v_reason:='retention_refused';
  END;
 END IF;
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_batches b WHERE b.org_id=s.org_id AND b.workspace_id=s.workspace_id
   AND b.origin_id=s.origin_id AND b.batch_id=s.batch_id AND b.user_id=s.user_id AND b.digest=s.digest
   AND b.state='committed' AND b.publication_state='buffered' AND b.buffer_receipt_kind='staged') INTO v_batch;
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_privacy_authority WHERE singleton AND state='fenced') INTO v_fenced;

 IF s.receipt_state='preparing' THEN
  v_status:=CASE WHEN s.cleanup_state='live' AND s.expires_at>clock_timestamp() THEN 'pending' ELSE 'gone' END;
  v_reason:=coalesce(v_reason,CASE WHEN v_status='pending' THEN 'stage_preparing' ELSE 'stage_expired' END);
 ELSIF act.stage_token IS DISTINCT FROM s.stage_token OR s.cleanup_state<>'live' OR NOT v_batch THEN
  v_status:='gone';v_reason:=coalesce(v_reason,CASE WHEN s.cleanup_state<>'live' THEN 'stage_'||s.cleanup_state
   WHEN NOT v_batch THEN 'batch_not_buffered' ELSE 'stage_not_active' END);
 ELSIF v_reason='retention_refused' THEN
  v_status:='refused';
 ELSIF s.gc_lease_until>clock_timestamp() OR s.gc_replacement_stage_token IS NOT NULL THEN
  v_status:='deferred';v_reason:='gc_in_flight';
 ELSIF v_fenced THEN
  v_status:='deferred';v_reason:='privacy_fenced';
 ELSE
  v_status:='live';
 END IF;
 RETURN jsonb_build_object('found',true,'status',v_status,'reason',v_reason,'retained',v_retained,
  'tenant',jsonb_build_object('orgId',s.org_id,'workspaceId',s.workspace_id,'userId',s.user_id),
  'originId',s.origin_id,'batchId',s.batch_id,'claimToken',s.claim_token,
  'stage',jsonb_build_object('stageToken',s.stage_token,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'createdAt',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
END $stage$;

CREATE FUNCTION sessions.buffered_search_feed_advance(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp SET lock_timeout='2s' AS $advance$
DECLARE v_created timestamptz;v_token uuid;v_indexed int;v_skipped int;
BEGIN
 -- Admission (the 20261105043860 guard): only the genuine buffered_search_feed_runtime login, no SET ROLE.
 IF session_user::text IS DISTINCT FROM 'buffered_search_feed_runtime' OR coalesce(current_setting('role', true), 'none') <> 'none' THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='buffered search feed: caller is not the buffered_search_feed_runtime login';END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['cursor','createdAt','stageToken','indexed','skipped']);
 IF jsonb_typeof(p->'cursor') IS DISTINCT FROM 'string' OR p->>'cursor'!~'^[a-z][a-z0-9-]{0,62}$'
  OR jsonb_typeof(p->'createdAt') IS DISTINCT FROM 'string' OR p->>'createdAt'!~'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$'
  OR jsonb_typeof(p->'stageToken') IS DISTINCT FROM 'string' OR p->>'stageToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='buffered search feed: advance malformed';END IF;
 v_created:=(p->>'createdAt')::timestamptz;v_token:=(p->>'stageToken')::uuid;
 v_indexed:=sessions.ingest_bounded(p->'indexed',0,1000);v_skipped:=sessions.ingest_bounded(p->'skipped',0,1000);
 UPDATE sessions.buffered_search_feed_cursors SET after_created_at=v_created,after_stage_token=v_token,
  stages_indexed=stages_indexed+v_indexed,stages_skipped=stages_skipped+v_skipped,updated_at=clock_timestamp()
  WHERE name=p->>'cursor' AND (after_stage_token IS NULL OR (v_created,v_token)>(after_created_at,after_stage_token));
 RETURN jsonb_build_object('advanced',FOUND);
END $advance$;

REVOKE ALL ON FUNCTION sessions.buffered_search_feed_page(jsonb)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.buffered_search_feed_page(jsonb) TO buffered_search_feed_runtime;
REVOKE ALL ON FUNCTION sessions.buffered_search_feed_stage(jsonb)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.buffered_search_feed_stage(jsonb) TO buffered_search_feed_runtime;
REVOKE ALL ON FUNCTION sessions.buffered_search_feed_advance(jsonb)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_session_ingest, bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.buffered_search_feed_advance(jsonb) TO buffered_search_feed_runtime;

-- USAGE on the schema, no CREATE: the phase 2 verifier's contract, as 20261105043850 grants its installer.
GRANT USAGE ON SCHEMA sessions TO buffered_search_feed_runtime;

-- As in 20261105043860: strip EXECUTE from every other grantee present at creation (default
-- privileges, a pre-existing runtime role).
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY['sessions.buffered_search_feed_page(jsonb)'::regprocedure,
                              'sessions.buffered_search_feed_stage(jsonb)'::regprocedure,
                              'sessions.buffered_search_feed_advance(jsonb)'::regprocedure] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND r.rolname <> 'buffered_search_feed_runtime'
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
