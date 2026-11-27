-- Buffered search feed: a sargable page keyset (slot 062350). Separate from 062330 (custom plans on the stage RPC
-- and gc_ordinals); needs its own review and go.
--
-- Why. sessions.buffered_search_feed_page(jsonb) (20261105043870, never redefined since) selects the next page with
--   s.cleanup_state='live' AND s.receipt_state='acknowledged' AND s.created_at>=c.since
--   AND (c.after_created_at IS NULL OR (s.created_at,s.stage_token)>(c.after_created_at,c.after_stage_token))
--   AND EXISTS(<the batch's buffered outbox row whose active stage is s>)  ORDER BY created_at,stage_token LIMIT n
-- Two plan failures on the real database (read-only EXPLAINs):
--   - custom plan (literal values, NULL cursor): the planner multiplies the five outbox join columns' selectivities,
--     estimates the EXISTS semi-join at about 1 row, and so never walks ingest_stage_intents_cleanup_idx in order to
--     stop at the limit: Parallel Seq Scan of the intents (~752k) hash-joined to a Parallel Seq Scan of the outbox
--     (~748k), then Sort + Limit (cost ~504k). In reality nearly every live acknowledged stage matches;
--   - generic plan: the OR keyset is only a Filter, so the Index Cond is cleanup_state='live' AND created_at>=since
--     and every page re-walks every live stage from `since` up to the cursor (~84k rows; 17.8 s in the live trial).
--
-- What changes. CREATE OR REPLACE of sessions.buffered_search_feed_page(jsonb) only. The 043870 body is kept
-- byte for byte except two predicates of the page query (and its comment):
--   (1) the keyset is one row comparison, with s.created_at>=c.since kept:
--       (s.created_at,s.stage_token)>(coalesce(c.after_created_at,c.since-interval '1 microsecond'),
--                                     coalesce(c.after_stage_token,'00000000-0000-0000-0000-000000000000'::uuid))
--       Equivalence (created_at and stage_token are NOT NULL; the cursor CHECK makes after_created_at and
--       after_stage_token both NULL or both set): cursor set -> the coalesces return the cursor, so this is the old
--       row comparison verbatim; NULL cursor -> the old predicate is TRUE, and timestamptz has microsecond
--       resolution, so created_at>=since implies created_at>since-1us and the row comparison is TRUE on its first
--       column for every row the kept created_at>=since admits (a row at exactly `since` included). `since` is a
--       4-digit-year literal (input check), so since-1us never overflows;
--   (2) EXISTS(<outbox row>) becomes (SELECT true FROM <the same outbox predicate> LIMIT 1) IS TRUE: TRUE exactly
--       when such a row exists (the outbox key is its primary key, so at most one), NULL -> not TRUE otherwise. A
--       scalar subquery is never pulled up into a join, so it is a per-row primary-key probe and the planner cannot
--       hash-join the two tables; with ORDER BY matching the index, the plan is Limit <- ordered index scan.
-- ORDER BY s.created_at,s.stage_token, LIMIT, the jsonb output and the after/cursor fields are unchanged. The
-- function also gains SET plan_cache_mode = force_custom_plan (its keyset values are PL/pgSQL variables; the row
-- comparison is an Index Cond under a generic plan too).
-- Same signature, return type, VOLATILE, SECURITY DEFINER, search_path, function lock_timeout and owner. The ACL is
-- kept: CREATE OR REPLACE keeps it, so there is no GRANT/REVOKE here (043870's EXECUTE to
-- buffered_search_feed_runtime only; src/hoopdb/sessions-function-acl.json unchanged), as in 045010.
-- Never creates or alters a role, table or index.
--
-- Rollout. Code only: applying this to any real database needs Khaliq's explicit OK. The feeder's next page call
-- uses the new body. Applying it changes the migration-set generation (src/hoopdb/migrate.ts). Down path (generated
-- by the buffered-search-feed-page-keyset boundary lane): restores 043870's exact page function (its proconfig too),
-- supabase/rollbacks/20261105062350_buffered_search_feed_page_keyset.down.sql.
SET LOCAL lock_timeout='5s';

DO $buffered_search_feed_page_keyset$ BEGIN
 IF to_regprocedure('sessions.buffered_search_feed_page(jsonb)') IS NULL
  OR NOT (SELECT prosecdef FROM pg_proc WHERE oid=to_regprocedure('sessions.buffered_search_feed_page(jsonb)')) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffered search feed page keyset: requires 20261105043870';END IF;
END $buffered_search_feed_page_keyset$;

CREATE OR REPLACE FUNCTION sessions.buffered_search_feed_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, sessions, pg_temp SET lock_timeout='2s'
 SET plan_cache_mode = force_custom_plan AS $page$
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
 -- ingest_stage_intents_cleanup_idx(cleanup_state,created_at,stage_token) serves this keyset. 062350: the keyset is
 -- one row comparison (an Index Cond); a NULL cursor starts strictly after (since - 1 microsecond, the nil uuid). The
 -- outbox check is a per-row scalar subquery on the outbox primary key, never a (hash) join: the scan walks the
 -- index in order and stops at the limit.
 SELECT coalesce(jsonb_agg(jsonb_build_object(
   'stageToken',s.stage_token,
   'createdAt',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'tenant',jsonb_build_object('orgId',s.org_id,'workspaceId',s.workspace_id,'userId',s.user_id),
   'originId',s.origin_id,'batchId',s.batch_id) ORDER BY s.created_at,s.stage_token),'[]')
  INTO v_rows FROM (
   SELECT s.* FROM sessions.ingest_stage_intents s
    WHERE s.cleanup_state='live' AND s.receipt_state='acknowledged' AND s.created_at>=c.since
      AND (s.created_at,s.stage_token)>(coalesce(c.after_created_at,c.since-interval '1 microsecond'),
       coalesce(c.after_stage_token,'00000000-0000-0000-0000-000000000000'::uuid))
      AND (SELECT true FROM sessions.ingest_buffer_outbox o WHERE o.org_id=s.org_id AND o.workspace_id=s.workspace_id
       AND o.origin_id=s.origin_id AND o.batch_id=s.batch_id AND o.state='buffered' AND o.active_stage_token=s.stage_token
       LIMIT 1) IS TRUE
    ORDER BY s.created_at,s.stage_token LIMIT v_limit) s;
 RETURN jsonb_build_object('cursor',v_name,'since',to_char(c.since AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'after',CASE WHEN c.after_stage_token IS NULL THEN NULL ELSE jsonb_build_object(
   'createdAt',to_char(c.after_created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'stageToken',c.after_stage_token) END,
  'stages',v_rows);
END $page$;
