-- DRAFT, NO MIGRATION SLOT. Buffered-stage search feed (INGEST_BUFFERED_SEARCH_FEED, default off).
--
-- Not under supabase/migrations on purpose: the registry keeper (layered-sessions-pipeline) assigns the
-- slot, and the function-boundary lane, ACL manifest, wrapper and verifiers are generated in that PR.
-- Applying this file to any real database needs Khaliq's separate OK in his own words. The PGlite suite
-- src/session-pipeline/__tests__/buffered-search-feed.pglite.test.ts applies it on top of every migration.
--
-- What it gives the session pipeline (service_role, like the large-read RPCs) and nothing else:
--   buffered_search_feed_page(p)    keyset page of acknowledged, live, ACTIVE buffered stages after a
--                                   named cursor. No transcript bytes, no retention decision, no locks.
--   buffered_search_feed_stage(p)   one stage's indexability plus the ordinals retention keeps now. The
--                                   retention decision is sessions.ingest_buffer_gc_ordinals, the exact
--                                   authority the buffered GC and the history publisher use: whatever GC
--                                   would erase is never indexed. One short transaction per call (the
--                                   lazy branch holds the receipt seat clock FOR SHARE, as one GC claim does).
--   buffered_search_feed_advance(p) moves a cursor forward only (monotonic), after ClickHouse writes.
--
-- Fail closed. Status is 'live' only when the stage is its batch's active outbox stage, acknowledged,
-- cleanup_state 'live', the batch is committed + buffered + staged, no GC lease or replacement is in
-- flight, and the 041000 privacy authority is not fenced. A retention refusal (55000 from the GC
-- authority: malformed or missing ACK decisions) yields status 'refused' and retained [].
-- 40001/55P03/57014 propagate so the caller retries.
SET LOCAL lock_timeout='5s';

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
REVOKE ALL ON sessions.buffered_search_feed_cursors FROM PUBLIC,anon,authenticated,service_role;

CREATE FUNCTION sessions.buffered_search_feed_page(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp SET lock_timeout='2s' AS $page$
DECLARE v_name text;v_since timestamptz;v_limit int;c sessions.buffered_search_feed_cursors%ROWTYPE;v_rows jsonb;
BEGIN
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
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp SET lock_timeout='2s' AS $stage$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;act sessions.ingest_stage_intents%ROWTYPE;o sessions.ingest_buffer_outbox%ROWTYPE;
 v_status text;v_reason text;v_retained jsonb:='[]';v_batch boolean;v_fenced boolean;
BEGIN
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
  'originId',s.origin_id,'batchId',s.batch_id,
  'stage',jsonb_build_object('stageToken',s.stage_token,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'createdAt',to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
END $stage$;

CREATE FUNCTION sessions.buffered_search_feed_advance(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp SET lock_timeout='2s' AS $advance$
DECLARE v_created timestamptz;v_token uuid;v_indexed int;v_skipped int;
BEGIN
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

REVOKE ALL ON FUNCTION sessions.buffered_search_feed_page(jsonb),sessions.buffered_search_feed_stage(jsonb),
 sessions.buffered_search_feed_advance(jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.buffered_search_feed_page(jsonb),sessions.buffered_search_feed_stage(jsonb),
 sessions.buffered_search_feed_advance(jsonb) TO service_role;
