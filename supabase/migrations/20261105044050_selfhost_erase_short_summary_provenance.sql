-- SLOT 044050. Forward fix to 20261105044010 (and 20261105043910): the self-host erase cleared
-- session_directory.short_summary but left its four provenance columns (short_summary_scrub_ruleset,
-- short_summary_scrub_version, short_summary_source, short_summary_generated_at) set. CHECK
-- session_directory_short_summary_provenance (20261030020000) requires each of them to be NULL exactly when
-- short_summary is, so erasing a session whose directory row carried a summary and still had another live
-- pointer (the row is not tombstoned, so session_directory_tombstone_clears_summary does not clear them)
-- failed with 23514 and the self-host selection route answered 503.
--
-- The directory write now clears all five columns together, as the tombstone trigger and the history-scope
-- invalidation (20261105026000) do. Replacement only: the body is 044010's byte for byte except that UPDATE's
-- SET list; signature, SECURITY DEFINER, search_path, owner and ACL are 043910's (CREATE OR REPLACE keeps them).
--
-- Does not change: search_text is not rebuilt here (as in 044010), and the managed-head retire trigger
-- (sessions.ingest_publication_retire_fence, 20261104060000) is not touched.
--
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
CREATE OR REPLACE FUNCTION sessions.ingest_selfhost_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selfhost_selection$
DECLARE
 v_org text;v_ws text;v_user text;v_named text;v_origin text;v_sessions jsonb;v_machine text;v_pair record;v_canonical text;
 v_updated bigint:=0;
 c_erased_revision CONSTANT bigint:=9007199254740991;
 c_turn_origin CONSTANT text:='relayhistory:conversation_turns:v1';
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','enabled']);
 IF p->'enabled' IS DISTINCT FROM 'false'::jsonb THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: only enabled:false is served here';
 END IF;
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_named:=sessions.ingest_identifier(p->'originId',false);
 -- The claim's lock (ingest_lock_fences), held before the ownership read below. A claim that holds it
 -- first finishes, and its ownership row is visible to that read; one that comes later waits for this erase.
 PERFORM sessions.ingest_lock_fences(v_org,v_ws,v_user);
 -- Listed before any fence: the selection deletes the ownership rows of the origin it fences.
 FOR v_origin,v_sessions IN
  WITH pairs AS (SELECT DISTINCT f.source,f.session_id FROM sessions.ingest_fence_pairs(p->'sessions') AS f),
  targets AS (
   SELECT o.origin_id,x.source,x.session_id FROM pairs AS x JOIN sessions.ingest_session_ownership AS o
    ON o.org_id=v_org AND o.workspace_id=v_ws AND o.user_id=v_user AND o.source=x.source AND o.session_id=x.session_id
   UNION SELECT v_named,x.source,x.session_id FROM pairs AS x
   UNION SELECT c_turn_origin,x.source,x.session_id FROM pairs AS x)
  SELECT t.origin_id,jsonb_agg(jsonb_build_array(t.source,t.session_id)) FROM targets AS t GROUP BY t.origin_id ORDER BY t.origin_id COLLATE "C"
 LOOP
  v_updated:=v_updated+(sessions.ingest_set_session_uploads(jsonb_build_object('tenant',p->'tenant','originId',v_origin,'sessions',v_sessions,'enabled',false))->>'updated')::bigint;
  -- src/session-ingest/writer.ts liveMachineId: the machine this origin's manifests are written under.
  v_machine:='srv_'||left(encode(sha256(convert_to(v_ws||chr(31)||v_origin||chr(31)||v_user,'UTF8')),'hex'),32);
  FOR v_pair IN SELECT f.source,f.session_id FROM sessions.ingest_fence_pairs(v_sessions) AS f LOOP
   v_canonical:=NULL;
   UPDATE sessions.session_index AS s SET deleted_at=coalesce(s.deleted_at,clock_timestamp()),indexed_at=clock_timestamp(),
     revision=greatest(s.revision,c_erased_revision)
    WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_pair.source AND s.session_id=v_pair.session_id AND s.machine_id=v_machine
    RETURNING coalesce(s.canonical_session_id,s.session_id) INTO v_canonical;
   -- Only a session this caller actually has a pointer for: a selection naming someone else's session id
   -- must not touch that session's directory row.
   IF NOT FOUND THEN CONTINUE;END IF;
   -- The retire trigger's directory write: clear derived text, tombstone only with no live pointer left.
   UPDATE sessions.session_directory AS d SET short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
     short_summary_source=NULL,short_summary_generated_at=NULL,
     deleted_at=CASE WHEN EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id
       AND q.harness=d.harness AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL)
      THEN d.deleted_at ELSE coalesce(d.deleted_at,clock_timestamp()) END
    WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id IN(v_pair.session_id,v_canonical);
  END LOOP;
 END LOOP;
 RETURN jsonb_build_object('updated',v_updated);
END $selfhost_selection$;
