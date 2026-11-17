-- Generated turns selection origin-guard rollback. Restores the exact public and self-host selection bodies.
-- Code only: hosted execution needs Khaliq's separate explicit approval.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='120s';
SELECT pg_advisory_xact_lock(961996,1);
DO $rollback_guard$ DECLARE v_sha text; BEGIN
 IF to_regclass('sessions.__96bulls_migrations') IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: migration ledger required';END IF;
 LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'upstream:20261105062060_turns_selection_origin_guard.sql')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: a later migration is applied; roll it back first';END IF;
 SELECT sha256 INTO v_sha FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062060_turns_selection_origin_guard.sql' FOR UPDATE;
 IF NOT FOUND OR v_sha IS DISTINCT FROM '2a439d8b584ca3752417516a7a2d56b8ba5108bff98581a37be1f0431d9c921f'
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='turns selection origin guard: own applied checksum required';END IF;
END $rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.ingest_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selection$
DECLARE t jsonb;v_org text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_org:=p#>>'{tenant,orgId}';
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) OR p?'control' OR p?'generation'
  OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=v_org)
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND publication_state='buffered') THEN
  RETURN sessions.ingest_apply_ordered_control(p,'selection');
 END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','enabled']);
 t:=p->'tenant';PERFORM sessions.ingest_exact_keys(t,ARRAY['orgId','workspaceId','userId']);
 IF jsonb_typeof(p->'enabled') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: enabled is malformed';
 END IF;
 RETURN jsonb_build_object('updated',sessions.ingest_apply_selection(
  sessions.ingest_identifier(t->'orgId',false),sessions.ingest_identifier(t->'workspaceId',true),
  sessions.ingest_identifier(t->'userId',false),sessions.ingest_identifier(p->'originId',false),
  p->'sessions',(p->>'enabled')::boolean));
END $selection$;
CREATE OR REPLACE FUNCTION sessions.ingest_selfhost_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selfhost_selection$
DECLARE
 v_org text;v_ws text;v_user text;v_named text;v_origin text;v_sessions jsonb;v_machine text;v_pair record;v_canonical text;
 v_updated bigint:=0;
 v_member_machine text;v_member_user text;v_dir record;
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
     search_text=sessions.session_directory_search_text(d.title,d.task_title,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,NULL),
     deleted_at=CASE WHEN EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id
       AND q.harness=d.harness AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL)
      THEN d.deleted_at ELSE coalesce(d.deleted_at,clock_timestamp()) END
    WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id IN(v_pair.session_id,v_canonical);
   -- 057030 (#566): the erased pointer's directory membership, kept as src/hoopdb/session-directory-projector.ts keeps
   -- it: machines are coalesce(canonical_machine_id,machine_id) and users are user_id of the LIVE pointers of the logical
   -- session; a tombstoned row has empty arrays and no membership rows at all.
   SELECT coalesce(s.canonical_machine_id,s.machine_id),s.user_id INTO v_member_machine,v_member_user FROM sessions.session_index AS s
    WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_pair.source AND s.session_id=v_pair.session_id AND s.machine_id=v_machine;
   FOR v_dir IN SELECT d.session_id,d.deleted_at IS NOT NULL AS gone,
      EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id AND q.harness=d.harness
        AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL
        AND coalesce(q.canonical_machine_id,q.machine_id)=v_member_machine) AS machine_live,
      EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id AND q.harness=d.harness
        AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL
        AND q.user_id=v_member_user) AS user_live
     FROM sessions.session_directory AS d
     WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id IN(v_pair.session_id,v_canonical)
     ORDER BY d.session_id COLLATE "C"
   LOOP
    DELETE FROM sessions.session_directory_machines AS m WHERE m.org_id=v_org AND m.workspace_id=v_ws AND m.harness=v_pair.source
      AND m.session_id=v_dir.session_id AND (v_dir.gone OR (m.machine_id=v_member_machine AND NOT v_dir.machine_live));
    DELETE FROM sessions.session_directory_users AS u WHERE u.org_id=v_org AND u.workspace_id=v_ws AND u.harness=v_pair.source
      AND u.session_id=v_dir.session_id AND (v_dir.gone OR (u.user_id=v_member_user AND NOT v_dir.user_live));
    IF v_dir.gone OR NOT v_dir.machine_live OR NOT v_dir.user_live THEN
     UPDATE sessions.session_directory AS d SET
       machine_ids=CASE WHEN v_dir.gone THEN '{}'::text[] WHEN v_dir.machine_live THEN d.machine_ids ELSE array_remove(d.machine_ids,v_member_machine) END,
       user_ids=CASE WHEN v_dir.gone THEN '{}'::text[] WHEN v_dir.user_live THEN d.user_ids ELSE array_remove(d.user_ids,v_member_user) END
      WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id=v_dir.session_id;
    END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 RETURN jsonb_build_object('updated',v_updated);
END $selfhost_selection$;
DROP FUNCTION sessions.ingest_set_session_uploads_internal(jsonb);
DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:20261105062060_turns_selection_origin_guard.sql' AND sha256='2a439d8b584ca3752417516a7a2d56b8ba5108bff98581a37be1f0431d9c921f';
COMMIT;
