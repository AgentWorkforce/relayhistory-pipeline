-- Self-host per-session erase: one transaction that fences a session and makes it
-- unreadable. Called only by the self-host session ingest (SERVICE=session-ingest,
-- src/session-ingest/selfhost-server.ts) when RELAYHISTORY_SELFHOST_SESSION_SELECTION
-- is exactly "true"; the hosted Worker never calls it. Design: docs/design/selfhost-privacy-controls.md
-- section 7.1 (a per-session erase is a selection with enabled:false).
--
-- Why this exists. On a canonical (inline) acceptance self-host the pointer row comes from the
-- manifest event, not from a publication head, so sessions.ingest_publication_retire_fence does
-- not tombstone it. The existing selection writes the fence only; the read API answers from
-- session_index, whose rule is "every pointer row tombstoned" (session-read-api/liveness.ts).
-- This function runs the unchanged sessions.ingest_set_session_uploads, then tombstones the
-- caller's own pointer row (origin's machine only: selection is per origin, as hosted) and the
-- logical directory row, the same writes the retire trigger makes for a managed head.
--
-- Scope:
--  * enabled:false only. enabled:true is refused here (a tombstone's revision is raised below,
--    and a future re-enable needs its own design).
--  * The tombstone raises the row's revision past any manifest revision a queued event can
--    carry, so `upsert_session_index` (revision-fenced, `excluded.revision > s.revision`) can
--    never undelete it.
--  * Unreadable, not bytes-gone: chunks and manifests stay until an eraser runs in self-host.
--  * A session whose first manifest event has not been indexed yet has no row to tombstone.
--    Its later event creates a live row; closing that needs the indexer to consult the fence.
--
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
CREATE FUNCTION sessions.ingest_selfhost_set_session_uploads(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $selfhost_selection$
DECLARE
 v_org text;v_ws text;v_machine text;v_result jsonb;v_pair record;v_canonical text;
 c_erased_revision CONSTANT bigint:=9007199254740991;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','sessions','enabled','machineId']);
 IF p->'enabled' IS DISTINCT FROM 'false'::jsonb THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: only enabled:false is served here';
 END IF;
 IF (p->>'machineId') IS NULL OR p->>'machineId' !~ '^srv_[0-9a-f]{32}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: machineId is malformed';
 END IF;
 v_machine:=p->>'machineId';
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 -- The fence first (and its own validation of tenant, origin and sessions).
 v_result:=sessions.ingest_set_session_uploads(p-'machineId');
 FOR v_pair IN SELECT DISTINCT source,session_id FROM sessions.ingest_fence_pairs(p->'sessions') LOOP
  v_canonical:=NULL;
  UPDATE sessions.session_index AS s SET deleted_at=coalesce(s.deleted_at,clock_timestamp()),indexed_at=clock_timestamp(),
    revision=greatest(s.revision,c_erased_revision)
   WHERE s.org_id=v_org AND s.workspace_id=v_ws AND s.harness=v_pair.source AND s.session_id=v_pair.session_id AND s.machine_id=v_machine
   RETURNING coalesce(s.canonical_session_id,s.session_id) INTO v_canonical;
  -- Only a session this caller actually has a pointer for: a selection naming someone else's session id
  -- must not touch that session's directory row.
  IF NOT FOUND THEN CONTINUE;END IF;
  -- The retire trigger's directory write: clear derived text, tombstone only with no live pointer left.
  UPDATE sessions.session_directory AS d SET short_summary=NULL,
    deleted_at=CASE WHEN EXISTS(SELECT 1 FROM sessions.session_index AS q WHERE q.org_id=d.org_id AND q.workspace_id=d.workspace_id
      AND q.harness=d.harness AND coalesce(q.canonical_session_id,q.session_id)=d.session_id AND q.deleted_at IS NULL)
     THEN d.deleted_at ELSE coalesce(d.deleted_at,clock_timestamp()) END
   WHERE d.org_id=v_org AND d.workspace_id=v_ws AND d.harness=v_pair.source AND d.session_id IN(v_pair.session_id,v_canonical);
 END LOOP;
 RETURN v_result;
END $selfhost_selection$;
REVOKE ALL ON FUNCTION sessions.ingest_selfhost_set_session_uploads(jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.ingest_selfhost_set_session_uploads(jsonb) TO bulls96_session_ingest;
