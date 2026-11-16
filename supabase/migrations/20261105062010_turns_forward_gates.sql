-- SLOT 062010. Relay turns forward activation gates. Code only: applying this migration,
-- importing the legacy fence census, enabling publication, or changing any runtime flag each
-- requires Khaliq's separate explicit approval.
--
-- Privacy invariant: an erasure under ANY origin makes the logical (tenant, source, session)
-- permanently fenced for the relay turns origin. Writers register before their batch can be
-- admitted. The upload_deletions trigger and registration RPC both materialize the ordinary
-- turn-origin fence/version rows, so all existing claim, buffered-read, GC and publication
-- checks apply without a turns-only bypass. Whichever transaction wins the race, the loser
-- observes or creates the durable fence; a later fence-version bump retires an already-published
-- generation through ingest_publication_retire_fence.

CREATE TABLE sessions.turn_session_fences (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  first_fenced_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  fence_basis text NOT NULL CHECK (fence_basis IN ('live','legacy_import')),
  PRIMARY KEY (org_id,workspace_id,source,session_id)
);

CREATE TABLE sessions.turn_writers (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id,workspace_id,user_id,source,session_id)
);

CREATE INDEX turn_writers_session
 ON sessions.turn_writers(org_id,workspace_id,source,session_id,user_id);

ALTER TABLE sessions.turn_session_fences ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.turn_writers ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.turn_session_fences,sessions.turn_writers
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_read_api;

-- A source-free deployment preflight. Its existence proves this entire migration committed;
-- the fixed response is understood only by ingest code that also registers before admission.
CREATE FUNCTION sessions.ingest_turn_capability(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $capability$
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 IF p IS NULL OR p<>'{}'::jsonb
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='turn capability: empty input required';END IF;
 RETURN jsonb_build_object('turnFence','registered-v1');
END $capability$;

-- Materialize one permanent logical turn fence for every registered writer. The ordinary
-- fence-version update is the invalidation signal; upload_deletions is the keep predicate.
CREATE FUNCTION sessions.ingest_turn_materialize_fence(
 p_org text,p_ws text,p_source text,p_session text
) RETURNS integer LANGUAGE plpgsql VOLATILE
SET search_path=pg_catalog,pg_temp AS $materialize$
DECLARE w record;v_n integer:=0;
BEGIN
 FOR w IN SELECT user_id FROM sessions.turn_writers
   WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session
   ORDER BY user_id COLLATE "C"
 LOOP
  INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch)
   VALUES(p_org,p_ws,w.user_id,'relayhistory:conversation_turns:v1',p_source,p_session,1)
  ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id)
   DO UPDATE SET epoch=f.epoch+1;
  INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
   VALUES(p_org,p_ws,w.user_id,'relayhistory:conversation_turns:v1',p_source,p_session)
  ON CONFLICT DO NOTHING;
  DELETE FROM sessions.ingest_record_revisions r
   WHERE r.org_id=p_org AND r.workspace_id=p_ws AND r.user_id=w.user_id
    AND r.origin_id='relayhistory:conversation_turns:v1' AND r.source=p_source AND r.session_id=p_session;
  DELETE FROM sessions.ingest_session_ownership o
   WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=w.user_id
    AND o.origin_id='relayhistory:conversation_turns:v1' AND o.source=p_source AND o.session_id=p_session;
  v_n:=v_n+1;
 END LOOP;
 RETURN v_n;
END $materialize$;

-- Every existing selection/reset/operator erase eventually inserts upload_deletions. Turn-origin
-- rows are the materialized result and are ignored to prevent recursion.
CREATE FUNCTION sessions.ingest_turn_capture_erasure() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $capture$
BEGIN
 IF NEW.session_id IS NULL OR NEW.origin_id='relayhistory:conversation_turns:v1' THEN RETURN NEW;END IF;
 -- Serialize registration and every erasure for one logical session. Without this lock,
 -- registration can insert an uncommitted writer while an erasure inserts an uncommitted
 -- fence; each transaction then misses the other's row and neither materializes the fence.
 PERFORM pg_advisory_xact_lock(hashtextextended(
  NEW.org_id||chr(31)||NEW.workspace_id||chr(31)||NEW.source||chr(31)||NEW.session_id,0));
 INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis)
 VALUES(NEW.org_id,NEW.workspace_id,NEW.source,NEW.session_id,'live') ON CONFLICT DO NOTHING;
 PERFORM sessions.ingest_turn_materialize_fence(NEW.org_id,NEW.workspace_id,NEW.source,NEW.session_id);
 RETURN NEW;
END $capture$;

CREATE TRIGGER ingest_turn_capture_erasure
 AFTER INSERT ON sessions.upload_deletions
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_turn_capture_erasure();

-- Called by POST /v1/turns/batches after strict parsing and before any enqueue/claim. It registers
-- the exact user/source/session writers and returns the pairs already fenced. A fenced pair is not
-- rejected: the existing claim path drops it and produces the normal durable receipt.
CREATE FUNCTION sessions.ingest_turn_register(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $register$
DECLARE t jsonb;v_org text;v_ws text;v_user text;x record;v_fenced jsonb:='[]'::jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','sessions']);
 t:=p->'tenant';PERFORM sessions.ingest_exact_keys(t,ARRAY['orgId','workspaceId','userId']);
 v_org:=sessions.ingest_identifier(t->'orgId',false);
 v_ws:=sessions.ingest_identifier(t->'workspaceId',true);
 v_user:=sessions.ingest_identifier(t->'userId',false);
 IF jsonb_typeof(p->'sessions') IS DISTINCT FROM'array' OR jsonb_array_length(p->'sessions') NOT BETWEEN 1 AND 500
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='turn register: sessions must contain 1..500 pairs';END IF;
 FOR x IN SELECT f.source,f.session_id FROM sessions.ingest_fence_pairs(p->'sessions') f
          ORDER BY f.source COLLATE "C",f.session_id COLLATE "C"
 LOOP
  PERFORM pg_advisory_xact_lock(hashtextextended(
   v_org||chr(31)||v_ws||chr(31)||x.source||chr(31)||x.session_id,0));
  INSERT INTO sessions.turn_writers(org_id,workspace_id,user_id,source,session_id)
   VALUES(v_org,v_ws,v_user,x.source,x.session_id) ON CONFLICT DO NOTHING;
  IF EXISTS(SELECT 1 FROM sessions.turn_session_fences q
    WHERE q.org_id=v_org AND q.workspace_id=v_ws AND q.source=x.source AND q.session_id=x.session_id) THEN
   PERFORM sessions.ingest_turn_materialize_fence(v_org,v_ws,x.source,x.session_id);
   v_fenced:=v_fenced||jsonb_build_array(jsonb_build_array(x.source,x.session_id));
  END IF;
 END LOOP;
 RETURN jsonb_build_object('fenced',v_fenced);
END $register$;

-- Operator/import boundary for the frozen Neon conversation_turn_fences census. The caller is
-- service_role only; batches are bounded and every row is materialized immediately. Replays are
-- idempotent. The runbook verifier compares the source/target key digests before forward may flip.
CREATE FUNCTION sessions.ingest_turn_fences_import(p_rows jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $import$
DECLARE x record;v_n integer:=0;
BEGIN
 PERFORM sessions.ingest_require_role('service_role');
 IF jsonb_typeof(p_rows) IS DISTINCT FROM'array' OR jsonb_array_length(p_rows)>500
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='turn fence import: at most 500 rows';END IF;
 FOR x IN SELECT * FROM jsonb_to_recordset(p_rows)
  AS r(org_id text,workspace_id text,source text,session_id text)
  ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"
 LOOP
  IF x.org_id IS NULL OR x.workspace_id IS NULL OR x.source IS NULL OR x.session_id IS NULL
  THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='turn fence import: incomplete row';END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
   x.org_id||chr(31)||x.workspace_id||chr(31)||x.source||chr(31)||x.session_id,0));
  INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis)
   VALUES(x.org_id,x.workspace_id,x.source,x.session_id,'legacy_import') ON CONFLICT DO NOTHING;
  PERFORM sessions.ingest_turn_materialize_fence(x.org_id,x.workspace_id,x.source,x.session_id);
  v_n:=v_n+1;
 END LOOP;
 RETURN jsonb_build_object('processed',v_n);
END $import$;

-- Source-free immediate read-your-writes. Candidate discovery is exact-session and owner-only;
-- each candidate must survive the complete existing target-stage privacy predicate.
CREATE FUNCTION sessions.buffered_turn_sources_for(
 p_workspace text,p_cutover_epoch text,p_head_epoch text,p_head_version text,p_head_digest text,
 p_head_kind text,p_head_pending boolean,p_privacy_baseline_sha256 text,p_session_id text
) RETURNS TABLE(source text) LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,sessions,pg_temp AS $sources$
DECLARE sc record;
BEGIN
 SELECT * INTO STRICT sc FROM sessions.owner_buffered_read_scope(p_workspace,p_cutover_epoch,p_head_epoch,
  p_head_version,p_head_digest,p_head_kind,p_head_pending,p_privacy_baseline_sha256);
 IF p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='turn sources: session required';END IF;
 IF NOT sc.gate_open THEN RETURN;END IF;
 RETURN QUERY
  SELECT DISTINCT c.source FROM sessions.ingest_claim_sessions c
   WHERE c.org_id=sc.org_id AND c.workspace_id=sc.workspace_id AND c.user_id=sc.user_id
    AND c.origin_id='relayhistory:conversation_turns:v1' AND c.session_id=p_session_id
    AND EXISTS(SELECT 1 FROM sessions.buffered_session_stages_for_target(
      p_workspace,c.source,p_session_id,p_cutover_epoch,p_head_epoch,p_head_version,p_head_digest,
      p_head_kind,p_head_pending,p_privacy_baseline_sha256))
   ORDER BY c.source COLLATE "C" LIMIT 5;
END $sources$;

-- Turn-only canonical pointers remain addressable by GET turns but never become a live directory
-- row. A later non-turn pointer makes the predicate false and the normal projector can restore it.
CREATE FUNCTION sessions.aa_session_directory_hide_turn_only() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $hide_turn_only$
BEGIN
 IF NEW.deleted_at IS NULL
  AND EXISTS(SELECT 1 FROM sessions.session_index s WHERE s.org_id=NEW.org_id AND s.workspace_id=NEW.workspace_id
    AND s.harness=NEW.harness AND coalesce(s.canonical_session_id,s.session_id)=NEW.session_id AND s.deleted_at IS NULL)
  AND NOT EXISTS(SELECT 1 FROM sessions.session_index s WHERE s.org_id=NEW.org_id AND s.workspace_id=NEW.workspace_id
    AND s.harness=NEW.harness AND coalesce(s.canonical_session_id,s.session_id)=NEW.session_id AND s.deleted_at IS NULL
    AND NOT EXISTS(SELECT 1 FROM sessions.turn_writers w
      WHERE w.org_id=s.org_id AND w.workspace_id=s.workspace_id AND w.user_id=s.user_id
       AND w.source=s.harness AND w.session_id=s.session_id
       AND s.machine_id='srv_'||left(encode(sha256(convert_to(s.workspace_id||chr(31)||'relayhistory:conversation_turns:v1'||chr(31)||s.user_id,'UTF8')),'hex'),32)))
 THEN NEW.deleted_at:=clock_timestamp();END IF;
 RETURN NEW;
END $hide_turn_only$;

CREATE TRIGGER aa_session_directory_hide_turn_only
 BEFORE INSERT OR UPDATE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.aa_session_directory_hide_turn_only();

REVOKE ALL ON FUNCTION sessions.ingest_turn_materialize_fence(text,text,text,text),
 sessions.ingest_turn_capture_erasure(),sessions.ingest_turn_capability(jsonb),sessions.ingest_turn_register(jsonb),
 sessions.ingest_turn_fences_import(jsonb),
 sessions.buffered_turn_sources_for(text,text,text,text,text,text,boolean,text,text),
 sessions.aa_session_directory_hide_turn_only()
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.ingest_turn_register(jsonb) TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.ingest_turn_capability(jsonb) TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.ingest_turn_fences_import(jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.buffered_turn_sources_for(text,text,text,text,text,text,boolean,text,text) TO bulls96_read_api;

