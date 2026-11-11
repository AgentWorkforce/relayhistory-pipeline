-- Slot 20261105058010. Operator per-user erase of BUFFERED desktop uploads (default off; code only: applying it to
-- any real database needs Khaliq's explicit OK). Design and runbook: docs/operator-user-erase.md.
--
-- A data-subject deletion request covers everything one user uploaded to one workspace. The per-session erase
-- (20261105050000) refuses lazy/buffered orgs (RH011) and needs a session key; a buffered upload that was never
-- published has no session_index row at all. This lane removes such a user's whole ingest footprint:
--
--   operator_user_erase_begin    ledger row, revoke every auth session, capture the user's origins (the byte
--                                eraser derives the S3 machine prefixes from them), refuse published state
--   operator_user_erase_step     one bounded batch per call, in foreign-key order; restartable; the last call
--                                purges the remaining per-user rows and asserts none is left
--   operator_user_erase_complete the orchestrator reports that a fresh listing found zero object versions
--
-- All are owner-only (no EXECUTE grant): the operator job connects as the schema owner. begin and step take the
-- global erase gate pg_advisory_xact_lock(961996,50000) exclusively (writers and the buffered GC share it), then
-- step takes the user's ingest_fence_owners row, the order the buffered GC uses, so a GC claim never interleaves.
-- A step refuses to touch a user whose stage still holds a live GC lease (it answers retry_after_ms instead): GC
-- PUTs a redacted child under that lease, and SQL cannot stop that in-flight object write.
--
-- Scope: users with published or canonical state (session_index, publication heads/generations, custody,
-- frozen-source sessions, record revisions) are refused with RH040: their sessions go through the per-session
-- erase, which also owns search documents and canonical transcript bytes. 20261105058000 provides the
-- foreign-key indexes that keep each slot/seat delete an index probe.

CREATE TABLE sessions.operator_user_erasures (
 request_id text PRIMARY KEY,
 org_id text NOT NULL,
 workspace_id text NOT NULL,
 user_id text NOT NULL,
 state text NOT NULL CHECK (state IN ('purging','metadata_purged','bytes_verified')),
 -- Device origins captured at begin; cleared when the bytes are verified gone.
 origins text[] NOT NULL,
 tokens_revoked integer NOT NULL DEFAULT 0,
 deleted jsonb NOT NULL DEFAULT '{}'::jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 metadata_purged_at timestamptz,
 bytes_verified_at timestamptz,
 CHECK ((state='purging')=(metadata_purged_at IS NULL)),
 CHECK ((state='bytes_verified')=(bytes_verified_at IS NOT NULL)),
 CHECK (state<>'bytes_verified' OR cardinality(origins)=0)
);
-- One active erase per user; a later request after completion is a new erase.
CREATE UNIQUE INDEX operator_user_erasures_active ON sessions.operator_user_erasures(org_id,workspace_id,user_id)
 WHERE state<>'bytes_verified';
ALTER TABLE sessions.operator_user_erasures ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.operator_user_erasures FROM PUBLIC;

-- Adds per-table delete counts into the ledger row.
CREATE FUNCTION sessions.operator_user_erase_count(p_request text, p_counts jsonb) RETURNS void
LANGUAGE sql VOLATILE SET search_path=pg_catalog,pg_temp AS $count$
 UPDATE sessions.operator_user_erasures AS e
 SET deleted=(SELECT coalesce(jsonb_object_agg(k, coalesce((e.deleted->>k)::bigint,0)+coalesce((p_counts->>k)::bigint,0)),'{}'::jsonb)
              FROM (SELECT jsonb_object_keys(e.deleted) UNION SELECT jsonb_object_keys(p_counts)) AS keys(k)),
     updated_at=clock_timestamp()
 WHERE e.request_id=p_request;
$count$;

CREATE FUNCTION sessions.operator_user_erase_render(p_request text) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $render$
 SELECT jsonb_build_object('request_id',e.request_id,'state',e.state,'origins',to_jsonb(e.origins),
   'tokens_revoked',e.tokens_revoked,'deleted',e.deleted,'done',e.state<>'purging')
 FROM sessions.operator_user_erasures AS e WHERE e.request_id=p_request;
$render$;

CREATE FUNCTION sessions.operator_user_erase_begin(p_request text,p_org text,p_ws text,p_user text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $begin$
DECLARE v_org text;v_ws text;v_user text;v_request text;e sessions.operator_user_erasures%ROWTYPE;
 v_origins text[];v_revoked integer;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator user erase: read-committed transaction required';END IF;
 v_request:=sessions.ingest_identifier(to_jsonb(p_request),false);
 v_org:=sessions.ingest_identifier(to_jsonb(p_org),false);
 v_ws:=sessions.ingest_identifier(to_jsonb(p_ws),true);
 v_user:=sessions.ingest_identifier(to_jsonb(p_user),false);
 PERFORM pg_advisory_xact_lock(961996,50000);

 SELECT * INTO e FROM sessions.operator_user_erasures WHERE request_id=v_request FOR UPDATE;
 IF FOUND THEN
  IF (e.org_id,e.workspace_id,e.user_id) IS DISTINCT FROM (v_org,v_ws,v_user) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: request id belongs to another user';END IF;
  RETURN sessions.operator_user_erase_render(v_request);
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.operator_user_erasures WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
   AND state<>'bytes_verified') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: another request is active for this user';END IF;

 -- Published or canonical state is the per-session erase's (search documents, canonical bytes).
 IF EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_heads WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_custody_records WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_owned_sessions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_record_revisions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user) THEN
  RAISE EXCEPTION USING ERRCODE='RH040',
   MESSAGE='operator user erase: the user has published sessions; erase them with the per-session operator erase first';
 END IF;

 SELECT coalesce(array_agg(DISTINCT o ORDER BY o),'{}') INTO v_origins FROM (
  SELECT origin_id FROM sessions.ingest_batches WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_claim_attempts WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_stage_intents WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_buffer_outbox WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_session_ownership WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.upload_deletions WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user
  UNION SELECT origin_id FROM sessions.ingest_lazy_native_slots WHERE org_id=v_org AND workspace_id=v_ws AND current_owner=v_user
 ) AS u(o);
 -- Un-owned slots are erased by origin, so an origin must be this user's alone (a shared device origin would let
 -- them belong to someone else). Never observed in production; refused rather than guessed.
 IF EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=v_org AND workspace_id=v_ws AND origin_id=ANY(v_origins) AND user_id<>v_user)
  OR EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=v_org AND workspace_id=v_ws AND origin_id=ANY(v_origins)
    AND current_owner IS NOT NULL AND current_owner<>v_user) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: a device origin of this user is shared with another user';
 END IF;

 -- Revoke first: no token of this user can upload or read while the erase runs. Mirror rows from Neon never clear
 -- a revocation (20261104020000 auth_sessions_mirror); relayhistory-cloud must also revoke its Neon copy.
 UPDATE sessions.auth_sessions SET revoked_at=clock_timestamp(),revoked_reason='operator_user_erase',updated_at=clock_timestamp()
 WHERE org_id=v_org AND workspace_id=v_ws AND user_id=v_user AND revoked_at IS NULL;
 GET DIAGNOSTICS v_revoked=ROW_COUNT;

 INSERT INTO sessions.operator_user_erasures(request_id,org_id,workspace_id,user_id,state,origins,tokens_revoked)
 VALUES(v_request,v_org,v_ws,v_user,'purging',v_origins,v_revoked);
 RETURN sessions.operator_user_erase_render(v_request);
END
$begin$;

CREATE FUNCTION sessions.operator_user_erase_step(p_request text,p_limit integer) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $step$
DECLARE e sessions.operator_user_erasures%ROWTYPE;v_lease timestamptz;v_keys text[];v_seats text[];
 v_claims uuid[];v_stages uuid[];v_n bigint;v_c jsonb:='{}'::jsonb;v_round integer:=0;v_probe text:='absent';
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator user erase: read-committed transaction required';END IF;
 IF p_limit IS NULL OR p_limit<1 OR p_limit>20000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: limit must be 1..20000';END IF;
 PERFORM pg_advisory_xact_lock(961996,50000);
 SELECT * INTO e FROM sessions.operator_user_erasures
  WHERE request_id=sessions.ingest_identifier(to_jsonb(p_request),false) FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: unknown request';END IF;
 IF e.state<>'purging' THEN RETURN sessions.operator_user_erase_render(e.request_id);END IF;

 -- The buffered GC's lock order: user fence first. A live GC lease may still PUT a redacted child object.
 PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id FOR UPDATE;
 SELECT max(gc_lease_until) INTO v_lease FROM sessions.ingest_stage_intents
  WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id AND gc_lease_until>clock_timestamp();
 IF v_lease IS NOT NULL THEN
  RETURN sessions.operator_user_erase_render(e.request_id)||jsonb_build_object('phase','gc_lease_active',
   'retry_after_ms',ceil(extract(epoch FROM v_lease-clock_timestamp())*1000)::bigint+1000);
 END IF;

 -- Phase 1: one batch of the user's lazy slots and the receipt rows hanging off them.
 SELECT array_agg(key_sha256),array_agg(DISTINCT current_seat_key) FILTER (WHERE current_seat_key IS NOT NULL)
 INTO v_keys,v_seats FROM (
  SELECT key_sha256,current_seat_key FROM sessions.ingest_lazy_native_slots
  WHERE org_id=e.org_id AND workspace_id=e.workspace_id
    AND (current_owner=e.user_id OR (current_owner IS NULL AND origin_id=ANY(e.origins)))
  LIMIT p_limit) AS s;
 IF v_keys IS NOT NULL THEN
  v_seats:=coalesce(v_seats,'{}');
  DELETE FROM sessions.ingest_lazy_ack_decisions WHERE native_key_sha256=ANY(v_keys) OR seat_key_sha256=ANY(v_seats);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_ack_decisions',v_n);
  DELETE FROM sessions.ingest_history_reconcile_outbox WHERE native_key_sha256=ANY(v_keys);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_reconcile_outbox',v_n);
  DELETE FROM sessions.ingest_receipt_record_heads WHERE key_sha256=ANY(v_keys) OR winner_key_sha256=ANY(v_seats);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_record_heads',v_n);
  DELETE FROM sessions.ingest_receipt_ord_seats WHERE key_sha256=ANY(v_seats) OR candidate_key_sha256=ANY(v_keys);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_ord_seats',v_n);
  DELETE FROM sessions.ingest_lazy_native_slots WHERE key_sha256=ANY(v_keys);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_native_slots',v_n);
  -- A seat still referenced by another slot (never expected) is kept rather than failing the batch.
  DELETE FROM sessions.ingest_receipt_seats AS s WHERE s.key_sha256=ANY(v_seats)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots AS l WHERE l.current_seat_key=s.key_sha256);
  GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_seats',v_n);
  PERFORM sessions.operator_user_erase_count(e.request_id,v_c);
  RETURN sessions.operator_user_erase_render(e.request_id)||jsonb_build_object('phase','slots');
 END IF;

 -- Phase 2: everything else of the user, in foreign-key order, then prove nothing is left.
 SELECT coalesce(array_agg(claim_token),'{}') INTO v_claims FROM sessions.ingest_claim_attempts
  WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 SELECT coalesce(array_agg(stage_token),'{}') INTO v_stages FROM sessions.ingest_stage_intents
  WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 DELETE FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=ANY(v_claims);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_ack_decisions',v_n);
 DELETE FROM sessions.ingest_receipt_ord_seats WHERE claim_token=ANY(v_claims);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_receipt_ord_seats',v_n);
 DELETE FROM sessions.ingest_publication_deferrals WHERE stage_token=ANY(v_stages);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_deferrals',v_n);
 DELETE FROM sessions.ingest_publication_images WHERE attempt_token IN
  (SELECT attempt_token FROM sessions.ingest_publication_attempts WHERE stage_token=ANY(v_stages));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_images',v_n);
 DELETE FROM sessions.ingest_publication_attempts WHERE stage_token=ANY(v_stages);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_publication_attempts',v_n);
 DELETE FROM sessions.ingest_history_plan_refs WHERE plan_token IN
  (SELECT plan_token FROM sessions.ingest_history_plans WHERE stage_token=ANY(v_stages));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_plan_refs',v_n);
 DELETE FROM sessions.ingest_history_plan_carriers WHERE plan_token IN
  (SELECT plan_token FROM sessions.ingest_history_plans WHERE stage_token=ANY(v_stages));
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_plan_carriers',v_n);
 DELETE FROM sessions.ingest_history_plans WHERE stage_token=ANY(v_stages);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_history_plans',v_n);
 DELETE FROM sessions.ingest_buffer_outbox WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_buffer_outbox',v_n);
 -- Stage intents reference each other (parent, GC replacement; RESTRICT): leaves first.
 LOOP
  DELETE FROM sessions.ingest_stage_intents AS si WHERE si.stage_token=ANY(v_stages)
   AND NOT EXISTS(SELECT 1 FROM sessions.ingest_stage_intents AS c WHERE c.stage_token<>si.stage_token
     AND (c.parent_stage_token=si.stage_token OR c.gc_replacement_stage_token=si.stage_token));
  GET DIAGNOSTICS v_n=ROW_COUNT;
  v_c:=v_c||jsonb_build_object('ingest_stage_intents',coalesce((v_c->>'ingest_stage_intents')::bigint,0)+v_n);
  v_round:=v_round+1;
  EXIT WHEN v_n=0 OR v_round>=10000;
 END LOOP;
 DELETE FROM sessions.ingest_claim_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_claim_sessions',v_n);
 DELETE FROM sessions.ingest_claim_attempts WHERE claim_token=ANY(v_claims);
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_claim_attempts',v_n);
 DELETE FROM sessions.ingest_batches WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_batches',v_n);
 DELETE FROM sessions.ingest_session_ownership WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_session_ownership',v_n);
 DELETE FROM sessions.ingest_fence_versions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_fence_versions',v_n);
 DELETE FROM sessions.upload_deletions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_deletions',v_n);
 DELETE FROM sessions.upload_resets WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('upload_resets',v_n);
 DELETE FROM sessions.ingest_control_receipts WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_control_receipts',v_n);
 DELETE FROM sessions.ingest_source_control_heads WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_source_control_heads',v_n);
 DELETE FROM sessions.ingest_lazy_user_seals WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_lazy_user_seals',v_n);
 DELETE FROM sessions.ingest_seed_fence_baselines WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_seed_fence_baselines',v_n);
 DELETE FROM sessions.ingest_fence_owners WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('ingest_fence_owners',v_n);
 DELETE FROM sessions.auth_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id;
 GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('auth_sessions',v_n);
 -- history.probe_connections lives in the history migration track: only when present and deletable here.
 IF to_regclass('history.probe_connections') IS NOT NULL THEN
  IF has_table_privilege('history.probe_connections','DELETE') THEN
   EXECUTE 'DELETE FROM history.probe_connections WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3'
    USING e.org_id,e.workspace_id,e.user_id;
   GET DIAGNOSTICS v_n=ROW_COUNT;v_c:=v_c||jsonb_build_object('history.probe_connections',v_n);v_probe:='erased';
  ELSE v_probe:='not_permitted';END IF;
 END IF;

 -- Nothing of the user may remain in any table this lane owns (index-backed probes only).
 IF EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND current_owner=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_stage_intents WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts WHERE claim_token=ANY(v_claims))
  OR EXISTS(SELECT 1 FROM sessions.ingest_claim_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.auth_sessions WHERE org_id=e.org_id AND workspace_id=e.workspace_id AND user_id=e.user_id)
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches AS b WHERE b.org_id=e.org_id AND b.workspace_id=e.workspace_id
    AND b.origin_id=ANY(e.origins) AND b.user_id=e.user_id) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: rows of the user remain after the purge';
 END IF;

 PERFORM sessions.operator_user_erase_count(e.request_id,v_c);
 UPDATE sessions.operator_user_erasures SET state='metadata_purged',metadata_purged_at=clock_timestamp(),updated_at=clock_timestamp()
 WHERE request_id=e.request_id;
 RETURN sessions.operator_user_erase_render(e.request_id)||jsonb_build_object('phase','purged','probe_connections',v_probe);
END
$step$;

-- The orchestrator calls this only after a fresh, strict listing found zero versions and zero delete markers under
-- every machine prefix of the captured origins. The origins are then cleared: the ledger keeps counts only.
CREATE FUNCTION sessions.operator_user_erase_complete(p_request text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $complete$
DECLARE e sessions.operator_user_erasures%ROWTYPE;
BEGIN
 SELECT * INTO e FROM sessions.operator_user_erasures
  WHERE request_id=sessions.ingest_identifier(to_jsonb(p_request),false) FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator user erase: unknown request';END IF;
 IF e.state='purging' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator user erase: metadata purge is not finished';END IF;
 IF e.state='metadata_purged' THEN
  UPDATE sessions.operator_user_erasures SET state='bytes_verified',bytes_verified_at=clock_timestamp(),origins='{}',
   updated_at=clock_timestamp() WHERE request_id=e.request_id;
 END IF;
 RETURN sessions.operator_user_erase_render(e.request_id);
END
$complete$;

REVOKE ALL ON FUNCTION sessions.operator_user_erase_count(text,jsonb),sessions.operator_user_erase_render(text),
 sessions.operator_user_erase_begin(text,text,text,text),sessions.operator_user_erase_step(text,integer),
 sessions.operator_user_erase_complete(text) FROM PUBLIC;
