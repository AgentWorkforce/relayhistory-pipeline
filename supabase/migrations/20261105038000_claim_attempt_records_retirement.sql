-- Slot 20261105038000 assigned by the registry owner (layered-sessions-pipeline);
-- drafted under the provisional slot 20261105037000, which #266 now owns.
--
-- Terminal original-claim records leave SQL; a hash and an S3 pointer stay.
--
-- ingest_claim_attempts.records is the per-record authority snapshot of
-- a claim attempt (ord, source, session_id, record_id, revision_id, revision,
-- digest; up to 500 elements, ~33 KB TOAST per row). Every SQL reader reaches it
-- either through the caller's current claim token while the batch is still
-- pending (prepare/commit), or through a stage intent that is not redacted
-- (ACK seats, GC ordinals/kept, canonical publication, history lineage). Once the
-- batch is committed AND every stage intent of the attempt is redacted (or none
-- ever existed, as for canonical-mode attempts), no reader can reach the column.
-- Those rows are "terminal".
--
-- This migration does NOT move or clear anything by itself. It adds:
--  * hash/count/pointer columns (metadata-only ALTER; no table rewrite, no TOAST read);
--  * guards: records become NULL only by the one retirement transition, retired is
--    final, and a retired attempt can never again be referenced by a stage intent
--    (so terminal stays terminal, fail closed);
--  * owner-only candidate listing and retirement RPCs used by an operator export
--    step that writes the exact records text to S3 first, reads it back, and only
--    then asks SQL to verify the SHA-256 and clear the column.
-- No application role gains any privilege. No reader function is replaced.

ALTER TABLE sessions.ingest_claim_attempts
 ADD COLUMN records_sha256 text CHECK(records_sha256 IS NULL OR records_sha256 ~ '^[0-9a-f]{64}$'),
 ADD COLUMN records_count integer CHECK(records_count IS NULL OR records_count BETWEEN 0 AND 500),
 ADD COLUMN records_archive_key text CHECK(records_archive_key IS NULL OR octet_length(records_archive_key) BETWEEN 1 AND 1024),
 ADD COLUMN records_retired_at timestamptz;
ALTER TABLE sessions.ingest_claim_attempts ADD CONSTRAINT ingest_claim_attempts_records_retired_shape CHECK(
 (records_retired_at IS NULL AND records_archive_key IS NULL) OR
 (records_retired_at IS NOT NULL AND records IS NULL AND records_sha256 IS NOT NULL
  AND records_count IS NOT NULL AND records_archive_key IS NOT NULL)) NOT VALID;
-- New columns are NULL on every existing row, so validation needs no TOAST read.
ALTER TABLE sessions.ingest_claim_attempts VALIDATE CONSTRAINT ingest_claim_attempts_records_retired_shape;

-- Records clear only through the retirement transition (non-null -> NULL with the
-- matching hash/count/pointer), and retired is final.
CREATE FUNCTION sessions.ingest_claim_attempt_records_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $guard$
BEGIN
 IF OLD.records_retired_at IS NOT NULL THEN
  IF ROW(NEW.records,NEW.records_sha256,NEW.records_count,NEW.records_archive_key,NEW.records_retired_at)
    IS DISTINCT FROM ROW(OLD.records,OLD.records_sha256,OLD.records_count,OLD.records_archive_key,OLD.records_retired_at)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: retired claim records are final';END IF;
  RETURN NEW;
 END IF;
 IF NEW.records_retired_at IS NOT NULL THEN
  IF OLD.records IS NULL OR NEW.records IS NOT NULL
    OR NEW.records_sha256 IS DISTINCT FROM encode(sha256(convert_to(OLD.records::text,'UTF8')),'hex')
    OR NEW.records_count IS DISTINCT FROM jsonb_array_length(OLD.records)
  THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: invalid claim records retirement';END IF;
  RETURN NEW;
 END IF;
 -- Live rows keep today's behaviour for every non-retirement write; the column can
 -- only become NULL, and the retirement metadata can only appear, via retirement.
 IF OLD.records IS NOT NULL AND NEW.records IS NULL
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: claim records clear only by retirement';END IF;
 IF NEW.records_sha256 IS DISTINCT FROM OLD.records_sha256 OR NEW.records_count IS DISTINCT FROM OLD.records_count
   OR NEW.records_archive_key IS DISTINCT FROM OLD.records_archive_key
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: claim records metadata set only by retirement';END IF;
 RETURN NEW;
END $guard$;
CREATE TRIGGER ingest_claim_attempt_records_guard BEFORE UPDATE OF records,records_sha256,records_count,records_archive_key,records_retired_at
 ON sessions.ingest_claim_attempts FOR EACH ROW EXECUTE FUNCTION sessions.ingest_claim_attempt_records_guard();

-- A retired attempt is never again reachable by a reader: no stage intent may be
-- created for it, re-pointed at it, or brought back from 'redacted'.
CREATE FUNCTION sessions.ingest_stage_intent_retired_attempt_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $stage_guard$
BEGIN
 IF (TG_OP='INSERT' OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
     OR (OLD.cleanup_state='redacted' AND NEW.cleanup_state<>'redacted'))
   AND EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a
     WHERE a.claim_token=NEW.claim_token AND a.records_retired_at IS NOT NULL)
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: claim attempt records are retired';END IF;
 RETURN NEW;
END $stage_guard$;
CREATE TRIGGER ingest_stage_intent_retired_attempt_guard BEFORE INSERT OR UPDATE OF claim_token,cleanup_state
 ON sessions.ingest_stage_intents FOR EACH ROW EXECUTE FUNCTION sessions.ingest_stage_intent_retired_attempt_guard();

-- Deterministic, org- and date-scoped archive key. SQL is the only authority for
-- it; the operator tool never derives keys itself.
CREATE FUNCTION sessions.ingest_claim_attempt_records_key(p_root text,p_org text,p_ws text,p_day timestamptz,p_claim uuid,p_sha text)
RETURNS text LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $records_key$
DECLARE v_ws text;
BEGIN
 IF p_root IS NULL OR p_root!~'^[a-z0-9][a-z0-9-]{0,62}$' OR p_org IS NULL OR p_org!~'^[A-Za-z0-9._-]{1,128}$'
   OR p_org IN('.','..') OR p_ws IS NULL OR p_day IS NULL OR p_claim IS NULL OR p_sha IS NULL OR p_sha!~'^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: claim records key input';END IF;
 v_ws:=regexp_replace(regexp_replace(p_ws,'[^A-Za-z0-9._-]','_','g'),'^\.','_');
 IF v_ws='' THEN v_ws:='_';END IF;
 IF length(v_ws)>128 THEN v_ws:=left(v_ws,119)||'-'||left(encode(sha256(convert_to(p_ws,'UTF8')),'hex'),8);END IF;
 RETURN p_root||'/'||p_org||'/claim-attempt-records/v1/ws='||v_ws||'/d='||to_char(p_day AT TIME ZONE 'UTC','YYYY/MM/DD')
  ||'/c='||left(encode(sha256(convert_to(p_claim::text,'UTF8')),'hex'),32)||'-'||left(p_sha,16)||'.json.zst';
END $records_key$;

-- Terminal = committed batch AND no stage intent of this attempt that is not redacted.
-- Caller holds USER FENCE and BATCH locks when correctness depends on it (retire).
CREATE FUNCTION sessions.ingest_claim_attempt_records_terminal(p_claim uuid) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $terminal$
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
   USING(org_id,workspace_id,origin_id,batch_id)
   WHERE a.claim_token=p_claim AND b.state='committed' AND b.committed_at IS NOT NULL)
 AND NOT EXISTS(SELECT 1 FROM sessions.ingest_stage_intents s
   WHERE s.claim_token=p_claim AND s.cleanup_state<>'redacted')
$terminal$;

-- Owner-only, bounded, keyset-paginated page of terminal attempts that still hold
-- records. Returns the exact records text the archive must contain, its hash, and
-- the SQL-derived archive key. Read-only.
CREATE FUNCTION sessions.ingest_claim_attempt_records_candidates(p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $candidates$
DECLARE v_limit int;v_after uuid;v_root text;v_out jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['keyRoot','limit','afterClaimToken']);
 v_root:=p->>'keyRoot';
 IF jsonb_typeof(p->'keyRoot') IS DISTINCT FROM 'string' OR v_root!~'^[a-z0-9][a-z0-9-]{0,62}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid key root';END IF;
 v_limit:=sessions.ingest_bounded(p->'limit',1,100);
 IF p->'afterClaimToken'='null'::jsonb THEN v_after:='00000000-0000-0000-0000-000000000000';
 ELSE v_after:=(p->>'afterClaimToken')::uuid;END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('claimToken',q.claim_token,'orgId',q.org_id,'recordsText',q.t,
   'recordsSha256',q.h,'recordsCount',q.n,
   'archiveKey',sessions.ingest_claim_attempt_records_key(v_root,q.org_id,q.workspace_id,q.committed_at,q.claim_token,q.h))
   ORDER BY q.claim_token),'[]') INTO v_out
 FROM(SELECT c.claim_token,c.org_id,c.workspace_id,c.committed_at,c.records::text t,
       encode(sha256(convert_to(c.records::text,'UTF8')),'hex') h,jsonb_array_length(c.records) n
  FROM(SELECT a.claim_token,a.org_id,a.workspace_id,a.records,b.committed_at
   FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id)
   WHERE a.claim_token>v_after AND a.records IS NOT NULL AND a.records_retired_at IS NULL
    AND b.state='committed' AND a.org_id~'^[A-Za-z0-9._-]{1,128}$'
    AND NOT EXISTS(SELECT 1 FROM sessions.ingest_stage_intents s WHERE s.claim_token=a.claim_token AND s.cleanup_state<>'redacted')
   ORDER BY a.claim_token LIMIT v_limit)c)q;
 RETURN jsonb_build_object('candidates',v_out);
END $candidates$;

-- Owner-only retirement of ONE attempt after its archive object was written and
-- read back. Takes USER FENCE -> BATCH -> ATTEMPT locks (producer/GC order), then
-- re-proves terminality, hash and key under those locks. Idempotent.
CREATE FUNCTION sessions.ingest_claim_attempt_records_retire(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $retire$
DECLARE a sessions.ingest_claim_attempts%ROWTYPE;b sessions.ingest_batches%ROWTYPE;v_claim uuid;v_sha text;v_key text;v_root text;v_expected text;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['keyRoot','claimToken','recordsSha256','archiveKey']);
 v_root:=p->>'keyRoot';v_claim:=(p->>'claimToken')::uuid;v_sha:=sessions.ingest_digest(p->'recordsSha256');v_key:=p->>'archiveKey';
 IF jsonb_typeof(p->'archiveKey') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: archive key required';END IF;
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=v_claim;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: unknown claim attempt';END IF;
 PERFORM sessions.ingest_lock_fences(a.org_id,a.workspace_id,a.user_id);
 SELECT * INTO b FROM sessions.ingest_batches WHERE org_id=a.org_id AND workspace_id=a.workspace_id
  AND origin_id=a.origin_id AND batch_id=a.batch_id FOR UPDATE;
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=v_claim FOR UPDATE;
 IF a.records_retired_at IS NOT NULL THEN
  IF a.records_sha256=v_sha AND a.records_archive_key=v_key THEN
   RETURN jsonb_build_object('retired',true,'already',true);END IF;
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: claim records retired differently';
 END IF;
 IF a.records IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: claim attempt has no records';END IF;
 IF b.state IS DISTINCT FROM 'committed' OR NOT sessions.ingest_claim_attempt_records_terminal(v_claim)
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim records still reachable';END IF;
 IF encode(sha256(convert_to(a.records::text,'UTF8')),'hex') IS DISTINCT FROM v_sha
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: archive hash mismatch';END IF;
 v_expected:=sessions.ingest_claim_attempt_records_key(v_root,a.org_id,a.workspace_id,b.committed_at,a.claim_token,v_sha);
 IF v_key IS DISTINCT FROM v_expected THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: archive key mismatch';END IF;
 UPDATE sessions.ingest_claim_attempts SET records=NULL,records_sha256=v_sha,records_count=jsonb_array_length(a.records),
  records_archive_key=v_key,records_retired_at=clock_timestamp() WHERE claim_token=v_claim;
 RETURN jsonb_build_object('retired',true,'already',false);
END $retire$;

REVOKE ALL ON FUNCTION sessions.ingest_claim_attempt_records_guard(),sessions.ingest_stage_intent_retired_attempt_guard(),
 sessions.ingest_claim_attempt_records_key(text,text,text,timestamptz,uuid,text),sessions.ingest_claim_attempt_records_terminal(uuid),
 sessions.ingest_claim_attempt_records_candidates(jsonb),sessions.ingest_claim_attempt_records_retire(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
