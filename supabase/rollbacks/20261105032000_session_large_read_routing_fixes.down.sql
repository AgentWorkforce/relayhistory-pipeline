-- Rollback for 20261105032000_session_large_read_routing_fixes.sql.
-- Restores the previous bodies verbatim: route_session_large_read and discover_session_large_reads
-- from 20261105021000_session_large_read_jobs.sql, claim_session_large_read from
-- 20261105026000_canonical_history_lineage.sql. CREATE OR REPLACE keeps owner and ACL.
-- Dropping session_large_read_scope_errors forgets recorded discovery refusals; the restored
-- discovery no longer reads them. Jobs failed by isolated claim refusals stay failed.
DO $rollback_guard$
DECLARE v_later boolean;
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_migrations WHERE name LIKE ''upstream:%'' AND name COLLATE "C">''upstream:20261105032000_session_large_read_routing_fixes.sql'')'
      INTO v_later;
    IF v_later THEN
      RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='large read routing fixes: a later migration is applied; roll it back first';
    END IF;
  END IF;
END
$rollback_guard$;
CREATE OR REPLACE FUNCTION sessions.route_session_large_read(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $route$
DECLARE c jsonb;b numeric;n numeric;unknown_count bigint;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('delegated',false); END IF;
 SELECT coalesce(sum(p.raw_bytes),0),coalesce(sum(p.event_count),0),count(*) FILTER(WHERE p.raw_bytes IS NULL OR p.event_count IS NULL OR p.raw_bytes=0 OR p.event_count=0) INTO b,n,unknown_count FROM sessions.session_index p
 WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
 AND coalesce(p.canonical_session_id,p.session_id)=c->'identity'->>'sessionId' AND p.deleted_at IS NULL;
 -- A conservative selector, not a source validation waiver. The large worker
 -- still validates all actual manifests/chunks/records using its explicit budget.
 IF unknown_count=0 AND b<=67108864 AND n<=100000 THEN RETURN jsonb_build_object('delegated',false); END IF;
 c:=sessions.enqueue_session_large_read(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'enqueued')::boolean THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='large read: durable handoff unavailable'; END IF;
 RETURN jsonb_build_object('delegated',true,'identity',c->'identity','source',c->'source');
END $route$;

CREATE OR REPLACE FUNCTION sessions.discover_session_large_reads()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $discover$
DECLARE cursor_id text[];last_id text[];s record;c jsonb;scanned integer:=0;changed integer:=0;
BEGIN
 SELECT after_identity INTO cursor_id FROM sessions.session_large_read_scan WHERE singleton FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('busy',true,'scanned',0,'changed',0,'cycleComplete',false); END IF;
 FOR s IN SELECT p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id) canonical_id
 FROM sessions.session_index p WHERE p.deleted_at IS NULL AND (cursor_id IS NULL OR
 ROW(p.org_id COLLATE "C",p.workspace_id COLLATE "C",p.harness COLLATE "C",coalesce(p.canonical_session_id,p.session_id) COLLATE "C")>
 ROW(cursor_id[1] COLLATE "C",cursor_id[2] COLLATE "C",cursor_id[3] COLLATE "C",cursor_id[4] COLLATE "C"))
 GROUP BY p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id)
 HAVING bool_or(p.raw_bytes IS NULL OR p.event_count IS NULL OR p.raw_bytes=0 OR p.event_count=0) OR sum(p.raw_bytes)>67108864 OR sum(p.event_count)>100000
 ORDER BY p.org_id COLLATE "C",p.workspace_id COLLATE "C",p.harness COLLATE "C",coalesce(p.canonical_session_id,p.session_id) COLLATE "C" LIMIT 128 LOOP
  scanned:=scanned+1;last_id:=ARRAY[s.org_id,s.workspace_id,s.harness,s.canonical_id];
  c:=sessions.enqueue_session_large_read(s.org_id,s.workspace_id,s.harness,s.canonical_id);
  IF coalesce((c->>'changed')::boolean,false) THEN changed:=changed+1;END IF;
 END LOOP;
 UPDATE sessions.session_large_read_scan SET after_identity=CASE WHEN scanned<128 THEN NULL ELSE last_id END,updated_at=now() WHERE singleton;
 RETURN jsonb_build_object('busy',false,'scanned',scanned,'changed',changed,'cycleComplete',scanned<128);
END $discover$;

CREATE OR REPLACE FUNCTION sessions.claim_session_large_read(p_nonce uuid,p_lease_seconds integer DEFAULT 120)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE j sessions.session_large_read_jobs%ROWTYPE;c jsonb;
BEGIN
 IF p_nonce IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 30 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='large read: invalid lease'; END IF;
 FOR j IN SELECT * FROM sessions.session_large_read_jobs WHERE
 ((state='queued' AND available_at<=clock_timestamp()) OR (state='running' AND lease_expires_at<=clock_timestamp()))
 AND NOT(sessions.ingest_history_live()AND EXISTS(SELECT 1 FROM sessions.ingest_history_scope_current h WHERE h.org_id=session_large_read_jobs.org_id AND h.workspace_id=session_large_read_jobs.workspace_id AND h.harness=session_large_read_jobs.harness AND h.session_id=session_large_read_jobs.session_id))
 ORDER BY available_at,updated_at LIMIT 100 FOR UPDATE SKIP LOCKED LOOP
  IF j.attempts>=5 THEN
   UPDATE sessions.session_large_read_jobs SET state='failed',lease_nonce=NULL,lease_expires_at=NULL,last_error='attempts_exhausted',updated_at=now() WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END IF;
  c:=sessions.capture_session_replay_pages(j.org_id,j.workspace_id,j.harness,j.session_id);
  IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean OR c->'source'->>'epoch' IS DISTINCT FROM j.source_epoch::text
   OR c->'source'->>'fingerprint' IS DISTINCT FROM j.source_fingerprint THEN
   UPDATE sessions.session_large_read_jobs SET state='superseded',lease_nonce=NULL,lease_expires_at=NULL,updated_at=now()
    WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END IF;
  UPDATE sessions.session_large_read_jobs SET state='running',lease_nonce=p_nonce,lease_expires_at=clock_timestamp()+make_interval(secs=>p_lease_seconds),attempts=attempts+1,last_error=NULL,updated_at=now()
   WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
  RETURN jsonb_build_object('identity',c->'identity','source',c->'source','leaseNonce',p_nonce::text);
 END LOOP;
 RETURN NULL;
END $claim$;

DROP TABLE IF EXISTS sessions.session_large_read_scope_errors;
DO $ledger$
BEGIN
  IF to_regclass('sessions.__96bulls_migrations') IS NOT NULL THEN
    DELETE FROM sessions.__96bulls_migrations
      WHERE name='upstream:20261105032000_session_large_read_routing_fixes.sql';
  END IF;
END
$ledger$;
