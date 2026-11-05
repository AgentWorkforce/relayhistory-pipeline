-- Large-read routing fixes: activation blockers for SESSION_LARGE_READ_HANDOFF_ENABLED
-- (#184 findings 2 and 5, delta finding 1; docs/canonical-large-worker-runtime.md).
--  1. discover_session_large_reads isolates each scope's enqueue in its own subtransaction,
--     records the refusal (bounded code and message only) and retries it a bounded number of times.
--  2. route_session_large_read and discovery select the large worker on measured size only:
--     0/NULL raw_bytes/event_count contribute nothing and never delegate a scope by themselves.
--  3. claim_session_large_read isolates each job's capture, so one refusing job is retried with
--     backoff and then failed instead of wedging every claim for every tenant.
-- Signatures, owner, SECURITY DEFINER, search_path and ACLs are unchanged: CREATE OR REPLACE keeps
-- the owner and ACL, so this file has no GRANT/REVOKE on any function.
-- Metadata-only: no payload storage or transcript bytes.
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
DO $creator$
DECLARE me oid;
BEGIN
 SELECT oid INTO STRICT me FROM pg_roles WHERE rolname=current_user;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE oid IN('sessions.route_session_large_read(text,text,text,text)'::regprocedure,
  'sessions.discover_session_large_reads()'::regprocedure,'sessions.claim_session_large_read(uuid,integer)'::regprocedure)AND proowner<>me)
 OR EXISTS(SELECT 1 FROM pg_class WHERE oid IN('sessions.session_large_read_jobs'::regclass,'sessions.session_large_read_scan'::regclass,
  'sessions.session_index'::regclass)AND relowner<>me) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='large read routing: original coherent creator required';END IF;
END $creator$;

-- One row per scope whose discovery enqueue raised. A later successful enqueue deletes it.
-- 'retrying' rows are skipped until available_at; after 5 attempts the row is 'failed' and the
-- scope is skipped by discovery until an operator deletes the row (explicit repair).
-- message is either a known fixed refusal text or 'unclassified': never row data.
CREATE TABLE sessions.session_large_read_scope_errors(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 state text NOT NULL CHECK(state IN('retrying','failed')),
 sqlstate text NOT NULL CHECK(sqlstate~'^[0-9A-Z]{5}$'),
 message text NOT NULL CHECK(length(message) BETWEEN 1 AND 128),
 attempts integer NOT NULL CHECK(attempts BETWEEN 1 AND 5),
 available_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id));
ALTER TABLE sessions.session_large_read_scope_errors ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_large_read_scope_errors FROM PUBLIC,anon,authenticated,service_role,
 bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE OR REPLACE FUNCTION sessions.route_session_large_read(p_org text,p_workspace text,p_harness text,p_session text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $route$
DECLARE c jsonb;b numeric;n numeric;
BEGIN
 c:=sessions.capture_session_replay_pages(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'exists')::boolean OR (c->>'blocked')::boolean THEN RETURN jsonb_build_object('delegated',false); END IF;
 SELECT coalesce(sum(p.raw_bytes),0),coalesce(sum(p.event_count),0) INTO b,n FROM sessions.session_index p
 WHERE p.org_id=p_org AND p.workspace_id=p_workspace AND p.harness=p_harness
 AND coalesce(p.canonical_session_id,p.session_id)=c->'identity'->>'sessionId' AND p.deleted_at IS NULL;
 -- Measured size only: a 0/NULL raw_bytes/event_count adds nothing, so an unmeasured scope stays on
 -- the normal stages. A selector, not a source validation waiver: the large worker and the normal
 -- readers still validate all actual manifests/chunks/records against their own budgets.
 IF b<=67108864 AND n<=100000 THEN RETURN jsonb_build_object('delegated',false); END IF;
 c:=sessions.enqueue_session_large_read(p_org,p_workspace,p_harness,p_session);
 IF NOT (c->>'enqueued')::boolean THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='large read: durable handoff unavailable'; END IF;
 RETURN jsonb_build_object('delegated',true,'identity',c->'identity','source',c->'source');
END $route$;

CREATE OR REPLACE FUNCTION sessions.discover_session_large_reads()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $discover$
DECLARE cursor_id text[];last_id text[];s record;c jsonb;scanned integer:=0;changed integer:=0;failed integer:=0;skipped integer:=0;
 e sessions.session_large_read_scope_errors%ROWTYPE;v_state text;v_msg text;
BEGIN
 SELECT after_identity INTO cursor_id FROM sessions.session_large_read_scan WHERE singleton FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('busy',true,'scanned',0,'changed',0,'cycleComplete',false,'failed',0,'skipped',0); END IF;
 FOR s IN SELECT p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id) canonical_id
 FROM sessions.session_index p WHERE p.deleted_at IS NULL AND (cursor_id IS NULL OR
 ROW(p.org_id COLLATE "C",p.workspace_id COLLATE "C",p.harness COLLATE "C",coalesce(p.canonical_session_id,p.session_id) COLLATE "C")>
 ROW(cursor_id[1] COLLATE "C",cursor_id[2] COLLATE "C",cursor_id[3] COLLATE "C",cursor_id[4] COLLATE "C"))
 GROUP BY p.org_id,p.workspace_id,p.harness,coalesce(p.canonical_session_id,p.session_id)
 HAVING coalesce(sum(p.raw_bytes),0)>67108864 OR coalesce(sum(p.event_count),0)>100000
 ORDER BY p.org_id COLLATE "C",p.workspace_id COLLATE "C",p.harness COLLATE "C",coalesce(p.canonical_session_id,p.session_id) COLLATE "C" LIMIT 128 LOOP
  scanned:=scanned+1;last_id:=ARRAY[s.org_id,s.workspace_id,s.harness,s.canonical_id];
  SELECT * INTO e FROM sessions.session_large_read_scope_errors x
   WHERE x.org_id=s.org_id AND x.workspace_id=s.workspace_id AND x.harness=s.harness AND x.session_id=s.canonical_id FOR UPDATE;
  IF FOUND AND (e.state='failed' OR e.available_at>clock_timestamp()) THEN skipped:=skipped+1; CONTINUE; END IF;
  BEGIN
   c:=sessions.enqueue_session_large_read(s.org_id,s.workspace_id,s.harness,s.canonical_id);
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS v_state=RETURNED_SQLSTATE,v_msg=MESSAGE_TEXT;
   -- Transient/server-wide failures (serialization, resources, operator intervention, system,
   -- internal, connection) are not a property of this scope: abort the page as before.
   IF left(v_state,2) IN('08','40','53','57','58','XX') THEN RAISE; END IF;
   INSERT INTO sessions.session_large_read_scope_errors AS x(org_id,workspace_id,harness,session_id,state,sqlstate,message,attempts,available_at)
   VALUES(s.org_id,s.workspace_id,s.harness,s.canonical_id,'retrying',v_state,
    CASE WHEN length(v_msg)<=128 AND v_msg~'^(replay capture|large read): [a-z ]+$' THEN v_msg ELSE 'unclassified' END,
    1,clock_timestamp()+interval '30 seconds')
   ON CONFLICT(org_id,workspace_id,harness,session_id) DO UPDATE SET
    state=CASE WHEN x.attempts+1>=5 THEN 'failed' ELSE 'retrying' END,sqlstate=excluded.sqlstate,message=excluded.message,
    attempts=least(x.attempts+1,5),available_at=excluded.available_at,updated_at=now();
   failed:=failed+1;
   CONTINUE;
  END;
  DELETE FROM sessions.session_large_read_scope_errors x
   WHERE x.org_id=s.org_id AND x.workspace_id=s.workspace_id AND x.harness=s.harness AND x.session_id=s.canonical_id;
  IF coalesce((c->>'changed')::boolean,false) THEN changed:=changed+1;END IF;
 END LOOP;
 UPDATE sessions.session_large_read_scan SET after_identity=CASE WHEN scanned<128 THEN NULL ELSE last_id END,updated_at=now() WHERE singleton;
 RETURN jsonb_build_object('busy',false,'scanned',scanned,'changed',changed,'cycleComplete',scanned<128,'failed',failed,'skipped',skipped);
END $discover$;

CREATE OR REPLACE FUNCTION sessions.claim_session_large_read(p_nonce uuid,p_lease_seconds integer DEFAULT 120)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE j sessions.session_large_read_jobs%ROWTYPE;c jsonb;v_state text;
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
  BEGIN
   c:=sessions.capture_session_replay_pages(j.org_id,j.workspace_id,j.harness,j.session_id);
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS v_state=RETURNED_SQLSTATE;
   IF left(v_state,2) IN('08','40','53','57','58','XX') THEN RAISE; END IF;
   -- A per-job capture refusal (54000 capacity, 55000 ambiguous/epoch, ...) counts as an attempt
   -- with the retry backoff; the fifth fails the job. Bounded code only, never the message.
   UPDATE sessions.session_large_read_jobs SET state=CASE WHEN attempts+1>=5 THEN 'failed' ELSE 'queued' END,
    available_at=clock_timestamp()+interval '30 seconds',lease_nonce=NULL,lease_expires_at=NULL,attempts=attempts+1,
    last_error='capture_'||lower(v_state),updated_at=now()
    WHERE org_id=j.org_id AND workspace_id=j.workspace_id AND harness=j.harness AND session_id=j.session_id;
   CONTINUE;
  END;
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
