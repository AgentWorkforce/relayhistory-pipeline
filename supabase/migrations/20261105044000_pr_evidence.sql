SET LOCAL lock_timeout='5s';
-- F3 PR evidence, registry slot 044000 (confirmed by the registry owner). The PR-evidence source that
-- Cloud's F10 consumer (#6037) reads instead of Neon's sessions.session_pull_request_evidence_v1.
-- Depends on: session replay snapshot (05020000/05026000), canonical_feature_journal (05025000),
-- F10 project events (05033000). Replaces no existing function.
--
-- Contract:
--  * Evidence is derived by the SP session-pr-evidence worker from the canonical S3 source (the
--    same reader/projection as replay and search), never from GitHub or Neon. Rows are metadata
--    only (URL/owner/repo/number/branch/tool id+name/kind/role/two timestamps); no transcript text.
--  * Each scope's evidence is bound to the exact replay-source snapshot it was read from (epoch +
--    sha256 fingerprint, the publish_session_replay_pages fence). F10 events carry the same
--    fingerprint, so Cloud is served evidence ONLY for the event's own source; otherwise 'pending'.
--  * Default off: no settings row = no wakeups, no claims. Enabling is an operator action.
--  * Cloud's actor reads through its own event only; there is no arbitrary-session lookup.
--  * Retention-ready: every row carries org_id plus built_at/requested_at.
CREATE TABLE sessions.session_pr_evidence_settings(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 enabled boolean NOT NULL DEFAULT false,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions.session_pr_evidence_wakeups(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 request_version bigint NOT NULL DEFAULT 1 CHECK(request_version>0),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_token uuid,lease_until timestamptz,
 last_error text CHECK(last_error IS NULL OR last_error~'^[a-z0-9_]{1,64}$'),
 PRIMARY KEY(org_id,workspace_id,harness,session_id),
 CHECK((lease_token IS NULL)=(lease_until IS NULL))
);
CREATE INDEX session_pr_evidence_wakeups_due ON sessions.session_pr_evidence_wakeups(not_before,requested_at);
CREATE TABLE sessions.session_pr_evidence_state(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 source_epoch bigint NOT NULL CHECK(source_epoch>0),
 source_fingerprint text NOT NULL CHECK(source_fingerprint~'^[0-9a-f]{64}$'),
 extractor_version text NOT NULL CHECK(extractor_version~'^[a-z0-9][a-z0-9.+-]{0,63}$'),
 status text NOT NULL CHECK(status IN('ready','incomplete')),
 reason text,
 row_count integer NOT NULL CHECK(row_count BETWEEN 0 AND 10000),
 evidence_sha256 text NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
 built_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id),
 CHECK((status='ready'AND reason IS NULL)OR(status='incomplete'AND reason~'^[a-z0-9_]{1,64}$'AND row_count=0))
);
CREATE INDEX session_pr_evidence_state_org_time ON sessions.session_pr_evidence_state(org_id,built_at);
CREATE TABLE sessions.session_pr_evidence(
 org_id text NOT NULL,workspace_id text NOT NULL,harness text NOT NULL,session_id text NOT NULL,
 ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 9999),
 pull_request_url text CHECK(pull_request_url IS NULL OR pull_request_url~'^https://github\.com/[A-Za-z0-9-]+/[A-Za-z0-9_.-]+/pull/[0-9]{1,9}$'),
 owner text NOT NULL CHECK(owner~'^[A-Za-z0-9-]{1,255}$'),
 repo text NOT NULL CHECK(repo~'^[A-Za-z0-9_.-]{1,255}$'),
 number integer CHECK(number IS NULL OR number BETWEEN 0 AND 999999999),
 head_ref text CHECK(head_ref IS NULL OR octet_length(head_ref)<=4096),
 tool_use_id text CHECK(tool_use_id IS NULL OR octet_length(tool_use_id) BETWEEN 1 AND 1024),
 tool_name text CHECK(tool_name IS NULL OR octet_length(tool_name)<=1024),
 evidence_kind text NOT NULL CHECK(evidence_kind IN('gh_pr_create','mcp_create_pull_request','commit_link',
  'mcp_pull_request_tool','gh_pr_command','git_push')),
 role text NOT NULL CHECK(role IN('creator','contributor')),
 detected_at timestamptz NOT NULL,last_seen_at timestamptz NOT NULL,
 built_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,harness,session_id,ordinal),
 FOREIGN KEY(org_id,workspace_id,harness,session_id)REFERENCES sessions.session_pr_evidence_state ON DELETE CASCADE,
 CHECK((number IS NULL)=(pull_request_url IS NULL)),
 CHECK(number IS NOT NULL OR head_ref<>''),
 CHECK(detected_at<=last_seen_at)
);
ALTER TABLE sessions.session_pr_evidence_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_pr_evidence_wakeups ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_pr_evidence_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_pr_evidence ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_pr_evidence_settings,sessions.session_pr_evidence_wakeups,sessions.session_pr_evidence_state,
 sessions.session_pr_evidence FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_project_events;

-- Arms a wakeup for every canonical publish while enabled, for ALL scopes (independent of the F10
-- canary rows), so evidence is complete before F10 widens. Never fails the creator transaction on
-- a malformed scope; it only skips it.
CREATE FUNCTION sessions.session_pr_evidence_journal_changed()RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $changed$
DECLARE h text:=NEW.scope->>'harness';s text:=NEW.scope->>'sessionId';
BEGIN
 IF NEW.kind<>'publish'OR h IS NULL OR s IS NULL OR h=''OR s=''
  OR NOT EXISTS(SELECT 1 FROM sessions.session_pr_evidence_settings WHERE enabled)THEN RETURN NEW;END IF;
 INSERT INTO sessions.session_pr_evidence_wakeups AS w(org_id,workspace_id,harness,session_id)VALUES(NEW.org_id,NEW.workspace_id,h,s)
 ON CONFLICT(org_id,workspace_id,harness,session_id)DO UPDATE SET request_version=w.request_version+1,requested_at=clock_timestamp(),
  not_before=clock_timestamp(),attempts=0,last_error=NULL;
 RETURN NEW;
END $changed$;
CREATE TRIGGER session_pr_evidence_after_journal AFTER INSERT ON sessions.canonical_feature_journal
 FOR EACH ROW EXECUTE FUNCTION sessions.session_pr_evidence_journal_changed();

-- Erasure: a deleted or tombstoned logical session loses its evidence and wakeup in the same txn.
CREATE FUNCTION sessions.session_pr_evidence_forget()RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $forget$
DECLARE o record;
BEGIN
 IF TG_OP='UPDATE'AND NEW.deleted_at IS NULL THEN RETURN NEW;END IF;
 o:=CASE WHEN TG_OP='DELETE'THEN OLD ELSE NEW END;
 DELETE FROM sessions.session_pr_evidence_state WHERE org_id=o.org_id AND workspace_id=o.workspace_id AND harness=o.harness AND session_id=o.session_id;
 DELETE FROM sessions.session_pr_evidence_wakeups WHERE org_id=o.org_id AND workspace_id=o.workspace_id AND harness=o.harness AND session_id=o.session_id;
 RETURN NULL;
END $forget$;
CREATE TRIGGER session_pr_evidence_forget AFTER DELETE OR UPDATE OF deleted_at ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.session_pr_evidence_forget();

CREATE FUNCTION sessions.session_pr_evidence_scope(p jsonb)RETURNS text[]
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $scope$
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['orgId','workspaceId','source','sessionId']);
 IF jsonb_typeof(p->'orgId')IS DISTINCT FROM'string'OR jsonb_typeof(p->'workspaceId')IS DISTINCT FROM'string'
  OR jsonb_typeof(p->'source')IS DISTINCT FROM'string'OR jsonb_typeof(p->'sessionId')IS DISTINCT FROM'string'
  OR p->>'orgId'=''OR p->>'workspaceId'=''OR p->>'source'!~'^[a-z0-9][a-z0-9._-]{0,63}$'OR char_length(p->>'sessionId')NOT BETWEEN 1 AND 255
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='pr evidence: invalid scope';END IF;
 RETURN ARRAY[p->>'orgId',p->>'workspaceId',p->>'source',p->>'sessionId'];
END $scope$;

-- Worker: lease due wakeups and capture each scope's exact replay source (pointers + fingerprint).
CREATE FUNCTION sessions.session_pr_evidence_claim_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE w sessions.session_pr_evidence_wakeups%ROWTYPE;r jsonb:='[]';n integer;t uuid;c jsonb;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit']);
 IF jsonb_typeof(p->'limit')IS DISTINCT FROM'number'OR(p->>'limit')::numeric<>trunc((p->>'limit')::numeric)OR(p->>'limit')::numeric NOT BETWEEN 1 AND 10
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='pr evidence: invalid batch bound';END IF;n:=(p->>'limit')::integer;
 IF NOT EXISTS(SELECT 1 FROM sessions.session_pr_evidence_settings WHERE enabled)THEN RETURN r;END IF;
 FOR w IN SELECT*FROM sessions.session_pr_evidence_wakeups WHERE not_before<=clock_timestamp()AND(lease_until IS NULL OR lease_until<=clock_timestamp())
  ORDER BY requested_at,org_id,workspace_id,harness,session_id LIMIT n FOR UPDATE SKIP LOCKED LOOP
  BEGIN
   c:=sessions.capture_session_replay_pages(w.org_id,w.workspace_id,w.harness,w.session_id);
  EXCEPTION WHEN OTHERS THEN
   UPDATE sessions.session_pr_evidence_wakeups SET attempts=attempts+1,last_error='capture_refused',
    not_before=clock_timestamp()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(attempts,10))))
    WHERE org_id=w.org_id AND workspace_id=w.workspace_id AND harness=w.harness AND session_id=w.session_id;
   CONTINUE;
  END;
  -- History-authoritative scopes: the legacy replay capture reports them blocked; like
  -- session_history_capture (05026000), a ready history authority on a live directory row is readable.
  IF c#>>'{snapshot,history,required}'='true'AND c#>>'{snapshot,history,ready}'='true'
   AND jsonb_typeof(c#>'{snapshot,directory}')='object'AND c#>>'{snapshot,directory,deleted_at}'IS NULL THEN
   c:=jsonb_set(c,'{blocked}','false'::jsonb);END IF;
  t:=gen_random_uuid();
  -- A lease that expires without an install (a slow or crashed read) is not re-claimed before
  -- its backoff: not_before moves with every claim; a new publish resets it.
  UPDATE sessions.session_pr_evidence_wakeups SET lease_token=t,lease_until=clock_timestamp()+interval'300 seconds',attempts=attempts+1,
   not_before=clock_timestamp()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(attempts,10))))
   WHERE org_id=w.org_id AND workspace_id=w.workspace_id AND harness=w.harness AND session_id=w.session_id;
  r:=r||jsonb_build_array(jsonb_build_object('scope',jsonb_build_object('orgId',w.org_id,'workspaceId',w.workspace_id,'source',w.harness,'sessionId',w.session_id),
   'requestVersion',w.request_version::text,'leaseToken',t::text,'exists',c->'exists','blocked',c->'blocked','source',c->'source',
   'manifestKeys',coalesce((SELECT jsonb_agg(x->>'manifest_key' ORDER BY ord)FROM jsonb_array_elements(c#>'{snapshot,pointers}')WITH ORDINALITY AS a(x,ord)),'[]'::jsonb)));
 END LOOP;
 RETURN r;
END $claim$;

-- Worker: install one scope's evidence for the exact source it read, under the replay-publication row
-- lock every pointer mutation also takes. Any drift refuses; the wakeup stays pending for a re-read.
CREATE FUNCTION sessions.session_pr_evidence_install_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $install$
DECLARE k text[];w sessions.session_pr_evidence_wakeups%ROWTYPE;live jsonb;fp text;st text;n integer;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['scope','requestVersion','leaseToken','source','extractorVersion','status','reason','rows']);
 k:=sessions.session_pr_evidence_scope(p->'scope');
 PERFORM sessions.ingest_exact_keys(p->'source',ARRAY['epoch','fingerprint']);
 st:=p->>'status';
 IF jsonb_typeof(p->'requestVersion')IS DISTINCT FROM'string'OR p->>'requestVersion'!~'^[1-9][0-9]{0,18}$'
  OR jsonb_typeof(p->'leaseToken')IS DISTINCT FROM'string'OR p->>'leaseToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  OR jsonb_typeof(p#>'{source,epoch}')IS DISTINCT FROM'string'OR p#>>'{source,epoch}'!~'^[1-9][0-9]{0,18}$'
  OR jsonb_typeof(p#>'{source,fingerprint}')IS DISTINCT FROM'string'OR p#>>'{source,fingerprint}'!~'^[0-9a-f]{64}$'
  OR jsonb_typeof(p->'extractorVersion')IS DISTINCT FROM'string'
  OR st IS NULL OR st NOT IN('ready','incomplete','absent')OR jsonb_typeof(p->'rows')IS DISTINCT FROM'array'
  OR(st='ready'AND p->'reason'<>'null'::jsonb)OR(st<>'ready'AND(jsonb_typeof(p->'reason')IS DISTINCT FROM'string'OR jsonb_array_length(p->'rows')<>0))
  OR jsonb_array_length(p->'rows')>10000
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='pr evidence: invalid install';END IF;
 -- Lock order: the replay-publication row first (as pointer mutations and the creator path do),
 -- then the wakeup row the journal trigger also updates. Never the reverse.
 PERFORM 1 FROM sessions.session_replay_publications a WHERE a.org_id=k[1]AND a.workspace_id=k[2]AND a.harness=k[3]AND a.session_id=k[4]
  AND a.epoch=(p#>>'{source,epoch}')::bigint FOR UPDATE;
 IF FOUND THEN live:=sessions.session_replay_source_snapshot(k[1],k[2],k[3],k[4]);END IF;
 SELECT*INTO w FROM sessions.session_pr_evidence_wakeups WHERE org_id=k[1]AND workspace_id=k[2]AND harness=k[3]AND session_id=k[4]
  AND lease_token=(p->>'leaseToken')::uuid AND lease_until>clock_timestamp()FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('ok',false,'code','lease_lost');END IF;
 fp:=CASE WHEN live IS NOT NULL THEN encode(sha256(convert_to(live::text,'UTF8')),'hex')END;
 IF fp IS DISTINCT FROM p#>>'{source,fingerprint}'THEN
  UPDATE sessions.session_pr_evidence_wakeups SET lease_token=NULL,lease_until=NULL,last_error='source_changed'
   WHERE org_id=k[1]AND workspace_id=k[2]AND harness=k[3]AND session_id=k[4];
  RETURN jsonb_build_object('ok',false,'code','source_changed');
 END IF;
 DELETE FROM sessions.session_pr_evidence_state WHERE org_id=k[1]AND workspace_id=k[2]AND harness=k[3]AND session_id=k[4];
 IF st<>'absent'THEN
  n:=jsonb_array_length(p->'rows');
  FOR i IN 0..n-1 LOOP
   PERFORM sessions.ingest_exact_keys(p->'rows'->i,ARRAY['pull_request_url','owner','repo','number','head_ref','tool_use_id','tool_name',
    'evidence_kind','role','detected_at','last_seen_at']);
  END LOOP;
  INSERT INTO sessions.session_pr_evidence_state(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint,extractor_version,status,reason,row_count,evidence_sha256)
   VALUES(k[1],k[2],k[3],k[4],(p#>>'{source,epoch}')::bigint,fp,p->>'extractorVersion',st,CASE WHEN st='ready'THEN NULL ELSE p->>'reason'END,n,
    encode(sha256(convert_to((p->'rows')::text,'UTF8')),'hex'));
  INSERT INTO sessions.session_pr_evidence(org_id,workspace_id,harness,session_id,ordinal,pull_request_url,owner,repo,number,head_ref,
   tool_use_id,tool_name,evidence_kind,role,detected_at,last_seen_at)
  SELECT k[1],k[2],k[3],k[4],(e.ord-1)::integer,x.pull_request_url,x.owner,x.repo,x.number,x.head_ref,x.tool_use_id,x.tool_name,
   x.evidence_kind,x.role,x.detected_at,x.last_seen_at
  FROM jsonb_array_elements(p->'rows')WITH ORDINALITY AS e(v,ord)
  CROSS JOIN LATERAL jsonb_to_record(e.v)AS x(pull_request_url text,owner text,repo text,number integer,head_ref text,tool_use_id text,
   tool_name text,evidence_kind text,role text,detected_at timestamptz,last_seen_at timestamptz);
 END IF;
 IF w.request_version=(p->>'requestVersion')::bigint THEN
  DELETE FROM sessions.session_pr_evidence_wakeups WHERE org_id=k[1]AND workspace_id=k[2]AND harness=k[3]AND session_id=k[4];
 ELSE
  -- A newer publish arrived mid-build: keep the wakeup, due now, for the next read.
  UPDATE sessions.session_pr_evidence_wakeups SET lease_token=NULL,lease_until=NULL,not_before=clock_timestamp(),attempts=0,last_error=NULL
   WHERE org_id=k[1]AND workspace_id=k[2]AND harness=k[3]AND session_id=k[4];
 END IF;
 RETURN jsonb_build_object('ok',true,'status',st);
END $install$;

-- Worker: give a claimed wakeup back with a code and bounded backoff.
CREATE FUNCTION sessions.session_pr_evidence_release_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $release$
DECLARE k text[];
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['scope','leaseToken','code']);
 k:=sessions.session_pr_evidence_scope(p->'scope');
 IF jsonb_typeof(p->'leaseToken')IS DISTINCT FROM'string'OR p->>'leaseToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  OR jsonb_typeof(p->'code')IS DISTINCT FROM'string'OR p->>'code'!~'^[a-z0-9_]{1,64}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='pr evidence: invalid release';END IF;
 UPDATE sessions.session_pr_evidence_wakeups SET lease_token=NULL,lease_until=NULL,last_error=p->>'code',
  not_before=clock_timestamp()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(greatest(attempts-1,0),10))))
  WHERE org_id=k[1]AND workspace_id=k[2]AND harness=k[3]AND session_id=k[4]AND lease_token=(p->>'leaseToken')::uuid;
 RETURN jsonb_build_object('ok',FOUND);
END $release$;

-- Cloud (bulls96_project_events): the evidence of ONE unacknowledged F10 event at its exact
-- generation, only when built from that event's own source. Never fabricates empty evidence.
CREATE FUNCTION sessions.session_pr_evidence_read_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $read$
DECLARE q sessions.canonical_project_events%ROWTYPE;s sessions.session_pr_evidence_state%ROWTYPE;rows jsonb;
BEGIN
 PERFORM sessions.canonical_project_events_guard(p,false);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['schemaGeneration','scope','eventId','generation']);
 IF jsonb_typeof(p->'eventId')IS DISTINCT FROM'string'OR p->>'eventId'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  OR jsonb_typeof(p->'generation')IS DISTINCT FROM'string'OR p->>'generation'!~'^[1-9][0-9]{0,18}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='pr evidence: invalid event';END IF;
 SELECT*INTO q FROM sessions.canonical_project_events WHERE event_id=(p->>'eventId')::uuid AND org_id=p#>>'{scope,orgId}'
  AND workspace_id=p#>>'{scope,workspaceId}'AND schema_generation=p->>'schemaGeneration'AND generation=(p->>'generation')::bigint
  AND acknowledged_at IS NULL;
 IF NOT FOUND THEN RETURN jsonb_build_object('status','missing');END IF;
 IF NOT sessions.canonical_project_event_is_current(q,p->>'schemaGeneration',false)THEN RETURN jsonb_build_object('status','deferred');END IF;
 IF q.publication_kind='retire'THEN RETURN jsonb_build_object('status','ready','retired',true,'evidence','[]'::jsonb);END IF;
 SELECT*INTO s FROM sessions.session_pr_evidence_state WHERE org_id=q.org_id AND workspace_id=q.workspace_id AND harness=q.harness AND session_id=q.session_id;
 IF NOT FOUND OR s.source_epoch IS DISTINCT FROM q.source_epoch OR s.source_fingerprint IS DISTINCT FROM q.source_fingerprint THEN
  RETURN jsonb_build_object('status','pending');END IF;
 IF s.status<>'ready'THEN RETURN jsonb_build_object('status','incomplete','reason',s.reason);END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('pull_request_url',e.pull_request_url,'owner',e.owner,'repo',e.repo,'number',e.number,
   'head_ref',e.head_ref,'tool_use_id',e.tool_use_id,'tool_name',e.tool_name,'evidence_kind',e.evidence_kind,'role',e.role,
   'detected_at',e.detected_at,'last_seen_at',e.last_seen_at)ORDER BY e.ordinal),'[]'::jsonb)INTO rows
  FROM sessions.session_pr_evidence e WHERE e.org_id=q.org_id AND e.workspace_id=q.workspace_id AND e.harness=q.harness AND e.session_id=q.session_id;
 IF jsonb_array_length(rows)<>s.row_count THEN RAISE EXCEPTION USING ERRCODE='XX000',MESSAGE='pr evidence: row count drift';END IF;
 RETURN jsonb_build_object('status','ready','retired',false,'extractorVersion',s.extractor_version,'evidence',rows);
END $read$;

-- Operator only (no grant): bounded backfill of directory scopes with no evidence state yet.
CREATE FUNCTION sessions.session_pr_evidence_enqueue_v1(p jsonb)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $enqueue$
DECLARE n integer;a text[];armed integer;last text[];
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit','after']);
 IF jsonb_typeof(p->'limit')IS DISTINCT FROM'number'OR(p->>'limit')::numeric<>trunc((p->>'limit')::numeric)OR(p->>'limit')::numeric NOT BETWEEN 1 AND 5000
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='pr evidence: invalid backfill bound';END IF;n:=(p->>'limit')::integer;
 IF p->'after'<>'null'::jsonb THEN a:=sessions.session_pr_evidence_scope(p->'after');END IF;
 WITH page AS(SELECT d.org_id,d.workspace_id,d.harness,d.session_id FROM sessions.session_directory d
   WHERE d.deleted_at IS NULL AND(a IS NULL OR(d.org_id,d.workspace_id,d.harness,d.session_id)>(a[1],a[2],a[3],a[4]))
   ORDER BY d.org_id,d.workspace_id,d.harness,d.session_id LIMIT n),
  ins AS(INSERT INTO sessions.session_pr_evidence_wakeups(org_id,workspace_id,harness,session_id)
   SELECT x.* FROM page x WHERE NOT EXISTS(SELECT 1 FROM sessions.session_pr_evidence_state s WHERE s.org_id=x.org_id
    AND s.workspace_id=x.workspace_id AND s.harness=x.harness AND s.session_id=x.session_id)
   ON CONFLICT DO NOTHING RETURNING 1)
 SELECT(SELECT count(*)FROM ins),(SELECT ARRAY[org_id,workspace_id,harness,session_id]FROM page ORDER BY org_id DESC,workspace_id DESC,harness DESC,session_id DESC LIMIT 1)
 INTO armed,last;
 RETURN jsonb_build_object('armed',armed,'next',CASE WHEN last IS NULL THEN NULL ELSE
  jsonb_build_object('orgId',last[1],'workspaceId',last[2],'source',last[3],'sessionId',last[4])END);
END $enqueue$;

REVOKE ALL ON FUNCTION sessions.session_pr_evidence_journal_changed(),sessions.session_pr_evidence_forget(),
 sessions.session_pr_evidence_scope(jsonb),sessions.session_pr_evidence_claim_v1(jsonb),sessions.session_pr_evidence_install_v1(jsonb),
 sessions.session_pr_evidence_release_v1(jsonb),sessions.session_pr_evidence_read_v1(jsonb),sessions.session_pr_evidence_enqueue_v1(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_project_events;
GRANT EXECUTE ON FUNCTION sessions.session_pr_evidence_claim_v1(jsonb),sessions.session_pr_evidence_install_v1(jsonb),
 sessions.session_pr_evidence_release_v1(jsonb)TO service_role;
GRANT EXECUTE ON FUNCTION sessions.session_pr_evidence_read_v1(jsonb)TO bulls96_project_events;
