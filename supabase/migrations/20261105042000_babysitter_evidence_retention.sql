-- Slot 20261105042000 assigned by the registry owner (layered-sessions-pipeline).
-- Lands only after 20261105040000 (#288) and 20261105041000 (#289) are on main.
--
-- Babysitter evidence retention (session_lineage / turn_receipt, retention_class
-- standard_30d, payload.expires_at_ms) on the new store. The Neon sweep
-- (relayhistory-cloud expireBabysitterEvidence) cleared payloads in Neon only.
-- Here the payload bytes live in S3 only, so this migration holds METADATA ONLY:
-- which retention buckets exist, when they may be erased, how far a large erase got
-- (a content-free resumable checkpoint), and a CONTENT-FREE key index of the evidence
-- records (identity, revision, operation, line position and expiry; never a payload).
--
--  * babysitter_retention_settings: owner-only singleton kill switch. Default
--    enabled=false, dry_run=true. Nothing is ever claimed while disabled, and an
--    erase cannot be recorded while dry_run is set.
--  * babysitter_retention_objects: one row per retention bucket, the per-(machine,
--    source, session, reset epoch, expiry hour) directory
--    h=_babysitter/d=<erase date>/s=<object_ref>/ holding one create-only object per
--    batch (no manifest, no pointer). Never collapsed across sessions, so the
--    per-session deletion fence stays immediate. A bucket holds ONLY Babysitter
--    lines: every other copy of the record (session chunk, staged and buffered
--    objects, canonical generations) is a payload-free stub, so no mixed object is
--    ever registered or erased.
--    Rows are registered BEFORE the bucket PUT, so a bucket without a row cannot
--    exist. The reset epoch is part of the bucket key: a write after a user reset
--    always opens a new bucket, so a reset never erases post-reset evidence.
--  * babysitter_retention_audit: append-only, counts and bucket coordinates only.
--  * babysitter_evidence_index: the content-free key index the reader pages by Neon's native
--    (origin_id, record_id) order, one row per (record, revision) routed under retention:
--    upserts point at their exact bucket object line; DELETES are content-free tombstone
--    rows (no object), so include_deleted returns Neon's delete row exactly once. Rows die
--    with their bucket's erase (and are hidden the moment the bucket is fenced or expired).
--
-- Erase/write fence (no lost writes): a registration is refused while the bucket
-- is leased, and a bucket is claimable only HOLD_OFF (15 min, far past a request's
-- wall time) after its last registration. Every PUT happens inside its own
-- request, right after its registration, so no PUT can be in flight while an
-- erase runs. The sweeper also never starts an erase with less than 2 min of
-- lease left. write_seq is checked again at completion as defense in depth.
--
-- Due (after the hold-off): the expiry hour has passed, or the bucket's session is
-- deleted, or its user reset after the bucket's epoch. The revision fence
-- (ingest_record_revisions) is never touched: a re-sent equal revision stays stale.
--
-- Plain transactional DDL only (no CONCURRENTLY, no VACUUM): migrate.ts wraps the
-- file in BEGIN/COMMIT with its ledger insert. The new tables are empty, so the
-- index build is trivial. No role is created or altered.

CREATE TABLE sessions.babysitter_retention_settings(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 enabled boolean NOT NULL DEFAULT false,
 dry_run boolean NOT NULL DEFAULT true,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO sessions.babysitter_retention_settings(singleton,enabled,dry_run) VALUES(true,false,true);

CREATE TABLE sessions.babysitter_retention_objects(
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
 machine_id text NOT NULL CHECK(machine_id~'^srv_[0-9a-f]{32}$'),
 object_ref text NOT NULL CHECK(object_ref~'^_bx[0-9]{10}-[0-9a-f]{16}$'),
 -- The records' own (source, session); NULL session = the origin's no-session evidence.
 source text NOT NULL CHECK(octet_length(source) BETWEEN 1 AND 512),
 session_id text CHECK(session_id IS NULL OR octet_length(session_id) BETWEEN 1 AND 512),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),
 -- The session's upload fence epoch (ingest_fence_versions) the CLAIM saw; 0 for no-session evidence.
 -- It increments on every disable/reset and survives re-enable, so a bucket written before a
 -- disable stays fenced (hidden and erasable) forever, even after the session is re-enabled.
 fence_epoch bigint NOT NULL CHECK(fence_epoch>=0),
 erase_after timestamptz NOT NULL,
 write_seq bigint NOT NULL DEFAULT 1 CHECK(write_seq>0),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN('pending','leased','erased')),
 lease_token uuid,lease_until timestamptz,leased_write_seq bigint,lease_reason text CHECK(lease_reason IN('expired','fenced')),
 registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 erased_at timestamptz,erased_versions bigint CHECK(erased_versions IS NULL OR erased_versions>=0),
 erase_reason text CHECK(erase_reason IN('expired','fenced')),
 -- A large bucket erases across bounded runs (eraser.ts eraseStep): the content-free resume
 -- checkpoint {v, bucket, prefix, scope, afterKey, inKey} of the last run whose deletes
 -- succeeded. It is bound (scope) to the bucket, generation, epochs and write_seq; a new
 -- registration clears it. NULL: start from the beginning. Never proof of erasure.
 erase_checkpoint jsonb CHECK(erase_checkpoint IS NULL OR (jsonb_typeof(erase_checkpoint)='object' AND octet_length(erase_checkpoint::text)<=8192)),
 erase_runs integer NOT NULL DEFAULT 0 CHECK(erase_runs>=0),
 PRIMARY KEY(org_id,workspace_id,machine_id,object_ref),
 CHECK((state='leased')=(lease_token IS NOT NULL AND lease_until IS NOT NULL AND leased_write_seq IS NOT NULL AND lease_reason IS NOT NULL)),
 CHECK((state='erased')=(erased_at IS NOT NULL AND erased_versions IS NOT NULL AND erase_reason IS NOT NULL))
);
CREATE INDEX babysitter_retention_objects_due_idx ON sessions.babysitter_retention_objects(state,erase_after);
CREATE UNIQUE INDEX babysitter_retention_objects_lease_idx ON sessions.babysitter_retention_objects(lease_token) WHERE lease_token IS NOT NULL;

CREATE TABLE sessions.babysitter_retention_audit(
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 at timestamptz NOT NULL DEFAULT clock_timestamp(),
 action text NOT NULL CHECK(action IN('configured','reopened','claimed','released','raced','progress','erased')),
 org_id text,workspace_id text,machine_id text,object_ref text CHECK(object_ref IS NULL OR object_ref~'^_bx[0-9]{10}-[0-9a-f]{16}$'),
 reason text,versions bigint,write_seq bigint,detail jsonb CHECK(detail IS NULL OR jsonb_typeof(detail)='object')
);

CREATE TABLE sessions.babysitter_evidence_index(
 org_id text NOT NULL,workspace_id text NOT NULL,machine_id text NOT NULL,object_ref text NOT NULL,
 origin_id text NOT NULL CHECK(octet_length(origin_id) BETWEEN 1 AND 512),
 record_id text NOT NULL CHECK(octet_length(record_id) BETWEEN 1 AND 512),
 revision bigint NOT NULL CHECK(revision>0),
 revision_id text NOT NULL CHECK(octet_length(revision_id) BETWEEN 1 AND 512),
 -- The record's content digest (prepare's per-record sha256): the identity of the BODY a commit
 -- source must have accepted (an equal revision with another body is not this row).
 digest text NOT NULL CHECK(digest~'^[0-9a-f]{64}$'),
 operation text NOT NULL CHECK(operation IN('upsert','delete')),
 kind text NOT NULL CHECK(kind IN('session_lineage','turn_receipt')),
 -- upsert: the exact create-only object (b=<sha32(batch digest)>.ndjson.zst) and 0-based line.
 object_name text CHECK(object_name~'^b=[0-9a-f]{32}\.ndjson\.zst$'),
 line integer CHECK(line BETWEEN 0 AND 100000),
 -- upsert: payload.expires_at_ms (Neon's read-time expiry predicate); NULL for a tombstone.
 expires_at_ms bigint CHECK(expires_at_ms IS NULL OR expires_at_ms>0),
 PRIMARY KEY(org_id,workspace_id,origin_id,record_id,revision),
 FOREIGN KEY(org_id,workspace_id,machine_id,object_ref) REFERENCES sessions.babysitter_retention_objects(org_id,workspace_id,machine_id,object_ref) ON DELETE CASCADE,
 CHECK((operation='upsert')=(object_name IS NOT NULL AND line IS NOT NULL AND expires_at_ms IS NOT NULL)),
 CHECK(operation='upsert' OR (object_name IS NULL AND line IS NULL AND expires_at_ms IS NULL))
);
-- Neon's native page order, C collation: (origin_id, record_id), newest revision first.
CREATE INDEX babysitter_evidence_index_page_idx ON sessions.babysitter_evidence_index
 (org_id,workspace_id,kind,origin_id COLLATE "C",record_id COLLATE "C",revision DESC);
CREATE INDEX babysitter_evidence_index_bucket_idx ON sessions.babysitter_evidence_index(org_id,workspace_id,machine_id,object_ref);

ALTER TABLE sessions.babysitter_retention_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_retention_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_retention_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_evidence_index ENABLE ROW LEVEL SECURITY;
-- The sweeper's own least-privilege role (supabase/bootstrap/session_ingest_roles.sql, NOLOGIN; LOGIN
-- only by the operator step babysitter_retention_role.sql). It may run the claim/fence/complete RPCs and
-- nothing else: not register, not any ingest, reset or commit RPC, not any table. Fail closed if the
-- bootstrap has not provisioned it (42704) or it exists elevated (55000).
DO $retention_role$
DECLARE r pg_roles%ROWTYPE;
BEGIN
 SELECT * INTO r FROM pg_roles WHERE rolname='bulls96_babysitter_retention';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='42704',MESSAGE='babysitter retention: role bulls96_babysitter_retention absent; run the role bootstrap first';END IF;
 -- NOLOGIN at install: the bootstrap creates it so; LOGIN comes only from the later operator step
 -- (babysitter_retention_role.sql, checked by babysitter_retention_verify.sql). A pre-existing role that
 -- can already log in is refused.
 IF r.rolcanlogin OR r.rolsuper OR r.rolbypassrls OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication
  OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='babysitter retention: role bulls96_babysitter_retention can log in, is elevated or is a member';END IF;
END $retention_role$;
REVOKE ALL ON sessions.babysitter_retention_settings,sessions.babysitter_retention_objects,sessions.babysitter_retention_audit,sessions.babysitter_evidence_index
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_babysitter_retention;

-- The audit is append-only for everyone, owner included (only the rollback drops it).
CREATE FUNCTION sessions.babysitter_retention_audit_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $guard$
BEGIN
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='babysitter retention: audit is append-only';
END $guard$;
CREATE TRIGGER babysitter_retention_audit_append_only BEFORE UPDATE OR DELETE ON sessions.babysitter_retention_audit
 FOR EACH ROW EXECUTE FUNCTION sessions.babysitter_retention_audit_guard();

-- The bucket segment: _bx<yyyymmddhh of the erase hour>-<16 hex>. Returns the erase
-- hour it names, or raises on any other shape.
CREATE FUNCTION sessions.babysitter_retention_bucket_hour(p_ref text) RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $hour$
DECLARE v timestamptz;
BEGIN
 IF p_ref IS NULL OR p_ref!~'^_bx[0-9]{10}-[0-9a-f]{16}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: bucket ref is malformed';END IF;
 BEGIN
  v:=make_timestamptz(substr(p_ref,4,4)::int,substr(p_ref,8,2)::int,substr(p_ref,10,2)::int,substr(p_ref,12,2)::int,0,0,'UTC');
 EXCEPTION WHEN others THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: bucket hour is invalid';
 END;
 IF to_char(v AT TIME ZONE 'UTC','YYYYMMDDHH24')<>substr(p_ref,4,10) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: bucket hour is invalid';END IF;
 RETURN v;
END $hour$;

-- Register (or re-arm) one batch's retention buckets, before their S3 PUT, and RETURN
-- their refs: SQL is the key authority. Each bucket is bound to the epochs the batch's
-- CLAIM saw (ingest_claim_attempts.epochs): the session's fence epoch and the user's reset
-- epoch. A batch whose claimed sessions are fenced now (disabled, reset, or disabled and
-- re-enabled since the claim) is refused (40001: re-claim drops them), so a pre-disable
-- write can never land under a post-enable key. The current schema generation is required.
-- p: {tenant{orgId,workspaceId,userId}, originId, batchId, batchDigest, claimToken, generation,
--     resetEpoch, buckets:[{source, sessionId, eraseAfterMs}],
--     records:[{bucket (index into buckets), originId, recordId, revision (decimal string),
--               revisionId, digest, operation upsert|delete, kind, line (upsert) | null, expiresAtMs (upsert) | null}]}
-- Each record becomes one content-free key-index row (the reader's native order and tombstones).
-- returns {objects:[{ref, source, sessionId, eraseAfterMs}] in input order, reopened}
CREATE FUNCTION sessions.ingest_babysitter_retention_register(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $register$
DECLARE v_org text;v_ws text;v_user text;v_origin text;v_machine text;v_epoch bigint;v_epochs jsonb;v_pairs jsonb;v_fenced jsonb;
 o jsonb;v_ref text;v_source text;v_session text;v_after timestamptz;v_fence bigint;v_hour text;v_refs text[]:='{}';v_object text;
 v_rec jsonb;v_b integer;v_op text;v_rev bigint;v_rid text;v_n integer;
 v_prev sessions.babysitter_retention_objects%ROWTYPE;r sessions.babysitter_retention_objects%ROWTYPE;out jsonb:='[]'::jsonb;reopened integer:=0;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','generation','resetEpoch','buckets','records']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_origin:=sessions.ingest_identifier(p->'originId',false);
 IF jsonb_typeof(p->'resetEpoch') IS DISTINCT FROM 'number' OR (p->>'resetEpoch')::numeric<>trunc((p->>'resetEpoch')::numeric)
  OR (p->>'resetEpoch')::numeric<0 OR (p->>'resetEpoch')::numeric>9007199254740991 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: reset epoch malformed';END IF;
 v_epoch:=(p->>'resetEpoch')::bigint;
 IF v_epoch<>(SELECT count(*) FROM sessions.upload_resets x WHERE x.org_id=v_org AND x.workspace_id=v_ws AND x.user_id=v_user) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: reset epoch changed';END IF;
 IF jsonb_typeof(p->'buckets') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'buckets') NOT BETWEEN 1 AND 500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: buckets malformed';END IF;
 -- The claim's own epoch snapshot (P0002/40001 if this is not that live claim attempt).
 v_epochs:=sessions.ingest_attempt_epochs(v_org,v_ws,v_user,v_origin,sessions.ingest_identifier(p->'batchId',false),
  sessions.ingest_digest(p->'batchDigest'),p->>'claimToken');
 SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_array(x->>'source',x->>'sessionId')),'[]'::jsonb) INTO v_pairs
  FROM jsonb_array_elements(p->'buckets') x WHERE x->'sessionId'<>'null'::jsonb;
 IF jsonb_array_length(v_pairs)>0 THEN
  v_fenced:=sessions.ingest_attempt_fenced(v_org,v_ws,v_user,v_origin,v_epochs,v_pairs);
  IF jsonb_array_length(v_fenced)>0 THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: a claimed session is fenced now';END IF;
 END IF;
 v_machine:='srv_'||left(encode(sha256(convert_to(v_ws||chr(31)||v_origin||chr(31)||v_user,'UTF8')),'hex'),32);
 FOR o IN SELECT x FROM jsonb_array_elements(p->'buckets') x LOOP
  PERFORM sessions.ingest_exact_keys(o,ARRAY['source','sessionId','eraseAfterMs']);
  v_source:=sessions.ingest_identifier(o->'source',false);
  v_session:=CASE WHEN o->'sessionId'='null'::jsonb THEN NULL ELSE sessions.ingest_identifier(o->'sessionId',false) END;
  IF jsonb_typeof(o->'eraseAfterMs') IS DISTINCT FROM 'number' OR (o->>'eraseAfterMs')::numeric<>trunc((o->>'eraseAfterMs')::numeric)
   OR (o->>'eraseAfterMs')::numeric%3600000<>0 OR (o->>'eraseAfterMs')::numeric<=0
   OR (o->>'eraseAfterMs')::numeric>(extract(epoch FROM clock_timestamp())*1000+32::numeric*86400000) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: erase hour malformed';END IF;
  v_after:=to_timestamp((o->>'eraseAfterMs')::numeric/1000);
  v_fence:=CASE WHEN v_session IS NULL THEN 0 ELSE (SELECT (e->>'epoch')::bigint FROM jsonb_array_elements(v_epochs) e
   WHERE e->>'source'=v_source AND e->>'session_id'=v_session LIMIT 1) END;
  IF v_fence IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: session outside claim attempt';END IF;
  v_hour:=to_char(v_after AT TIME ZONE 'UTC','YYYYMMDDHH24');
  v_ref:='_bx'||v_hour||'-'||left(encode(sha256(convert_to(v_source||chr(31)||coalesce(v_session,'')||chr(31)||v_epoch::text||chr(31)||v_fence::text,'UTF8')),'hex'),16);
  SELECT * INTO v_prev FROM sessions.babysitter_retention_objects WHERE org_id=v_org AND workspace_id=v_ws
   AND machine_id=v_machine AND object_ref=v_ref FOR UPDATE;
  -- Never write into a bucket an erase may be running on: retry after the lease.
  IF FOUND AND v_prev.state='leased' AND v_prev.lease_until>clock_timestamp() THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: bucket is being erased';END IF;
  INSERT INTO sessions.babysitter_retention_objects AS t(org_id,workspace_id,user_id,origin_id,machine_id,object_ref,source,session_id,reset_epoch,fence_epoch,erase_after)
   VALUES(v_org,v_ws,v_user,v_origin,v_machine,v_ref,v_source,v_session,v_epoch,v_fence,v_after)
  ON CONFLICT(org_id,workspace_id,machine_id,object_ref) DO UPDATE
   SET write_seq=t.write_seq+1,last_written_at=clock_timestamp(),
    state='pending',lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL,
    erased_at=NULL,erased_versions=NULL,erase_reason=NULL,erase_checkpoint=NULL
  RETURNING * INTO r;
  IF r.user_id<>v_user OR r.origin_id<>v_origin OR r.source<>v_source OR r.session_id IS DISTINCT FROM v_session
   OR r.reset_epoch<>v_epoch OR r.fence_epoch<>v_fence THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: ref identity changed';END IF;
  IF v_prev.state='erased' THEN
   reopened:=reopened+1;
   INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,write_seq)
    VALUES('reopened',r.org_id,r.workspace_id,r.machine_id,r.object_ref,r.write_seq);
  END IF;
  out:=out||jsonb_build_array(jsonb_build_object('ref',v_ref,'source',v_source,'sessionId',v_session,'eraseAfterMs',(o->'eraseAfterMs')));
  v_refs:=v_refs||v_ref;
 END LOOP;
 -- The content-free key index: one row per routed (record, revision), upsert or delete.
 IF jsonb_typeof(p->'records') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'records') NOT BETWEEN 1 AND 5000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: records malformed';END IF;
 v_object:='b='||left(encode(sha256(convert_to(p->>'batchDigest','UTF8')),'hex'),32)||'.ndjson.zst';
 v_n:=jsonb_array_length(p->'buckets');
 FOR v_rec IN SELECT e FROM jsonb_array_elements(p->'records') e LOOP
  PERFORM sessions.ingest_exact_keys(v_rec,ARRAY['bucket','originId','recordId','revision','revisionId','digest','operation','kind','line','expiresAtMs']);
  -- Prepare binds every record's origin to its batch's (so to this user's machine).
  IF sessions.ingest_identifier(v_rec->'originId',false)<>v_origin THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record origin is not the batch origin';END IF;
  IF jsonb_typeof(v_rec->'bucket') IS DISTINCT FROM 'number' OR (v_rec->>'bucket')::numeric<>trunc((v_rec->>'bucket')::numeric)
   OR (v_rec->>'bucket')::numeric<0 OR (v_rec->>'bucket')::numeric>=v_n THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record bucket malformed';END IF;
  v_b:=(v_rec->>'bucket')::integer+1;
  IF jsonb_typeof(v_rec->'revision') IS DISTINCT FROM 'string' OR v_rec->>'revision'!~'^[1-9][0-9]{0,17}$' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record revision malformed';END IF;
  v_rev:=(v_rec->>'revision')::bigint;v_op:=v_rec->>'operation';v_rid:=sessions.ingest_identifier(v_rec->'revisionId',false);
  IF jsonb_typeof(v_rec->'operation') IS DISTINCT FROM 'string' OR v_op NOT IN('upsert','delete')
   OR jsonb_typeof(v_rec->'kind') IS DISTINCT FROM 'string' OR v_rec->>'kind' NOT IN('session_lineage','turn_receipt') THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record operation or kind malformed';END IF;
  IF v_op='upsert' AND (jsonb_typeof(v_rec->'line') IS DISTINCT FROM 'number' OR (v_rec->>'line')::numeric<>trunc((v_rec->>'line')::numeric)
    OR (v_rec->>'line')::numeric NOT BETWEEN 0 AND 100000 OR jsonb_typeof(v_rec->'expiresAtMs') IS DISTINCT FROM 'number'
    OR (v_rec->>'expiresAtMs')::numeric<>trunc((v_rec->>'expiresAtMs')::numeric) OR (v_rec->>'expiresAtMs')::numeric NOT BETWEEN 1 AND 9007199254740991)
   OR v_op='delete' AND (v_rec->'line'<>'null'::jsonb OR v_rec->'expiresAtMs'<>'null'::jsonb) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record position malformed';END IF;
  INSERT INTO sessions.babysitter_evidence_index AS t(org_id,workspace_id,machine_id,object_ref,origin_id,record_id,revision,revision_id,digest,operation,kind,object_name,line,expires_at_ms)
   VALUES(v_org,v_ws,v_machine,v_refs[v_b],v_origin,sessions.ingest_identifier(v_rec->'recordId',false),v_rev,v_rid,sessions.ingest_digest(v_rec->'digest'),v_op,v_rec->>'kind',
    CASE WHEN v_op='upsert' THEN v_object END,CASE WHEN v_op='upsert' THEN (v_rec->>'line')::integer END,CASE WHEN v_op='upsert' THEN (v_rec->>'expiresAtMs')::bigint END)
  -- A retried or re-batched revision re-points at the latest registration (the one that commits).
  ON CONFLICT(org_id,workspace_id,origin_id,record_id,revision) DO UPDATE
   SET machine_id=EXCLUDED.machine_id,object_ref=EXCLUDED.object_ref,digest=EXCLUDED.digest,operation=EXCLUDED.operation,kind=EXCLUDED.kind,
    object_name=EXCLUDED.object_name,line=EXCLUDED.line,expires_at_ms=EXCLUDED.expires_at_ms
   -- Only this user's own earlier registration of the same revision may be re-pointed.
   WHERE t.revision_id=EXCLUDED.revision_id AND t.machine_id=EXCLUDED.machine_id;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: revision identity changed';END IF;
 END LOOP;
 RETURN jsonb_build_object('objects',out,'reopened',reopened);
END $register$;

-- Fenced = durably invisible and erasable: the session's fence epoch moved past the bucket's
-- (any disable or reset since the claim, even if re-enabled since), or the user reset.
CREATE FUNCTION sessions.babysitter_retention_fenced(o sessions.babysitter_retention_objects) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $fenced$
 SELECT (o.session_id IS NOT NULL AND coalesce((SELECT f.epoch FROM sessions.ingest_fence_versions f WHERE f.org_id=o.org_id
    AND f.workspace_id=o.workspace_id AND f.user_id=o.user_id AND f.origin_id=o.origin_id AND f.source=o.source
    AND f.session_id=o.session_id),0)>o.fence_epoch)
  OR (SELECT count(*) FROM sessions.upload_resets r WHERE r.org_id=o.org_id AND r.workspace_id=o.workspace_id
    AND r.user_id=o.user_id)>o.reset_epoch
$fenced$;

-- Whether one bucket may be erased now, and why. Nothing is due within the 15 min hold-off
-- after its last registration (no PUT can still be in flight).
--  fenced   babysitter_retention_fenced (a disable or reset since its claim, durably);
--  expired  its expiry hour has passed.
CREATE FUNCTION sessions.babysitter_retention_due_reason(o sessions.babysitter_retention_objects) RETURNS text
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $due$
 SELECT CASE
  WHEN o.last_written_at+interval '15 minutes'>clock_timestamp() THEN NULL
  WHEN sessions.babysitter_retention_fenced(o) THEN 'fenced'
  WHEN o.erase_after<=clock_timestamp() THEN 'expired'
  ELSE NULL END
$due$;

-- Lease up to p.limit due buckets. Nothing is claimed while disabled.
-- p: {generation, limit}
CREATE FUNCTION sessions.ingest_babysitter_retention_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE s sessions.babysitter_retention_settings%ROWTYPE;q sessions.babysitter_retention_objects%ROWTYPE;
 n integer;t uuid;why text;out jsonb:='[]'::jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_babysitter_retention');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','limit']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'limit') IS DISTINCT FROM 'number' OR (p->>'limit')::numeric<>trunc((p->>'limit')::numeric)
  OR (p->>'limit')::numeric NOT BETWEEN 1 AND 200 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: invalid batch bound';END IF;
 n:=(p->>'limit')::integer;
 SELECT * INTO STRICT s FROM sessions.babysitter_retention_settings WHERE singleton FOR SHARE;
 IF NOT s.enabled THEN RETURN jsonb_build_object('enabled',false,'dryRun',s.dry_run,'objects','[]'::jsonb);END IF;
 FOR q IN SELECT * FROM sessions.babysitter_retention_objects o
  WHERE (o.state='pending' OR (o.state='leased' AND o.lease_until<=clock_timestamp()))
   AND sessions.babysitter_retention_due_reason(o) IS NOT NULL
  ORDER BY o.erase_after,o.org_id COLLATE "C",o.workspace_id COLLATE "C",o.machine_id,o.object_ref COLLATE "C"
  LIMIT n FOR UPDATE SKIP LOCKED LOOP
  why:=sessions.babysitter_retention_due_reason(q);
  IF why IS NULL THEN CONTINUE;END IF;
  t:=gen_random_uuid();
  UPDATE sessions.babysitter_retention_objects SET state='leased',lease_token=t,lease_until=clock_timestamp()+interval '5 minutes',
   leased_write_seq=write_seq,lease_reason=why
   WHERE org_id=q.org_id AND workspace_id=q.workspace_id AND machine_id=q.machine_id AND object_ref=q.object_ref
   RETURNING * INTO q;
  INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,reason,write_seq)
   VALUES('claimed',q.org_id,q.workspace_id,q.machine_id,q.object_ref,why,q.write_seq);
  out:=out||jsonb_build_array(jsonb_build_object('leaseToken',t::text,'leaseUntil',to_char(q.lease_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
   'orgId',q.org_id,'workspaceId',q.workspace_id,'userId',q.user_id,'originId',q.origin_id,'machineId',q.machine_id,'ref',q.object_ref,'reason',why,
   'fenceEpoch',q.fence_epoch,'resetEpoch',q.reset_epoch,'writeSeq',q.write_seq,'eraseRuns',q.erase_runs,'checkpoint',q.erase_checkpoint));
 END LOOP;
 RETURN jsonb_build_object('enabled',true,'dryRun',s.dry_run,'objects',out);
END $claim$;

-- Finish one lease:
--  'erased'   a full strict traversal then a FRESH strict zero inventory under this lease
--             (eraser.ts verifyErased), with write_seq unchanged since the claim (no new
--             registration, hence no unresolved PUT); its key-index rows go with it;
--  'progress' a bounded run ended (budget/deadline) or a proof found versions behind the
--             cursor: persist the content-free checkpoint (NULL: restart), stay pending;
--  'released' dry run, refusal or failure: stay pending, KEEP the last checkpoint.
-- An erase whose write_seq moved stays pending (defense in depth).
-- p: {generation, leaseToken, outcome, versions, checkpoint}
CREATE FUNCTION sessions.ingest_babysitter_retention_complete(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $complete$
DECLARE s sessions.babysitter_retention_settings%ROWTYPE;q sessions.babysitter_retention_objects%ROWTYPE;v_outcome text;v_versions bigint;v_cp jsonb;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_babysitter_retention');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','leaseToken','outcome','versions','checkpoint']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'leaseToken') IS DISTINCT FROM 'string' OR p->>'leaseToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: lease token malformed';END IF;
 v_outcome:=p->>'outcome';
 IF jsonb_typeof(p->'outcome') IS DISTINCT FROM 'string' OR v_outcome NOT IN('erased','released','progress') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: outcome malformed';END IF;
 v_cp:=CASE WHEN p->'checkpoint'='null'::jsonb THEN NULL ELSE p->'checkpoint' END;
 IF v_cp IS NOT NULL AND (v_outcome<>'progress' OR jsonb_typeof(v_cp)<>'object' OR octet_length(v_cp::text)>8192
   OR v_cp->>'v' IS DISTINCT FROM '1' OR jsonb_typeof(v_cp->'prefix') IS DISTINCT FROM 'string' OR jsonb_typeof(v_cp->'scope') IS DISTINCT FROM 'string') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: checkpoint malformed';END IF;
 IF jsonb_typeof(p->'versions') IS DISTINCT FROM 'number' OR (p->>'versions')::numeric<>trunc((p->>'versions')::numeric)
  OR (p->>'versions')::numeric NOT BETWEEN 0 AND 1000000000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: versions malformed';END IF;
 v_versions:=(p->>'versions')::bigint;
 SELECT * INTO STRICT s FROM sessions.babysitter_retention_settings WHERE singleton FOR SHARE;
 IF v_outcome IN('erased','progress') AND (NOT s.enabled OR s.dry_run) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='babysitter retention: erase recorded while disabled or dry-run';END IF;
 SELECT * INTO q FROM sessions.babysitter_retention_objects WHERE lease_token=(p->>'leaseToken')::uuid AND state='leased' FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('ok',false,'code','lease_lost');END IF;
 IF v_cp IS NOT NULL AND (right(v_cp->>'prefix',length(q.object_ref)+4)<>'/s='||q.object_ref||'/' OR strpos(v_cp->>'prefix','/m='||q.machine_id||'/')=0) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: checkpoint is for another bucket';END IF;
 IF v_outcome='progress' THEN
  UPDATE sessions.babysitter_retention_objects SET state='pending',lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL,
   erase_checkpoint=CASE WHEN q.write_seq=q.leased_write_seq THEN v_cp END,erase_runs=erase_runs+1
   WHERE lease_token=q.lease_token;
  INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,reason,versions,write_seq)
   VALUES('progress',q.org_id,q.workspace_id,q.machine_id,q.object_ref,q.lease_reason,v_versions,q.write_seq);
  RETURN jsonb_build_object('ok',true,'state','pending');
 END IF;
 IF v_outcome='released' OR q.write_seq<>q.leased_write_seq THEN
  UPDATE sessions.babysitter_retention_objects SET state='pending',lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL
   WHERE lease_token=q.lease_token;
  INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,reason,versions,write_seq)
   VALUES(CASE WHEN v_outcome='released' THEN 'released' ELSE 'raced' END,q.org_id,q.workspace_id,q.machine_id,q.object_ref,q.lease_reason,v_versions,q.write_seq);
  RETURN jsonb_build_object('ok',true,'state','pending');
 END IF;
 UPDATE sessions.babysitter_retention_objects SET state='erased',erased_at=clock_timestamp(),erased_versions=v_versions,erase_reason=q.lease_reason,
  lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL,erase_checkpoint=NULL WHERE lease_token=q.lease_token;
 DELETE FROM sessions.babysitter_evidence_index WHERE org_id=q.org_id AND workspace_id=q.workspace_id AND machine_id=q.machine_id AND object_ref=q.object_ref;
 INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,reason,versions,write_seq)
  VALUES('erased',q.org_id,q.workspace_id,q.machine_id,q.object_ref,q.lease_reason,v_versions,q.write_seq);
 RETURN jsonb_build_object('ok',true,'state','erased');
END $complete$;

-- The erase fence, re-checked before EVERY page's deletes of a bounded erase run (and
-- before the zero-inventory proof): the lease must still be held (with 30 s to spare)
-- under the CURRENT schema generation. A stale generation is refused even with a
-- valid lease (55000), so nothing is deleted. p: {generation, leaseToken}
CREATE FUNCTION sessions.ingest_babysitter_retention_fence(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $fence$
DECLARE q sessions.babysitter_retention_objects%ROWTYPE;s sessions.babysitter_retention_settings%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_babysitter_retention');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','leaseToken']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'leaseToken') IS DISTINCT FROM 'string' OR p->>'leaseToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: lease token malformed';END IF;
 SELECT * INTO STRICT s FROM sessions.babysitter_retention_settings WHERE singleton FOR SHARE;
 IF NOT s.enabled OR s.dry_run THEN RETURN jsonb_build_object('ok',false,'code','disabled');END IF;
 SELECT * INTO q FROM sessions.babysitter_retention_objects WHERE lease_token=(p->>'leaseToken')::uuid AND state='leased';
 IF NOT FOUND OR q.lease_until<=clock_timestamp()+interval '30 seconds' THEN RETURN jsonb_build_object('ok',false,'code','lease_lost');END IF;
 RETURN jsonb_build_object('ok',true);
END $fence$;

-- READER (Neon GET /v1/delivery/records parity for Babysitter kinds), for the read API role
-- only, tenant-pinned by the read API's verified binding (session_directory_read_api_scope).
-- ONE page in Neon's native (origin_id, record_id) order (C collation) over the content-free
-- key index. Per record its NEWEST indexed revision decides, as Neon's one row per record does:
--   bucket fenced (disable/reset since its claim, even if re-enabled) or erased -> not listed
--     (logical delete, immediate, before any physical erase);
--   newest revision is not the committed one (ingest_record_revisions, or the lazy-current native
--     custody slot for buffered acceptance: uncommitted, or a newer revision outside the index)
--     -> not listed (never a stale payload, never a guessed tombstone);
--   source / session filter on that newest row (Neon filters its one row);
--   delete -> Neon's tombstone (operation delete, payload null) under include_deleted, else hidden;
--   upsert past its expiry (payload.expires_at_ms, or the bucket hour) -> a tombstone with the
--     line's revision under include_deleted (Neon's read-time predicate), else hidden;
--   upsert otherwise -> live, with the exact object line for the reader to fetch.
-- Bounded: at most p.limit+1 rows emitted and at most 1000 records scanned. A scan that hits
-- the bound returns more=true and scannedTo, so an incomplete page resumes exactly there and
-- never claims completeness.
-- p: {kind, source|null, sessionId|null, after: [originId, recordId] | null, limit 1..101, includeDeleted}
CREATE FUNCTION sessions.babysitter_evidence_page_for(p_workspace_id text,p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE v_org text;r record;v_rows jsonb:='[]'::jsonb;v_emitted integer:=0;v_scanned integer:=0;v_limit integer;v_inc boolean;
 v_a0 text;v_a1 text;v_last jsonb;v_now timestamptz:=clock_timestamp();v_now_ms bigint;v_src text;v_sess text;v_any_sess boolean;
BEGIN
 v_org:=sessions.session_directory_read_api_scope(p_workspace_id);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['kind','source','sessionId','after','limit','includeDeleted']);
 IF jsonb_typeof(p->'kind') IS DISTINCT FROM 'string' OR p->>'kind' NOT IN('session_lineage','turn_receipt')
  OR jsonb_typeof(p->'includeDeleted') IS DISTINCT FROM 'boolean'
  OR jsonb_typeof(p->'limit') IS DISTINCT FROM 'number' OR (p->>'limit')::numeric<>trunc((p->>'limit')::numeric) OR (p->>'limit')::numeric NOT BETWEEN 1 AND 101
  OR (p->'source'<>'null'::jsonb AND jsonb_typeof(p->'source') IS DISTINCT FROM 'string')
  OR (p->'sessionId'<>'null'::jsonb AND jsonb_typeof(p->'sessionId') IS DISTINCT FROM 'string') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter evidence: page request malformed';END IF;
 IF p->'after'<>'null'::jsonb THEN
  IF jsonb_typeof(p->'after') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'after')<>2
   OR jsonb_typeof(p->'after'->0) IS DISTINCT FROM 'string' OR jsonb_typeof(p->'after'->1) IS DISTINCT FROM 'string' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter evidence: cursor malformed';END IF;
  v_a0:=p->'after'->>0;v_a1:=p->'after'->>1;
 END IF;
 v_limit:=(p->>'limit')::integer;v_inc:=(p->>'includeDeleted')::boolean;
 v_src:=p->>'source';v_sess:=p->>'sessionId';v_any_sess:=p->'sessionId'='null'::jsonb;
 v_now_ms:=floor(extract(epoch FROM v_now)*1000)::bigint;
 FOR r IN
  SELECT DISTINCT ON (i.origin_id COLLATE "C",i.record_id COLLATE "C")
   i.origin_id,i.record_id,i.revision,i.revision_id,i.operation,i.object_name,i.line,i.expires_at_ms,
   o.machine_id,o.object_ref,o.source,o.session_id,o.erase_after,o.state,sessions.babysitter_retention_fenced(o) AS fenced,
   i.digest,c.revision AS c_rev,c.revision_id AS c_rid,c.digest AS c_digest,n.current_revision AS n_rev,n.current_digest AS n_digest
  FROM sessions.babysitter_evidence_index i
  JOIN sessions.babysitter_retention_objects o
   ON o.org_id=i.org_id AND o.workspace_id=i.workspace_id AND o.machine_id=i.machine_id AND o.object_ref=i.object_ref
  LEFT JOIN sessions.ingest_record_revisions c
   ON c.org_id=i.org_id AND c.workspace_id=i.workspace_id AND c.origin_id=i.origin_id AND c.record_id=i.record_id
   AND c.user_id=o.user_id
  -- Buffered acceptance under the lazy-current policy commits into the native custody slot instead.
  LEFT JOIN sessions.ingest_lazy_native_slots n
   ON n.key_sha256=encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',i.org_id,i.workspace_id,i.origin_id,i.record_id)::text,'UTF8')),'hex')
   AND n.current_owner=o.user_id
  WHERE i.org_id=v_org AND i.workspace_id=p_workspace_id AND i.kind=p->>'kind'
   AND (v_a0 IS NULL OR (i.origin_id COLLATE "C",i.record_id COLLATE "C")>(v_a0 COLLATE "C",v_a1 COLLATE "C"))
  ORDER BY i.origin_id COLLATE "C",i.record_id COLLATE "C",i.revision DESC
 LOOP
  IF v_scanned>=1000 THEN RETURN jsonb_build_object('rows',v_rows,'more',true,'scannedTo',v_last);END IF;
  v_scanned:=v_scanned+1;
  v_last:=jsonb_build_array(r.origin_id,r.record_id);
  CONTINUE WHEN r.fenced OR r.state='erased';
  -- Committed: no commit source holds a newer revision, and one holds exactly THIS BODY: the same
  -- revision, revision id and content digest (ingest_record_revisions), or the same revision and
  -- digest in the native custody slot (an equal revision whose body the ack refused keeps the
  -- slot's earlier digest, so it is not this row).
  CONTINUE WHEN coalesce(r.c_rev>r.revision,false) OR coalesce(r.n_rev>r.revision,false)
   OR NOT (coalesce(r.c_rev=r.revision AND r.c_rid=r.revision_id AND r.c_digest=r.digest,false)
        OR coalesce(r.n_rev=r.revision AND r.n_digest=r.digest,false));
  CONTINUE WHEN (v_src IS NOT NULL AND r.source<>v_src) OR (NOT v_any_sess AND r.session_id IS DISTINCT FROM v_sess);
  IF r.operation='delete' OR r.erase_after<=v_now OR r.expires_at_ms<=v_now_ms THEN
   CONTINUE WHEN NOT v_inc;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('originId',r.origin_id,'recordId',r.record_id,'revision',r.revision::text,'revisionId',r.revision_id,
    'operation','delete','source',r.source,'sessionId',r.session_id));
  ELSE
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('originId',r.origin_id,'recordId',r.record_id,'revision',r.revision::text,'revisionId',r.revision_id,
    'operation','upsert','source',r.source,'sessionId',r.session_id,'machineId',r.machine_id,'ref',r.object_ref,'object',r.object_name,'line',r.line));
  END IF;
  v_emitted:=v_emitted+1;
  IF v_emitted>v_limit THEN RETURN jsonb_build_object('rows',v_rows,'more',true,'scannedTo',v_last);END IF;
 END LOOP;
 RETURN jsonb_build_object('rows',v_rows,'more',false,'scannedTo',v_last);
END $page$;

-- Owner-only switch (the production lead, with Khaliq's OK). p: {enabled, dryRun}
CREATE FUNCTION sessions.babysitter_retention_configure(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $configure$
DECLARE s sessions.babysitter_retention_settings%ROWTYPE;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['enabled','dryRun']);
 IF jsonb_typeof(p->'enabled') IS DISTINCT FROM 'boolean' OR jsonb_typeof(p->'dryRun') IS DISTINCT FROM 'boolean' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: settings malformed';END IF;
 UPDATE sessions.babysitter_retention_settings SET enabled=(p->>'enabled')::boolean,dry_run=(p->>'dryRun')::boolean,updated_at=clock_timestamp()
  WHERE singleton RETURNING * INTO STRICT s;
 INSERT INTO sessions.babysitter_retention_audit(action,detail) VALUES('configured',jsonb_build_object('enabled',s.enabled,'dryRun',s.dry_run));
 RETURN jsonb_build_object('enabled',s.enabled,'dryRun',s.dry_run);
END $configure$;

REVOKE ALL ON FUNCTION sessions.babysitter_retention_audit_guard(),sessions.babysitter_retention_bucket_hour(text),
 sessions.ingest_babysitter_retention_register(jsonb),sessions.babysitter_retention_fenced(sessions.babysitter_retention_objects),
 sessions.babysitter_retention_due_reason(sessions.babysitter_retention_objects),sessions.ingest_babysitter_retention_fence(jsonb),
 sessions.babysitter_evidence_page_for(text,jsonb),
 sessions.ingest_babysitter_retention_claim(jsonb),sessions.ingest_babysitter_retention_complete(jsonb),sessions.babysitter_retention_configure(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_babysitter_retention,bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.ingest_babysitter_retention_register(jsonb) TO bulls96_session_ingest;
GRANT USAGE ON SCHEMA sessions TO bulls96_babysitter_retention;
GRANT EXECUTE ON FUNCTION sessions.ingest_babysitter_retention_claim(jsonb),sessions.ingest_babysitter_retention_fence(jsonb),
 sessions.ingest_babysitter_retention_complete(jsonb) TO bulls96_babysitter_retention;
GRANT EXECUTE ON FUNCTION sessions.babysitter_evidence_page_for(text,jsonb) TO bulls96_read_api;
