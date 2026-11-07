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
--    (origin_id, record_id) order: IMMUTABLE candidate rows per (record, revision, claim);
--    only the ACK-accepted candidate is served. Upserts point at their exact bucket object
--    line (and go with the bucket's erase); deletes are PERMANENT payload-free tombstones with
--    no bucket, removed only by a privacy fence (disable/reset, C3).
--  * babysitter_write_obligations: one durable unresolved-write obligation per bucket object KEY
--    (the shared write-obligation.ts contract: row version, resume checkpoint, re-sweep backoff);
--    a bucket is erased only when none remains.
--  * babysitter_write_invocations: one row per DISPATCHED PUT at a key, resolved only by its own
--    terminal evidence (acked with its own version id, or refused).
--  * babysitter_write_reservations: every version an erase is about to delete, reserved under
--    the key row's lock first, so a publisher and the erase can never both win.
--  * babysitter_purge_cursor: owner-only singleton holding the privacy purge's keyset cursor
--    over the permanent tombstones (bounded work per claim), apart from the settings row.
--
-- Erase/write fence: a registration is refused while the bucket is leased, and a bucket is
-- claimable only after a 15 min hold-off since its last registration. That narrows races but
-- is NOT a proof that no earlier PUT can still commit (a lost or timed-out response may land
-- late). The proof is the durable obligation per registered object (babysitter_write_obligations):
-- a bucket is erased only when every obligation is complete (write-obligation.ts), and a late
-- version found by a later sweep is erased then. write_seq is checked again at completion.
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
-- The privacy purge's keyset cursor over the permanent tombstones (bounded work per claim). Its own
-- singleton row, NOT the settings row: a claim holds settings FOR SHARE and takes this row
-- FOR UPDATE SKIP LOCKED, so concurrent claims never upgrade a shared settings lock (which removes
-- that lock-upgrade deadlock cycle; it is not a claim about every lock in the system); a claim
-- that finds the cursor busy skips the purge for this call and still claims buckets.
CREATE TABLE sessions.babysitter_purge_cursor(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 purge_after jsonb CHECK(purge_after IS NULL OR (jsonb_typeof(purge_after)='array' AND jsonb_array_length(purge_after)=6))
);
INSERT INTO sessions.babysitter_purge_cursor(singleton) VALUES(true);

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
 action text NOT NULL CHECK(action IN('configured','reopened','claimed','released','raced','progress','purged','erased')),
 org_id text,workspace_id text,machine_id text,object_ref text CHECK(object_ref IS NULL OR object_ref~'^_bx[0-9]{10}-[0-9a-f]{16}$'),
 reason text,versions bigint,write_seq bigint,detail jsonb CHECK(detail IS NULL OR jsonb_typeof(detail)='object')
);

-- The content-free key index: one IMMUTABLE row per (record, revision, registering claim). A row
-- is a candidate placement, never overwritten: a later attempt at an equal revision (a changed
-- body, a delete, a retry) adds its OWN row, and the reader serves only the candidate the
-- commit ACCEPTED (the immutable ACK decision / committed revision of that body), so a losing
-- attempt can neither become visible nor displace the accepted winner's original line.
--   upsert   points at its exact create-only object line in a retention bucket (cascades with
--            the bucket's erase, when the payload is gone anyway);
--   delete   a PERMANENT payload-free tombstone row with no bucket (machine/object NULL, so the
--            bucket FK does not apply): Neon's include_deleted row outlives every payload. It is
--            removed only by a privacy fence (disable/reset), like every other identity (C3).
CREATE TABLE sessions.babysitter_evidence_index(
 org_id text NOT NULL,workspace_id text NOT NULL,
 origin_id text NOT NULL CHECK(octet_length(origin_id) BETWEEN 1 AND 512),
 record_id text NOT NULL CHECK(octet_length(record_id) BETWEEN 1 AND 512),
 revision bigint NOT NULL CHECK(revision>0),
 revision_id text NOT NULL CHECK(octet_length(revision_id) BETWEEN 1 AND 512),
 -- prepare's per-record sha256: the BODY identity (the ACK's candidate digest).
 digest text NOT NULL CHECK(digest~'^[0-9a-f]{64}$'),
 operation text NOT NULL CHECK(operation IN('upsert','delete')),
 kind text NOT NULL CHECK(kind IN('session_lineage','turn_receipt')),
 -- The registering claim attempt (its immutable ACK decisions / committed batch decide acceptance).
 claim_token uuid NOT NULL,
 -- Privacy identity (the claim's epochs): a fenced row is hidden at once and purged.
 user_id text NOT NULL,source text NOT NULL CHECK(octet_length(source) BETWEEN 1 AND 512),
 session_id text CHECK(session_id IS NULL OR octet_length(session_id) BETWEEN 1 AND 512),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),fence_epoch bigint NOT NULL CHECK(fence_epoch>=0),
 -- upsert only: the bucket, the exact object (b=<sha32(batch digest)>.ndjson.zst), 0-based line, payload expiry.
 machine_id text,object_ref text,
 object_name text CHECK(object_name~'^b=[0-9a-f]{32}\.ndjson\.zst$'),
 line integer CHECK(line BETWEEN 0 AND 100000),
 expires_at_ms bigint CHECK(expires_at_ms IS NULL OR expires_at_ms>0),
 registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,origin_id,record_id,revision,claim_token),
 FOREIGN KEY(org_id,workspace_id,machine_id,object_ref) REFERENCES sessions.babysitter_retention_objects(org_id,workspace_id,machine_id,object_ref) ON DELETE CASCADE,
 CHECK((operation='upsert')=(machine_id IS NOT NULL AND object_ref IS NOT NULL AND object_name IS NOT NULL AND line IS NOT NULL AND expires_at_ms IS NOT NULL)),
 CHECK(operation='upsert' OR (machine_id IS NULL AND object_ref IS NULL AND object_name IS NULL AND line IS NULL AND expires_at_ms IS NULL))
);
-- Neon's native page order, C collation: (origin_id, record_id), newest revision first.
-- The tie-break (registered_at DESC, claim_token) makes the same inputs always pick the same candidate.
CREATE INDEX babysitter_evidence_index_page_idx ON sessions.babysitter_evidence_index
 (org_id,workspace_id,kind,origin_id COLLATE "C",record_id COLLATE "C",revision DESC,registered_at DESC,claim_token);
CREATE INDEX babysitter_evidence_index_bucket_idx ON sessions.babysitter_evidence_index(org_id,workspace_id,machine_id,object_ref) WHERE machine_id IS NOT NULL;
-- The privacy purge's bounded keyset scan over the permanent tombstones (deletes only).
CREATE INDEX babysitter_evidence_index_tombstone_idx ON sessions.babysitter_evidence_index(org_id,workspace_id,origin_id,record_id,revision,claim_token) WHERE operation='delete';

-- Durable unresolved-write obligations (the shared src/migration/neon-s3-erasure/write-obligation.ts
-- contract): one row per bucket object KEY (its compare-and-set row version, resume checkpoint and
-- re-sweep backoff) and one row per DISPATCHED PUT invocation at that key, recorded by register
-- BEFORE the PUT. An invocation resolves only on its OWN terminal evidence, recorded by its
-- writer: refused (a definite refusal of that request), or acked with its own version id and
-- that exact version absent from a fresh strict COMPLETE inventory. A dispatched invocation
-- stays UNRESOLVED indefinitely (nonterminal; a listed version, even with an identical ETag,
-- never resolves it): its bucket is erased as cleanup on each re-sweep (at most daily) but never
-- marked erased. Every version is RESERVED (babysitter_write_reservations) before its delete.
CREATE TABLE sessions.babysitter_write_obligations(
 org_id text NOT NULL,workspace_id text NOT NULL,machine_id text NOT NULL,object_ref text NOT NULL,
 object_name text NOT NULL CHECK(object_name~'^b=[0-9a-f]{32}\.ndjson\.zst$'),
 rev bigint NOT NULL DEFAULT 1 CHECK(rev>0),
 erase_checkpoint jsonb CHECK(erase_checkpoint IS NULL OR (jsonb_typeof(erase_checkpoint)='object' AND octet_length(erase_checkpoint::text)<=8192)),
 sweeps integer NOT NULL DEFAULT 0 CHECK(sweeps>=0),
 next_sweep_at timestamptz,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,machine_id,object_ref,object_name),
 FOREIGN KEY(org_id,workspace_id,machine_id,object_ref) REFERENCES sessions.babysitter_retention_objects(org_id,workspace_id,machine_id,object_ref)
);
CREATE TABLE sessions.babysitter_write_invocations(
 org_id text NOT NULL,workspace_id text NOT NULL,machine_id text NOT NULL,object_ref text NOT NULL,object_name text NOT NULL,
 -- The dispatching delivery's claim token: one PUT per bucket object per claim, never reused.
 invocation_id uuid NOT NULL,
 outcome text NOT NULL DEFAULT 'dispatched' CHECK(outcome IN('dispatched','acked','refused')),
 version_id text CHECK(version_id IS NULL OR octet_length(version_id) BETWEEN 1 AND 1024),
 dispatched_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,machine_id,object_ref,object_name,invocation_id),
 FOREIGN KEY(org_id,workspace_id,machine_id,object_ref,object_name) REFERENCES sessions.babysitter_write_obligations(org_id,workspace_id,machine_id,object_ref,object_name),
 CHECK((outcome='acked')=(version_id IS NOT NULL))
);
-- Deletion reservations (distinct from outcome evidence): every version a sweep is about to delete,
-- reserved under the key row's lock first. Babysitter has no publishers, so nothing can contend;
-- the reservation keeps the shared contract (a future owner must refuse a reserved version).
CREATE TABLE sessions.babysitter_write_reservations(
 org_id text NOT NULL,workspace_id text NOT NULL,machine_id text NOT NULL,object_ref text NOT NULL,object_name text NOT NULL,
 version_id text NOT NULL CHECK(octet_length(version_id) BETWEEN 1 AND 1024),
 reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,machine_id,object_ref,object_name,version_id),
 FOREIGN KEY(org_id,workspace_id,machine_id,object_ref,object_name) REFERENCES sessions.babysitter_write_obligations(org_id,workspace_id,machine_id,object_ref,object_name) ON DELETE CASCADE
);

ALTER TABLE sessions.babysitter_retention_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_purge_cursor ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_retention_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_retention_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_evidence_index ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_write_obligations ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_write_invocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.babysitter_write_reservations ENABLE ROW LEVEL SECURITY;
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
REVOKE ALL ON sessions.babysitter_retention_settings,sessions.babysitter_purge_cursor,sessions.babysitter_retention_objects,sessions.babysitter_retention_audit,sessions.babysitter_evidence_index,
 sessions.babysitter_write_obligations,sessions.babysitter_write_invocations,sessions.babysitter_write_reservations FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_babysitter_retention;

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
--     records:[{bucket (upsert: index into buckets; delete: null), originId, recordId, revision
--               (decimal string), revisionId, digest, operation upsert|delete, kind, source, sessionId,
--               line (upsert) | null, expiresAtMs (upsert) | null}]}
-- Each record becomes one IMMUTABLE content-free candidate row (native order and permanent
-- tombstones); each bucket object gets its durable write obligation BEFORE the PUT.
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
 IF jsonb_typeof(p->'buckets') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'buckets')>500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: buckets malformed';END IF;
 -- The claim's own epoch snapshot (P0002/40001 if this is not that live claim attempt).
 v_epochs:=sessions.ingest_attempt_epochs(v_org,v_ws,v_user,v_origin,sessions.ingest_identifier(p->'batchId',false),
  sessions.ingest_digest(p->'batchDigest'),p->>'claimToken');
 SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_array(x->>'source',x->>'sessionId')),'[]'::jsonb) INTO v_pairs
  FROM (SELECT bx.e FROM jsonb_array_elements(p->'buckets') bx(e) UNION ALL
        SELECT rx.e FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p->'records')='array' THEN p->'records' ELSE '[]'::jsonb END) rx(e)) y(x)
  WHERE jsonb_typeof(x)='object' AND x->'sessionId'<>'null'::jsonb;
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
 -- The content-free key index: one IMMUTABLE candidate row per routed (record, revision) of
 -- this claim, upsert or delete. Never re-pointed: an equal-revision attempt adds its own row
 -- and only the ACK-accepted candidate is ever served (babysitter_evidence_page_for).
 IF jsonb_typeof(p->'records') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'records') NOT BETWEEN 1 AND 5000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: records malformed';END IF;
 v_object:='b='||left(encode(sha256(convert_to(p->>'batchDigest','UTF8')),'hex'),32)||'.ndjson.zst';
 v_n:=jsonb_array_length(p->'buckets');
 FOR v_rec IN SELECT e FROM jsonb_array_elements(p->'records') e LOOP
  PERFORM sessions.ingest_exact_keys(v_rec,ARRAY['bucket','originId','recordId','revision','revisionId','digest','operation','kind','source','sessionId','line','expiresAtMs']);
  -- Prepare binds every record's origin to its batch's (so to this user's machine).
  IF sessions.ingest_identifier(v_rec->'originId',false)<>v_origin THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record origin is not the batch origin';END IF;
  IF jsonb_typeof(v_rec->'revision') IS DISTINCT FROM 'string' OR v_rec->>'revision'!~'^[1-9][0-9]{0,17}$' THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record revision malformed';END IF;
  v_rev:=(v_rec->>'revision')::bigint;v_op:=v_rec->>'operation';v_rid:=sessions.ingest_identifier(v_rec->'revisionId',false);
  IF jsonb_typeof(v_rec->'operation') IS DISTINCT FROM 'string' OR v_op NOT IN('upsert','delete')
   OR jsonb_typeof(v_rec->'kind') IS DISTINCT FROM 'string' OR v_rec->>'kind' NOT IN('session_lineage','turn_receipt') THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record operation or kind malformed';END IF;
  v_source:=sessions.ingest_identifier(v_rec->'source',false);
  v_session:=CASE WHEN v_rec->'sessionId'='null'::jsonb THEN NULL ELSE sessions.ingest_identifier(v_rec->'sessionId',false) END;
  v_fence:=CASE WHEN v_session IS NULL THEN 0 ELSE (SELECT (e->>'epoch')::bigint FROM jsonb_array_elements(v_epochs) e
   WHERE e->>'source'=v_source AND e->>'session_id'=v_session LIMIT 1) END;
  IF v_fence IS NULL THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='babysitter retention: session outside claim attempt';END IF;
  IF v_op='upsert' THEN
   IF jsonb_typeof(v_rec->'bucket') IS DISTINCT FROM 'number' OR (v_rec->>'bucket')::numeric<>trunc((v_rec->>'bucket')::numeric)
    OR (v_rec->>'bucket')::numeric<0 OR (v_rec->>'bucket')::numeric>=v_n
    OR jsonb_typeof(v_rec->'line') IS DISTINCT FROM 'number' OR (v_rec->>'line')::numeric<>trunc((v_rec->>'line')::numeric)
    OR (v_rec->>'line')::numeric NOT BETWEEN 0 AND 100000 OR jsonb_typeof(v_rec->'expiresAtMs') IS DISTINCT FROM 'number'
    OR (v_rec->>'expiresAtMs')::numeric<>trunc((v_rec->>'expiresAtMs')::numeric) OR (v_rec->>'expiresAtMs')::numeric NOT BETWEEN 1 AND 9007199254740991 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record position malformed';END IF;
   v_b:=(v_rec->>'bucket')::integer+1;
   -- The record must live in its own (source, session) bucket.
   IF (p->'buckets'->(v_b-1)->>'source')<>v_source OR (p->'buckets'->(v_b-1)->'sessionId')<>(v_rec->'sessionId') THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record is not in its session bucket';END IF;
  ELSIF v_rec->'bucket'<>'null'::jsonb OR v_rec->'line'<>'null'::jsonb OR v_rec->'expiresAtMs'<>'null'::jsonb THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: record position malformed';
  END IF;
  INSERT INTO sessions.babysitter_evidence_index AS t(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,operation,kind,claim_token,
    user_id,source,session_id,reset_epoch,fence_epoch,machine_id,object_ref,object_name,line,expires_at_ms)
   VALUES(v_org,v_ws,v_origin,sessions.ingest_identifier(v_rec->'recordId',false),v_rev,v_rid,sessions.ingest_digest(v_rec->'digest'),v_op,v_rec->>'kind',
    (p->>'claimToken')::uuid,v_user,v_source,v_session,v_epoch,v_fence,
    CASE WHEN v_op='upsert' THEN v_machine END,CASE WHEN v_op='upsert' THEN v_refs[v_b] END,CASE WHEN v_op='upsert' THEN v_object END,
    CASE WHEN v_op='upsert' THEN (v_rec->>'line')::integer END,CASE WHEN v_op='upsert' THEN (v_rec->>'expiresAtMs')::bigint END)
  ON CONFLICT(org_id,workspace_id,origin_id,record_id,revision,claim_token) DO NOTHING;
  -- Idempotent only for the identical candidate of the same claim; never an update.
  IF NOT EXISTS(SELECT 1 FROM sessions.babysitter_evidence_index i WHERE i.org_id=v_org AND i.workspace_id=v_ws AND i.origin_id=v_origin
    AND i.record_id=v_rec->>'recordId' AND i.revision=v_rev AND i.claim_token=(p->>'claimToken')::uuid
    AND i.revision_id=v_rid AND i.digest=v_rec->>'digest' AND i.operation=v_op AND i.kind=v_rec->>'kind' AND i.user_id=v_user
    AND i.object_name IS NOT DISTINCT FROM CASE WHEN v_op='upsert' THEN v_object END
    AND i.line IS NOT DISTINCT FROM CASE WHEN v_op='upsert' THEN (v_rec->>'line')::integer END) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: candidate identity changed';END IF;
 END LOOP;
 -- One durable obligation per bucket object and one DISPATCHED invocation (this claim), BEFORE the PUT.
 FOR v_b IN 1..coalesce(array_length(v_refs,1),0) LOOP
  INSERT INTO sessions.babysitter_write_obligations AS w(org_id,workspace_id,machine_id,object_ref,object_name)
   VALUES(v_org,v_ws,v_machine,v_refs[v_b],v_object)
  ON CONFLICT(org_id,workspace_id,machine_id,object_ref,object_name) DO UPDATE SET rev=w.rev+1;
  INSERT INTO sessions.babysitter_write_invocations(org_id,workspace_id,machine_id,object_ref,object_name,invocation_id)
   VALUES(v_org,v_ws,v_machine,v_refs[v_b],v_object,(p->>'claimToken')::uuid)
  ON CONFLICT(org_id,workspace_id,machine_id,object_ref,object_name,invocation_id) DO NOTHING;
 END LOOP;
 RETURN jsonb_build_object('objects',out,'reopened',reopened);
END $register$;

-- Fenced = durably invisible and erasable: the session's fence epoch moved past the bucket's
-- (any disable or reset since the claim, even if re-enabled since), or the user reset.
CREATE FUNCTION sessions.babysitter_evidence_fenced(p_org text,p_ws text,p_user text,p_origin text,p_source text,p_session text,p_fence bigint,p_reset bigint) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $evfenced$
 SELECT (p_session IS NOT NULL AND coalesce((SELECT f.epoch FROM sessions.ingest_fence_versions f WHERE f.org_id=p_org
    AND f.workspace_id=p_ws AND f.user_id=p_user AND f.origin_id=p_origin AND f.source=p_source
    AND f.session_id=p_session),0)>p_fence)
  OR (SELECT count(*) FROM sessions.upload_resets r WHERE r.org_id=p_org AND r.workspace_id=p_ws AND r.user_id=p_user)>p_reset
$evfenced$;
CREATE FUNCTION sessions.babysitter_retention_fenced(o sessions.babysitter_retention_objects) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $fenced$
 SELECT sessions.babysitter_evidence_fenced(o.org_id,o.workspace_id,o.user_id,o.origin_id,o.source,o.session_id,o.fence_epoch,o.reset_epoch)
$fenced$;

-- Whether one bucket may be erased now, and why. Nothing is due within the 15 min hold-off
-- after its last registration (a race narrower, not a proof: the obligations are the proof),
-- nor while one of its unresolved obligations is backing off (next_sweep_at).
--  fenced   babysitter_retention_fenced (a disable or reset since its claim, durably);
--  expired  its expiry hour has passed.
CREATE FUNCTION sessions.babysitter_retention_due_reason(o sessions.babysitter_retention_objects) RETURNS text
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $due$
 SELECT CASE
  WHEN o.last_written_at+interval '15 minutes'>clock_timestamp() THEN NULL
  WHEN EXISTS(SELECT 1 FROM sessions.babysitter_write_obligations w WHERE w.org_id=o.org_id AND w.workspace_id=o.workspace_id
    AND w.machine_id=o.machine_id AND w.object_ref=o.object_ref AND w.next_sweep_at>clock_timestamp()) THEN NULL
  WHEN sessions.babysitter_retention_fenced(o) THEN 'fenced'
  WHEN o.erase_after<=clock_timestamp() THEN 'expired'
  ELSE NULL END
$due$;

-- Lease up to p.limit due buckets. Nothing is claimed while disabled.
-- p: {generation, limit}
CREATE FUNCTION sessions.ingest_babysitter_retention_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE s sessions.babysitter_retention_settings%ROWTYPE;q sessions.babysitter_retention_objects%ROWTYPE;
 n integer;t uuid;why text;out jsonb:='[]'::jsonb;v_purged bigint:=0;
 v_after jsonb;v_seen bigint;v_last jsonb;
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
 -- Privacy (C3): permanent tombstones of a fenced identity (disable/reset since their claim) are
 -- purged in bounded steps; they were hidden from the reader the moment the fence committed. Each
 -- claim scans at most 1000 tombstones after a persisted keyset cursor (wrapping at the end), so the
 -- work stays bounded however many tombstones accumulate.
 IF NOT s.dry_run THEN
  -- One statement over a bounded CTE: no temp or other caller-controllable relation (a caller with
  -- TEMP could pre-create one with a trigger that would run as this definer's owner); every object is
  -- schema-qualified under the fixed search_path. The cursor row is skipped, not waited on, if busy.
  SELECT c.purge_after INTO v_after FROM sessions.babysitter_purge_cursor c WHERE c.singleton FOR UPDATE SKIP LOCKED;
  IF FOUND THEN
   WITH win AS MATERIALIZED (
     SELECT x.org_id,x.workspace_id,x.origin_id,x.record_id,x.revision,x.claim_token,
      sessions.babysitter_evidence_fenced(x.org_id,x.workspace_id,x.user_id,x.origin_id,x.source,x.session_id,x.fence_epoch,x.reset_epoch) AS fenced
     FROM sessions.babysitter_evidence_index x
     -- Sargable: one row comparison against the cursor, or (no cursor) against a key below every
     -- real one ('' sorts first and revision>0 by CHECK), so it is always an index range scan.
     WHERE x.operation='delete' AND (x.org_id,x.workspace_id,x.origin_id,x.record_id,x.revision,x.claim_token)>
       (coalesce(v_after->>0,''),coalesce(v_after->>1,''),coalesce(v_after->>2,''),coalesce(v_after->>3,''),
        coalesce((v_after->>4)::bigint,0),coalesce((v_after->>5)::uuid,'00000000-0000-0000-0000-000000000000'::uuid))
     ORDER BY x.org_id,x.workspace_id,x.origin_id,x.record_id,x.revision,x.claim_token LIMIT 1000),
   gone AS (DELETE FROM sessions.babysitter_evidence_index i USING win w
     WHERE w.fenced AND i.operation='delete' AND i.org_id=w.org_id AND i.workspace_id=w.workspace_id AND i.origin_id=w.origin_id
      AND i.record_id=w.record_id AND i.revision=w.revision AND i.claim_token=w.claim_token RETURNING 1)
   SELECT (SELECT count(*) FROM gone),(SELECT count(*) FROM win),
    (SELECT jsonb_build_array(w.org_id,w.workspace_id,w.origin_id,w.record_id,w.revision::text,w.claim_token::text) FROM win w
     ORDER BY w.org_id DESC,w.workspace_id DESC,w.origin_id DESC,w.record_id DESC,w.revision DESC,w.claim_token DESC LIMIT 1)
   INTO v_purged,v_seen,v_last;
   -- Skip a no-op write (NULL->NULL: a short scan from the start).
   IF (CASE WHEN v_seen<1000 THEN NULL ELSE v_last END) IS DISTINCT FROM v_after THEN
    UPDATE sessions.babysitter_purge_cursor SET purge_after=CASE WHEN v_seen<1000 THEN NULL ELSE v_last END WHERE singleton;
   END IF;
   IF v_purged>0 THEN INSERT INTO sessions.babysitter_retention_audit(action,versions) VALUES('purged',v_purged);END IF;
  END IF;
 END IF;
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
--             (eraser.ts verifyErased), with write_seq unchanged since the claim AND no write
--             obligation left (every one completed and retired: write-obligation.ts); its
--             upsert key-index rows go with it (tombstones are permanent);
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
 IF v_outcome='erased' AND EXISTS(SELECT 1 FROM sessions.babysitter_write_obligations w WHERE w.org_id=q.org_id AND w.workspace_id=q.workspace_id
   AND w.machine_id=q.machine_id AND w.object_ref=q.object_ref) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='babysitter retention: unresolved write obligations remain';END IF;
 IF v_outcome='released' OR q.write_seq<>q.leased_write_seq THEN
  UPDATE sessions.babysitter_retention_objects SET state='pending',lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL
   WHERE lease_token=q.lease_token;
  INSERT INTO sessions.babysitter_retention_audit(action,org_id,workspace_id,machine_id,object_ref,reason,versions,write_seq)
   VALUES(CASE WHEN v_outcome='released' THEN 'released' ELSE 'raced' END,q.org_id,q.workspace_id,q.machine_id,q.object_ref,q.lease_reason,v_versions,q.write_seq);
  RETURN jsonb_build_object('ok',true,'state','pending');
 END IF;
 UPDATE sessions.babysitter_retention_objects SET state='erased',erased_at=clock_timestamp(),erased_versions=v_versions,erase_reason=q.lease_reason,
  lease_token=NULL,lease_until=NULL,leased_write_seq=NULL,lease_reason=NULL,erase_checkpoint=NULL WHERE lease_token=q.lease_token;
 DELETE FROM sessions.babysitter_evidence_index WHERE org_id=q.org_id AND workspace_id=q.workspace_id AND machine_id=q.machine_id AND object_ref=q.object_ref
  AND operation='upsert';
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

-- The writer's own outcome for each bucket PUT it dispatched (write-obligation.ts writer side),
-- bound to its claim token (the invocation id): acked (its confirmed response, with the exact
-- version id it created; "null" on an unversioned store) or refused (a definite refusal of that
-- request, e.g. 412 If-None-Match). Only a dispatched invocation moves; nothing is retired here,
-- and an unknown outcome is simply never reported (the invocation stays dispatched).
-- p: {tenant{orgId,workspaceId,userId}, originId, generation, claimToken, objects:[{ref, object, outcome, versionId|null}]}
CREATE FUNCTION sessions.ingest_babysitter_retention_written(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $written$
DECLARE v_org text;v_ws text;v_user text;v_origin text;v_machine text;o jsonb;v_n integer:=0;v_k integer;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','generation','claimToken','objects']);
 PERFORM sessions.ingest_exact_keys(p->'tenant',ARRAY['orgId','workspaceId','userId']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'claimToken') IS DISTINCT FROM 'string' OR p->>'claimToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  OR jsonb_typeof(p->'objects') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'objects') NOT BETWEEN 1 AND 500 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: write outcome malformed';END IF;
 v_org:=sessions.ingest_identifier(p#>'{tenant,orgId}',false);
 v_ws:=sessions.ingest_identifier(p#>'{tenant,workspaceId}',true);
 v_user:=sessions.ingest_identifier(p#>'{tenant,userId}',false);
 v_origin:=sessions.ingest_identifier(p->'originId',false);
 v_machine:='srv_'||left(encode(sha256(convert_to(v_ws||chr(31)||v_origin||chr(31)||v_user,'UTF8')),'hex'),32);
 FOR o IN SELECT x FROM jsonb_array_elements(p->'objects') x LOOP
  PERFORM sessions.ingest_exact_keys(o,ARRAY['ref','object','outcome','versionId']);
  IF o->>'ref' IS NULL OR o->>'ref'!~'^_bx[0-9]{10}-[0-9a-f]{16}$' OR o->>'object' IS NULL OR o->>'object'!~'^b=[0-9a-f]{32}\.ndjson\.zst$'
   OR o->>'outcome' IS NULL OR o->>'outcome' NOT IN('acked','refused')
   OR (o->>'outcome'='acked' AND (jsonb_typeof(o->'versionId') IS DISTINCT FROM 'string' OR octet_length(o->>'versionId') NOT BETWEEN 1 AND 1024))
   OR (o->>'outcome'='refused' AND o->'versionId'<>'null'::jsonb) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: write outcome malformed';END IF;
  UPDATE sessions.babysitter_write_invocations w SET outcome=o->>'outcome',version_id=CASE WHEN o->>'outcome'='acked' THEN o->>'versionId' END
   WHERE w.org_id=v_org AND w.workspace_id=v_ws AND w.machine_id=v_machine AND w.object_ref=o->>'ref' AND w.object_name=o->>'object'
    AND w.invocation_id=(p->>'claimToken')::uuid AND w.outcome='dispatched';
  GET DIAGNOSTICS v_k=ROW_COUNT;
  IF v_k>0 THEN
   UPDATE sessions.babysitter_write_obligations w SET rev=w.rev+1 WHERE w.org_id=v_org AND w.workspace_id=v_ws AND w.machine_id=v_machine
    AND w.object_ref=o->>'ref' AND w.object_name=o->>'object';
  END IF;
  v_n:=v_n+v_k;
 END LOOP;
 RETURN jsonb_build_object('updated',v_n);
END $written$;

-- The sweeper's ObligationStore (write-obligation.ts) for the bucket it holds leased under the
-- CURRENT generation. Every mutation is a compare-and-set on the key row's version (rev) and
-- answers {ok:true, rev} or {ok:false}; each call is its own statement, so it is durable before
-- the remote deletes that follow it.
-- p: {generation, leaseToken, action list|load|reserve|checkpoint|resolve|reschedule|retire, object, rev, invocations, versions, checkpoint}
CREATE FUNCTION sessions.ingest_babysitter_retention_obligation(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $obligation$
DECLARE q sessions.babysitter_retention_objects%ROWTYPE;w sessions.babysitter_write_obligations%ROWTYPE;v_act text;v jsonb;v_rev bigint;v_ids uuid[];
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_babysitter_retention');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['generation','leaseToken','action','object','rev','invocations','versions','checkpoint']);
 IF jsonb_typeof(p->'generation') IS DISTINCT FROM 'string' OR p->>'generation'!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: generation malformed';END IF;
 PERFORM sessions.ingest_require_generation(p->>'generation');
 IF jsonb_typeof(p->'leaseToken') IS DISTINCT FROM 'string' OR p->>'leaseToken'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: lease token malformed';END IF;
 v_act:=p->>'action';
 IF v_act IS NULL OR v_act NOT IN('list','load','reserve','checkpoint','resolve','reschedule','retire')
  OR (v_act<>'list' AND (p->>'object' IS NULL OR p->>'object'!~'^b=[0-9a-f]{32}\.ndjson\.zst$'))
  OR (v_act NOT IN('list','load') AND (jsonb_typeof(p->'rev') IS DISTINCT FROM 'number' OR (p->>'rev')::numeric<>trunc((p->>'rev')::numeric) OR (p->>'rev')::numeric<1))
  OR jsonb_typeof(p->'invocations') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'invocations')>1000
  OR jsonb_typeof(p->'versions') IS DISTINCT FROM 'array' OR jsonb_array_length(p->'versions')>1000
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p->'versions')='array' THEN p->'versions' ELSE '[]'::jsonb END) x
   WHERE jsonb_typeof(x) IS DISTINCT FROM 'string' OR octet_length(x#>>'{}') NOT BETWEEN 1 AND 1024)
  OR (p->'checkpoint'<>'null'::jsonb AND (jsonb_typeof(p->'checkpoint')<>'object' OR octet_length((p->'checkpoint')::text)>8192)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: obligation request malformed';END IF;
 SELECT * INTO q FROM sessions.babysitter_retention_objects WHERE lease_token=(p->>'leaseToken')::uuid AND state='leased';
 IF NOT FOUND OR q.lease_until<=clock_timestamp()+interval '30 seconds' THEN RETURN jsonb_build_object('ok',false,'code','lease_lost');END IF;
 IF v_act='list' THEN
  SELECT coalesce(jsonb_agg(o.object_name ORDER BY o.object_name COLLATE "C"),'[]'::jsonb) INTO v
   FROM (SELECT object_name FROM sessions.babysitter_write_obligations o WHERE o.org_id=q.org_id AND o.workspace_id=q.workspace_id
    AND o.machine_id=q.machine_id AND o.object_ref=q.object_ref ORDER BY o.object_name COLLATE "C" LIMIT 1000) o;
  RETURN jsonb_build_object('ok',true,'objects',v);
 END IF;
 SELECT * INTO w FROM sessions.babysitter_write_obligations o WHERE o.org_id=q.org_id AND o.workspace_id=q.workspace_id
  AND o.machine_id=q.machine_id AND o.object_ref=q.object_ref AND o.object_name=p->>'object' FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('ok',CASE WHEN v_act='load' THEN true ELSE false END,'obligation',NULL);END IF;
 IF v_act='load' THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('id',i.invocation_id::text,'outcome',i.outcome,'versionId',i.version_id) ORDER BY i.invocation_id),'[]'::jsonb) INTO v
   FROM sessions.babysitter_write_invocations i WHERE i.org_id=w.org_id AND i.workspace_id=w.workspace_id AND i.machine_id=w.machine_id
    AND i.object_ref=w.object_ref AND i.object_name=w.object_name;
  RETURN jsonb_build_object('ok',true,'obligation',jsonb_build_object('rev',w.rev,'invocations',v,'checkpoint',w.erase_checkpoint,'sweeps',w.sweeps));
 END IF;
 -- Compare-and-set: a changed row (a new invocation, a writer outcome, another sweep) refuses.
 IF w.rev<>(p->>'rev')::bigint THEN RETURN jsonb_build_object('ok',false,'code','rev_changed');END IF;
 v_rev:=w.rev+1;
 IF v_act='reserve' THEN
  -- Every version about to be deleted, under this key row's lock (FOR UPDATE above): refused when
  -- the row changed. (No Babysitter owner exists to contend; a future one must refuse these.)
  INSERT INTO sessions.babysitter_write_reservations(org_id,workspace_id,machine_id,object_ref,object_name,version_id)
   SELECT w.org_id,w.workspace_id,w.machine_id,w.object_ref,w.object_name,x FROM jsonb_array_elements_text(p->'versions') x
  ON CONFLICT DO NOTHING;
 ELSIF v_act='checkpoint' THEN
  IF p->'checkpoint'='null'::jsonb OR right(p->'checkpoint'->>'prefix',length('/s='||q.object_ref||'/raw/'))<>'/s='||q.object_ref||'/raw/'
   OR strpos(p->'checkpoint'->>'prefix','/m='||q.machine_id||'/h=_babysitter/')=0 THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='babysitter retention: obligation checkpoint is for another directory';END IF;
  UPDATE sessions.babysitter_write_obligations o SET erase_checkpoint=p->'checkpoint',rev=v_rev WHERE o.org_id=w.org_id AND o.workspace_id=w.workspace_id
   AND o.machine_id=w.machine_id AND o.object_ref=w.object_ref AND o.object_name=w.object_name;
  RETURN jsonb_build_object('ok',true,'rev',v_rev);
 ELSIF v_act='resolve' THEN
  -- Clears exactly these invocations; the sweeper calls it only on each one's own terminal evidence.
  SELECT ARRAY(SELECT x::uuid FROM jsonb_array_elements_text(p->'invocations') x) INTO v_ids;
  DELETE FROM sessions.babysitter_write_invocations i WHERE i.org_id=w.org_id AND i.workspace_id=w.workspace_id AND i.machine_id=w.machine_id
   AND i.object_ref=w.object_ref AND i.object_name=w.object_name AND i.invocation_id=ANY(v_ids)
   -- never a dispatched one: it has no terminal outcome of its own (write-obligation.ts)
   AND i.outcome IN('acked','refused');
 ELSIF v_act='reschedule' THEN
  UPDATE sessions.babysitter_write_obligations o SET sweeps=o.sweeps+1,erase_checkpoint=NULL,rev=v_rev,
   next_sweep_at=clock_timestamp()+LEAST(interval '1 day',interval '1 minute'*power(2,LEAST(o.sweeps,11)))
   WHERE o.org_id=w.org_id AND o.workspace_id=w.workspace_id AND o.machine_id=w.machine_id AND o.object_ref=w.object_ref AND o.object_name=w.object_name;
  RETURN jsonb_build_object('ok',true,'rev',v_rev);
 ELSE
  -- Retire: only with no invocation left (each resolved on its own evidence) and a fresh zero inventory.
  IF EXISTS(SELECT 1 FROM sessions.babysitter_write_invocations i WHERE i.org_id=w.org_id AND i.workspace_id=w.workspace_id AND i.machine_id=w.machine_id
    AND i.object_ref=w.object_ref AND i.object_name=w.object_name) THEN RETURN jsonb_build_object('ok',false,'code','unresolved');END IF;
  DELETE FROM sessions.babysitter_write_obligations o WHERE o.org_id=w.org_id AND o.workspace_id=w.workspace_id AND o.machine_id=w.machine_id
   AND o.object_ref=w.object_ref AND o.object_name=w.object_name;
  RETURN jsonb_build_object('ok',true,'rev',v_rev);
 END IF;
 UPDATE sessions.babysitter_write_obligations o SET rev=v_rev WHERE o.org_id=w.org_id AND o.workspace_id=w.workspace_id AND o.machine_id=w.machine_id
  AND o.object_ref=w.object_ref AND o.object_name=w.object_name;
 RETURN jsonb_build_object('ok',true,'rev',v_rev);
END $obligation$;

-- READER (Neon GET /v1/delivery/records parity for Babysitter kinds), for the read API role
-- only, tenant-pinned by the read API's verified binding (session_directory_read_api_scope).
-- ONE page in Neon's native (origin_id, record_id) order (C collation) over the content-free
-- key index. Per record, its NEWEST ACCEPTED candidate decides:
--   accepted  = the registering claim's batch is committed AND that exact body was accepted:
--               the immutable lazy ACK decision (ingest_lazy_ack_decisions: claim token,
--               candidate digest, body_accepted) or the committed revision row with the same
--               revision, revision id, digest and user (canonical modes). Never an equal
--               revision, a same owner, a batch HTTP success or a digest alone.
--   hidden    when its privacy identity is fenced (disable/reset since its claim, even if
--             re-enabled), when its bucket was erased, or when a commit source holds a NEWER
--             revision of the record (newer live or delete wins even if not indexed);
--   filtered  on the decided row's source / session (Neon filters its one row);
--   delete    -> Neon's tombstone (operation delete, payload null) under include_deleted, else
--                hidden; permanent (no bucket), independent of every payload's lifetime;
--   upsert    -> past its expiry (payload.expires_at_ms or the bucket hour): a payload-free
--                tombstone with its revision under include_deleted, else hidden; otherwise live,
--                with its exact ORIGINAL object line for the reader to fetch.
-- Bounded: at most p.limit+1 rows emitted and at most 1000 candidate rows scanned (a record is
-- always finished once started). A bounded page returns more=true and scannedTo (the last
-- record fully decided), so it resumes exactly there and never claims completeness.
-- p: {kind, source|null, sessionId|null, after: [originId, recordId] | null, limit 1..101, includeDeleted}
CREATE FUNCTION sessions.babysitter_evidence_page_for(p_workspace_id text,p jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $page$
DECLARE v_org text;r record;v_rows jsonb:='[]'::jsonb;v_emitted integer:=0;v_scanned integer:=0;v_limit integer;v_inc boolean;
 v_a0 text;v_a1 text;v_last jsonb;v_cur_o text;v_cur_r text;v_done boolean:=false;v_now timestamptz:=clock_timestamp();v_now_ms bigint;
 v_src text;v_sess text;v_any_sess boolean;v_newer boolean;v_bucket sessions.babysitter_retention_objects%ROWTYPE;v_fenced_rev bigint;
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
  SELECT i.* FROM sessions.babysitter_evidence_index i
  WHERE i.org_id=v_org AND i.workspace_id=p_workspace_id AND i.kind=p->>'kind'
   AND (v_a0 IS NULL OR (i.origin_id COLLATE "C",i.record_id COLLATE "C")>(v_a0 COLLATE "C",v_a1 COLLATE "C"))
  ORDER BY i.origin_id COLLATE "C",i.record_id COLLATE "C",i.revision DESC,i.registered_at DESC,i.claim_token
 LOOP
  IF v_cur_o IS DISTINCT FROM r.origin_id OR v_cur_r IS DISTINCT FROM r.record_id THEN
   -- The previous record is fully decided: the only point a bounded page may stop.
   IF v_cur_o IS NOT NULL THEN v_last:=jsonb_build_array(v_cur_o,v_cur_r);END IF;
   IF v_scanned>=1000 THEN RETURN jsonb_build_object('rows',v_rows,'more',true,'scannedTo',v_last);END IF;
   v_cur_o:=r.origin_id;v_cur_r:=r.record_id;v_done:=false;v_fenced_rev:=NULL;
  END IF;
  v_scanned:=v_scanned+1;
  CONTINUE WHEN v_done;
  -- Only the ACCEPTED candidate of this revision counts (see the header); others are skipped.
  CONTINUE WHEN NOT EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
     ON b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id
    WHERE a.claim_token=r.claim_token AND a.org_id=r.org_id AND a.workspace_id=r.workspace_id AND a.user_id=r.user_id AND b.state='committed')
   OR NOT (EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=r.claim_token AND d.candidate_digest=r.digest AND d.body_accepted)
        OR EXISTS(SELECT 1 FROM sessions.ingest_record_revisions c WHERE c.org_id=r.org_id AND c.workspace_id=r.workspace_id AND c.origin_id=r.origin_id
              AND c.record_id=r.record_id AND c.revision=r.revision AND c.revision_id=r.revision_id AND c.digest=r.digest AND c.user_id=r.user_id));
  -- A FENCED accepted candidate (disable/reset since its claim) is never decisive by itself: an
  -- unfenced accepted candidate of the SAME revision (re-sent after a re-enable/reset) may still
  -- decide. But nothing OLDER than a fenced candidate is ever served (no superseded body exposed).
  IF sessions.babysitter_evidence_fenced(r.org_id,r.workspace_id,r.user_id,r.origin_id,r.source,r.session_id,r.fence_epoch,r.reset_epoch) THEN
   v_fenced_rev:=coalesce(v_fenced_rev,r.revision);
   CONTINUE;
  END IF;
  IF v_fenced_rev IS NOT NULL AND r.revision<v_fenced_rev THEN v_done:=true;CONTINUE;END IF;
  v_done:=true;   -- this record's newest unfenced accepted candidate decides; older rows are never served
  -- A newer committed revision (live or delete), indexed or not, wins: never a stale row.
  v_newer:=EXISTS(SELECT 1 FROM sessions.ingest_record_revisions c WHERE c.org_id=r.org_id AND c.workspace_id=r.workspace_id AND c.origin_id=r.origin_id
     AND c.record_id=r.record_id AND c.revision>r.revision)
   OR EXISTS(SELECT 1 FROM sessions.ingest_lazy_native_slots n
     WHERE n.key_sha256=encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',r.org_id,r.workspace_id,r.origin_id,r.record_id)::text,'UTF8')),'hex')
      AND n.current_owner IS NOT NULL AND n.current_revision>r.revision);
  CONTINUE WHEN v_newer;
  CONTINUE WHEN (v_src IS NOT NULL AND r.source<>v_src) OR (NOT v_any_sess AND r.session_id IS DISTINCT FROM v_sess);
  IF r.operation='upsert' THEN
   SELECT * INTO v_bucket FROM sessions.babysitter_retention_objects o
    WHERE o.org_id=r.org_id AND o.workspace_id=r.workspace_id AND o.machine_id=r.machine_id AND o.object_ref=r.object_ref;
   CONTINUE WHEN NOT FOUND OR v_bucket.state='erased';
  END IF;
  IF r.operation='delete' OR v_bucket.erase_after<=v_now OR r.expires_at_ms<=v_now_ms THEN
   CONTINUE WHEN NOT v_inc;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('originId',r.origin_id,'recordId',r.record_id,'revision',r.revision::text,'revisionId',r.revision_id,
    'operation','delete','source',r.source,'sessionId',r.session_id));
  ELSE
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('originId',r.origin_id,'recordId',r.record_id,'revision',r.revision::text,'revisionId',r.revision_id,
    'operation','upsert','source',r.source,'sessionId',r.session_id,'machineId',r.machine_id,'ref',r.object_ref,'object',r.object_name,'line',r.line));
  END IF;
  v_emitted:=v_emitted+1;
  IF v_emitted>v_limit THEN RETURN jsonb_build_object('rows',v_rows,'more',true,'scannedTo',jsonb_build_array(r.origin_id,r.record_id));END IF;
 END LOOP;
 RETURN jsonb_build_object('rows',v_rows,'more',false,'scannedTo',CASE WHEN v_cur_o IS NULL THEN v_last ELSE jsonb_build_array(v_cur_o,v_cur_r) END);
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
 sessions.babysitter_evidence_fenced(text,text,text,text,text,text,bigint,bigint),sessions.ingest_babysitter_retention_written(jsonb),
 sessions.ingest_babysitter_retention_obligation(jsonb),
 sessions.babysitter_evidence_page_for(text,jsonb),
 sessions.ingest_babysitter_retention_claim(jsonb),sessions.ingest_babysitter_retention_complete(jsonb),sessions.babysitter_retention_configure(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_babysitter_retention,bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.ingest_babysitter_retention_register(jsonb),sessions.ingest_babysitter_retention_written(jsonb) TO bulls96_session_ingest;
GRANT USAGE ON SCHEMA sessions TO bulls96_babysitter_retention;
GRANT EXECUTE ON FUNCTION sessions.ingest_babysitter_retention_claim(jsonb),sessions.ingest_babysitter_retention_fence(jsonb),
 sessions.ingest_babysitter_retention_obligation(jsonb),
 sessions.ingest_babysitter_retention_complete(jsonb) TO bulls96_babysitter_retention;
GRANT EXECUTE ON FUNCTION sessions.babysitter_evidence_page_for(text,jsonb) TO bulls96_read_api;
