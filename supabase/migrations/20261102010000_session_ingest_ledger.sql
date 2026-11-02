-- The session ingest's ledger (src/session-ingest/ledger.ts): what relayhistory's
-- `accept_delivery_batch` decided in Neon that is not transcript content, so the
-- 96Bulls ingest (POST /v1/delivery/batches in AWS) needs no Neon. Code only:
-- applying it to a real database needs Khaliq's explicit OK.
--
-- Tables (no transcript content anywhere: ids, revisions, digests, times):
--   ingest_batches            (org, ws, origin, batch_id) -> digest, receipt_id, state.
--                             Idempotency: a retry with the same digest replays, another
--                             digest under the same id is a 409. The receipt itself is
--                             never stored; it is content-addressed and recomputed.
--   ingest_record_revisions   (org, ws, origin, record_id) -> the newest COMMITTED
--                             revision, its revision id and digest. Written at commit
--                             only, so a resumed batch never mistakes its own unfinished
--                             write for a stored one.
--   upload_deletions          deletion fences, Neon 0038 deleted_upload_sessions' shape.
--   upload_resets             a user's "delete all sessions" resets, Neon 0039's shape.
--   upload_erasure_rearm      sessions a tombstone or reset reached after an ingest
--                             claim: the erasure job sweeps them again.
--   ingest_tenant_slots       in-flight batch leases per user and org (replaces Neon's
--                             429 delivery_busy), expiring by TTL.
--
-- Revision rules: relayhistory-cloud #250 (0071), which replace 0026's. A stored
-- revision at or above the submitted one is a replay (an equal revision with another
-- digest keeps the stored row); the only revision 409 is in-batch reuse of a NEW
-- revision by two identities with different content. A batch id reused with other
-- content is a 409.
--
-- Functions (SECURITY DEFINER, pinned search_path, each refusing every caller but
-- its one role):
--   ingest_claim_batch(jsonb)    bulls96_session_ingest. Before any object write: replay
--                                / batch 409, deletion filter, in-batch revision 409,
--                                stale records, pending claim; when nothing needs
--                                writing, the batch is committed here (replay_fast).
--   ingest_commit_batch(jsonb)   bulls96_session_ingest. After the object writes:
--                                pending -> committed, the written revisions, and the
--                                fence recheck that re-arms erasure.
--   ingest_acquire_slot(jsonb)   bulls96_session_ingest. A soft per-user / per-org limit.
--   ingest_release_slot(jsonb)   bulls96_session_ingest.
--   ingest_seed_records(jsonb)   bulls96_session_ingest_seeder (the Neon delivery_records
--                                seed tool). Never lowers a stored revision.
--
-- Concurrency. No advisory lock of any kind (per user, org, origin or batch): batch
-- identity is decided by the primary key (INSERT ... ON CONFLICT, then a re-read),
-- and the only row locks are those of the batch's own row and of the revision rows a
-- commit upserts, taken in record_id order so two commits never deadlock. A claim
-- reads revision rows without locking them.
--
-- Roles. bulls96_session_ingest and bulls96_session_ingest_seeder are created here
-- NOLOGIN when absent (so the grants below, and the function ACL manifest, are exact
-- after migrate). The ingest role's bootstrap (supabase/bootstrap/session_ingest_role.sql)
-- makes it LOGIN with its settings; passwords are set out of band. An existing role
-- with elevated attributes fails the migration closed.

DO $role$
DECLARE
  v_name text;
  r pg_roles%ROWTYPE;
BEGIN
  FOREACH v_name IN ARRAY ARRAY['bulls96_session_ingest', 'bulls96_session_ingest_seeder'] LOOP
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
    IF NOT FOUND THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', v_name);
    ELSIF r.rolsuper OR r.rolbypassrls OR r.rolinherit
       OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('role %s exists with elevated attributes or INHERIT', v_name);
    END IF;
  END LOOP;
END
$role$;


CREATE TABLE sessions.ingest_batches (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  origin_id text NOT NULL,
  batch_id text NOT NULL,
  user_id text NOT NULL,
  digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
  receipt_id text NOT NULL CHECK (receipt_id ~ '^rhr_[0-9a-f]{64}$'),
  state text NOT NULL CHECK (state IN ('pending', 'committed')),
  claimed_at timestamptz NOT NULL DEFAULT now(),
  committed_at timestamptz,
  CHECK ((state = 'committed') = (committed_at IS NOT NULL)),
  PRIMARY KEY (org_id, workspace_id, origin_id, batch_id)
);

CREATE TABLE sessions.ingest_record_revisions (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  origin_id text NOT NULL,
  record_id text NOT NULL,
  revision bigint NOT NULL CHECK (revision > 0),
  revision_id text NOT NULL,
  digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id)
);

CREATE TABLE sessions.upload_deletions (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  origin_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, user_id, origin_id, source, session_id)
);

CREATE TABLE sessions.upload_resets (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  request_id text NOT NULL,
  reset_at timestamptz NOT NULL DEFAULT now(),
  deleted bigint NOT NULL DEFAULT 0 CHECK (deleted >= 0),
  PRIMARY KEY (org_id, workspace_id, user_id, request_id)
);
CREATE INDEX upload_resets_user_time_idx
  ON sessions.upload_resets (org_id, workspace_id, user_id, reset_at);

CREATE TABLE sessions.upload_erasure_rearm (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  origin_id text NOT NULL,
  machine_id text NOT NULL CHECK (machine_id ~ '^srv_[0-9a-f]{32}$'),
  source text NOT NULL,
  session_id text NOT NULL,
  queued_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, user_id, origin_id, machine_id, source, session_id)
);

ALTER TABLE sessions.ingest_batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_record_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.upload_deletions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.upload_resets ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.upload_erasure_rearm ENABLE ROW LEVEL SECURITY;

CREATE TABLE sessions.ingest_tenant_slots (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  holder text NOT NULL,
  acquired_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, user_id, holder)
);
CREATE INDEX ingest_tenant_slots_org_expiry_idx ON sessions.ingest_tenant_slots (org_id, expires_at);
ALTER TABLE sessions.ingest_tenant_slots ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON sessions.ingest_batches, sessions.ingest_record_revisions,
  sessions.upload_deletions, sessions.upload_resets, sessions.upload_erasure_rearm,
  sessions.ingest_tenant_slots
  FROM PUBLIC, anon, authenticated, bulls96_session_ingest, bulls96_session_ingest_seeder;


-- The caller must be `p_role`: inside SECURITY DEFINER the `role` setting still
-- names a SET ROLE caller, and session_user names a direct login. Whatever EXECUTE
-- grants a later migration adds, no other role gets through.
CREATE FUNCTION sessions.ingest_require_role(p_role text)
RETURNS void
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $guard$
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM p_role THEN
    RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = 'session ingest: caller is not the function''s role';
  END IF;
END
$guard$;


-- One delivery identifier, as relayhistory's `identifier()`: non-empty, at most
-- 512 UTF-8 bytes, no control characters. `p_empty_ok` admits '' (a workspace).
CREATE FUNCTION sessions.ingest_identifier(p jsonb, p_empty_ok boolean)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $ident$
DECLARE
  v text;
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'string' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: identifier is not a string';
  END IF;
  v := p #>> '{}';
  IF (v = '' AND NOT p_empty_ok) OR octet_length(v) > 512 OR v ~ '[\x01-\x1f\x7f]' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: identifier is malformed';
  END IF;
  RETURN v;
END
$ident$;

-- The keys an object must have, exactly.
CREATE FUNCTION sessions.ingest_exact_keys(p jsonb, p_keys text[])
RETURNS void
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $keys$
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'object'
     OR (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(p) AS k)
        IS DISTINCT FROM (SELECT array_agg(k ORDER BY k) FROM unnest(p_keys) AS k) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: input shape is malformed';
  END IF;
END
$keys$;

-- The batch's (source, session_id) pairs: an array of distinct two-identifier arrays.
CREATE FUNCTION sessions.ingest_session_pairs(p jsonb)
RETURNS TABLE (source text, session_id text)
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $pairs$
DECLARE
  e jsonb;
  seen text[] := ARRAY[]::text[];
  k text;
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'array' OR jsonb_array_length(p) > 500 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: sessions is malformed';
  END IF;
  FOR e IN SELECT x FROM jsonb_array_elements(p) AS x LOOP
    IF jsonb_typeof(e) <> 'array' OR jsonb_array_length(e) <> 2 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: session pair is malformed';
    END IF;
    source := sessions.ingest_identifier(e -> 0, false);
    session_id := sessions.ingest_identifier(e -> 1, false);
    k := jsonb_build_array(source, session_id)::text;
    IF k = ANY (seen) THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: session pairs repeat';
    END IF;
    seen := seen || k;
    RETURN NEXT;
  END LOOP;
END
$pairs$;

-- Tenant, origin, batch id and digest common to claim and commit.
CREATE FUNCTION sessions.ingest_batch_key(p jsonb,
  OUT org_id text, OUT workspace_id text, OUT user_id text,
  OUT origin_id text, OUT batch_id text, OUT digest text)
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $key$
BEGIN
  PERFORM sessions.ingest_exact_keys(p -> 'tenant', ARRAY['orgId', 'workspaceId', 'userId']);
  org_id := sessions.ingest_identifier(p #> '{tenant,orgId}', false);
  workspace_id := sessions.ingest_identifier(p #> '{tenant,workspaceId}', true);
  user_id := sessions.ingest_identifier(p #> '{tenant,userId}', false);
  origin_id := sessions.ingest_identifier(p -> 'originId', false);
  batch_id := sessions.ingest_identifier(p -> 'batchId', false);
  IF jsonb_typeof(p -> 'batchDigest') IS DISTINCT FROM 'string'
     OR (p ->> 'batchDigest') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: batchDigest is malformed';
  END IF;
  digest := p ->> 'batchDigest';
END
$key$;


-- A positive safe-integer revision (JavaScript's Number.isSafeInteger, > 0).
CREATE FUNCTION sessions.ingest_revision(p jsonb)
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $rev$
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'number'
     OR (p #>> '{}') !~ '^[1-9][0-9]{0,15}$'
     OR (p #>> '{}')::numeric > 9007199254740991 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: revision is malformed';
  END IF;
  RETURN (p #>> '{}')::bigint;
END
$rev$;

-- A lowercase hex SHA-256.
CREATE FUNCTION sessions.ingest_digest(p jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $digest$
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'string' OR (p #>> '{}') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: digest is malformed';
  END IF;
  RETURN p #>> '{}';
END
$digest$;

-- The batch's records, validated and normalized: an array of
-- {ord, source, session_id, record_id, revision_id, revision, digest}, `ord` the
-- 0-based index in the input. A record with no session (null, or '' as the local
-- feed spells it) has session_id null and is never deleted.
CREATE FUNCTION sessions.ingest_records(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $records$
DECLARE
  v_e jsonb;
  v_out jsonb := '[]'::jsonb;
  v_session text;
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'array' OR jsonb_array_length(p) > 500 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: records is malformed';
  END IF;
  FOR v_e IN SELECT x FROM jsonb_array_elements(p) AS x LOOP
    PERFORM sessions.ingest_exact_keys(v_e, ARRAY['source', 'sessionId', 'recordId', 'revisionId', 'revision', 'digest']);
    IF jsonb_typeof(v_e -> 'sessionId') = 'null' OR v_e -> 'sessionId' = '""'::jsonb THEN
      v_session := NULL;
    ELSE
      v_session := sessions.ingest_identifier(v_e -> 'sessionId', false);
    END IF;
    v_out := v_out || jsonb_build_array(jsonb_build_object(
      'ord', jsonb_array_length(v_out),
      'source', sessions.ingest_identifier(v_e -> 'source', false),
      'session_id', v_session,
      'record_id', sessions.ingest_identifier(v_e -> 'recordId', false),
      'revision_id', sessions.ingest_identifier(v_e -> 'revisionId', false),
      'revision', sessions.ingest_revision(v_e -> 'revision'),
      'digest', sessions.ingest_digest(v_e -> 'digest')));
  END LOOP;
  RETURN v_out;
END
$records$;

CREATE FUNCTION sessions.ingest_claim_batch(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $claim$
DECLARE
  k record;
  v_receipt text;
  v_batch sessions.ingest_batches%ROWTYPE;
  v_state text;
  v_pairs jsonb;
  v_items jsonb;
  v_live jsonb;
  v_deleted jsonb;
  v_conflicts jsonb;
  v_count integer;
  v_stale jsonb;
  v_resets integer;
  v_fast boolean;
  v_inserted boolean;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['tenant', 'originId', 'batchId', 'batchDigest', 'receiptId', 'sessions', 'records']);
  k := sessions.ingest_batch_key(p);
  IF jsonb_typeof(p -> 'receiptId') IS DISTINCT FROM 'string'
     OR (p ->> 'receiptId') !~ '^rhr_[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: receiptId is malformed';
  END IF;
  v_receipt := p ->> 'receiptId';
  -- Validated and normalized once; every later step reads these values only. No
  -- temporary table: a definer function must not read objects the caller can create.
  v_pairs := (SELECT coalesce(jsonb_agg(jsonb_build_array(x.source, x.session_id)), '[]'::jsonb)
                FROM sessions.ingest_session_pairs(p -> 'sessions') AS x);
  v_items := sessions.ingest_records(p -> 'records');

  -- (a) A known batch id: its receipt, a resume, or a batch-id 409. The primary key
  -- decides a race between two first claims (the insert below), not a lock.
  SELECT * INTO v_batch FROM sessions.ingest_batches AS b
   WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
     AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
  IF FOUND THEN
    IF v_batch.digest <> k.digest THEN
      RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
    END IF;
    IF v_batch.state = 'committed' THEN
      RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
    END IF;
    v_state := 'resume';
  ELSE
    v_state := 'claimed';
  END IF;

  -- (b) The batch's sessions this user deleted from this origin. As Neon 0038, their
  -- records are dropped before the revision checks (the receipt still acknowledges
  -- them), so only the rest is judged below.
  SELECT coalesce(jsonb_agg(jsonb_build_array(d.source, d.session_id)
           ORDER BY d.source COLLATE "C", d.session_id COLLATE "C"), '[]'::jsonb)
    INTO v_deleted
    FROM sessions.upload_deletions AS d
    JOIN (SELECT x.pair ->> 0 AS source, x.pair ->> 1 AS session_id FROM jsonb_array_elements(v_pairs) AS x(pair)
          UNION
          SELECT i.item ->> 'source', i.item ->> 'session_id' FROM jsonb_array_elements(v_items) AS i(item)
           WHERE i.item ->> 'session_id' IS NOT NULL) AS s
      ON s.source = d.source AND s.session_id = d.session_id
   WHERE d.org_id = k.org_id AND d.workspace_id = k.workspace_id
     AND d.user_id = k.user_id AND d.origin_id = k.origin_id;
  v_live := (SELECT coalesce(jsonb_agg(i.item ORDER BY (i.item ->> 'ord')::integer), '[]'::jsonb)
               FROM jsonb_array_elements(v_items) AS i(item)
              WHERE i.item ->> 'session_id' IS NULL
                 OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_deleted) AS d(pair)
                                 WHERE d.pair ->> 0 = i.item ->> 'source' AND d.pair ->> 1 = i.item ->> 'session_id'));

  -- (c) #250's revision rules over the committed revisions, read without a lock
  -- (the primary key serves record_id = ANY of the batch's ids). In identity order
  -- (record_id, revision, revision_id): a record whose stored revision is at or
  -- above its own is stale (a replay, whatever its digest); a NEW revision (none
  -- stored, or the stored one lower) must not be reused in the batch, by its first
  -- peer in identity order, with both another revision id and another digest.
  WITH sorted AS (
    SELECT i.*, row_number() OVER (
             ORDER BY i.record_id COLLATE "C", i.revision, i.revision_id COLLATE "C", i.ord) AS rn
      FROM jsonb_to_recordset(v_live) AS i(ord integer, source text, session_id text,
                                            record_id text, revision_id text, revision bigint, digest text)
  ), stored AS (
    SELECT c.record_id, c.revision
      FROM sessions.ingest_record_revisions AS c
     WHERE c.org_id = k.org_id AND c.workspace_id = k.workspace_id AND c.origin_id = k.origin_id
       AND c.record_id = ANY (ARRAY(SELECT DISTINCT s.record_id FROM sorted AS s))
  ), peers AS (
    SELECT DISTINCT ON (s.record_id, s.revision) s.record_id, s.revision,
           s.revision_id AS peer_revision_id, s.digest AS peer_digest
      FROM sorted AS s ORDER BY s.record_id, s.revision, s.rn
  ), judged AS (
    SELECT s.rn, s.ord, s.record_id, s.revision_id, s.revision, s.digest,
           c.revision IS NOT NULL AND c.revision >= s.revision AS stale,
           (c.revision IS NULL OR c.revision < s.revision)
             AND p.peer_revision_id <> s.revision_id AND p.peer_digest <> s.digest AS twin,
           p.peer_revision_id, p.peer_digest
      FROM sorted AS s
      JOIN peers AS p ON p.record_id = s.record_id AND p.revision = s.revision
      LEFT JOIN stored AS c ON c.record_id = s.record_id
  ), numbered AS (
    SELECT j.*, row_number() OVER (PARTITION BY j.twin ORDER BY j.rn) AS twin_rn FROM judged AS j
  )
  SELECT count(*) FILTER (WHERE n.twin)::integer,
         coalesce(jsonb_agg(jsonb_build_object(
           'recordId', n.record_id,
           'submittedRevisionId', n.revision_id,
           'submittedRevision', n.revision,
           'submittedDigest', n.digest,
           'currentRevisionId', n.peer_revision_id,
           'currentRevision', n.revision,
           'currentDigest', n.peer_digest) ORDER BY n.rn) FILTER (WHERE n.twin AND n.twin_rn <= 100), '[]'::jsonb),
         coalesce(jsonb_agg(n.ord ORDER BY n.ord) FILTER (WHERE n.stale), '[]'::jsonb),
         bool_and(n.stale) IS NOT FALSE
    INTO v_count, v_conflicts, v_stale, v_fast
    FROM numbered AS n;
  IF v_count > 0 THEN
    RETURN jsonb_build_object('kind', 'revision_conflict', 'conflicts', v_conflicts, 'count', v_count);
  END IF;

  SELECT count(*)::integer INTO v_resets FROM sessions.upload_resets AS r
   WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id AND r.user_id = k.user_id;

  -- (d) The claim. When nothing needs writing (every record stale or deleted), the
  -- batch is committed here and the caller writes nothing (replay_fast).
  IF v_state = 'claimed' THEN
    INSERT INTO sessions.ingest_batches
           (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at)
    VALUES (k.org_id, k.workspace_id, k.origin_id, k.batch_id, k.user_id, k.digest, v_receipt,
            CASE WHEN v_fast THEN 'committed' ELSE 'pending' END, CASE WHEN v_fast THEN now() END)
    ON CONFLICT (org_id, workspace_id, origin_id, batch_id) DO NOTHING
    RETURNING true INTO v_inserted;
    IF v_inserted IS NULL THEN
      -- A concurrent first claim of this batch id won: answer as a retry would.
      SELECT * INTO v_batch FROM sessions.ingest_batches AS b
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
      IF v_batch.digest <> k.digest THEN
        RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
      END IF;
      IF v_batch.state = 'committed' THEN
        RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
      END IF;
      v_state := 'resume';
    END IF;
  END IF;
  IF v_fast THEN
    IF v_state = 'resume' THEN
      UPDATE sessions.ingest_batches AS b
         SET state = 'committed', committed_at = coalesce(b.committed_at, now())
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id AND b.digest = k.digest;
    END IF;
    v_state := 'replay_fast';
  END IF;

  RETURN jsonb_build_object('kind', v_state, 'deleted', v_deleted, 'stale', v_stale, 'resets', v_resets);
END
$claim$;

CREATE FUNCTION sessions.ingest_commit_batch(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $commit$
DECLARE
  k record;
  v_machine text;
  v_items jsonb;
  v_claimed_at timestamptz;
  v_rearmed integer;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['tenant', 'originId', 'batchId', 'batchDigest', 'sessions', 'records', 'machineId']);
  k := sessions.ingest_batch_key(p);
  IF jsonb_typeof(p -> 'machineId') IS DISTINCT FROM 'string'
     OR (p ->> 'machineId') !~ '^srv_[0-9a-f]{32}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: machineId is malformed';
  END IF;
  v_machine := p ->> 'machineId';
  v_items := sessions.ingest_records(p -> 'records');
  PERFORM sessions.ingest_session_pairs(p -> 'sessions');

  -- The batch's own row lock; nothing wider.
  UPDATE sessions.ingest_batches AS b
     SET state = 'committed', committed_at = coalesce(b.committed_at, now())
   WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
     AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id AND b.digest = k.digest
  RETURNING b.claimed_at INTO v_claimed_at;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'session ingest: no claim for this batch';
  END IF;

  -- The written revisions become stored: per record its highest (first in identity
  -- order), inserted, or replacing a lower one only (#250: an equal revision keeps
  -- the stored row). Rows are touched in record_id order, so two commits sharing
  -- records lock them in the same order. Idempotent.
  INSERT INTO sessions.ingest_record_revisions AS t
         (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest)
  SELECT DISTINCT ON (i.record_id) k.org_id, k.workspace_id, k.origin_id,
         i.record_id, i.revision, i.revision_id, i.digest
    FROM jsonb_to_recordset(v_items) AS i(ord integer, record_id text, revision_id text, revision bigint, digest text)
   ORDER BY i.record_id, i.revision DESC, i.revision_id, i.ord
  ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE
     SET revision = EXCLUDED.revision, revision_id = EXCLUDED.revision_id,
         digest = EXCLUDED.digest, updated_at = now()
   WHERE EXCLUDED.revision > t.revision;

  -- The fence again: a tombstone, or a reset since the claim, that this batch's
  -- objects may have outrun. Those sessions are swept again.
  WITH reached AS (
    SELECT s.source, s.session_id
      FROM sessions.ingest_session_pairs(p -> 'sessions') AS s
     WHERE EXISTS (SELECT 1 FROM sessions.upload_deletions AS d
                    WHERE d.org_id = k.org_id AND d.workspace_id = k.workspace_id
                      AND d.user_id = k.user_id AND d.origin_id = k.origin_id
                      AND d.source = s.source AND d.session_id = s.session_id)
        OR EXISTS (SELECT 1 FROM sessions.upload_resets AS r
                    WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id
                      AND r.user_id = k.user_id AND r.reset_at >= v_claimed_at)
  ), queued AS (
    INSERT INTO sessions.upload_erasure_rearm
           (org_id, workspace_id, user_id, origin_id, machine_id, source, session_id)
    SELECT k.org_id, k.workspace_id, k.user_id, k.origin_id, v_machine, r.source, r.session_id
      FROM reached AS r
    ON CONFLICT DO NOTHING
  )
  SELECT count(*)::integer INTO v_rearmed FROM reached;
  RETURN jsonb_build_object('rearmed', v_rearmed);
END
$commit$;

-- A bounded integer setting.
CREATE FUNCTION sessions.ingest_bounded(p jsonb, p_min integer, p_max integer)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $bounded$
BEGIN
  IF p IS NULL OR jsonb_typeof(p) <> 'number' OR (p #>> '{}') !~ '^[0-9]{1,9}$'
     OR (p #>> '{}')::integer NOT BETWEEN p_min AND p_max THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: setting is out of range';
  END IF;
  RETURN (p #>> '{}')::integer;
END
$bounded$;

-- A lease for one in-flight batch, under both the user's and the org's limit.
-- Soft: the counts and the insert are one statement but not serialized, so two
-- racing acquires can both pass by one. A lease expires by TTL, so a crashed
-- invocation frees itself; the same holder re-acquiring refreshes its lease.
-- A refusal carries a jittered retry of 2-10 s (relayhistory DELIVERY_BUSY_RETRY_AFTER).
CREATE FUNCTION sessions.ingest_acquire_slot(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $acquire$
DECLARE
  v_org text;
  v_ws text;
  v_user text;
  v_holder text;
  v_max_user integer;
  v_max_org integer;
  v_ttl integer;
  v_n integer;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['org_id', 'workspace_id', 'user_id', 'holder', 'max_per_user', 'max_per_org', 'ttl_seconds']);
  v_org := sessions.ingest_identifier(p -> 'org_id', false);
  v_ws := sessions.ingest_identifier(p -> 'workspace_id', true);
  v_user := sessions.ingest_identifier(p -> 'user_id', false);
  v_holder := sessions.ingest_identifier(p -> 'holder', false);
  v_max_user := sessions.ingest_bounded(p -> 'max_per_user', 1, 1000);
  v_max_org := sessions.ingest_bounded(p -> 'max_per_org', 1, 100000);
  v_ttl := sessions.ingest_bounded(p -> 'ttl_seconds', 5, 900);

  DELETE FROM sessions.ingest_tenant_slots AS s WHERE s.org_id = v_org AND s.expires_at <= now();
  INSERT INTO sessions.ingest_tenant_slots AS t (org_id, workspace_id, user_id, holder, expires_at)
  SELECT v_org, v_ws, v_user, v_holder, now() + make_interval(secs => v_ttl)
   WHERE EXISTS (SELECT 1 FROM sessions.ingest_tenant_slots AS s
                  WHERE s.org_id = v_org AND s.user_id = v_user AND s.holder = v_holder AND s.expires_at > now())
      OR ((SELECT count(*) FROM sessions.ingest_tenant_slots AS s
            WHERE s.org_id = v_org AND s.user_id = v_user AND s.expires_at > now()) < v_max_user
          AND (SELECT count(*) FROM sessions.ingest_tenant_slots AS s
                WHERE s.org_id = v_org AND s.expires_at > now()) < v_max_org)
  ON CONFLICT (org_id, user_id, holder) DO UPDATE
     SET expires_at = EXCLUDED.expires_at, workspace_id = EXCLUDED.workspace_id;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  IF v_n > 0 THEN
    RETURN jsonb_build_object('ok', true);
  END IF;
  RETURN jsonb_build_object('ok', false, 'retry_after_seconds', 2 + floor(random() * 9)::integer);
END
$acquire$;

CREATE FUNCTION sessions.ingest_release_slot(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $release$
DECLARE
  v_n integer;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  PERFORM sessions.ingest_exact_keys(p, ARRAY['org_id', 'user_id', 'holder']);
  DELETE FROM sessions.ingest_tenant_slots AS s
   WHERE s.org_id = sessions.ingest_identifier(p -> 'org_id', false)
     AND s.user_id = sessions.ingest_identifier(p -> 'user_id', false)
     AND s.holder = sessions.ingest_identifier(p -> 'holder', false);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN jsonb_build_object('released', v_n > 0);
END
$release$;

-- The Neon delivery_records seed: committed revisions copied in, the highest per
-- record kept. Never lowers a stored revision; an equal one keeps the stored row.
CREATE FUNCTION sessions.ingest_seed_records(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $seed$
DECLARE
  v_e jsonb;
  v_rows jsonb := '[]'::jsonb;
  v_n integer;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest_seeder');
  IF p IS NULL OR jsonb_typeof(p) <> 'array' OR jsonb_array_length(p) > 5000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest seed: input is malformed';
  END IF;
  FOR v_e IN SELECT x FROM jsonb_array_elements(p) AS x LOOP
    PERFORM sessions.ingest_exact_keys(v_e,
      ARRAY['org_id', 'workspace_id', 'origin_id', 'record_id', 'revision', 'revision_id', 'digest']);
    v_rows := v_rows || jsonb_build_array(jsonb_build_object(
      'org_id', sessions.ingest_identifier(v_e -> 'org_id', false),
      'workspace_id', sessions.ingest_identifier(v_e -> 'workspace_id', true),
      'origin_id', sessions.ingest_identifier(v_e -> 'origin_id', false),
      'record_id', sessions.ingest_identifier(v_e -> 'record_id', false),
      'revision', sessions.ingest_revision(v_e -> 'revision'),
      'revision_id', sessions.ingest_identifier(v_e -> 'revision_id', false),
      'digest', sessions.ingest_digest(v_e -> 'digest')));
  END LOOP;
  INSERT INTO sessions.ingest_record_revisions AS t
         (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest)
  SELECT DISTINCT ON (r.org_id, r.workspace_id, r.origin_id, r.record_id)
         r.org_id, r.workspace_id, r.origin_id, r.record_id, r.revision, r.revision_id, r.digest
    FROM jsonb_to_recordset(v_rows) AS r(org_id text, workspace_id text, origin_id text, record_id text,
                                         revision bigint, revision_id text, digest text)
   ORDER BY r.org_id, r.workspace_id, r.origin_id, r.record_id, r.revision DESC, r.revision_id
  ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE
     SET revision = EXCLUDED.revision, revision_id = EXCLUDED.revision_id,
         digest = EXCLUDED.digest, updated_at = now()
   WHERE EXCLUDED.revision > t.revision;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN jsonb_build_object('upserted', v_n);
END
$seed$;

REVOKE ALL ON FUNCTION sessions.ingest_require_role(text),
  sessions.ingest_identifier(jsonb, boolean), sessions.ingest_exact_keys(jsonb, text[]),
  sessions.ingest_session_pairs(jsonb), sessions.ingest_batch_key(jsonb),
  sessions.ingest_revision(jsonb), sessions.ingest_digest(jsonb), sessions.ingest_records(jsonb),
  sessions.ingest_bounded(jsonb, integer, integer),
  sessions.ingest_claim_batch(jsonb), sessions.ingest_commit_batch(jsonb),
  sessions.ingest_acquire_slot(jsonb), sessions.ingest_release_slot(jsonb),
  sessions.ingest_seed_records(jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT USAGE ON SCHEMA sessions TO bulls96_session_ingest, bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.ingest_claim_batch(jsonb), sessions.ingest_commit_batch(jsonb),
  sessions.ingest_acquire_slot(jsonb), sessions.ingest_release_slot(jsonb)
  TO bulls96_session_ingest;
GRANT EXECUTE ON FUNCTION sessions.ingest_seed_records(jsonb) TO bulls96_session_ingest_seeder;
