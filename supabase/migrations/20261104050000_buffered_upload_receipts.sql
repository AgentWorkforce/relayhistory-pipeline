-- Buffer-only acceptance. No canonical publication or queue TTL is ACK proof.
-- Original record metadata stays in SQL; transcript payload stays scrubbed in S3.
ALTER TABLE sessions.ingest_batches
 ADD COLUMN publication_state text NOT NULL DEFAULT 'canonical' CHECK(publication_state IN('buffered','canonical')),
 ADD COLUMN buffer_receipt_kind text CHECK(buffer_receipt_kind IN('staged','suppressed'));
ALTER TABLE sessions.ingest_claim_attempts
 ADD COLUMN records jsonb CHECK(records IS NULL OR jsonb_typeof(records)='array'),
 ADD COLUMN reset_epoch bigint CHECK(reset_epoch IS NULL OR reset_epoch>=0);

CREATE TABLE sessions.ingest_buffer_admission (
 org_id text PRIMARY KEY,
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256 ~ '^[0-9a-f]{64}$'),
 retention_contract_sha256 text NOT NULL CHECK(retention_contract_sha256 ~ '^[0-9a-f]{64}$'),
 valid_until timestamptz NOT NULL
);
-- Each intent is registered beforePUT. Retired keys remain durable cleanup
-- tickets, including unacknowledged/ambiguous old writes.
CREATE TABLE sessions.ingest_stage_intents (
 stage_token uuid PRIMARY KEY,
 claim_token uuid NOT NULL REFERENCES sessions.ingest_claim_attempts(claim_token) ON DELETE RESTRICT,
 org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,
 stage_key text NOT NULL UNIQUE CHECK(octet_length(stage_key) BETWEEN 1 AND 2048),
 stage_sha256 text NOT NULL CHECK(stage_sha256 ~ '^[0-9a-f]{64}$'),
 stage_bytes bigint NOT NULL CHECK(stage_bytes BETWEEN 1 AND 16777216),
 group_ordinals jsonb NOT NULL CHECK(jsonb_typeof(group_ordinals)='array'),
 reset_epoch bigint NOT NULL CHECK(reset_epoch>=0),
 receipt_state text NOT NULL DEFAULT 'preparing' CHECK(receipt_state IN('preparing','acknowledged')),
 cleanup_state text NOT NULL DEFAULT 'live' CHECK(cleanup_state IN('live','retired','redacted')),
 parent_stage_token uuid REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
 row_version bigint NOT NULL DEFAULT 1 CHECK(row_version>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 UNIQUE(claim_token,stage_key),
 FOREIGN KEY(org_id,workspace_id,origin_id,batch_id) REFERENCES sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id) ON DELETE RESTRICT
);
CREATE INDEX ingest_stage_intents_cleanup_idx ON sessions.ingest_stage_intents(cleanup_state,created_at,stage_token);
CREATE TABLE sessions.ingest_buffer_outbox (
 org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,
 user_id text NOT NULL,digest text NOT NULL,
 active_stage_token uuid REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
 state text NOT NULL DEFAULT 'buffered' CHECK(state IN('buffered','erased')),
 row_version bigint NOT NULL DEFAULT 1 CHECK(row_version>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,origin_id,batch_id),
 FOREIGN KEY(org_id,workspace_id,origin_id,batch_id) REFERENCES sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id) ON DELETE RESTRICT,
 CHECK((state='buffered')=(active_stage_token IS NOT NULL))
);
ALTER TABLE sessions.ingest_buffer_admission ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_stage_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_buffer_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_buffer_admission,sessions.ingest_stage_intents,sessions.ingest_buffer_outbox
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_require_buffer_admission(p_org text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $admission$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=p_org AND valid_until>clock_timestamp()) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered privacy or retention baseline not ready';
 END IF;
END
$admission$;

CREATE FUNCTION sessions.ingest_claim_batch_mode(p jsonb,p_mode text)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
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
  v_token uuid;
  v_epochs jsonb;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF p_mode NOT IN ('buffered','canonical') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid publication mode'; END IF;
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

  PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);

  -- (a) A known batch id: its receipt, a resume, or a batch-id 409. The primary key
  -- decides a race between two first claims (the insert below), not a lock.
  SELECT * INTO v_batch FROM sessions.ingest_batches AS b
   WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
     AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
  IF FOUND THEN
    IF v_batch.user_id<>k.user_id THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: batch owner mismatch'; END IF;
    IF v_batch.digest <> k.digest THEN
      RETURN jsonb_build_object('kind', 'batch_conflict', 'currentDigest', v_batch.digest);
    END IF;
    IF v_batch.state = 'committed' THEN
      IF v_batch.publication_state='buffered' AND v_batch.buffer_receipt_kind IS DISTINCT FROM 'suppressed'
        AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox AS o
          LEFT JOIN sessions.ingest_stage_intents AS t ON t.stage_token=o.active_stage_token
          WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id
            AND o.user_id=k.user_id AND o.digest=k.digest AND
            (o.state='erased' OR (t.receipt_state='acknowledged' AND t.cleanup_state='live'))) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered receipt proof missing';
      END IF;
      RETURN jsonb_build_object('kind', 'replay', 'receiptId', v_batch.receipt_id);
    END IF;
    IF v_batch.publication_state<>p_mode THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit publication transition required'; END IF;
    v_state := 'resume';
  ELSE
    v_state := 'claimed';
  END IF;

  IF p_mode='buffered' THEN PERFORM sessions.ingest_require_buffer_admission(k.org_id); END IF;

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
    IF p_mode='buffered' THEN RETURN jsonb_build_object('kind','baseline_pending'); END IF;
    RETURN jsonb_build_object('kind', 'revision_conflict', 'conflicts', v_conflicts, 'count', v_count);
  END IF;

  IF p_mode='buffered' THEN
    -- Unknown historical maxima cannot suppress accepted bytes. Potential twins
    -- above were deferred rather than incorrectly409ing a possibly stale batch.
    v_stale:='[]'::jsonb;
    v_fast:=jsonb_array_length(v_live)=0;
  END IF;

  SELECT count(*)::integer INTO v_resets FROM sessions.upload_resets AS r
   WHERE r.org_id = k.org_id AND r.workspace_id = k.workspace_id AND r.user_id = k.user_id;

  -- (d) The claim. When nothing needs writing (every record stale or deleted), the
  -- batch is committed here and the caller writes nothing (replay_fast).
  IF v_state = 'claimed' THEN
    INSERT INTO sessions.ingest_batches
           (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at,publication_state,buffer_receipt_kind)
    VALUES (k.org_id, k.workspace_id, k.origin_id, k.batch_id, k.user_id, k.digest, v_receipt,
            CASE WHEN v_fast THEN 'committed' ELSE 'pending' END, CASE WHEN v_fast THEN now() END,p_mode,CASE WHEN p_mode='buffered' AND v_fast THEN 'suppressed' END)
    ON CONFLICT (org_id, workspace_id, origin_id, batch_id) DO NOTHING
    RETURNING true INTO v_inserted;
    IF v_inserted IS NULL THEN
      -- A concurrent first claim of this batch id won: answer as a retry would.
      SELECT * INTO v_batch FROM sessions.ingest_batches AS b
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id;
      IF v_batch.user_id<>k.user_id THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='session ingest: batch owner mismatch'; END IF;
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
         SET state = 'committed', committed_at = coalesce(b.committed_at, now()),
             buffer_receipt_kind=CASE WHEN p_mode='buffered' THEN 'suppressed' ELSE b.buffer_receipt_kind END
       WHERE b.org_id = k.org_id AND b.workspace_id = k.workspace_id
         AND b.origin_id = k.origin_id AND b.batch_id = k.batch_id AND b.digest = k.digest;
    END IF;
    v_state := 'replay_fast';
  END IF;

  INSERT INTO sessions.ingest_session_ownership
    (org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT k.org_id,k.workspace_id,k.user_id,k.origin_id,s.source,s.session_id
  FROM (SELECT x.source,x.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS x
        UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_live) AS i(source text,session_id text)
          WHERE i.session_id IS NOT NULL) AS s
  WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions AS d
    WHERE d.org_id=k.org_id AND d.workspace_id=k.workspace_id AND d.user_id=k.user_id
      AND d.origin_id=k.origin_id AND d.source=s.source AND d.session_id=s.session_id)
  ON CONFLICT DO NOTHING;
  IF v_state IN ('claimed','resume') THEN
    v_token:=gen_random_uuid();
    SELECT coalesce(jsonb_agg(jsonb_build_object('source',x.source,'session_id',x.session_id,'epoch',coalesce(f.epoch,0),'reset_epoch',v_resets)
      ORDER BY x.source COLLATE "C",x.session_id COLLATE "C"),'[]'::jsonb) INTO v_epochs
    FROM (SELECT q.source,q.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS q
          UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
           WHERE i.session_id IS NOT NULL) AS x
    LEFT JOIN sessions.ingest_fence_versions AS f ON f.org_id=k.org_id AND f.workspace_id=k.workspace_id
      AND f.user_id=k.user_id AND f.origin_id=k.origin_id AND f.source=x.source AND f.session_id=x.session_id;
    INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch)
    VALUES(v_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_epochs,v_items,v_resets);
  END IF;
  RETURN jsonb_build_object('kind', v_state, 'claimToken',v_token, 'deleted', v_deleted, 'stale', v_stale, 'resets', v_resets);
END
$claim$;


REVOKE ALL ON FUNCTION sessions.ingest_claim_batch_mode(jsonb,text),sessions.ingest_require_buffer_admission(text)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
CREATE OR REPLACE FUNCTION sessions.ingest_claim_batch(p jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $canonical$
 SELECT sessions.ingest_claim_batch_mode(p,'canonical');
$canonical$;
CREATE FUNCTION sessions.ingest_claim_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $buffered$
 SELECT sessions.ingest_claim_batch_mode(p,'buffered');
$buffered$;
