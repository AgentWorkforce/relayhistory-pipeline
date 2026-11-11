-- Operator per-session erase, the upload-path fence check (lane B, M2). Slot 050010, confirmed by the program lead.
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- 050000 fences every owner the erase can see. This closes the rest: an owner or origin first seen after
-- the erase (a new device, another user, a re-enabled selection, or a claim that raced the erase's owner
-- read). Three places check sessions.session_erasure_fences:
--  * sessions.ingest_claim_batch_mode, after the batch-id answers (receipt, replay, 409) and before step (b);
--  * sessions.ingest_commit_batch and sessions.ingest_commit_buffered_batch, before ingest_attempt_fenced;
--  * BEFORE INSERT OR UPDATE row guards on session_index and session_directory: a live row for
--    a fenced physical or canonical id is skipped, including delayed directory projection.
--
-- The check (lead ruling, hot path of the claim timeouts): a plain read of the fence primary key
-- (org_id, workspace_id, source, session_id), all four columns, one probe per distinct pair of the
-- batch, without locking the fence rows. The separate common writer gate below runs before USER
-- and pointer/directory row locks: the user-fence path gains one fixed shared advisory acquisition,
-- including claims/commits/ACKs. Ordinary writers share it; erase owns it exclusively. The probe's
-- selection extension runs only if a permanent fence matches.
--
-- On a match, a pair this user and origin have not already deselected is deselected now with
-- sessions.ingest_apply_selection (enabled:false), the same write sessions.ingest_set_session_uploads
-- makes and 050000 made for every owner it saw: fence epoch bumped, upload_deletions written, stored
-- revisions and ownership removed. The claim then drops its records (the receipt still acknowledges
-- them), and the commit reports them fenced (records skipped, objects rearmed for the sweeper).
-- In an ordered org (lazy, buffered, or with buffer admission) the selection would go through the ordered
-- control path, which cannot be driven from here: the call is refused with RH012 (not retryable).
-- 050000 refuses to erase in such an org, so this only happens if the org became ordered after an erase.
--
-- Claim/commit bodies are 043850's plus one fence PERFORM each. Publication scope and buffer GC
-- preserve their predecessor bodies plus GLOBAL-before-USER acquisition. Owners and ACLs are kept.

CREATE FUNCTION sessions.ingest_erase_fence_extend(p_org text,p_ws text,p_user text,p_origin text,p_pairs jsonb)
RETURNS integer
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $extend$
DECLARE v_hits jsonb;
BEGIN
 -- The hot path: a plain EXISTS, one primary-key probe per pair on all four columns. Nothing is locked.
 -- LATERAL ... LIMIT 1 keeps the probe parameterized per pair: the planner cannot turn it into a join
 -- that scans the (org, workspace) prefix of the fence key.
 IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p_pairs) AS x(pair)
   CROSS JOIN LATERAL (SELECT 1 FROM sessions.session_erasure_fences AS f
     WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.source=x.pair->>0 AND f.session_id=x.pair->>1 LIMIT 1) AS h) THEN
  RETURN 0;
 END IF;
 -- A fence matched. Only pairs not already deselected for this user and origin are written again, so a
 -- client that keeps sending a fenced session does not bump its fence epoch on every claim and commit.
 SELECT jsonb_agg(jsonb_build_array(h.source,h.session_id) ORDER BY h.source COLLATE "C",h.session_id COLLATE "C") INTO v_hits
  FROM (SELECT DISTINCT x.pair->>0 AS source,x.pair->>1 AS session_id FROM jsonb_array_elements(p_pairs) AS x(pair)) AS h
  CROSS JOIN LATERAL (SELECT 1 FROM sessions.session_erasure_fences AS f
     WHERE f.org_id=p_org AND f.workspace_id=p_ws AND f.source=h.source AND f.session_id=h.session_id LIMIT 1) AS hit
  WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions AS d
     WHERE d.org_id=p_org AND d.workspace_id=p_ws AND d.user_id=p_user AND d.origin_id=p_origin
       AND d.source=h.source AND d.session_id=h.session_id);
 IF v_hits IS NULL THEN RETURN 0;END IF;
 IF sessions.ingest_operator_erase_ordered(p_org) THEN
  RAISE EXCEPTION USING ERRCODE='RH012',MESSAGE='operator erase: a fenced session cannot be deselected in an ordered org';
 END IF;
 RETURN sessions.ingest_apply_selection(p_org,p_ws,p_user,p_origin,v_hits,false);
END
$extend$;

-- Acquired before row locks, including UPDATE/ON CONFLICT, so erasure cannot miss an
-- uncommitted writer or deadlock by waiting on a row whose trigger waits on erase.
CREATE FUNCTION sessions.ingest_erase_fence_index_lock() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $index_lock$
BEGIN
 PERFORM pg_advisory_xact_lock_shared(961996,50000);
 IF current_setting('transaction_isolation')<>'read committed' AND
   (SELECT is_called FROM sessions.session_erasure_writer_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator erase: writer requires a fresh read-committed snapshot';END IF;
 RETURN NULL;
END $index_lock$;
CREATE TRIGGER ingest_erase_fence_index_lock BEFORE INSERT OR UPDATE OR DELETE ON sessions.session_index
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_erase_fence_index_lock();

CREATE TRIGGER ingest_erase_fence_directory_lock BEFORE INSERT OR UPDATE OR DELETE ON sessions.session_directory
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_erase_fence_index_lock();

-- Every ingest_lock_fences call first INSERTs this row, including existing-row
-- ON CONFLICT. Seeders that insert it directly are covered too. Gate before USER.
CREATE TRIGGER ingest_erase_fence_owner_lock BEFORE INSERT OR UPDATE ON sessions.ingest_fence_owners
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_erase_fence_index_lock();

-- Any writer of sessions.session_index or session_directory: a live row for a fenced id is skipped (RETURN NULL), as the
-- publication pointer guard skips a guarded write. A tombstone (deleted_at set) always passes.
-- SECURITY DEFINER: the writer (search_consumer_runtime, the ingest role) cannot read the fences.
CREATE FUNCTION sessions.ingest_erase_fence_index_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $index_guard$
BEGIN
 IF NEW.deleted_at IS NULL AND (
   EXISTS(SELECT 1 FROM sessions.session_erasure_fences AS f WHERE f.org_id=NEW.org_id AND f.workspace_id=NEW.workspace_id
     AND f.source=NEW.harness AND f.session_id=NEW.session_id)
   OR ((to_jsonb(NEW)->>'canonical_session_id') IS NOT NULL AND EXISTS(SELECT 1 FROM sessions.session_erasure_fences AS f
     WHERE f.org_id=NEW.org_id AND f.workspace_id=NEW.workspace_id AND f.source=NEW.harness AND f.session_id=(to_jsonb(NEW)->>'canonical_session_id')))) THEN
  RETURN NULL;
 END IF;
 RETURN NEW;
END
$index_guard$;

CREATE TRIGGER ingest_erase_fence_index_guard BEFORE INSERT OR UPDATE ON sessions.session_index
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_erase_fence_index_guard();

CREATE TRIGGER ingest_erase_fence_directory_guard BEFORE INSERT OR UPDATE ON sessions.session_directory
 FOR EACH ROW EXECUTE FUNCTION sessions.ingest_erase_fence_index_guard();

REVOKE ALL ON FUNCTION sessions.ingest_erase_fence_extend(text,text,text,text,jsonb),
  sessions.ingest_erase_fence_index_guard(),sessions.ingest_erase_fence_index_lock()
  FROM PUBLIC,anon,authenticated,service_role;
DO $revoke_consumer$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='search_consumer_runtime') THEN
  REVOKE ALL ON FUNCTION sessions.ingest_erase_fence_extend(text,text,text,text,jsonb),
   sessions.ingest_erase_fence_index_guard(),sessions.ingest_erase_fence_index_lock() FROM search_consumer_runtime;
 END IF;
END $revoke_consumer$;

-- sessions.ingest_claim_batch_mode: 20261105043850's body plus the fence extension before step (b).
CREATE OR REPLACE FUNCTION sessions.ingest_claim_batch_mode(p jsonb,p_mode text)
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
  v_acq jsonb;v_acq_pairs jsonb;v_nulls jsonb;
BEGIN
 IF p_mode='canonical' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit buffered witness required';END IF;
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF p_mode NOT IN ('buffered','canonical') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='session ingest: invalid publication mode'; END IF;
  -- Flag off: an `acquisition` key is malformed input, exactly as before this migration.
  PERFORM sessions.ingest_exact_keys(CASE WHEN sessions.ingest_acquisition_hooks_live() THEN p-'acquisition' ELSE p END,
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

  IF p_mode='canonical' AND (
    EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=k.org_id) OR
    EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=k.org_id AND publication_state='buffered')) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: explicit buffered publication authority required';
  END IF;
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

  -- 050010: a pair under an operator erase fence is deselected for this user and origin, so (b) drops
  -- its records exactly as for any deselected session. One PK probe per distinct pair, no lock.
  PERFORM sessions.ingest_erase_fence_extend(k.org_id,k.workspace_id,k.user_id,k.origin_id,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(x.source,x.session_id)),'[]'::jsonb)
       FROM (SELECT q.pair->>0 AS source,q.pair->>1 AS session_id FROM jsonb_array_elements(v_pairs) AS q(pair)
             UNION SELECT i.item->>'source',i.item->>'session_id' FROM jsonb_array_elements(v_items) AS i(item)
              WHERE i.item->>'session_id' IS NOT NULL) AS x));

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
  ), stored AS MATERIALIZED (
    SELECT c.record_id, c.revision
      FROM sessions.ingest_record_revisions AS c
     WHERE c.org_id = k.org_id AND c.workspace_id = k.workspace_id AND c.origin_id = k.origin_id
       AND c.record_id = ANY (ARRAY(SELECT DISTINCT s.record_id FROM sorted AS s))
       AND NOT(p_mode='buffered' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton))
    UNION ALL
    SELECT n.record_id,n.current_revision AS revision FROM sessions.ingest_lazy_native_slots n
     WHERE p_mode='buffered' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
      AND n.org_id=k.org_id AND n.workspace_id=k.workspace_id AND n.origin_id=k.origin_id
      AND n.current_owner=k.user_id AND n.current_revision IS NOT NULL
      -- Probe the existing global-native PK for this bounded batch. An owner
      -- index alone would scan every current record the user has ever uploaded.
      AND n.key_sha256=ANY(ARRAY(SELECT encode(sha256(convert_to(
        jsonb_build_array('global-native-custody-v1',k.org_id,k.workspace_id,k.origin_id,s.record_id)::text,'UTF8')),'hex')
        FROM sorted s))
      AND n.record_id=ANY(ARRAY(SELECT DISTINCT s.record_id FROM sorted s))
  ), peers AS (
    SELECT DISTINCT ON (s.record_id, s.revision) s.record_id, s.revision,
           s.revision_id AS peer_revision_id, s.digest AS peer_digest
      FROM sorted AS s ORDER BY s.record_id, s.revision, s.rn
  ), judged AS (
    SELECT s.rn, s.ord, s.record_id, s.revision_id, s.revision, s.digest,
           c.revision IS NOT NULL AND c.revision >= s.revision AS stale,
           (c.revision IS NULL OR c.revision < s.revision OR
             (c.revision=s.revision AND p_mode='buffered' AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)))
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
    IF p_mode='buffered' AND NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton) THEN
      RETURN jsonb_build_object('kind','baseline_pending'); END IF;
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

  -- Acquisition admission (factory acquisition owner, design §5.1): LA, then _admit, before the ownership insert.
  IF v_state IN ('claimed','resume') THEN
    v_acq_pairs:=sessions.ingest_acquisition_sorted((SELECT coalesce(jsonb_agg(jsonb_build_array(x.source,x.session_id)),'[]'::jsonb)
      FROM (SELECT q.source,q.session_id FROM sessions.ingest_session_pairs(p -> 'sessions') AS q
            UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
             WHERE i.session_id IS NOT NULL) AS x));
    v_nulls:=(SELECT coalesce(jsonb_agg(DISTINCT i.source),'[]'::jsonb) FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
      WHERE i.session_id IS NULL);
    PERFORM sessions.ingest_acquisition_lock(k.org_id,k.workspace_id,v_acq_pairs,false);
    v_acq:=sessions.ingest_acquisition_admit(k.org_id,k.workspace_id,k.user_id,k.origin_id,v_acq_pairs,v_nulls,p->'acquisition');
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
    INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch,acquisition)
    VALUES(v_token,k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_epochs,v_items,v_resets,coalesce(v_acq,'[]'::jsonb));
  END IF;
  RETURN jsonb_build_object('kind', v_state, 'claimToken',v_token, 'deleted', v_deleted, 'stale', v_stale, 'resets', v_resets);
END
$claim$;

-- sessions.ingest_commit_buffered_batch: 20261105043850's body plus the fence extension before ingest_attempt_fenced.
CREATE OR REPLACE FUNCTION sessions.ingest_commit_buffered_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $bcommit$
DECLARE k record;a sessions.ingest_claim_attempts%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;v_generation text;
 v_pairs jsonb;v_fenced jsonb;v_invalid boolean;v_stage uuid;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM sessions.ingest_exact_keys(p,ARRAY['tenant','originId','batchId','batchDigest','claimToken','stageToken','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 PERFORM sessions.ingest_require_generation(v_generation);
 k:=sessions.ingest_batch_key(p);
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);
 PERFORM sessions.ingest_lazy_attempt((p->>'claimToken')::uuid);
 PERFORM sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
 v_stage:=(p->>'stageToken')::uuid;
 SELECT * INTO a FROM sessions.ingest_claim_attempts WHERE claim_token=(p->>'claimToken')::uuid;
 IF a.schema_generation IS DISTINCT FROM v_generation
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: claim generation changed';END IF;
 PERFORM 1 FROM sessions.ingest_batches b WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id
  AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id AND b.user_id=k.user_id AND b.digest=k.digest
  AND b.publication_state='buffered' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: no buffered batch'; END IF;
 SELECT * INTO t FROM sessions.ingest_stage_intents WHERE stage_token=v_stage AND claim_token=a.claim_token FOR UPDATE;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.cleanup_state<>'live'
   OR (t.receipt_state='preparing' AND t.expires_at<=clock_timestamp()) THEN
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_array(r.source,r.session_id)),'[]'::jsonb) INTO v_pairs
 FROM (SELECT DISTINCT r.source,r.session_id FROM jsonb_to_recordset(a.records) r(ord integer,source text,session_id text)
  WHERE r.session_id IS NOT NULL AND r.ord IN(
   SELECT (o::text)::int FROM jsonb_array_elements(t.group_ordinals) g CROSS JOIN LATERAL jsonb_array_elements(g) o)) r;
 -- 050010: an erase fence the claim did not see. This org is ordered, so a hit is refused (RH012).
 PERFORM sessions.ingest_erase_fence_extend(k.org_id,k.workspace_id,k.user_id,k.origin_id,v_pairs);
 v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,a.epochs,v_pairs);
 v_invalid:=jsonb_array_length(v_fenced)>0 OR a.reset_epoch IS NULL OR a.reset_epoch<>(SELECT count(*) FROM sessions.upload_resets r
  WHERE r.org_id=k.org_id AND r.workspace_id=k.workspace_id AND r.user_id=k.user_id);
 IF v_invalid THEN
  UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1 WHERE stage_token=v_stage;
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 IF t.receipt_state='acknowledged' THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id
    AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id AND o.digest=k.digest AND o.active_stage_token=v_stage) THEN
   PERFORM sessions.ingest_require_buffer_admission(k.org_id);
   RETURN jsonb_build_object('kind','committed')||CASE WHEN EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
  THEN jsonb_build_object('custodyAck',sessions.ingest_lazy_attempt_ack((p->>'claimToken')::uuid)) ELSE '{}'::jsonb END;
  END IF;
  RETURN jsonb_build_object('kind','intent_invalid');
 END IF;
 IF EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=k.org_id AND o.workspace_id=k.workspace_id
   AND o.origin_id=k.origin_id AND o.batch_id=k.batch_id) THEN RETURN jsonb_build_object('kind','intent_invalid'); END IF;
 PERFORM sessions.ingest_acquisition_lock(k.org_id,k.workspace_id,v_pairs,false);
 PERFORM sessions.ingest_acquisition_refuse(sessions.ingest_acquisition_fenced(k.org_id,k.workspace_id,a.acquisition,v_pairs,'admit'));
 -- Receipt and durable recovery pointer are one transaction; no queue proof.
 INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,user_id,origin_id,batch_id,digest,active_stage_token)
 VALUES(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,v_stage);
 UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged',row_version=row_version+1 WHERE stage_token=v_stage;
 UPDATE sessions.ingest_batches b SET state='committed',committed_at=coalesce(b.committed_at,clock_timestamp()),buffer_receipt_kind='staged'
 WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id AND b.batch_id=k.batch_id
  AND b.user_id=k.user_id AND b.digest=k.digest AND b.publication_state='buffered';
 IF clock_timestamp()>=t.expires_at THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered intent expired';END IF;
 PERFORM sessions.ingest_require_buffer_admission(k.org_id);
 RETURN jsonb_build_object('kind','committed')||CASE WHEN EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
  THEN jsonb_build_object('custodyAck',sessions.ingest_lazy_attempt_ack((p->>'claimToken')::uuid)) ELSE '{}'::jsonb END;
END
$bcommit$;

-- sessions.ingest_commit_batch: 20261105043850's body plus the fence extension before ingest_attempt_fenced.
CREATE OR REPLACE FUNCTION sessions.ingest_commit_batch(p jsonb)
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
  v_epochs jsonb;
  v_fenced jsonb;
  v_pairs jsonb;
  v_invalidated boolean;
BEGIN
  PERFORM sessions.ingest_require_role('bulls96_session_ingest');
  IF EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
    USING(org_id,workspace_id,origin_id,batch_id)
    WHERE a.claim_token::text=p->>'claimToken' AND (b.publication_state='buffered' OR b.buffer_receipt_kind='staged')) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: buffered publication refused';
  END IF;

  PERFORM sessions.ingest_exact_keys(p,
    ARRAY['tenant', 'originId', 'batchId', 'batchDigest', 'sessions', 'records', 'machineId', 'claimToken']);
  k := sessions.ingest_batch_key(p);
  IF jsonb_typeof(p -> 'machineId') IS DISTINCT FROM 'string'
     OR (p ->> 'machineId') !~ '^srv_[0-9a-f]{32}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session ingest: machineId is malformed';
  END IF;
  v_machine := p ->> 'machineId';
  v_items := sessions.ingest_records(p -> 'records');
  PERFORM sessions.ingest_session_pairs(p -> 'sessions');

  PERFORM sessions.ingest_lock_fences(k.org_id,k.workspace_id,k.user_id);

  v_epochs:=sessions.ingest_attempt_epochs(k.org_id,k.workspace_id,k.user_id,k.origin_id,k.batch_id,k.digest,p->>'claimToken');
  v_pairs:=(SELECT coalesce(jsonb_agg(jsonb_build_array(x.source,x.session_id)),'[]'::jsonb)
    FROM (SELECT q.source,q.session_id FROM sessions.ingest_session_pairs(p->'sessions') AS q
          UNION SELECT i.source,i.session_id FROM jsonb_to_recordset(v_items) AS i(source text,session_id text)
            WHERE i.session_id IS NOT NULL) AS x);
  -- 050010: an erase fence the claim did not see (the erase committed after it) deselects the pair now;
  -- the epoch bump makes ingest_attempt_fenced report it, so its records are skipped and its objects rearmed.
  PERFORM sessions.ingest_erase_fence_extend(k.org_id,k.workspace_id,k.user_id,k.origin_id,v_pairs);
  v_fenced:=sessions.ingest_attempt_fenced(k.org_id,k.workspace_id,k.user_id,k.origin_id,v_epochs,v_pairs);
  PERFORM sessions.ingest_acquisition_lock(k.org_id,k.workspace_id,v_pairs,false);
  PERFORM sessions.ingest_acquisition_refuse(sessions.ingest_acquisition_fenced(k.org_id,k.workspace_id,
    coalesce((SELECT c.acquisition FROM sessions.ingest_claim_attempts c WHERE c.claim_token=(p->>'claimToken')::uuid),'[]'::jsonb),v_pairs,'admit'));
  SELECT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) AS x(pair)
    WHERE NOT EXISTS(SELECT 1 FROM sessions.upload_deletions AS d
      WHERE d.org_id=k.org_id AND d.workspace_id=k.workspace_id AND d.user_id=k.user_id
        AND d.origin_id=k.origin_id AND d.source=x.pair->>0 AND d.session_id=x.pair->>1))
    INTO v_invalidated;
  SELECT b.claimed_at INTO v_claimed_at FROM sessions.ingest_batches AS b
   WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id
     AND b.batch_id=k.batch_id AND b.digest=k.digest AND b.user_id=k.user_id FOR UPDATE;
  IF NOT v_invalidated THEN
    UPDATE sessions.ingest_batches AS b SET state='committed',committed_at=coalesce(b.committed_at,now())
     WHERE b.org_id=k.org_id AND b.workspace_id=k.workspace_id AND b.origin_id=k.origin_id
       AND b.batch_id=k.batch_id AND b.digest=k.digest AND b.user_id=k.user_id;
  END IF;

  -- The written revisions become stored: per record its highest (first in identity
  -- order), inserted, or replacing a lower one only (#250: an equal revision keeps
  -- the stored row). Rows are touched in record_id order, so two commits sharing
  -- records lock them in the same order. Idempotent.
  INSERT INTO sessions.ingest_record_revisions AS t
         (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id)
  SELECT DISTINCT ON (i.record_id) k.org_id, k.workspace_id, k.origin_id,
         i.record_id, i.revision, i.revision_id, i.digest, k.user_id, i.source, i.session_id
    FROM jsonb_to_recordset(v_items) AS i(ord integer, source text, session_id text, record_id text,
                                          revision_id text, revision bigint, digest text)
   WHERE i.session_id IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_fenced) AS f(pair)
     WHERE f.pair->>0=i.source AND f.pair->>1=i.session_id)
   ORDER BY i.record_id, i.revision DESC, i.revision_id, i.ord
  ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE
     SET revision = EXCLUDED.revision, revision_id = EXCLUDED.revision_id,
         digest = EXCLUDED.digest, user_id = EXCLUDED.user_id, source = EXCLUDED.source,
         session_id = EXCLUDED.session_id, updated_at = now()
   WHERE EXCLUDED.revision > t.revision;

  -- The fence again: a tombstone, or a reset since the claim, that this batch's
  -- objects may have outrun. Those sessions are swept again.
  WITH reached AS (
    SELECT x.pair->>0 AS source,x.pair->>1 AS session_id FROM jsonb_array_elements(v_fenced) AS x(pair)
  ), queued AS (
    INSERT INTO sessions.upload_erasure_rearm
           (org_id, workspace_id, user_id, origin_id, machine_id, source, session_id)
    SELECT k.org_id, k.workspace_id, k.user_id, k.origin_id, v_machine, r.source, r.session_id
      FROM reached AS r
    ON CONFLICT DO NOTHING
  )
  SELECT count(*)::integer INTO v_rearmed FROM reached;
  RETURN jsonb_build_object('rearmed', v_rearmed,'invalidated',v_invalidated);
END
$commit$;

-- Lock-order replacement of 20261104060000_ingest_canonical_publication.sql: GLOBAL before direct USER NOWAIT.
CREATE OR REPLACE FUNCTION sessions.ingest_publication_scope(p jsonb,p_lock boolean) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $scope$
DECLARE v_org text;v_ws text;v_harness text;v_session text;v_machine text;v_key text;v_revision bigint;
 h sessions.ingest_publication_heads%ROWTYPE;v_reset bigint;v_fence bigint;v_current boolean;
BEGIN
 PERFORM sessions.ingest_exact_keys(p,ARRAY['orgId','workspaceId','harness','sessionId','machineId','manifestKey','revision','schemaGeneration']);
 PERFORM sessions.ingest_require_generation(p->>'schemaGeneration');
 v_org:=sessions.ingest_identifier(p->'orgId',false);v_ws:=sessions.ingest_identifier(p->'workspaceId',true);
 v_harness:=sessions.ingest_identifier(p->'harness',false);v_session:=sessions.ingest_identifier(p->'sessionId',false);
 v_machine:=sessions.ingest_identifier(p->'machineId',false);v_key:=p->>'manifestKey';
 IF jsonb_typeof(p->'manifestKey') IS DISTINCT FROM'string' OR octet_length(v_key) NOT BETWEEN 1 AND 2048
  OR jsonb_typeof(p->'revision') IS DISTINCT FROM'string' OR (p->>'revision')!~'^[1-9][0-9]{0,18}$'
 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='publication: invalid pointer evidence';END IF;
 v_revision:=(p->>'revision')::bigint;
 IF p_lock THEN
  PERFORM pg_advisory_xact_lock_shared(961996,50000);
  IF current_setting('transaction_isolation')<>'read committed' THEN
   RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: final writes require fresh snapshots';END IF;
  IF NOT pg_try_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:publication-pointer',v_org,v_ws,v_harness,v_session,v_machine),0))
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='publication: pointer scope busy';END IF;
 END IF;
 SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=v_org AND workspace_id=v_ws
  AND physical_source=v_harness AND physical_session=v_session AND machine_id=v_machine;
 IF NOT FOUND THEN
  RETURN jsonb_build_object('managed',position('/upload-generations/'IN v_key)>0,
   'current',position('/upload-generations/'IN v_key)=0,'generation',NULL,'revision',v_revision::text);
 END IF;
 IF p_lock THEN
  PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id FOR UPDATE NOWAIT;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication: user fence missing';END IF;
  SELECT * INTO h FROM sessions.ingest_publication_heads WHERE org_id=v_org AND workspace_id=v_ws
   AND physical_source=v_harness AND physical_session=v_session AND machine_id=v_machine FOR UPDATE NOWAIT;
 END IF;
 SELECT count(*) INTO v_reset FROM sessions.upload_resets WHERE org_id=h.org_id AND workspace_id=h.workspace_id AND user_id=h.user_id;
 SELECT coalesce(max(epoch),0) INTO v_fence FROM sessions.ingest_fence_versions WHERE org_id=h.org_id AND workspace_id=h.workspace_id
  AND user_id=h.user_id AND origin_id=h.origin_id AND source=h.source AND session_id=h.session_id;
 v_current:=h.state='live' AND h.current_key=v_key AND h.pointer_revision=v_revision
  AND h.reset_epoch=v_reset AND h.fence_epoch=v_fence
  AND EXISTS(SELECT 1 FROM sessions.ingest_publication_generations WHERE generation=h.generation AND state='live');
 RETURN jsonb_build_object('managed',true,'current',coalesce(v_current,false),'generation',h.generation,'revision',h.pointer_revision::text);
END $scope$;

-- Lock-order replacement of 20261105039000_buffered_gc_single_candidate.sql: GLOBAL before direct USER NOWAIT.
CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $claim$
DECLARE v_limit int;v_generation text;candidate record;s sessions.ingest_stage_intents%ROWTYPE;
 child sessions.ingest_stage_intents%ROWTYPE;parent sessions.ingest_stage_intents%ROWTYPE;
 v_keep jsonb;v_ordinals jsonb;v_phase text;v_ticket jsonb;v_tickets jsonb:='[]';v_has_outbox boolean;v_abort boolean;v_token uuid;v_outbox_version bigint;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 PERFORM pg_advisory_xact_lock_shared(961996,50000);
 PERFORM sessions.ingest_exact_keys(p,ARRAY['limit','generation']);
 v_generation:=sessions.ingest_digest(p->'generation');
 -- Marker first: metadata operations never wait for it while holding user/table
 -- locks. The migrator's PENDING update waits for this short operation to finish.
 PERFORM sessions.ingest_require_generation(v_generation);
 v_limit:=sessions.ingest_bounded(p->'limit',1,100);
 FOR candidate IN SELECT stage_token,org_id,workspace_id,user_id,origin_id,batch_id
  FROM sessions.ingest_stage_intents
  WHERE cleanup_state<>'redacted' AND (gc_lease_until IS NULL OR gc_lease_until<=clock_timestamp())
  ORDER BY coalesce(gc_checked_at,created_at),stage_token LIMIT 4
 LOOP
  -- GC-only admission hint; legacy writers do not take this advisory lock.
  -- The existing USER row lock below remains the authority shared with them.
  -- An acquisition-only subtransaction releases EVERY lock on a busy skip.
  -- No retention error, timeout, generation failure or mutation is swallowed.
  BEGIN
   IF NOT pg_try_advisory_xact_lock(hashtextextended(jsonb_build_array(
      '96bulls:buffer-gc-user-v1',candidate.org_id,candidate.workspace_id,candidate.user_id)::text,0)) THEN
    RAISE lock_not_available USING MESSAGE='buffer gc: user busy';
   END IF;
   PERFORM 1 FROM sessions.ingest_fence_owners WHERE org_id=candidate.org_id
    AND workspace_id=candidate.workspace_id AND user_id=candidate.user_id FOR UPDATE NOWAIT;
   IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: user fence missing';
   END IF;
   PERFORM 1 FROM sessions.ingest_batches WHERE org_id=candidate.org_id AND workspace_id=candidate.workspace_id
    AND origin_id=candidate.origin_id AND batch_id=candidate.batch_id FOR UPDATE NOWAIT;
   IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: batch missing';
   END IF;
   SELECT * INTO s FROM sessions.ingest_stage_intents WHERE stage_token=candidate.stage_token FOR UPDATE NOWAIT;
   IF NOT FOUND OR s.cleanup_state='redacted' OR s.gc_lease_until>clock_timestamp() THEN
    RAISE lock_not_available USING MESSAGE='buffer gc: candidate changed';
   END IF;
   PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
    AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE NOWAIT;
  EXCEPTION WHEN lock_not_available THEN
   CONTINUE;
  END;
  IF ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id) IS DISTINCT FROM
    ROW(candidate.org_id,candidate.workspace_id,candidate.user_id,candidate.origin_id,candidate.batch_id)
  THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: identity changed';END IF;
  -- A prospective child cannot be independently aborted during its parent's copy.
  IF s.parent_stage_token IS NOT NULL THEN
   SELECT * INTO parent FROM sessions.ingest_stage_intents WHERE stage_token=s.parent_stage_token;
   -- Prospective bytes have a deterministic key. They remain owned through
   -- repeated parent crashes; retiring them merely because a lease expired would
   -- make that same replacement key permanently unusable after its barrier.
   IF parent.gc_replacement_stage_token=s.stage_token AND parent.cleanup_state='live' AND parent.receipt_state='acknowledged'
    AND ROW(parent.org_id,parent.workspace_id,parent.user_id,parent.origin_id,parent.batch_id,parent.claim_token,parent.digest)
      IS NOT DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.claim_token,s.digest)
    AND EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=parent.org_id AND o.workspace_id=parent.workspace_id
      AND o.user_id=parent.user_id AND o.origin_id=parent.origin_id AND o.batch_id=parent.batch_id AND o.digest=parent.digest
      AND o.state='buffered' AND o.active_stage_token=parent.stage_token)
   THEN
    UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
    EXIT;
   END IF;
  END IF;
  PERFORM 1 FROM sessions.ingest_buffer_outbox WHERE org_id=s.org_id AND workspace_id=s.workspace_id
   AND origin_id=s.origin_id AND batch_id=s.batch_id FOR UPDATE;
  SELECT row_version INTO v_outbox_version FROM sessions.ingest_buffer_outbox WHERE active_stage_token=s.stage_token;
  v_has_outbox:=FOUND;
  v_abort:=false;
  IF s.cleanup_state='retired' THEN
   IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: current object cannot be cleaned';END IF;
   v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
  ELSE
   v_ordinals:=sessions.ingest_buffer_gc_ordinals(s.stage_token);
   SELECT coalesce(jsonb_agg(idx-1 ORDER BY idx),'[]') INTO v_keep
    FROM jsonb_array_elements(s.group_ordinals) WITH ORDINALITY g(value,idx)
    WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(value)o WHERE v_ordinals @> jsonb_build_array(o));
   IF s.receipt_state='preparing' THEN
    IF v_has_outbox THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: preparing current object';END IF;
    v_abort:=s.expires_at<=clock_timestamp() OR jsonb_array_length(v_ordinals)<>(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o);
    IF NOT v_abort THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     EXIT;
    END IF;
    v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
   ELSE
    IF NOT v_has_outbox THEN
     -- Acknowledged but unreferenced intent is a known retired object.
     v_abort:=true;v_keep:='[]';v_ordinals:='[]';v_phase:='cleanup';
    ELSIF jsonb_array_length(v_ordinals)=(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o) THEN
     UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp() WHERE stage_token=s.stage_token;
     EXIT;
    ELSE v_phase:='plan';END IF;
   END IF;
  END IF;
  -- A stale prospective child is never adopted into another keep set.
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token FOR UPDATE;
   IF NOT FOUND OR child.cleanup_state<>'live' OR child.group_ordinals IS DISTINCT FROM
     sessions.ingest_buffer_gc_groups(s.group_ordinals,v_ordinals)
   THEN
    IF FOUND AND NOT EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox WHERE active_stage_token=child.stage_token) THEN
     UPDATE sessions.ingest_stage_intents SET cleanup_state='retired',row_version=row_version+1,
      gc_lease_token=NULL,gc_lease_until=NULL,gc_lease_version=NULL WHERE stage_token=child.stage_token;
    END IF;
    s.gc_replacement_stage_token:=NULL;
   END IF;
  END IF;
  v_token:=gen_random_uuid();
  UPDATE sessions.ingest_stage_intents SET gc_generation=v_generation,gc_lease_token=v_token,gc_lease_until=clock_timestamp()+interval '5 minutes',
   row_version=row_version+1,gc_lease_version=row_version+1,gc_checked_at=clock_timestamp(),gc_keep_groups=v_keep,gc_keep_ordinals=v_ordinals,
   gc_replacement_stage_token=s.gc_replacement_stage_token,gc_outbox_version=v_outbox_version,
   cleanup_state=CASE WHEN v_abort THEN 'retired' ELSE cleanup_state END
   WHERE stage_token=s.stage_token RETURNING * INTO s;
  v_ticket:=jsonb_build_object('stageToken',s.stage_token,'claimToken',s.claim_token,'leaseToken',v_token,
   'rowVersion',s.row_version::text,'key',s.stage_key,'sha256',s.stage_sha256,'bytes',s.stage_bytes,
   'groupOrdinals',s.group_ordinals,'keepGroupIndexes',v_keep,'keepOrdinals',v_ordinals,'receiptState',s.receipt_state,'phase',v_phase,
   'resetEpoch',s.reset_epoch::text,'cleanupState',s.cleanup_state);
  IF s.gc_replacement_stage_token IS NOT NULL THEN
   SELECT * INTO child FROM sessions.ingest_stage_intents WHERE stage_token=s.gc_replacement_stage_token;
   v_ticket:=v_ticket||jsonb_build_object('replacement',jsonb_build_object('stageToken',child.stage_token,
    'key',child.stage_key,'sha256',child.stage_sha256,'bytes',child.stage_bytes,'groupOrdinals',child.group_ordinals));
  END IF;
  v_tickets:=v_tickets||jsonb_build_array(v_ticket);
  EXIT; -- At most ONE fully evaluated candidate per transaction, including healthy rotations.
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_tickets)t JOIN sessions.ingest_stage_intents i
   ON i.stage_token=(t->>'stageToken')::uuid WHERE i.gc_lease_token IS DISTINCT FROM (t->>'leaseToken')::uuid
    OR i.gc_lease_until<=clock_timestamp())
 THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='buffer gc: claim lease expired';END IF;
 IF octet_length(v_tickets::text)>16777216 THEN RAISE EXCEPTION USING ERRCODE='54000',MESSAGE='buffer gc: ticket byte bound';END IF;
 RETURN jsonb_build_object('tickets',v_tickets);
END $claim$;
