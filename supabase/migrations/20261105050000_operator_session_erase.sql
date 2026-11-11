-- Operator per-session hard delete, SQL side (lane B). Slot 050000, confirmed by the program lead.
-- Code only: applying this to any real database needs Khaliq's separate, explicit OK.
--
-- The erase key is (org_id, workspace_id, source, session_id): the whole logical session, every
-- user's copy, not one caller's selection (that is 043910's sessions.ingest_selfhost_set_session_uploads).
--
-- What one call does, in ONE transaction:
--  * an idempotent receipt per key (sessions.session_erasure_receipts). A repeat call, with the same
--    or another request id, returns the existing receipt and erases nothing again. A FAILED receipt is
--    retried in place (same row, same key; attempts is incremented);
--  * the session fence (sessions.session_erasure_fences): the key, and every alias session id whose
--    pointer row names the key as its canonical session. Written before anything else, and kept even
--    when owner fencing fails, so the 050010 claim/commit/index checks refuse new uploads at once;
--  * every owner (ownership rows, pointer rows, directory users) is locked through the per-user fence
--    row, in user id order, BEFORE its ownership is read, then fenced from every origin it uploaded the
--    session from plus the relay-turns origin, with the unchanged sessions.ingest_set_session_uploads
--    (enabled:false). That is exactly a deselection: fence epoch bumped, upload_deletions written,
--    stored revisions and ownership removed;
--  * every pointer row (session_id OR canonical_session_id in the fenced ids) is tombstoned with the
--    2^53-1 revision marker, so the revision-fenced upsert_session_index can never undelete it, and the
--    logical directory rows are tombstoned with their derived text cleared.
--
-- Bytes are not erased here. The receipt lists every artifact kind; the byte eraser (lane C) records
-- each one with sessions.ingest_operator_erase_record_step. A receipt is `completed` only when the
-- session is unreadable, every byte artifact is recorded, and no artifact is pending (table CHECKs).
--
-- Refusals:
--  * RH013: ambiguous physical alias, before any write;
--  * RH010: more than 256 aliases, before any write (no receipt/fence).
--  * RH011 (not retryable): the org is lazy, buffered or ordered (sessions.ingest_set_session_uploads
--    would route a selection to the ordered control path, which an operator erase cannot drive yet);
--  * RH010 (retryable): more than 64 owners, or owners still appearing after three lock rounds. The
--    receipt is kept as `failed` with the session fence in place, and a later call retries it.
--
-- Legacy storage is out of scope (`legacyScope` is always 'excluded'). Receipts and events hold ids
-- and counts only, never content. Receipts, events and fences are never deleted (trigger-enforced).

-- Non-MVCC epoch lets a stale repeatable-read writer detect that an erase has ever
-- crossed its snapshot. Sequence reads observe is_called independently of MVCC snapshots.
CREATE SEQUENCE sessions.session_erasure_writer_epoch;
REVOKE ALL ON SEQUENCE sessions.session_erasure_writer_epoch FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest;
CREATE TABLE sessions.session_erasure_fences (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT session_erasure_fences_pkey PRIMARY KEY (org_id, workspace_id, source, session_id)
);

CREATE FUNCTION sessions.ingest_operator_erase_artifact_keys() RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $keys$
 SELECT ARRAY['transcriptObjects','stagedObjects','bufferedBatches','bufferedRows','pointerRows','ledgerRows',
  'directoryRows','catalogRows','largeReadJobs','searchDocuments','briefDocuments','summaries','briefs',
  'replayPublications','analysisPublications','historyDerivedObjects']::text[]
$keys$;

-- Exactly the sixteen keys, each {count: non-negative integer, state: done|pending|not_applicable}.
CREATE FUNCTION sessions.ingest_operator_erase_artifacts_valid(p jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $artifacts$
 SELECT CASE WHEN jsonb_typeof(p) IS DISTINCT FROM 'object' THEN false ELSE
  (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(p) AS k)
   =(SELECT array_agg(k ORDER BY k COLLATE "C") FROM unnest(sessions.ingest_operator_erase_artifact_keys()) AS k)
  AND NOT EXISTS(SELECT 1 FROM jsonb_each(p) AS e WHERE jsonb_typeof(e.value) IS DISTINCT FROM 'object'
   OR (SELECT count(*) FROM jsonb_object_keys(e.value))<>2
   OR jsonb_typeof(e.value->'count') IS DISTINCT FROM 'number'
   OR (e.value->>'count')::numeric<0 OR (e.value->>'count')::numeric<>trunc((e.value->>'count')::numeric)
   OR (e.value->>'count')::numeric>9223372036854775807
   OR (e.value->>'state') IS NULL OR (e.value->>'state') NOT IN('done','pending','not_applicable'))
 END
$artifacts$;

CREATE FUNCTION sessions.ingest_operator_erase_pending(p jsonb,p_keys text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $pending$
 SELECT EXISTS(SELECT 1 FROM jsonb_each(p) AS e WHERE e.key=ANY(p_keys) AND e.value->>'state'='pending')
$pending$;

-- {step, code, retryable}: an artifact key or 'fence', a closed code (no text), a boolean.
CREATE FUNCTION sessions.ingest_operator_erase_failure_valid(p jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $failure$
 SELECT CASE WHEN jsonb_typeof(p) IS DISTINCT FROM 'object' THEN false ELSE
  (SELECT count(*) FROM jsonb_object_keys(p))=3
  AND jsonb_typeof(p->'step')='string' AND (p->>'step'='fence' OR p->>'step'=ANY(sessions.ingest_operator_erase_artifact_keys()))
  AND jsonb_typeof(p->'code')='string' AND (p->>'code')~'^[A-Za-z][A-Za-z0-9_.-]{0,63}$'
  AND jsonb_typeof(p->'retryable')='boolean'
 END
$failure$;

CREATE TABLE sessions.session_erasure_receipts (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  request_id text NOT NULL,
  status text NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed', 'failed')),
  requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  unreadable boolean NOT NULL DEFAULT false,
  unreadable_at timestamptz,
  bytes_erased boolean NOT NULL DEFAULT false,
  bytes_erased_at timestamptz,
  artifacts jsonb NOT NULL CHECK (sessions.ingest_operator_erase_artifacts_valid(artifacts)),
  uploads_fenced boolean NOT NULL DEFAULT false,
  failure jsonb CHECK (failure IS NULL OR sessions.ingest_operator_erase_failure_valid(failure)),
  legacy_scope text NOT NULL DEFAULT 'excluded' CHECK (legacy_scope = 'excluded'),
  actor_kind text NOT NULL CHECK (actor_kind IN ('user', 'bootstrap')),
  actor_id text NOT NULL CHECK (actor_id <> ''),
  attempts integer NOT NULL DEFAULT 1 CHECK (attempts >= 1),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT session_erasure_receipts_pkey PRIMARY KEY (org_id, workspace_id, source, session_id),
  CONSTRAINT session_erasure_receipts_request_key UNIQUE (request_id),
  CONSTRAINT session_erasure_receipts_unreadable_at CHECK (unreadable = (unreadable_at IS NOT NULL)),
  CONSTRAINT session_erasure_receipts_bytes_erased_at CHECK (bytes_erased = (bytes_erased_at IS NOT NULL)),
  CONSTRAINT session_erasure_receipts_unreadable_fenced CHECK (NOT unreadable OR uploads_fenced),
  CONSTRAINT session_erasure_receipts_failed CHECK ((status = 'failed') = (failure IS NOT NULL)),
  CONSTRAINT session_erasure_receipts_completed_at CHECK ((status = 'completed') = (completed_at IS NOT NULL)),
  -- completed iff unreadable AND bytes erased AND no artifact pending.
  CONSTRAINT session_erasure_receipts_completed CHECK (status <> 'completed' OR (unreadable AND bytes_erased
    AND NOT sessions.ingest_operator_erase_pending(artifacts, sessions.ingest_operator_erase_artifact_keys())))
);

CREATE TABLE sessions.session_erasure_events (
  event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  request_id text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('requested', 'repeated', 'retried', 'fenced', 'fence_failed', 'step', 'failed', 'completed')),
  step text,
  state text,
  count bigint CHECK (count IS NULL OR count >= 0),
  error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'),
  actor_kind text CHECK (actor_kind IS NULL OR actor_kind IN ('user', 'bootstrap')),
  actor_id text,
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (org_id, workspace_id, source, session_id) REFERENCES sessions.session_erasure_receipts
);
CREATE INDEX session_erasure_events_key_idx
  ON sessions.session_erasure_events (org_id, workspace_id, source, session_id, event_id);

ALTER TABLE sessions.session_erasure_fences ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_erasure_fences, sessions.session_erasure_receipts, sessions.session_erasure_events
  FROM PUBLIC, anon, authenticated, service_role;

-- Never deleted: receipts and fences are the proof (and the fence IS the guarantee); events are
-- append-only. Rollback refuses while any row exists, so it never needs to delete them either.
CREATE FUNCTION sessions.ingest_operator_erase_append_only() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $append_only$
BEGIN
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE=format('operator erase: %s on %s is refused',TG_OP,TG_TABLE_NAME);
END $append_only$;
CREATE TRIGGER session_erasure_fences_append_only BEFORE UPDATE OR DELETE ON sessions.session_erasure_fences
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_operator_erase_append_only();
CREATE TRIGGER session_erasure_fences_no_truncate BEFORE TRUNCATE ON sessions.session_erasure_fences
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_operator_erase_append_only();
CREATE TRIGGER session_erasure_receipts_never_deleted BEFORE DELETE ON sessions.session_erasure_receipts
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_operator_erase_append_only();
CREATE TRIGGER session_erasure_receipts_no_truncate BEFORE TRUNCATE ON sessions.session_erasure_receipts
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_operator_erase_append_only();
CREATE TRIGGER session_erasure_events_append_only BEFORE UPDATE OR DELETE ON sessions.session_erasure_events
  FOR EACH ROW EXECUTE FUNCTION sessions.ingest_operator_erase_append_only();
CREATE TRIGGER session_erasure_events_no_truncate BEFORE TRUNCATE ON sessions.session_erasure_events
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.ingest_operator_erase_append_only();

-- The org predicate sessions.ingest_set_session_uploads routes to ingest_apply_ordered_control.
-- Also read by 050010's fence extension (only after a fence hit, never on the hot path).
CREATE FUNCTION sessions.ingest_operator_erase_ordered(p_org text) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $ordered$
 SELECT EXISTS(SELECT 1 FROM sessions.ingest_lazy_policy WHERE singleton)
  OR EXISTS(SELECT 1 FROM sessions.ingest_buffer_admission WHERE org_id=p_org)
  OR EXISTS(SELECT 1 FROM sessions.ingest_batches WHERE org_id=p_org AND publication_state='buffered')
$ordered$;

-- The receipt as returned to callers: ids, flags, timestamps and counts only.
CREATE FUNCTION sessions.ingest_operator_erase_render(p_org text,p_ws text,p_source text,p_session text) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $render$
 SELECT jsonb_build_object('orgId',r.org_id,'workspaceId',r.workspace_id,'source',r.source,'sessionId',r.session_id,
   'requestId',r.request_id,'status',r.status,'requestedAt',r.requested_at,'completedAt',r.completed_at,
   'unreadable',r.unreadable,'unreadableAt',r.unreadable_at,'bytesErased',r.bytes_erased,'bytesErasedAt',r.bytes_erased_at,
   'artifacts',r.artifacts,'uploadsFenced',r.uploads_fenced,'failure',r.failure,'legacyScope',r.legacy_scope,
   'actor',jsonb_build_object('kind',r.actor_kind,'id',r.actor_id),'attempts',r.attempts)
 FROM sessions.session_erasure_receipts AS r
 WHERE r.org_id=p_org AND r.workspace_id=p_ws AND r.source=p_source AND r.session_id=p_session
$render$;

-- Derives unreadable / bytesErased / completed from the artifacts of a locked, non-failed receipt.
-- unreadable: uploads fenced and no pointer or directory row pending. bytesErased: no byte artifact
-- (every key but pointerRows, directoryRows and ledgerRows) pending. completed: both, nothing pending.
-- Alias closure is bounded and rejects a physical layout id naming distinct logical sessions.
-- The caller holds the pointer statement gate before using this result for an erase.
CREATE FUNCTION sessions.ingest_operator_erase_ids(p_org text,p_ws text,p_source text,p_session text) RETURNS text[]
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $ids$
DECLARE v_ids text[];
BEGIN
 WITH RECURSIVE related(id) AS (
  SELECT p_session UNION
  SELECT n.id FROM related AS rel JOIN sessions.session_index AS s
   ON s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
    AND(s.session_id=rel.id OR s.canonical_session_id=rel.id)
   CROSS JOIN LATERAL(VALUES(s.session_id),(s.canonical_session_id)) AS n(id) WHERE n.id IS NOT NULL
 ) SELECT array_agg(id ORDER BY id COLLATE "C") INTO v_ids FROM(SELECT id FROM related LIMIT 257) AS bounded;
 IF cardinality(v_ids)>256 THEN RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: alias cap exceeded before fencing';END IF;
 IF EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source
  AND session_id=ANY(v_ids) GROUP BY session_id HAVING count(DISTINCT coalesce(canonical_session_id,session_id))>1) THEN
  RAISE EXCEPTION USING ERRCODE='RH013',MESSAGE='operator erase: ambiguous physical alias';END IF;
 RETURN v_ids;
END $ids$;

CREATE FUNCTION sessions.ingest_operator_erase_settle(p_org text,p_ws text,p_source text,p_session text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $settle$
DECLARE r sessions.session_erasure_receipts%ROWTYPE;v_unreadable boolean;v_bytes boolean;v_ids text[];
 c_read CONSTANT text[]:=ARRAY['pointerRows','directoryRows'];
BEGIN
 SELECT * INTO STRICT r FROM sessions.session_erasure_receipts
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 IF r.status<>'in_progress' THEN RETURN;END IF;
 v_ids:=sessions.ingest_operator_erase_ids(p_org,p_ws,p_source,p_session);
 v_unreadable:=r.uploads_fenced AND NOT sessions.ingest_operator_erase_pending(r.artifacts,c_read)
  AND NOT EXISTS(SELECT 1 FROM sessions.session_index WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source
   AND(session_id=ANY(v_ids) OR canonical_session_id=ANY(v_ids)) AND deleted_at IS NULL)
  AND NOT EXISTS(SELECT 1 FROM sessions.session_directory WHERE org_id=p_org AND workspace_id=p_ws AND harness=p_source
   AND session_id=ANY(v_ids) AND deleted_at IS NULL);
 v_bytes:=NOT sessions.ingest_operator_erase_pending(r.artifacts,
  ARRAY(SELECT k FROM unnest(sessions.ingest_operator_erase_artifact_keys()) AS k WHERE k NOT IN('pointerRows','directoryRows','ledgerRows')));
 UPDATE sessions.session_erasure_receipts SET
   unreadable=v_unreadable,unreadable_at=CASE WHEN v_unreadable THEN coalesce(unreadable_at,clock_timestamp()) END,
   bytes_erased=v_bytes,bytes_erased_at=CASE WHEN v_bytes THEN coalesce(bytes_erased_at,clock_timestamp()) END,
   status=CASE WHEN v_unreadable AND v_bytes AND NOT sessions.ingest_operator_erase_pending(artifacts,sessions.ingest_operator_erase_artifact_keys())
     THEN 'completed' ELSE 'in_progress' END,
   completed_at=CASE WHEN v_unreadable AND v_bytes AND NOT sessions.ingest_operator_erase_pending(artifacts,sessions.ingest_operator_erase_artifact_keys())
     THEN clock_timestamp() END,
   updated_at=clock_timestamp()
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_receipts WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source
   AND session_id=p_session AND status='completed') THEN
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind)
  VALUES(p_org,p_ws,p_source,p_session,r.request_id,'completed');
 END IF;
END $settle$;

-- Every user with a copy of any fenced id, sorted (COLLATE "C"). A plain read: callers lock after.
CREATE FUNCTION sessions.ingest_operator_erase_owners(p_org text,p_ws text,p_source text,p_ids text[]) RETURNS text[]
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $owners$
 SELECT coalesce(array_agg(u.user_id ORDER BY u.user_id COLLATE "C"),'{}'::text[]) FROM (
  SELECT o.user_id FROM sessions.ingest_session_ownership AS o
   WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.source=p_source AND o.session_id=ANY(p_ids)
  UNION SELECT s.user_id FROM sessions.session_index AS s
   WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
     AND (s.session_id=ANY(p_ids) OR s.canonical_session_id=ANY(p_ids))
  UNION SELECT d.user_id FROM sessions.session_directory_users AS d
   WHERE d.org_id=p_org AND d.workspace_id=p_ws AND d.harness=p_source AND d.session_id=ANY(p_ids)) AS u
$owners$;

-- Locks every owner, fences every (owner, origin), tombstones pointer and directory rows, records
-- the counts. RH010 when the owner set is over 64 or does not settle in three lock rounds.
CREATE FUNCTION sessions.ingest_operator_erase_fence(p_org text,p_ws text,p_source text,p_session text,p_ids text[]) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $fence$
DECLARE v_locked text[]:='{}';v_users text[];v_user text;v_origin text;v_pairs jsonb;v_round integer:=0;
 v_ledger bigint:=0;v_pointers bigint:=0;v_dirs bigint:=0;v_n bigint;v_live boolean;v_artifacts jsonb;
 c_erased_revision CONSTANT bigint:=9007199254740991;
 c_turn_origin CONSTANT text:='relayhistory:conversation_turns:v1';
 c_max_owners CONSTANT integer:=64;
BEGIN
 v_pairs:=(SELECT jsonb_agg(jsonb_build_array(p_source,i) ORDER BY i COLLATE "C") FROM unnest(p_ids) AS i);
 -- Lock, then read ownership again under the locks: an owner whose claim committed between the
 -- read and the lock is caught by the next round. Locks are taken in user id order per round.
 LOOP
  v_round:=v_round+1;
  v_users:=sessions.ingest_operator_erase_owners(p_org,p_ws,p_source,p_ids);
  IF cardinality(v_users)>c_max_owners THEN
   RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: owner cap exceeded';END IF;
  EXIT WHEN v_users<@v_locked;
  IF v_round>3 THEN RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: owners did not settle';END IF;
  FOREACH v_user IN ARRAY v_users LOOP
   IF NOT v_user=ANY(v_locked) THEN
    -- A newly visible owner sorting before a held lock would reverse the global lock order.
    -- Abort this attempt rather than deadlock with another erase; its session fence is kept.
    IF EXISTS(SELECT 1 FROM unnest(v_locked) AS held WHERE held COLLATE "C">v_user COLLATE "C") THEN
     RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: owner lock order changed';END IF;
    PERFORM sessions.ingest_lock_fences(p_org,p_ws,v_user);
    v_locked:=v_locked||v_user;
   END IF;
  END LOOP;
 END LOOP;
 FOREACH v_user IN ARRAY v_users LOOP
  FOR v_origin IN
   SELECT u.origin_id FROM (SELECT o.origin_id FROM sessions.ingest_session_ownership AS o
     WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=v_user AND o.source=p_source AND o.session_id=ANY(p_ids)
    UNION SELECT c_turn_origin) AS u
   ORDER BY u.origin_id COLLATE "C"
  LOOP
   v_ledger:=v_ledger+(sessions.ingest_set_session_uploads(jsonb_build_object(
     'tenant',jsonb_build_object('orgId',p_org,'workspaceId',p_ws,'userId',v_user),
     'originId',v_origin,'sessions',v_pairs,'enabled',false))->>'updated')::bigint;
  END LOOP;
 END LOOP;
 -- Every copy's pointer row: the marker revision keeps upsert_session_index from undeleting it.
 UPDATE sessions.session_index AS s SET deleted_at=coalesce(s.deleted_at,clock_timestamp()),indexed_at=clock_timestamp(),
   revision=greatest(s.revision,c_erased_revision)
  WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
    AND (s.session_id=ANY(p_ids) OR s.canonical_session_id=ANY(p_ids));
 GET DIAGNOSTICS v_pointers=ROW_COUNT;
 -- A guarded pointer (a live managed publication head) refuses the write by skipping it; it stays
 -- pending for the publication retirement path rather than being reported done.
 v_live:=EXISTS(SELECT 1 FROM sessions.session_index AS s WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
   AND (s.session_id=ANY(p_ids) OR s.canonical_session_id=ANY(p_ids))
   AND (s.deleted_at IS NULL OR s.revision<c_erased_revision));
 UPDATE sessions.session_directory AS d SET short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
   short_summary_source=NULL,short_summary_generated_at=NULL,
   search_text=sessions.session_directory_search_text(d.title,d.task_title,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,NULL),deleted_at=coalesce(d.deleted_at,clock_timestamp())
  WHERE d.org_id=p_org AND d.workspace_id=p_ws AND d.harness=p_source AND d.session_id=ANY(p_ids);
 GET DIAGNOSTICS v_dirs=ROW_COUNT;
 SELECT artifacts INTO STRICT v_artifacts FROM sessions.session_erasure_receipts
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 v_artifacts:=v_artifacts
  ||jsonb_build_object('pointerRows',jsonb_build_object('count',v_pointers,'state',CASE WHEN v_live THEN 'pending' ELSE 'done' END))
  ||jsonb_build_object('ledgerRows',jsonb_build_object('count',v_ledger,'state','done'))
  ||jsonb_build_object('directoryRows',jsonb_build_object('count',v_dirs,'state','done'));
 UPDATE sessions.session_erasure_receipts SET artifacts=v_artifacts,uploads_fenced=true,updated_at=clock_timestamp()
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 v_n:=cardinality(v_users);
 INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,count)
 SELECT p_org,p_ws,p_source,p_session,r.request_id,'fenced',v_n FROM sessions.session_erasure_receipts AS r
  WHERE r.org_id=p_org AND r.workspace_id=p_ws AND r.source=p_source AND r.session_id=p_session;
END $fence$;

CREATE FUNCTION sessions.ingest_operator_erase_session(p_org text,p_ws text,p_source text,p_session text,
  p_request text,p_actor_kind text,p_actor_id text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $operator_erase$
DECLARE v_org text;v_ws text;v_source text;v_session text;v_request text;v_actor text;
 r sessions.session_erasure_receipts%ROWTYPE;v_ids text[];v_code text;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_org:=sessions.ingest_identifier(to_jsonb(p_org),false);
 v_ws:=sessions.ingest_identifier(to_jsonb(p_ws),true);
 v_source:=sessions.ingest_identifier(to_jsonb(p_source),false);
 v_session:=sessions.ingest_identifier(to_jsonb(p_session),false);
 v_request:=sessions.ingest_identifier(to_jsonb(p_request),false);
 v_actor:=sessions.ingest_identifier(to_jsonb(p_actor_id),false);
 IF p_actor_kind IS NULL OR p_actor_kind NOT IN('user','bootstrap') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: actor kind is malformed';END IF;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_receipts AS x WHERE x.request_id=v_request
   AND (x.org_id,x.workspace_id,x.source,x.session_id) IS DISTINCT FROM (v_org,v_ws,v_source,v_session)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: request id belongs to another session';END IF;

 -- Global statement gate: writers share it before row locks; an erase owns it exclusively.
 -- This covers direct UPDATE and INSERT/ON CONFLICT as well as the composed consumer.
 PERFORM pg_advisory_xact_lock(961996,50000);
 -- Reject lossy alias collisions and oversized closure before receipt/fence writes.
 v_ids:=sessions.ingest_operator_erase_ids(v_org,v_ws,v_source,v_session);
 -- A repeat (any request id) answers from the existing receipt; only a failed one is retried.
 SELECT * INTO r FROM sessions.session_erasure_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session FOR UPDATE;
 IF FOUND AND r.status<>'failed' THEN
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
  VALUES(v_org,v_ws,v_source,v_session,v_request,'repeated',p_actor_kind,v_actor);
  RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
 END IF;

 IF sessions.ingest_operator_erase_ordered(v_org) THEN
  RAISE EXCEPTION USING ERRCODE='RH011',MESSAGE='operator erase: lazy, buffered or ordered org is not supported';END IF;

 IF FOUND THEN
  UPDATE sessions.session_erasure_receipts SET status='in_progress',failure=NULL,attempts=attempts+1,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
  VALUES(v_org,v_ws,v_source,v_session,v_request,'retried',p_actor_kind,v_actor);
 ELSE
  -- The primary key decides a race between two first calls: the loser waits, then repeats.
  INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id,request_id,artifacts,actor_kind,actor_id)
  SELECT v_org,v_ws,v_source,v_session,v_request,
    jsonb_object_agg(k,jsonb_build_object('count',0,'state','pending')),p_actor_kind,v_actor
   FROM unnest(sessions.ingest_operator_erase_artifact_keys()) AS k
  ON CONFLICT ON CONSTRAINT session_erasure_receipts_pkey DO NOTHING;
  IF NOT FOUND THEN
   INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
   VALUES(v_org,v_ws,v_source,v_session,v_request,'repeated',p_actor_kind,v_actor);
   RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
  END IF;
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
  VALUES(v_org,v_ws,v_source,v_session,v_request,'requested',p_actor_kind,v_actor);
 END IF;
 SELECT * INTO STRICT r FROM sessions.session_erasure_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;

 -- Resolve aliases before writing any fence.
 v_ids:=sessions.ingest_operator_erase_ids(v_org,v_ws,v_source,v_session);
 -- First, and outside the block below: kept even when owner fencing fails.
 PERFORM nextval('sessions.session_erasure_writer_epoch'::regclass);
 INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id)
 SELECT v_org,v_ws,v_source,i FROM unnest(v_ids) AS i ON CONFLICT DO NOTHING;

 IF NOT r.uploads_fenced THEN
  BEGIN
   PERFORM sessions.ingest_operator_erase_fence(v_org,v_ws,v_source,v_session,v_ids);
  EXCEPTION WHEN SQLSTATE 'RH010' THEN
   GET STACKED DIAGNOSTICS v_code=RETURNED_SQLSTATE;
   UPDATE sessions.session_erasure_receipts SET status='failed',
     failure=jsonb_build_object('step','fence','code',v_code,'retryable',true),updated_at=clock_timestamp()
    WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
   INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,step,error_code)
   VALUES(v_org,v_ws,v_source,v_session,v_request,'fence_failed','fence',v_code);
   RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
  END;
 END IF;
 PERFORM sessions.ingest_operator_erase_settle(v_org,v_ws,v_source,v_session);
 RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
END $operator_erase$;

CREATE FUNCTION sessions.ingest_operator_erase_receipt(p_org text,p_ws text,p_source text,p_session text) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $operator_receipt$
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 -- NULL (JSON null) for a key never erased.
 RETURN sessions.ingest_operator_erase_render(sessions.ingest_identifier(to_jsonb(p_org),false),
  sessions.ingest_identifier(to_jsonb(p_ws),true),sessions.ingest_identifier(to_jsonb(p_source),false),
  sessions.ingest_identifier(to_jsonb(p_session),false));
END $operator_receipt$;

-- The byte eraser's report for one artifact kind. p_state: done | not_applicable | pending record the
-- artifact; failed (not retryable) | failed_retryable record the failure on the receipt (p_error_code
-- required) and leave the artifact as it was. A failed receipt takes no steps until the erase is
-- retried; a completed one accepts only an identical repeat.
CREATE FUNCTION sessions.ingest_operator_erase_record_step(p_org text,p_ws text,p_source text,p_session text,
  p_step text,p_state text,p_count bigint,p_error_code text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $operator_step$
DECLARE v_org text;v_ws text;v_source text;v_session text;r sessions.session_erasure_receipts%ROWTYPE;v_failed boolean;
BEGIN
 PERFORM sessions.ingest_require_role('bulls96_session_ingest');
 v_org:=sessions.ingest_identifier(to_jsonb(p_org),false);
 v_ws:=sessions.ingest_identifier(to_jsonb(p_ws),true);
 v_source:=sessions.ingest_identifier(to_jsonb(p_source),false);
 v_session:=sessions.ingest_identifier(to_jsonb(p_session),false);
 IF p_step IS NULL OR NOT p_step=ANY(sessions.ingest_operator_erase_artifact_keys()) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: step is malformed';END IF;
 IF p_state IS NULL OR p_state NOT IN('done','not_applicable','pending','failed','failed_retryable') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: state is malformed';END IF;
 v_failed:=p_state IN('failed','failed_retryable');
 IF v_failed<>(p_error_code IS NOT NULL) OR (p_error_code IS NOT NULL AND p_error_code!~'^[A-Za-z][A-Za-z0-9_.-]{0,63}$') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: error code is malformed';END IF;
 IF NOT v_failed AND (p_count IS NULL OR p_count<0) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: count is malformed';END IF;
 SELECT * INTO r FROM sessions.session_erasure_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0002',MESSAGE='operator erase: no receipt';END IF;
 IF r.status='completed' THEN
  IF NOT v_failed AND r.artifacts->p_step=jsonb_build_object('count',p_count,'state',p_state) THEN
   RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);END IF;
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator erase: receipt is completed';
 END IF;
 IF r.status='failed' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='operator erase: retry the erase first';END IF;
 IF v_failed THEN
  UPDATE sessions.session_erasure_receipts SET status='failed',
    failure=jsonb_build_object('step',p_step,'code',p_error_code,'retryable',p_state='failed_retryable'),updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,step,state,error_code)
  VALUES(v_org,v_ws,v_source,v_session,r.request_id,'failed',p_step,p_state,p_error_code);
 ELSE
  UPDATE sessions.session_erasure_receipts
   SET artifacts=jsonb_set(artifacts,ARRAY[p_step],jsonb_build_object('count',p_count,'state',p_state)),updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,step,state,count)
  VALUES(v_org,v_ws,v_source,v_session,r.request_id,'step',p_step,p_state,p_count);
  PERFORM sessions.ingest_operator_erase_settle(v_org,v_ws,v_source,v_session);
 END IF;
 RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
END $operator_step$;

REVOKE ALL ON FUNCTION sessions.ingest_operator_erase_artifact_keys(),
  sessions.ingest_operator_erase_artifacts_valid(jsonb),
  sessions.ingest_operator_erase_pending(jsonb,text[]),
  sessions.ingest_operator_erase_failure_valid(jsonb),
  sessions.ingest_operator_erase_append_only(),
  sessions.ingest_operator_erase_ordered(text),
  sessions.ingest_operator_erase_render(text,text,text,text),
  sessions.ingest_operator_erase_ids(text,text,text,text),
  sessions.ingest_operator_erase_settle(text,text,text,text),
  sessions.ingest_operator_erase_owners(text,text,text,text[]),
  sessions.ingest_operator_erase_fence(text,text,text,text,text[]),
  sessions.ingest_operator_erase_session(text,text,text,text,text,text,text),
  sessions.ingest_operator_erase_receipt(text,text,text,text),
  sessions.ingest_operator_erase_record_step(text,text,text,text,text,text,bigint,text)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION sessions.ingest_operator_erase_session(text,text,text,text,text,text,text),
  sessions.ingest_operator_erase_receipt(text,text,text,text),
  sessions.ingest_operator_erase_record_step(text,text,text,text,text,text,bigint,text)
  TO bulls96_session_ingest;
