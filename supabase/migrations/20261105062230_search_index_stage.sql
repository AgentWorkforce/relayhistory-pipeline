-- The `search-index` stage: the committed search wake (docs/handoffs/incremental-search-indexing.md, design M1
-- rev 9, milestone M2). Additive. Nothing runs until the writer (SEARCH_WAKE_ENQUEUE / the indexer's
-- PIPELINE_MANAGED_SEARCH_WAKE_ENABLED) and the consumer (SEARCH_JOBS_ENABLED) flags are turned on, each with its
-- own go. Applying this to any real database needs a separate, explicit go.
--
-- Producer half (promoted from supabase/proposals/continuous-publication/managed-search-wake.up.sql @ 6658d18f,
-- same semantics): the stage in the three stage CHECKs, the one-row-per-scope index (§3), the queue guard
-- triggers and sessions.enqueue_managed_search_wake_v1 (§2). Two mechanical differences from the proposal: the
-- CHECKs are restated as explicit lists (the 20261031000100 sets plus 'search-index', as §7 asks), so the
-- proposal's publication_private.search_stage_checks bookkeeping is not needed; and the consumer role is not
-- required to exist here (the guard compares its name only; provisioning it is a separate owner step).
--
-- Consumer half: sessions.search_catchup_cursor (§6), pipeline_dead_letters.resolution (§3.3b) and the sealed,
-- append-only sessions.search_stage_archive with its only write path sessions.archive_search_stage_v1() (§7, §8).
-- The rollback (supabase/rollbacks/20261105062230_search_index_stage.down.sql) refuses while any search-index
-- job, event or dead letter exists (§8) and never drops an archive that holds a row.
SET LOCAL lock_timeout = '5s';

LOCK TABLE sessions.pipeline_jobs, sessions.pipeline_events, sessions.pipeline_dead_letters IN ACCESS EXCLUSIVE MODE;

-- §7: the stage in all three closed lists (the 20261031000100 sets, plus 'search-index').
ALTER TABLE sessions.pipeline_events DROP CONSTRAINT pipeline_events_stage_check;
ALTER TABLE sessions.pipeline_events ADD CONSTRAINT pipeline_events_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'search-index', 'fetch'));
ALTER TABLE sessions.pipeline_dead_letters DROP CONSTRAINT pipeline_dead_letters_stage_check;
ALTER TABLE sessions.pipeline_dead_letters ADD CONSTRAINT pipeline_dead_letters_stage_check
  CHECK (stage IN ('ingest', 'deliver.put', 'object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'search-index', 'fetch'));
ALTER TABLE sessions.pipeline_jobs DROP CONSTRAINT pipeline_jobs_stage_check;
ALTER TABLE sessions.pipeline_jobs ADD CONSTRAINT pipeline_jobs_stage_check
  CHECK (stage IN ('object-landed', 'session-indexed',
    'summary-short', 'summary-long', 'status-classify', 'file-normalize',
    'file-maintenance', 'backfill-activation', 'search-index'));

-- §3: exactly one search-index row per scope, leased or not.
CREATE UNIQUE INDEX pipeline_jobs_search_index_scope ON sessions.pipeline_jobs (work_key)
  WHERE stage = 'search-index';

-- §3.3b: how a replayed letter resolved: 'completed' (the completed epoch equals the letter's) or 'superseded'
-- (a newer epoch covered it). NULL for every other letter and for an unresolved one.
ALTER TABLE sessions.pipeline_dead_letters ADD COLUMN resolution text
  CHECK (resolution IS NULL OR resolution IN ('completed', 'superseded'));

-- §2 (producer): ordinary queue writers can enqueue every existing stage, but cannot invent a managed scope,
-- replace its envelope or move another row into this stage. Only the owner (through the SECURITY DEFINER
-- functions below) and the dedicated consumer role (lease and acknowledge, never a changed message) pass.
CREATE FUNCTION sessions.guard_managed_search_wake_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $guard$
BEGIN
 IF current_user = pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid = 'sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure)) THEN
   IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
   RETURN NEW;
 END IF;
 IF current_user = 'bulls96_search_wake_consumer' THEN
   IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
   IF TG_OP = 'UPDATE' AND OLD.stage = 'search-index' AND NEW.stage = 'search-index'
    AND (NEW.work_key, NEW.trace_id, NEW.org_id, NEW.workspace_id, NEW.session_id, NEW.machine_id, NEW.message)
      IS NOT DISTINCT FROM (OLD.work_key, OLD.trace_id, OLD.org_id, OLD.workspace_id, OLD.session_id, OLD.machine_id, OLD.message)
   THEN RETURN NEW; END IF;
 END IF;
 RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'search wake: committed managed authority required';
END $guard$;
REVOKE ALL ON FUNCTION sessions.guard_managed_search_wake_v1() FROM PUBLIC;

-- Queue writers must never have authority to truncate stage history. TRUNCATE holds ACCESS EXCLUSIVE before this
-- trigger fires, so the check is race-free: a non-owner may still truncate a queue that holds no search-index row
-- (nothing of the stage is lost; existing queue tooling and fixtures keep working).
CREATE FUNCTION sessions.guard_managed_search_truncate_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $guard$
BEGIN
 IF current_user <> pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid = 'sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure))
    AND EXISTS (SELECT 1 FROM sessions.pipeline_jobs WHERE stage = 'search-index') THEN
   RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'search wake: queue truncate requires owner';
 END IF;
 RETURN NULL;
END $guard$;
REVOKE ALL ON FUNCTION sessions.guard_managed_search_truncate_v1() FROM PUBLIC;

-- §2 (producer): the managed wake, inside the publication-authority transaction. It re-proves the trigger's own
-- authority (session_publication_lock), reads the committed directory epoch, and carries the publish clock: the
-- triggering head's own published attempt AND every live sibling of the canonical scope must carry one (a NULL
-- anywhere raises P0002; the caller records the clock_unprovable refusal, §2 [rev 7]).
CREATE FUNCTION sessions.enqueue_managed_search_wake_v1(evidence jsonb, trace uuid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $wake$
DECLARE
 authority jsonb; h sessions.ingest_publication_heads; epoch bigint; canonical_id text;
 key text; wake jsonb; publication_time timestamptz;
BEGIN
 IF trace IS NULL OR trace = '00000000-0000-0000-0000-000000000000'::uuid
    OR coalesce(evidence->>'orgId', '') = '' OR coalesce(evidence->>'workspaceId', '') = '' THEN
   RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search wake: invalid trace or tenant context';
 END IF;
 authority := sessions.session_publication_lock(evidence);
 IF authority->'managed' IS DISTINCT FROM 'true'::jsonb OR authority->'current' IS DISTINCT FROM 'true'::jsonb THEN
   RAISE EXCEPTION 'search wake: publication no longer current';
 END IF;
 SELECT * INTO STRICT h FROM sessions.ingest_publication_heads
 WHERE generation = (authority->>'generation')::uuid AND current_key = evidence->>'manifestKey' AND state = 'live';
 IF h.source = '_none' AND h.session_id = '_origin' THEN RETURN; END IF;
 SELECT d.event_projection_epoch, d.session_id INTO STRICT epoch, canonical_id
 FROM sessions.session_index p JOIN sessions.session_directory d
   ON (d.org_id, d.workspace_id, d.harness, d.session_id) = (p.org_id, p.workspace_id, p.harness, coalesce(p.canonical_session_id, p.session_id))
 WHERE (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id) = (h.org_id, h.workspace_id, h.physical_source, h.physical_session, h.machine_id)
   AND p.manifest_key = h.current_key AND p.revision = h.pointer_revision AND p.deleted_at IS NULL AND p.scrubbed IS TRUE AND NOT p.scrub_quarantined AND d.deleted_at IS NULL;
 -- Resolve the exact image, stage identity and successful attempt that made
 -- this head current. Neither the queue clock nor the pointer clock is a substitute.
 SELECT a.published_at INTO STRICT publication_time
 FROM sessions.ingest_publication_images i
 JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token
 JOIN sessions.ingest_stage_intents s ON s.stage_token = a.stage_token
 WHERE i.manifest_key = h.current_key AND i.generation = h.generation AND i.pointer_revision = h.pointer_revision
   AND i.raw_sha256 = h.current_raw_sha256 AND a.schema_generation = evidence->>'schemaGeneration'
   AND (a.org_id, a.workspace_id, a.user_id, a.origin_id) = (h.org_id, h.workspace_id, h.user_id, h.origin_id)
   AND (s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest) = (a.org_id, a.workspace_id, a.user_id, a.origin_id, a.batch_id, a.digest)
   AND a.state = 'published' AND a.published_at IS NOT NULL;
 -- A managed scope may have multiple live machines. Require provenance for
 -- every eligible head; MAX alone silently ignores missing timestamps.
 SELECT max(a.published_at) INTO STRICT publication_time
 FROM sessions.ingest_publication_heads sibling
 JOIN sessions.session_index p ON
   (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.manifest_key, p.revision) =
   (sibling.org_id, sibling.workspace_id, sibling.physical_source, sibling.physical_session, sibling.machine_id, sibling.current_key, sibling.pointer_revision)
 LEFT JOIN sessions.ingest_publication_images i ON
   (i.manifest_key, i.generation, i.pointer_revision, i.raw_sha256) =
   (sibling.current_key, sibling.generation, sibling.pointer_revision, sibling.current_raw_sha256)
 -- The sibling's own image names its attempt exactly (attempt_token). The tenant and user must match; the origin is
 -- not compared here: the sibling only supplies the clock (it never validates the trigger), as §2's aggregate.
 LEFT JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
   AND (a.org_id, a.workspace_id, a.user_id) = (sibling.org_id, sibling.workspace_id, sibling.user_id)
 WHERE sibling.state = 'live' AND p.deleted_at IS NULL AND p.scrubbed IS TRUE AND NOT p.scrub_quarantined
   AND (p.org_id, p.workspace_id, p.harness, coalesce(p.canonical_session_id, p.session_id)) =
       (h.org_id, h.workspace_id, h.physical_source, canonical_id)
 HAVING count(*) > 0 AND count(*) = count(a.published_at);
 IF epoch IS NULL OR epoch NOT BETWEEN 0 AND 9007199254740991 OR h.pointer_revision > 9007199254740991 OR canonical_id <> h.session_id
    OR position(chr(31) IN concat(h.org_id, h.workspace_id, h.physical_source, canonical_id)) > 0
 THEN RAISE EXCEPTION 'search wake: ambiguous directory scope'; END IF;
 key := concat_ws(chr(31), 'search', h.org_id, h.workspace_id, h.physical_source, canonical_id);
 wake := jsonb_build_object('v', 1, 'stage', 'search-index', 'workKey', key, 'revision', h.pointer_revision,
   'ctx', jsonb_build_object('traceId', trace, 'orgId', h.org_id, 'workspaceId', h.workspace_id, 'harness', h.physical_source, 'sessionId', canonical_id),
   'payload', jsonb_build_object('epoch', epoch, 'generation', h.generation, 'schema_generation', evidence->>'schemaGeneration', 'clock', jsonb_build_object('source', 'publish', 'at', to_char(publication_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))));
 INSERT INTO sessions.pipeline_jobs AS jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, message, visible_at)
 VALUES ('search-index', key, trace, h.org_id, h.workspace_id, canonical_id, wake, now())
 ON CONFLICT (work_key) WHERE stage = 'search-index'
 DO UPDATE SET message = excluded.message, trace_id = excluded.trace_id,
   attempts = 0, dlq = false,
   visible_at = CASE WHEN jobs.lease_id IS NULL THEN least(jobs.visible_at, excluded.visible_at) ELSE jobs.visible_at END
 WHERE epoch > (jobs.message->'payload'->>'epoch')::bigint;
END $wake$;
REVOKE ALL ON FUNCTION sessions.enqueue_managed_search_wake_v1(jsonb, uuid) FROM PUBLIC;
-- The queue/schema owner grants EXECUTE to the independently qualified indexer login (and, for the Lambda writer,
-- its runtime role) in a separate, approved owner step. This migration grants it to no one.

-- §2 [rev 7] (R5g): the writer's entry point. Inside the authority transaction it re-checks the trigger's own
-- authority, then proves the publish clock exactly as enqueue_managed_search_wake_v1 does (the head's own published
-- attempt, and a published_at on every live sibling of its canonical scope). Provable: the enqueue runs (and raises,
-- rolling the authority back, on anything else, as before). Unprovable: FAIL CLOSED FOR SEARCH ONLY: no job, never
-- a pointer clock, and one durable refusal row (pipeline_events search-index / skipped / clock_unprovable,
-- idempotency key trace:search-index:skipped:1:<manifest key>) committed with the pointer, so the session stays
-- readable. SECURITY DEFINER: the publication tables it reads are not the writer's. Returns 'enqueued' or
-- 'clock_unprovable'. No grant here: the owner grants the qualified writer login with the enqueue.
CREATE FUNCTION sessions.managed_search_wake_v1(evidence jsonb, trace uuid) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $wake$
DECLARE
 authority jsonb; h sessions.ingest_publication_heads; canonical_id text; own_clocked bigint; siblings bigint; clocked bigint;
BEGIN
 IF trace IS NULL OR trace = '00000000-0000-0000-0000-000000000000'::uuid
    OR coalesce(evidence->>'orgId', '') = '' OR coalesce(evidence->>'workspaceId', '') = '' THEN
   RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search wake: invalid trace or tenant context';
 END IF;
 authority := sessions.session_publication_lock(evidence);
 IF authority->'managed' IS DISTINCT FROM 'true'::jsonb OR authority->'current' IS DISTINCT FROM 'true'::jsonb THEN
   RAISE EXCEPTION 'search wake: publication no longer current';
 END IF;
 SELECT * INTO h FROM sessions.ingest_publication_heads
  WHERE generation = (authority->>'generation')::uuid AND current_key = evidence->>'manifestKey' AND state = 'live';
 IF FOUND AND NOT (h.source = '_none' AND h.session_id = '_origin') THEN
   SELECT coalesce(p.canonical_session_id, p.session_id) INTO canonical_id
     FROM sessions.session_index p
    WHERE (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id) = (h.org_id, h.workspace_id, h.physical_source, h.physical_session, h.machine_id)
      AND p.manifest_key = h.current_key AND p.revision = h.pointer_revision AND p.deleted_at IS NULL;
   IF canonical_id IS NOT NULL THEN
     SELECT count(*) INTO own_clocked
       FROM sessions.ingest_publication_images i
       JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token
       JOIN sessions.ingest_stage_intents s ON s.stage_token = a.stage_token
      WHERE i.manifest_key = h.current_key AND i.generation = h.generation AND i.pointer_revision = h.pointer_revision
        AND i.raw_sha256 = h.current_raw_sha256 AND a.schema_generation = evidence->>'schemaGeneration'
        AND (a.org_id, a.workspace_id, a.user_id, a.origin_id) = (h.org_id, h.workspace_id, h.user_id, h.origin_id)
        AND (s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest) = (a.org_id, a.workspace_id, a.user_id, a.origin_id, a.batch_id, a.digest)
        AND a.state = 'published' AND a.published_at IS NOT NULL;
     SELECT count(*), count(a.published_at) INTO siblings, clocked
       FROM sessions.ingest_publication_heads sibling
       JOIN sessions.session_index p ON
         (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.manifest_key, p.revision) =
         (sibling.org_id, sibling.workspace_id, sibling.physical_source, sibling.physical_session, sibling.machine_id, sibling.current_key, sibling.pointer_revision)
       LEFT JOIN sessions.ingest_publication_images i ON
         (i.manifest_key, i.generation, i.pointer_revision, i.raw_sha256) =
         (sibling.current_key, sibling.generation, sibling.pointer_revision, sibling.current_raw_sha256)
       LEFT JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
         AND (a.org_id, a.workspace_id, a.user_id) = (sibling.org_id, sibling.workspace_id, sibling.user_id)
      WHERE sibling.state = 'live' AND p.deleted_at IS NULL AND p.scrubbed IS TRUE AND NOT p.scrub_quarantined
        AND (p.org_id, p.workspace_id, p.harness, coalesce(p.canonical_session_id, p.session_id)) =
            (h.org_id, h.workspace_id, h.physical_source, canonical_id);
     IF own_clocked = 0 OR siblings = 0 OR clocked < siblings THEN
       INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, harness, session_id)
       VALUES (trace, 'search-index', 'skipped',
               jsonb_build_object('code', 'clock_unprovable', 'name', 'SearchWakeRefused', 'retryable', false),
               jsonb_build_object('manifest_key_sha256', encode(sha256(convert_to(h.current_key, 'UTF8')), 'hex')),
               1, concat_ws(':', trace::text, 'search-index', 'skipped', '1', h.current_key),
               h.org_id, h.workspace_id, h.physical_source, canonical_id);
       RETURN 'clock_unprovable';
     END IF;
   END IF;
 END IF;
 PERFORM sessions.enqueue_managed_search_wake_v1(evidence, trace);
 RETURN 'enqueued';
END $wake$;
REVOKE ALL ON FUNCTION sessions.managed_search_wake_v1(jsonb, uuid) FROM PUBLIC;

CREATE TRIGGER guard_search_wake_delete BEFORE DELETE ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (OLD.stage = 'search-index') EXECUTE FUNCTION sessions.guard_managed_search_wake_v1();
CREATE TRIGGER guard_search_wake_truncate BEFORE TRUNCATE ON sessions.pipeline_jobs FOR EACH STATEMENT
 EXECUTE FUNCTION sessions.guard_managed_search_truncate_v1();
CREATE TRIGGER guard_search_wake_insert BEFORE INSERT ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (NEW.stage = 'search-index') EXECUTE FUNCTION sessions.guard_managed_search_wake_v1();
CREATE TRIGGER guard_search_wake_update BEFORE UPDATE ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (OLD.stage = 'search-index' OR NEW.stage = 'search-index') EXECUTE FUNCTION sessions.guard_managed_search_wake_v1();

-- §6: one global keyset cursor over stale scopes. One row, created here at the minimum key, lap 0.
CREATE TABLE sessions.search_catchup_cursor (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  org_id text NOT NULL DEFAULT '',
  workspace_id text NOT NULL DEFAULT '',
  harness text NOT NULL DEFAULT '',
  -- The directory's own key: in session_directory it IS the canonical session id.
  session_id text NOT NULL DEFAULT '',
  lap bigint NOT NULL DEFAULT 0 CHECK (lap >= 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.search_catchup_cursor DEFAULT VALUES;
ALTER TABLE sessions.search_catchup_cursor ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_catchup_cursor FROM PUBLIC, anon, authenticated;
-- Service-role only, like the pipeline tables. The catch-up enqueue writes search-index rows, which the queue guard
-- admits only for the owner, so the catch-up runs on an owner-qualified path; no runtime-role grant here.
GRANT SELECT, UPDATE ON sessions.search_catchup_cursor TO service_role;

-- §7, §8 [rev 8, rev 9]: the stage's audit archive. The rollback never drops it while it holds a row. Sealed and
-- append-only: INSERT, UPDATE, DELETE and TRUNCATE are refused (42501) for every role but the table's owner, so
-- the ONLY write path is sessions.archive_search_stage_v1() (SECURITY DEFINER, owned by the owner). Cleanup is a
-- separate, explicitly approved owner migration.
CREATE TABLE sessions.search_stage_archive (
  batch_id    uuid        NOT NULL,
  kind        text        NOT NULL CHECK (kind IN ('letter', 'event', 'manifest')),
  -- letter id | event sha256 | 'manifest'
  source_id   text        NOT NULL,
  -- to_jsonb(source row) | {letters, events, sha256}
  row         jsonb       NOT NULL,
  archived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id, kind, source_id)
);
CREATE UNIQUE INDEX search_stage_archive_one_manifest ON sessions.search_stage_archive (batch_id) WHERE kind = 'manifest';
-- No row-level security: a reader granted SELECT sees every archived row (no tenant data is filtered here; the
-- table is granted only to the operator and runtime roles), and the seal trigger, not a policy, refuses writes.

CREATE FUNCTION sessions.search_stage_archive_seal_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $seal$
BEGIN
 IF current_user <> (SELECT pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.oid = TG_RELID) THEN
   RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'search stage archive: append-only, written only by sessions.archive_search_stage_v1()';
 END IF;
 IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $seal$;
REVOKE ALL ON FUNCTION sessions.search_stage_archive_seal_v1() FROM PUBLIC;
CREATE TRIGGER search_stage_archive_seal BEFORE INSERT OR UPDATE OR DELETE ON sessions.search_stage_archive
  FOR EACH ROW EXECUTE FUNCTION sessions.search_stage_archive_seal_v1();
CREATE TRIGGER search_stage_archive_seal_truncate BEFORE TRUNCATE ON sessions.search_stage_archive
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.search_stage_archive_seal_v1();

-- §8 [rev 7, rev 8]: the failure-safe archive, in ONE transaction under the table locks. Refuses (55000, nothing
-- archived or deleted) while any search-index letter is not resolved or any search-index job row exists; then
-- copies every search-index letter and event into one batch, adds one manifest row {letters, events, sha256}
-- whose digest is §8's exact rule over the batch's letter and event rows, and deletes the archived rows.
CREATE FUNCTION sessions.archive_search_stage_v1() RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $archive$
DECLARE
 batch uuid := gen_random_uuid(); n_letters integer; n_events integer; digest text; deleted integer;
BEGIN
 LOCK TABLE sessions.pipeline_jobs, sessions.pipeline_dead_letters, sessions.pipeline_events IN SHARE ROW EXCLUSIVE MODE;
 IF EXISTS (SELECT 1 FROM sessions.pipeline_dead_letters WHERE stage = 'search-index' AND state <> 'resolved') THEN
   RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search stage archive: a search-index dead letter is not resolved';
 END IF;
 IF EXISTS (SELECT 1 FROM sessions.pipeline_jobs WHERE stage = 'search-index') THEN
   RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search stage archive: a search-index job exists';
 END IF;
 INSERT INTO sessions.search_stage_archive (batch_id, kind, source_id, row)
 SELECT batch, 'letter', d.id, to_jsonb(d) FROM sessions.pipeline_dead_letters d WHERE d.stage = 'search-index';
 GET DIAGNOSTICS n_letters = ROW_COUNT;
 INSERT INTO sessions.search_stage_archive (batch_id, kind, source_id, row)
 SELECT batch, 'event', encode(sha256(convert_to(to_jsonb(e)::text, 'UTF8')), 'hex'), to_jsonb(e)
   FROM sessions.pipeline_events e WHERE e.stage = 'search-index';
 GET DIAGNOSTICS n_events = ROW_COUNT;
 SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(a.kind, a.source_id, a.row)::text, E'\n'
          ORDER BY a.kind, a.source_id), ''), 'UTF8')), 'hex')
   INTO STRICT digest
   FROM sessions.search_stage_archive a WHERE a.batch_id = batch AND a.kind IN ('letter', 'event');
 INSERT INTO sessions.search_stage_archive (batch_id, kind, source_id, row)
 VALUES (batch, 'manifest', 'manifest', jsonb_build_object('letters', n_letters, 'events', n_events, 'sha256', digest));
 DELETE FROM sessions.pipeline_dead_letters WHERE stage = 'search-index';
 GET DIAGNOSTICS deleted = ROW_COUNT;
 IF deleted <> n_letters THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search stage archive: letter count changed'; END IF;
 DELETE FROM sessions.pipeline_events WHERE stage = 'search-index';
 GET DIAGNOSTICS deleted = ROW_COUNT;
 IF deleted <> n_events THEN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search stage archive: event count changed'; END IF;
 RETURN jsonb_build_object('batchId', batch, 'letters', n_letters, 'events', n_events, 'sha256', digest);
END $archive$;
REVOKE ALL ON FUNCTION sessions.archive_search_stage_v1() FROM PUBLIC;
-- The operator's service role; the migrator also grants the runtime role (RUNTIME_APPLICATION_FUNCTIONS, from the lane).
GRANT EXECUTE ON FUNCTION sessions.archive_search_stage_v1() TO service_role;

REVOKE ALL ON sessions.search_stage_archive FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT ON sessions.search_stage_archive TO service_role;
