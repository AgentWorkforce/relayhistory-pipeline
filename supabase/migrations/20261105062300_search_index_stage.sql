-- The `search-index` stage: the committed search wake (docs/handoffs/incremental-search-indexing.md, design M1
-- rev 9, milestone M2). Additive. Nothing runs until the writer (SEARCH_WAKE_ENQUEUE / the indexer's
-- PIPELINE_MANAGED_SEARCH_WAKE_ENABLED) and the consumer (SEARCH_JOBS_ENABLED) flags are turned on, each with its
-- own go. Applying this to any real database needs a separate, explicit go.
--
-- Producer half (promoted from supabase/proposals/continuous-publication/managed-search-wake.up.sql @ 6658d18f,
-- same semantics): the stage in the three stage CHECKs, the one-row-per-scope index (§3), the queue guard
-- triggers and sessions.enqueue_managed_search_wake_v1 (§2). Two mechanical differences from the proposal: the
-- CHECKs are restated as explicit lists (the 20261031000100 sets plus 'search-index', as §7 asks), so the
-- proposal's publication_private.search_stage_checks bookkeeping is not needed; and the proposal's dedicated
-- consumer role path is gone: the guard admits the owner only, and the runtime leases and settles through the
-- owner's claim/settle functions below.
--
-- Consumer half: sessions.search_catchup_cursor (§6), pipeline_dead_letters.resolution (§3.3b), the sealed,
-- append-only sessions.search_stage_archive with its only write path sessions.archive_search_stage_v1() (§7, §8), and
-- the runtime boundary: sessions.claim_search_jobs_v1 / sessions.settle_search_job_v1 (SECURITY DEFINER, the only
-- functions of the stage the runtime role may execute) over the owner-only claim receipts sessions.search_job_claims.
-- The rollback (supabase/rollbacks/20261105062300_search_index_stage.down.sql) refuses while any search-index
-- job, event or dead letter exists (§8) and never drops the archive or its seal, empty or not.
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
-- replace its envelope or move another row into this stage. Only the owner passes: directly, or through the
-- owner's SECURITY DEFINER functions (the wake, and the runtime's claim_search_jobs_v1 / settle_search_job_v1).
CREATE FUNCTION sessions.guard_managed_search_wake_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $guard$
BEGIN
 IF current_user = pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid = 'sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure)) THEN
   IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
   RETURN NEW;
 END IF;
 RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'search wake: committed managed authority required';
END $guard$;
REVOKE ALL ON FUNCTION sessions.guard_managed_search_wake_v1() FROM PUBLIC;

-- Queue writers must never have authority to truncate stage history.
-- Unconditional non-owner refusal. A conditional "refuse only while search-index rows exist" check is unsafe: an
-- invoker-mode presence check is filtered by the caller's RLS (a restricted role sees zero rows and truncates them
-- away), and a definer-mode one can only identify the caller by session_user, which a SET ROLE privilege drop does
-- not change. So only the owner (current_user, the effective role) may ever TRUNCATE the queue; everyone else
-- deletes rows through the guarded DELETE path. Trigger functions need no EXECUTE grant.
CREATE FUNCTION sessions.guard_managed_search_truncate_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $guard$
BEGIN
 IF current_user <> pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid = 'sessions.enqueue_managed_search_wake_v1(jsonb,uuid)'::regprocedure)) THEN
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
 -- The sibling's own image names its attempt exactly (attempt_token), and that attempt must carry the sibling
 -- head's OWN tenant, user and origin (attempt.origin_id = sibling.origin_id). Sibling and trigger origins may
 -- differ; another origin's attempt never clocks a sibling: it counts as unclocked, so the scope is refused.
 LEFT JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
   AND (a.org_id, a.workspace_id, a.user_id, a.origin_id) = (sibling.org_id, sibling.workspace_id, sibling.user_id, sibling.origin_id)
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
       -- Each sibling's attempt must match ITS OWN head's origin (as enqueue_managed_search_wake_v1): a mismatch is unclocked.
       LEFT JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
         AND (a.org_id, a.workspace_id, a.user_id, a.origin_id) = (sibling.org_id, sibling.workspace_id, sibling.user_id, sibling.origin_id)
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

-- §7, §8 [rev 8, rev 9]: the stage's audit archive. Created here and NEVER dropped by the rollback, empty or not
-- (§8: an audit artifact, removed only by a separate, explicitly approved cleanup). Sealed and append-only: INSERT,
-- UPDATE, DELETE and TRUNCATE are refused (42501) for every role but the table's owner, so the ONLY write path is
-- sessions.archive_search_stage_v1() (SECURITY DEFINER, owned by the owner).
-- Re-apply after the rollback: the archive, its manifest index, its seal function and both seal triggers may
-- already exist (the rollback keeps them). Each is therefore created only if absent (IF NOT EXISTS / OR REPLACE),
-- the migrator accepts exactly these retained objects for this step (RETAINED_BY_ROLLBACK,
-- src/hoopdb/migration-sentinels.ts), and the retained table's shape and owner are verified below before any use.
CREATE TABLE IF NOT EXISTS sessions.search_stage_archive (
  batch_id    uuid        NOT NULL,
  kind        text        NOT NULL CHECK (kind IN ('letter', 'event', 'manifest')),
  -- letter id | event sha256 | 'manifest'
  source_id   text        NOT NULL,
  -- to_jsonb(source row) | {letters, events, sha256}
  row         jsonb       NOT NULL,
  archived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id, kind, source_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS search_stage_archive_one_manifest ON sessions.search_stage_archive (batch_id) WHERE kind = 'manifest';
-- No row-level security: a reader granted SELECT sees every archived row (no tenant data is filtered here; the
-- table is granted only to the operator role), and the seal trigger, not a policy, refuses writes.
-- A retained archive must be exactly this one: the columns, both unique indexes, the kind CHECK, and owned by the
-- migrating role (the seal admits only the owner, and archive_search_stage_v1 below is owned by it). Else 55000.
DO $archive_shape$
BEGIN
 IF (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'sessions.search_stage_archive'::regclass) <> current_user
    OR (SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN ' not null' ELSE '' END, ', ' ORDER BY attnum)
          FROM pg_attribute WHERE attrelid = 'sessions.search_stage_archive'::regclass AND attnum > 0 AND NOT attisdropped)
       IS DISTINCT FROM 'batch_id uuid not null, kind text not null, source_id text not null, row jsonb not null, archived_at timestamp with time zone not null'
    OR (SELECT array_agg(pg_get_indexdef(indexrelid)::text ORDER BY pg_get_indexdef(indexrelid)::text) FROM pg_index WHERE indrelid = 'sessions.search_stage_archive'::regclass)
       IS DISTINCT FROM ARRAY['CREATE UNIQUE INDEX search_stage_archive_one_manifest ON sessions.search_stage_archive USING btree (batch_id) WHERE (kind = ''manifest''::text)',
                              'CREATE UNIQUE INDEX search_stage_archive_pkey ON sessions.search_stage_archive USING btree (batch_id, kind, source_id)']::text[]
    OR (SELECT array_agg(pg_get_constraintdef(oid)::text ORDER BY conname) FROM pg_constraint WHERE conrelid = 'sessions.search_stage_archive'::regclass AND contype = 'c')
       IS DISTINCT FROM ARRAY['CHECK ((kind = ANY (ARRAY[''letter''::text, ''event''::text, ''manifest''::text])))']::text[]
 THEN
   RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search stage archive: an existing sessions.search_stage_archive is not the stage''s archive; nothing applied';
 END IF;
END $archive_shape$;

CREATE OR REPLACE FUNCTION sessions.search_stage_archive_seal_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $seal$
BEGIN
 IF current_user <> (SELECT pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.oid = TG_RELID) THEN
   RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'search stage archive: append-only, written only by sessions.archive_search_stage_v1()';
 END IF;
 IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $seal$;
REVOKE ALL ON FUNCTION sessions.search_stage_archive_seal_v1() FROM PUBLIC;
CREATE OR REPLACE TRIGGER search_stage_archive_seal BEFORE INSERT OR UPDATE OR DELETE ON sessions.search_stage_archive
  FOR EACH ROW EXECUTE FUNCTION sessions.search_stage_archive_seal_v1();
CREATE OR REPLACE TRIGGER search_stage_archive_seal_truncate BEFORE TRUNCATE ON sessions.search_stage_archive
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.search_stage_archive_seal_v1();
-- A retained archive carries no other user trigger (only the two seal triggers above may fire on it). Else 55000.
DO $archive_triggers$
BEGIN
 IF (SELECT array_agg(tgname::text ORDER BY tgname) FROM pg_trigger WHERE tgrelid = 'sessions.search_stage_archive'::regclass AND NOT tgisinternal)
    IS DISTINCT FROM ARRAY['search_stage_archive_seal', 'search_stage_archive_seal_truncate']::text[]
 THEN
   RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search stage archive: the retained archive carries a trigger other than its seal; nothing applied';
 END IF;
END $archive_triggers$;

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
-- Operator-only: EXECUTE to service_role (the operator) and to no runtime role (not in RUNTIME_APPLICATION_FUNCTIONS).
GRANT EXECUTE ON FUNCTION sessions.archive_search_stage_v1() TO service_role;

REVOKE ALL ON sessions.search_stage_archive FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT ON sessions.search_stage_archive TO service_role;

-- §3.2, §3.3 [least privilege]: the runtime boundary of the stage is exactly two stage-scoped SECURITY DEFINER
-- functions, sessions.claim_search_jobs_v1(integer) and sessions.settle_search_job_v1(bigint, uuid, text). The runtime
-- role (the Lambda) gets EXECUTE on these two and nothing else of the stage: no search-index row DML (the queue guard
-- above still refuses it), no replay, catch-up, park or archive (operator paths, service_role or the owner only).
--
-- The claim receipt: what a claim saw, persisted with its lease, owner-only. Settlement reads the claimed epoch,
-- message and attempt ONLY from here, never from the caller, so a lease holder cannot claim a later epoch (and delete
-- later work) or park a fabricated message. One row per leased job; a reclaim after expiry replaces it; every
-- settlement deletes it in the settling transaction.
CREATE TABLE sessions.search_job_claims (
  job_id           bigint      PRIMARY KEY,
  lease_id         uuid        NOT NULL,
  claimed_epoch    bigint      NOT NULL CHECK (claimed_epoch >= 0),
  claimed_message  jsonb       NOT NULL,
  -- The attempt this claim counts. Only ever raised while the same lease holds (search_job_claim_attempts_v1).
  claimed_attempts integer     NOT NULL CHECK (claimed_attempts >= 1),
  claimed_at       timestamptz NOT NULL,
  lease_until      timestamptz NOT NULL
);
ALTER TABLE sessions.search_job_claims ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_job_claims FROM PUBLIC, anon, authenticated, service_role;

-- The receipt's attempt follows an owner-side raise of the leased row's attempts (an operator or test adjustment)
-- and survives a newer wake's budget reset (§3.1 sets attempts 0 on the leased row): the attempt the lease holder
-- is on stays the one that counts at settlement (§3.4: N leased at attempt 12, wake N+1, N exhausts). Only for the
-- same lease; a claim or a settlement (the lease changes) never fires it. Trigger function: no EXECUTE grant.
CREATE FUNCTION sessions.search_job_claim_attempts_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $attempts$
BEGIN
 UPDATE sessions.search_job_claims c
    SET claimed_attempts = greatest(c.claimed_attempts, OLD.attempts, NEW.attempts)
  WHERE c.job_id = OLD.id AND c.lease_id = OLD.lease_id;
 RETURN NULL;
END $attempts$;
REVOKE ALL ON FUNCTION sessions.search_job_claim_attempts_v1() FROM PUBLIC, anon, authenticated, service_role;
CREATE TRIGGER search_job_claim_attempts AFTER UPDATE OF attempts ON sessions.pipeline_jobs FOR EACH ROW
 WHEN (OLD.stage = 'search-index' AND OLD.lease_id IS NOT NULL AND NEW.lease_id IS NOT DISTINCT FROM OLD.lease_id
       AND NEW.attempts IS DISTINCT FROM OLD.attempts)
 EXECUTE FUNCTION sessions.search_job_claim_attempts_v1();

-- The scope's one dead letter id, exactly dlq.ts deadLetterId('search-index', ctx, workKey):
-- 'search-index:' || encodeURIComponent(org) || ':' || encodeURIComponent(ws) || ':' || workKey. encodeURIComponent
-- keeps A-Z a-z 0-9 - _ . ! ~ * ' ( ) and percent-encodes every other character's UTF-8 bytes in upper-case hex.
-- Owner-internal (settle_search_job_v1 calls it): no grant.
CREATE FUNCTION sessions.search_letter_id_v1(p_org text, p_ws text, p_work_key text) RETURNS text
LANGUAGE sql STABLE STRICT SET search_path = pg_catalog, pg_temp AS $letter$
 SELECT 'search-index:' || e.org || ':' || e.ws || ':' || p_work_key
   FROM (SELECT (SELECT coalesce(string_agg(CASE WHEN strpos('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*''()', c) > 0 THEN c
                     ELSE upper(regexp_replace(encode(convert_to(c, 'UTF8'), 'hex'), '(..)', '%\1', 'g')) END, '' ORDER BY i), '')
                   FROM regexp_split_to_table(p_org, '') WITH ORDINALITY AS o(c, i)) AS org,
                (SELECT coalesce(string_agg(CASE WHEN strpos('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*''()', c) > 0 THEN c
                     ELSE upper(regexp_replace(encode(convert_to(c, 'UTF8'), 'hex'), '(..)', '%\1', 'g')) END, '' ORDER BY i), '')
                   FROM regexp_split_to_table(p_ws, '') WITH ORDINALITY AS w(c, i)) AS ws) e
$letter$;
REVOKE ALL ON FUNCTION sessions.search_letter_id_v1(text, text, text) FROM PUBLIC, anon, authenticated, service_role;

-- §3.2: claim up to p_limit (clamped to 1..10) visible, non-dlq search-index rows: FOR UPDATE SKIP LOCKED, one
-- counted attempt, a fresh lease of the server's 300 s, and the receipt (the claim-time snapshot) upserted with it. A
-- reclaim after expiry replaces the previous lease's receipt. Returns the STORED snapshot.
CREATE FUNCTION sessions.claim_search_jobs_v1(p_limit integer)
RETURNS TABLE (job_id bigint, lease_id uuid, message jsonb, epoch bigint, attempts integer)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $claim$
 WITH next AS (
   SELECT j.id FROM sessions.pipeline_jobs j
    WHERE j.stage = 'search-index' AND NOT j.dlq AND j.visible_at <= clock_timestamp()
    ORDER BY j.visible_at, j.id
    LIMIT least(10, greatest(1, coalesce(p_limit, 1)))
    FOR UPDATE SKIP LOCKED
 ), leased AS (
   UPDATE sessions.pipeline_jobs AS j
      SET lease_id = gen_random_uuid(), visible_at = clock_timestamp() + interval '300 seconds', attempts = j.attempts + 1
     FROM next WHERE j.id = next.id AND j.stage = 'search-index'
   RETURNING j.id, j.lease_id, j.message, j.attempts, j.visible_at
 ), receipts AS (
   INSERT INTO sessions.search_job_claims AS c (job_id, lease_id, claimed_epoch, claimed_message, claimed_attempts, claimed_at, lease_until)
   SELECT l.id, l.lease_id, (l.message->'payload'->>'epoch')::bigint, l.message, l.attempts, clock_timestamp(), l.visible_at FROM leased l
   ON CONFLICT ON CONSTRAINT search_job_claims_pkey DO UPDATE
     SET lease_id = EXCLUDED.lease_id, claimed_epoch = EXCLUDED.claimed_epoch, claimed_message = EXCLUDED.claimed_message,
         claimed_attempts = EXCLUDED.claimed_attempts, claimed_at = EXCLUDED.claimed_at, lease_until = EXCLUDED.lease_until
   RETURNING c.job_id, c.lease_id, c.claimed_message, c.claimed_epoch, c.claimed_attempts
 )
 SELECT r.job_id, r.lease_id, r.claimed_message, r.claimed_epoch, r.claimed_attempts FROM receipts r ORDER BY r.job_id
$claim$;

-- §3.3 (+ §3.3b completion): settle one claimed search-index job. Outcomes are allowlisted ('done' | 'retry'; anything
-- else raises 22023). Bound to the stage, the job, the CURRENT unexpired lease (the row's lease and visible_at, and the
-- receipt's lease and lease_until, judged on clock_timestamp() after both row locks) and the receipt's recorded
-- epoch, message and attempt; anything else returns
-- {applied:false} and changes nothing. The caller never supplies an epoch, a message, an attempt or a delay.
--   done:  the current epoch at or below the claimed one: delete the row and resolve the scope's replaying letter
--          (§3.3b, completed | superseded); else a newer wake merged during the pass: reopen now, attempts 0.
--   retry: at the server's max attempts (12): park the CLAIMED snapshot into the scope's letter (§3.3, the
--          quarantine upsert; re-death refreshes it), then delete the row, or reopen it fresh if a newer wake merged;
--          else a newer wake merged: reopen now, attempts 0; else back off min(900, 30 * 2^(attempt-1)) seconds.
-- Every action deletes the receipt and writes the stage's pipeline_events row, all in this one transaction.
CREATE FUNCTION sessions.settle_search_job_v1(p_job_id bigint, p_lease uuid, p_outcome text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $settle$
DECLARE
 max_attempts constant integer := 12;
 j sessions.pipeline_jobs; r sessions.search_job_claims;
 current_epoch bigint; m jsonb; letter text; dead integer; attempt integer; delay integer; action text;
 status text; disc text; err jsonb := NULL; attrs jsonb; checked_at timestamptz;
BEGIN
 IF p_outcome IS NULL OR p_outcome NOT IN ('done', 'retry') THEN
   RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search settle: outcome must be done or retry';
 END IF;
 IF p_job_id IS NULL OR p_lease IS NULL THEN RETURN jsonb_build_object('applied', false); END IF;
 SELECT * INTO j FROM sessions.pipeline_jobs WHERE id = p_job_id AND stage = 'search-index' FOR UPDATE;
 IF NOT FOUND OR j.lease_id IS DISTINCT FROM p_lease THEN RETURN jsonb_build_object('applied', false); END IF;
 SELECT * INTO r FROM sessions.search_job_claims WHERE search_job_claims.job_id = p_job_id FOR UPDATE;
 IF NOT FOUND OR r.lease_id <> p_lease THEN RETURN jsonb_build_object('applied', false); END IF;
 -- Lease freshness is decided on the wall clock read AFTER both row locks, never on the transaction start: a
 -- settlement that waited on a lock (or sat in a long transaction) past the lease's end applies nothing.
 checked_at := clock_timestamp();
 IF j.visible_at <= checked_at OR r.lease_until <= checked_at THEN RETURN jsonb_build_object('applied', false); END IF;

 m := r.claimed_message;
 current_epoch := (j.message->'payload'->>'epoch')::bigint;
 attempt := greatest(1, r.claimed_attempts);
 disc := (m->>'workKey') || '@' || r.claimed_epoch::text;
 attrs := jsonb_build_object('work_key', m->>'workKey', 'epoch', r.claimed_epoch);
 letter := sessions.search_letter_id_v1(m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->>'workKey');

 IF p_outcome = 'done' THEN
   status := 'ok';
   IF current_epoch <= r.claimed_epoch THEN
     DELETE FROM sessions.pipeline_jobs WHERE id = j.id AND lease_id = p_lease;
     UPDATE sessions.pipeline_dead_letters SET state = 'resolved', resolved_at = now(),
            resolution = CASE WHEN (message->'payload'->>'epoch')::bigint = r.claimed_epoch THEN 'completed' ELSE 'superseded' END
      WHERE id = letter AND stage = 'search-index' AND state = 'replaying'
        AND (message->'payload'->>'epoch')::bigint <= r.claimed_epoch;
     action := 'deleted';
   ELSE
     UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = checked_at, attempts = 0 WHERE id = j.id AND lease_id = p_lease;
     attrs := attrs || jsonb_build_object('reason', 'newer_epoch_reopened');
     action := 'reopened';
   END IF;
 ELSIF attempt >= max_attempts THEN
   INSERT INTO sessions.pipeline_dead_letters AS d
          (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, first_dead_at, last_dead_at)
   -- Provenance wholly from the receipt's snapshot: the letter's trace and scope are the claimed message's, never
   -- the live row's (a newer wake merged into it carries its own trace).
   VALUES (letter, 'search-index', (m->'ctx'->>'traceId')::uuid, m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
           m->'ctx'->>'sessionId', m->'ctx'->>'machineId', m, now(), now())
   -- A re-death refreshes the letter wholly from this claim's snapshot: message, trace and scope columns alike.
   ON CONFLICT (id) DO UPDATE SET message = EXCLUDED.message, trace_id = EXCLUDED.trace_id, org_id = EXCLUDED.org_id,
          workspace_id = EXCLUDED.workspace_id, session_id = EXCLUDED.session_id, machine_id = EXCLUDED.machine_id,
          state = 'dead', last_dead_at = EXCLUDED.last_dead_at, dead_count = d.dead_count + 1, resolved_at = NULL, resolution = NULL
   RETURNING d.dead_count INTO dead;
   status := 'dead_lettered';
   disc := disc || ':' || dead::text;
   err := jsonb_build_object('name', 'SearchJobError', 'code', 'search_index_held_exhausted', 'retryable', false);
   attrs := attrs || jsonb_build_object('dead_letter_id', letter);
   IF current_epoch <= r.claimed_epoch THEN
     DELETE FROM sessions.pipeline_jobs WHERE id = j.id AND lease_id = p_lease;
     action := 'parked';
   ELSE
     UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = checked_at, attempts = 0 WHERE id = j.id AND lease_id = p_lease;
     action := 'parked_reopened';
   END IF;
 ELSE
   status := 'retrying';
   IF current_epoch > r.claimed_epoch THEN
     delay := 0;
     UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = checked_at, attempts = 0 WHERE id = j.id AND lease_id = p_lease;
   ELSE
     delay := least(900, 30 * power(2, least(attempt, 16) - 1))::integer;
     UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = checked_at + make_interval(secs => delay) WHERE id = j.id AND lease_id = p_lease;
   END IF;
   attrs := attrs || jsonb_build_object('delay_seconds', delay);
   action := 'retried';
 END IF;

 DELETE FROM sessions.search_job_claims WHERE search_job_claims.job_id = j.id;
 INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
 VALUES ((m->'ctx'->>'traceId')::uuid, 'search-index', status, err, attrs, attempt,
         concat_ws(':', m->'ctx'->>'traceId', 'search-index', status, attempt::text, disc),
         m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'machineId', m->'ctx'->>'harness', m->'ctx'->>'sessionId');
 RETURN jsonb_build_object('applied', true, 'action', action);
END $settle$;

-- The runtime boundary: EXECUTE on claim and settle only, PUBLIC revoked in this transaction. service_role is the
-- grant source; the migrator mirrors it onto the runtime role (RUNTIME_APPLICATION_FUNCTIONS). The operator paths
-- (replay, catch-up, park: owner-run TypeScript over the guarded queue; archive: service_role only) get no runtime
-- grant.
REVOKE ALL ON FUNCTION sessions.claim_search_jobs_v1(integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.settle_search_job_v1(bigint, uuid, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.claim_search_jobs_v1(integer) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.settle_search_job_v1(bigint, uuid, text) TO service_role;
