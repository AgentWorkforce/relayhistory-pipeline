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
  lease_until      timestamptz NOT NULL,
  -- Consecutive reclaims of an EXPIRED lease at this epoch (a worker that dies or hangs). Past the cap the claim
  -- parks the job instead of re-leasing it; any settlement deletes the receipt, so it resets the count.
  reclaim_count    integer     NOT NULL DEFAULT 0 CHECK (reclaim_count >= 0)
);
ALTER TABLE sessions.search_job_claims ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_job_claims FROM PUBLIC, anon, authenticated, service_role;

-- Bounded `busy` (scope lock contention), kept OUTSIDE the claim receipt: one row per contended job, at the epoch it
-- was contended at. Owner-only, no grant. A busy settlement never spends the failure budget and never parks; it holds
-- the job 30 s, escalates at 2 h of same-epoch contention (then at most hourly), and at 24 h HOLDS it (the 24 h cap,
-- held_at / next_probe_at): the probe lane (claim_search_probe_v1) then admits one probe an hour until an operator resumes it
-- (resume_search_contention_v1) or a newer epoch releases it. Any non-busy settlement, and a busy one that finds a
-- newer epoch, clears the row.
CREATE TABLE sessions.search_job_contention (
  job_id             bigint      PRIMARY KEY,
  epoch              bigint      NOT NULL,
  first_contended_at timestamptz NOT NULL,
  last_contended_at  timestamptz NOT NULL,
  contention_count   integer     NOT NULL CHECK (contention_count >= 0),
  last_escalated_at  timestamptz,
  -- The 24 h cap: held since, and when the next hourly probe claim is admitted. Both NULL when not held.
  held_at            timestamptz,
  next_probe_at      timestamptz,
  CHECK ((held_at IS NULL) = (next_probe_at IS NULL))
);
ALTER TABLE sessions.search_job_contention ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_job_contention FROM PUBLIC, anon, authenticated, service_role;

-- The fleet-wide probe budget for held jobs: one owner-only row, a token bucket shared by every concurrent prober
-- (claim_search_probe_v1 updates it under FOR UPDATE, so two probes never spend the same token). rate_per_minute (R)
-- and burst (C) are its tunable policy; defaults R = 6/min, C = 6, initial balance 6 tokens (an owner UPDATE changes
-- them, no code change). Probes admitted over any interval are at most C + R x elapsed minutes, fleet-wide.
CREATE TABLE sessions.search_probe_budget (
  singleton       boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
  rate_per_minute numeric     NOT NULL DEFAULT 6 CHECK (rate_per_minute > 0),
  burst           numeric     NOT NULL DEFAULT 6 CHECK (burst >= 1),
  tokens          numeric     NOT NULL DEFAULT 6 CHECK (tokens >= 0),
  refilled_at     timestamptz NOT NULL DEFAULT now(),
  CHECK (tokens <= burst)
);
INSERT INTO sessions.search_probe_budget DEFAULT VALUES;
ALTER TABLE sessions.search_probe_budget ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.search_probe_budget FROM PUBLIC, anon, authenticated, service_role;

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

-- Owner-internal: lease ONE job the calling claim function has already locked (FOR UPDATE), or park it. Counts the
-- attempt, takes the server's 300 s lease and upserts the receipt (the claim-time snapshot); returns the STORED
-- snapshot, or no row when it parked.
--   Expired-lease reclaim bound: a receipt still present means the previous lease expired unsettled (every settlement
--   deletes it); consecutive abandoned leases at one epoch are counted on the persisted receipt (reclaim_count), and at
--   exactly 6 the job is parked with the previous claim's snapshot (code expired_lease_reclaims) and its row, receipt
--   and contention deleted, atomically.
--   A newer epoch releases a hold (the contention resets). A probe (p_probe) moves the next probe an hour on and
--   writes exactly one durable scope_contention_probe event with its lease id; a park writes none.
-- Not SECURITY DEFINER and granted to no one: it runs only inside the owner's claim functions below.
CREATE FUNCTION sessions.search_job_lease_v1(p_job_id bigint, p_probe boolean)
RETURNS TABLE (job_id bigint, lease_id uuid, message jsonb, epoch bigint, attempts integer)
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $lease$
#variable_conflict use_column
DECLARE
 reclaim_cap constant integer := 6;
 q sessions.pipeline_jobs; prior sessions.search_job_claims; had_prior boolean;
 cur_epoch bigint; reclaims integer; at_ts timestamptz := clock_timestamp(); m jsonb; letter text; dead integer;
 new_lease uuid; new_attempts integer;
BEGIN
 SELECT * INTO q FROM sessions.pipeline_jobs x WHERE x.id = p_job_id AND x.stage = 'search-index' FOR UPDATE;
 IF NOT FOUND THEN RETURN; END IF;
 cur_epoch := (q.message->'payload'->>'epoch')::bigint;
 SELECT * INTO prior FROM sessions.search_job_claims c WHERE c.job_id = q.id FOR UPDATE;
 had_prior := FOUND;
 reclaims := CASE WHEN had_prior AND prior.claimed_epoch = cur_epoch THEN prior.reclaim_count + 1 ELSE 0 END;
 IF reclaims >= reclaim_cap THEN
   m := prior.claimed_message;
   letter := sessions.search_letter_id_v1(m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->>'workKey');
   INSERT INTO sessions.pipeline_dead_letters AS d
          (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, first_dead_at, last_dead_at)
   VALUES (letter, 'search-index', (m->'ctx'->>'traceId')::uuid, m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
           m->'ctx'->>'sessionId', m->'ctx'->>'machineId', m, at_ts, at_ts)
   ON CONFLICT (id) DO UPDATE SET message = EXCLUDED.message, trace_id = EXCLUDED.trace_id, org_id = EXCLUDED.org_id,
          workspace_id = EXCLUDED.workspace_id, session_id = EXCLUDED.session_id, machine_id = EXCLUDED.machine_id,
          state = 'dead', last_dead_at = EXCLUDED.last_dead_at, dead_count = d.dead_count + 1, resolved_at = NULL, resolution = NULL
   RETURNING d.dead_count INTO dead;
   INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
   VALUES ((m->'ctx'->>'traceId')::uuid, 'search-index', 'dead_lettered',
           jsonb_build_object('name', 'SearchJobError', 'code', 'expired_lease_reclaims', 'retryable', false),
           jsonb_build_object('work_key', m->>'workKey', 'epoch', prior.claimed_epoch, 'dead_letter_id', letter, 'expired_leases', reclaims),
           greatest(1, prior.claimed_attempts),
           concat_ws(':', m->'ctx'->>'traceId', 'search-index', 'dead_lettered', greatest(1, prior.claimed_attempts)::text,
                     (m->>'workKey') || '@' || prior.claimed_epoch::text, 'reclaims', prior.lease_id::text),
           m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'machineId', m->'ctx'->>'harness', m->'ctx'->>'sessionId');
   DELETE FROM sessions.pipeline_jobs x WHERE x.id = q.id;
   DELETE FROM sessions.search_job_claims c WHERE c.job_id = q.id;
   DELETE FROM sessions.search_job_contention k WHERE k.job_id = q.id;
   RETURN;
 END IF;
 DELETE FROM sessions.search_job_contention k WHERE k.job_id = q.id AND k.epoch < cur_epoch;
 IF p_probe THEN
   UPDATE sessions.search_job_contention k SET next_probe_at = at_ts + interval '1 hour'
    WHERE k.job_id = q.id AND k.held_at IS NOT NULL AND k.epoch = cur_epoch;
 END IF;
 UPDATE sessions.pipeline_jobs AS x
    SET lease_id = gen_random_uuid(), visible_at = at_ts + interval '300 seconds', attempts = x.attempts + 1
  WHERE x.id = q.id
 RETURNING x.lease_id, x.attempts INTO new_lease, new_attempts;
 INSERT INTO sessions.search_job_claims AS c (job_id, lease_id, claimed_epoch, claimed_message, claimed_attempts, claimed_at, lease_until, reclaim_count)
 VALUES (q.id, new_lease, cur_epoch, q.message, new_attempts, at_ts, at_ts + interval '300 seconds', reclaims)
 ON CONFLICT ON CONSTRAINT search_job_claims_pkey DO UPDATE
   SET lease_id = EXCLUDED.lease_id, claimed_epoch = EXCLUDED.claimed_epoch, claimed_message = EXCLUDED.claimed_message,
       claimed_attempts = EXCLUDED.claimed_attempts, claimed_at = EXCLUDED.claimed_at, lease_until = EXCLUDED.lease_until,
       reclaim_count = EXCLUDED.reclaim_count;
 IF p_probe THEN
   INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
   VALUES ((q.message->'ctx'->>'traceId')::uuid, 'search-index', 'retrying',
           jsonb_build_object('name', 'SearchJobError', 'code', 'scope_contention_probe', 'retryable', true),
           jsonb_build_object('work_key', q.message->>'workKey', 'epoch', cur_epoch, 'severity', 'warn', 'admitted_at', at_ts,
                              'next_probe_in_s', 3600),
           new_attempts,
           concat_ws(':', q.message->'ctx'->>'traceId', 'search-index', 'retrying', new_attempts::text,
                     (q.message->>'workKey') || '@' || cur_epoch::text, 'probe', new_lease::text),
           q.message->'ctx'->>'orgId', q.message->'ctx'->>'workspaceId', q.message->'ctx'->>'machineId', q.message->'ctx'->>'harness', q.message->'ctx'->>'sessionId');
 END IF;
 job_id := q.id; lease_id := new_lease; message := q.message; epoch := cur_epoch; attempts := new_attempts;
 RETURN NEXT;
END $lease$;
REVOKE ALL ON FUNCTION sessions.search_job_lease_v1(bigint, boolean) FROM PUBLIC, anon, authenticated, service_role;

-- §3.2: the ORDINARY lane. Claims up to p_limit (clamped to 1..10) visible, non-dlq search-index rows that are not held
-- at their current epoch (FOR UPDATE SKIP LOCKED), each leased through search_job_lease_v1. Held jobs are never
-- ordinary work, so probes never take an ordinary slot.
CREATE FUNCTION sessions.claim_search_jobs_v1(p_limit integer)
RETURNS TABLE (job_id bigint, lease_id uuid, message jsonb, epoch bigint, attempts integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $claim$
#variable_conflict use_column
DECLARE n integer := least(10, greatest(1, coalesce(p_limit, 1))); v_id bigint;
BEGIN
 FOR v_id IN SELECT x.id FROM sessions.pipeline_jobs x
            WHERE x.stage = 'search-index' AND NOT x.dlq AND x.visible_at <= clock_timestamp()
              AND NOT EXISTS (SELECT 1 FROM sessions.search_job_contention k
                               WHERE k.job_id = x.id AND k.held_at IS NOT NULL AND k.epoch = (x.message->'payload'->>'epoch')::bigint)
            ORDER BY x.visible_at, x.id LIMIT n FOR UPDATE SKIP LOCKED LOOP
   RETURN QUERY SELECT * FROM sessions.search_job_lease_v1(v_id, false);
 END LOOP;
END $claim$;

-- The PROBE lane (the 24 h cap), separate from ordinary capacity: at most ONE held job whose hourly probe is due
-- (oldest due first, then job id; FOR UPDATE SKIP LOCKED), and only if the fleet bucket (search_probe_budget, under
-- FOR UPDATE, shared by every concurrent prober) has a token, which it spends. The probe is leased like any claim and
-- writes its one durable scope_contention_probe event (search_job_lease_v1). No token, no due job: no row, no event.
CREATE FUNCTION sessions.claim_search_probe_v1()
RETURNS TABLE (job_id bigint, lease_id uuid, message jsonb, epoch bigint, attempts integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $probe$
#variable_conflict use_column
DECLARE probe_id bigint; b sessions.search_probe_budget; at_ts timestamptz;
BEGIN
 SELECT x.id INTO probe_id
   FROM sessions.pipeline_jobs x JOIN sessions.search_job_contention k ON k.job_id = x.id
  WHERE x.stage = 'search-index' AND NOT x.dlq AND x.visible_at <= clock_timestamp()
    AND k.held_at IS NOT NULL AND k.next_probe_at <= clock_timestamp() AND k.epoch = (x.message->'payload'->>'epoch')::bigint
  ORDER BY k.next_probe_at, k.job_id LIMIT 1
  FOR UPDATE OF x SKIP LOCKED;
 IF probe_id IS NULL THEN RETURN; END IF;
 SELECT * INTO b FROM sessions.search_probe_budget WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RETURN; END IF;
 at_ts := clock_timestamp();
 b.tokens := least(b.burst, b.tokens + greatest(0, extract(epoch FROM at_ts - b.refilled_at)) * b.rate_per_minute / 60.0);
 IF b.tokens < 1 THEN
   UPDATE sessions.search_probe_budget SET tokens = b.tokens, refilled_at = at_ts WHERE singleton;
   RETURN;
 END IF;
 UPDATE sessions.search_probe_budget SET tokens = b.tokens - 1, refilled_at = at_ts WHERE singleton;
 RETURN QUERY SELECT * FROM sessions.search_job_lease_v1(probe_id, true);
END $probe$;

-- §3.3 (+ §3.3b completion): settle one claimed search-index job. Outcomes are allowlisted ('done' | 'retry' | 'busy';
-- anything else raises 22023). 'busy' (scope lock contention) is bounded: see sessions.search_job_contention. Bound to the stage, the job, the CURRENT unexpired lease (the row's lease and visible_at, and the
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
 cont sessions.search_job_contention; extra jsonb := '{}'::jsonb; escalated boolean := false; held_now boolean := false; probe boolean := false;
 contended_s bigint;
BEGIN
 IF p_outcome IS NULL OR p_outcome NOT IN ('done', 'retry', 'busy') THEN
   RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search settle: outcome must be done, retry or busy';
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

 IF p_outcome = 'busy' AND current_epoch > r.claimed_epoch THEN
   -- Busy, but a newer wake merged during the lease: that is new work. Reopen now on a fresh budget; contention resets.
   UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = checked_at, attempts = 0 WHERE id = j.id AND lease_id = p_lease;
   DELETE FROM sessions.search_job_contention k WHERE k.job_id = j.id;
   status := 'retrying';
   err := jsonb_build_object('name', 'SearchJobError', 'code', 'scope_busy', 'retryable', true);
   attrs := attrs || jsonb_build_object('reason', 'newer_epoch_reopened', 'delay_seconds', 0);
   disc := disc || ':busy:' || p_lease::text;
   action := 'busy_reopened';
 ELSIF p_outcome = 'busy' THEN
   -- Busy at the claimed epoch (scope lock contention): never spends the failure budget, never parks. The attempt the
   -- claim counted is given back, the receipt goes, and the contention row (outside the receipt) sets the delay.
   SELECT * INTO cont FROM sessions.search_job_contention k WHERE k.job_id = j.id FOR UPDATE;
   IF NOT FOUND OR cont.epoch <> r.claimed_epoch THEN
     INSERT INTO sessions.search_job_contention AS k (job_id, epoch, first_contended_at, last_contended_at, contention_count, last_escalated_at, held_at, next_probe_at)
     VALUES (j.id, r.claimed_epoch, checked_at, checked_at, 1, NULL, NULL, NULL)
     ON CONFLICT ON CONSTRAINT search_job_contention_pkey DO UPDATE
       SET epoch = EXCLUDED.epoch, first_contended_at = EXCLUDED.first_contended_at, last_contended_at = EXCLUDED.last_contended_at,
           contention_count = 1, last_escalated_at = NULL, held_at = NULL, next_probe_at = NULL
     RETURNING * INTO cont;
   ELSE
     UPDATE sessions.search_job_contention k SET last_contended_at = checked_at, contention_count = k.contention_count + 1
      WHERE k.job_id = j.id RETURNING * INTO cont;
   END IF;
   contended_s := floor(extract(epoch FROM checked_at - cont.first_contended_at))::bigint;
   -- The 24 h cap (isolated here, in the ordinary claim's hold filter and in the probe lane): hold the job, one probe claim an hour.
   IF cont.held_at IS NOT NULL THEN
     probe := true;
     UPDATE sessions.search_job_contention k SET next_probe_at = checked_at + interval '1 hour' WHERE k.job_id = j.id RETURNING * INTO cont;
   ELSIF cont.first_contended_at <= checked_at - interval '24 hours' THEN
     held_now := true;
     UPDATE sessions.search_job_contention k SET held_at = checked_at, next_probe_at = checked_at + interval '1 hour'
      WHERE k.job_id = j.id RETURNING * INTO cont;
   END IF;
   IF cont.first_contended_at <= checked_at - interval '2 hours'
      AND (cont.last_escalated_at IS NULL OR cont.last_escalated_at <= checked_at - interval '1 hour') THEN
     escalated := true;
     UPDATE sessions.search_job_contention k SET last_escalated_at = checked_at WHERE k.job_id = j.id RETURNING * INTO cont;
   END IF;
   delay := 30;
   UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = checked_at + make_interval(secs => delay),
          attempts = greatest(0, r.claimed_attempts - 1)
    WHERE id = j.id AND lease_id = p_lease;
   status := 'retrying';
   err := jsonb_build_object('name', 'SearchJobError', 'code', 'scope_busy', 'retryable', true);
   attrs := attrs || jsonb_build_object('delay_seconds', delay, 'contention_count', cont.contention_count, 'contended_for_s', contended_s);
   disc := disc || ':busy:' || p_lease::text;
   IF escalated THEN
     INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
     VALUES ((m->'ctx'->>'traceId')::uuid, 'search-index', 'retrying',
             jsonb_build_object('name', 'SearchJobError', 'code', 'scope_contention_escalated', 'retryable', true),
             attrs || jsonb_build_object('severity', 'warn'), attempt,
             concat_ws(':', m->'ctx'->>'traceId', 'search-index', 'retrying', attempt::text, disc, 'escalated'),
             m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'machineId', m->'ctx'->>'harness', m->'ctx'->>'sessionId');
   END IF;
   -- The hold transition's alert (once per hold). A probe's event is the claim's (one per admitted probe), never here.
   IF held_now THEN
     INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
     VALUES ((m->'ctx'->>'traceId')::uuid, 'search-index', 'retrying',
             jsonb_build_object('name', 'SearchJobError', 'code', 'scope_contention_held', 'retryable', true),
             attrs || jsonb_build_object('severity', 'error',
                                         'next_probe_in_s', floor(extract(epoch FROM cont.next_probe_at - checked_at))::bigint), attempt,
             concat_ws(':', m->'ctx'->>'traceId', 'search-index', 'retrying', attempt::text, disc, 'held'),
             m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'machineId', m->'ctx'->>'harness', m->'ctx'->>'sessionId');
   END IF;
   extra := jsonb_build_object('escalated', escalated, 'held', cont.held_at IS NOT NULL, 'held_now', held_now, 'probe', probe,
                               'contended_for_s', contended_s, 'contention_count', cont.contention_count, 'delay_seconds', delay);
   action := 'busy';
 ELSIF p_outcome = 'done' THEN
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
 -- A done, a substantive retry or an exhaustion ends the contention episode.
 IF p_outcome <> 'busy' THEN DELETE FROM sessions.search_job_contention k WHERE k.job_id = j.id; END IF;
 INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
 VALUES ((m->'ctx'->>'traceId')::uuid, 'search-index', status, err, attrs, attempt,
         concat_ws(':', m->'ctx'->>'traceId', 'search-index', status, attempt::text, disc),
         m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'machineId', m->'ctx'->>'harness', m->'ctx'->>'sessionId');
 RETURN jsonb_build_object('applied', true, 'action', action) || extra;
END $settle$;

-- The 24 h cap's operator release: clears a job's hold (held_at, next_probe_at) and restarts its contention episode
-- (a fresh 24 h window, escalation cleared), with an audit pipeline_events row naming the supplied operator AND the
-- database identities (session_user, current_user) and the wall clock. search-index only; FOR UPDATE on the job and
-- its contention row; refuses ({applied:false}, nothing changed) while a lease is live or when the job is not held.
-- Owner SECURITY DEFINER; EXECUTE to service_role ONLY (operator), never the runtime role.
CREATE FUNCTION sessions.resume_search_contention_v1(p_job_id bigint, p_operator text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $resume$
DECLARE j sessions.pipeline_jobs; k sessions.search_job_contention; m jsonb; at_ts timestamptz;
BEGIN
 IF p_operator IS NULL OR p_operator !~ '^[A-Za-z0-9][A-Za-z0-9._@:-]{0,127}$' THEN
   RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search contention resume: operator required';
 END IF;
 SELECT * INTO j FROM sessions.pipeline_jobs q WHERE q.id = p_job_id AND q.stage = 'search-index' FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('applied', false, 'reason', 'no_job'); END IF;
 SELECT * INTO k FROM sessions.search_job_contention c WHERE c.job_id = p_job_id FOR UPDATE;
 IF NOT FOUND OR k.held_at IS NULL THEN RETURN jsonb_build_object('applied', false, 'reason', 'not_held'); END IF;
 at_ts := clock_timestamp();
 IF j.lease_id IS NOT NULL AND j.visible_at > at_ts THEN RETURN jsonb_build_object('applied', false, 'reason', 'lease_live'); END IF;
 UPDATE sessions.search_job_contention c
    SET held_at = NULL, next_probe_at = NULL, first_contended_at = at_ts, last_escalated_at = NULL
  WHERE c.job_id = p_job_id;
 m := j.message;
 INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, machine_id, harness, session_id)
 VALUES ((m->'ctx'->>'traceId')::uuid, 'search-index', 'retrying',
         jsonb_build_object('name', 'SearchJobError', 'code', 'scope_contention_resumed', 'retryable', true),
         jsonb_build_object('work_key', m->>'workKey', 'epoch', k.epoch, 'operator', p_operator, 'session_user', session_user::text,
                            'current_user', current_user::text, 'resumed_at', at_ts, 'contention_count', k.contention_count),
         1, concat_ws(':', m->'ctx'->>'traceId', 'search-index', 'retrying', '1', (m->>'workKey') || '@' || k.epoch::text, 'resumed',
                      to_char(at_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
         m->'ctx'->>'orgId', m->'ctx'->>'workspaceId', m->'ctx'->>'machineId', m->'ctx'->>'harness', m->'ctx'->>'sessionId');
 RETURN jsonb_build_object('applied', true, 'epoch', k.epoch);
END $resume$;
REVOKE ALL ON FUNCTION sessions.resume_search_contention_v1(bigint, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.resume_search_contention_v1(bigint, text) TO service_role;

-- The runtime boundary: EXECUTE on the ordinary claim, the probe claim and settle only, PUBLIC revoked in this
-- transaction. service_role is the
-- grant source; the migrator mirrors it onto the runtime role (RUNTIME_APPLICATION_FUNCTIONS). The operator paths
-- (replay, catch-up, park: owner-run TypeScript over the guarded queue; archive: service_role only) get no runtime
-- grant.
REVOKE ALL ON FUNCTION sessions.claim_search_jobs_v1(integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.claim_search_probe_v1() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.settle_search_job_v1(bigint, uuid, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.claim_search_jobs_v1(integer) TO service_role;
GRANT EXECUTE ON FUNCTION sessions.claim_search_probe_v1() TO service_role;
GRANT EXECUTE ON FUNCTION sessions.settle_search_job_v1(bigint, uuid, text) TO service_role;
