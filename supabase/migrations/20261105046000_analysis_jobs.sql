-- Registry slot 046000 (lane F1, session ANALYSIS panel off Neon). The full-analysis
-- request/job lifecycle that relayhistory ran on Neon (sessions.session_analysis_requests,
-- sessions.session_analysis_jobs; RHC migrations 0015, 0016, 0024, 0031, 0043, 0062), owned
-- by sessions-pipeline. The engine is src/session-analysis (relayhistory's, ported as-is).
--
-- Differences from relayhistory, all deliberate:
--   * One job kind. The brief half is sessions.session_briefs (src/session-brief, lane F2);
--     relayhistory's per-request brief job is not recreated, so there is no second brief
--     generator. A request links at most one analysis job.
--   * No evidence or result in Postgres (the 0004 no-text rule). A job carries its scope;
--     the worker reads canonical S3 evidence when it runs, and the result is an object under
--     the session's manifest directory, recorded here by key, SHA-256 and size.
--   * Scope is (org, workspace, session): the requesting workspace, not the whole org.
--   * Backfilled Neon history (origin 'neon_backfill') keeps its public v1 values
--     (status, error, v1 job id, versions) so the read is byte-equal to Neon's
--     read_session_analysis_v1 for those rows.
--
-- Admission limits are relayhistory's (0024 session_request_limit, 0043
-- sessions_admitted_today, 0062 request_session_analysis_v1): one request per session per
-- 2 minutes (a throttled request returns the latest), 30 per session per UTC day, 20
-- distinct other sessions admitted per org per UTC day; an analysis completed in the last
-- 30 minutes is reused (relayhistory 0024 collect: "defers its analysis").
--
-- Functions:
--   sessions.analysis_request_v1(org, workspace, user, session)  SECURITY DEFINER, EXECUTE
--     bulls96_analysis_admit only: the read Lambda's separate admission principal (NOLOGIN
--     until its operator step; provisioned before migrate by session_read_api_roles.sql).
--     The tenant must equal the transaction's bound bulls96_read.* settings (the verified
--     assertion), as session_directory_read_api_scope pins the read role.
--   sessions.session_analysis_source_revision(org, workspace, session)  the live pointer-set
--     revision (sha256 over the live pointers); reuse and publication compare it.
--   sessions.session_analysis_result_tickets  erase-safe publication: a ticket is recorded
--     (lease + liveness + revision checked) before a result object is written and retired
--     with the completion; the forget trigger tickets every result key it orphans; the
--     worker's sweeper deletes the object of an orphan or expired ticket.
--   sessions.analysis_read_v1(workspace, session)  SECURITY DEFINER, EXECUTE bulls96_read_api;
--     the tenant comes from sessions.session_directory_read_api_scope (034000).
--   sessions.session_analysis_v2_forget()  erasure trigger (session_briefs_forget pattern).
-- The worker (claim, complete, fail) uses the tables as service_role, as session-brief does.
--
-- Retention-ready: every row carries org_id and time columns; results sit under the
-- org/date-prefixed session keys. Code only; nothing here is applied by this repository.
-- Down path: supabase/rollbacks/20261105046000_analysis_jobs.down.sql.

CREATE TABLE sessions.session_analysis_jobs_v2 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512 AND btrim(org_id) <> ''),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id) <> ''),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512 AND btrim(session_id) <> ''),
  -- The harness the evidence was read from; NULL until the worker resolved it.
  harness text CHECK (harness ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
  status text NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_token uuid,
  lease_expires_at timestamptz,
  analysis_version text NOT NULL CHECK (analysis_version ~ '^[a-z0-9][a-z0-9._:-]{0,127}$'),
  schema_version integer NOT NULL CHECK (schema_version >= 0),
  model_version text NOT NULL CHECK (model_version ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$'),
  -- relayhistory's public v1 brief versions; '' for native jobs (the brief is session_briefs).
  summary_version text NOT NULL DEFAULT '' CHECK (char_length(summary_version) <= 128),
  summary_model_version text NOT NULL DEFAULT '' CHECK (char_length(summary_model_version) <= 128),
  source_fingerprint text CHECK (char_length(source_fingerprint) BETWEEN 1 AND 2048),
  -- canonicalSourceGeneration of the pointer set the evidence was read from.
  source_generation text CHECK (source_generation ~ '^[0-9a-f]{64}$'),
  -- session_analysis_source_revision at claim; equal at reserve and completion.
  source_revision text CHECK (source_revision ~ '^[0-9a-f]{64}$'),
  result_s3_key text CHECK (char_length(result_s3_key) BETWEEN 1 AND 1024),
  result_sha256 text CHECK (result_sha256 ~ '^[0-9a-f]{64}$'),
  result_bytes integer CHECK (result_bytes BETWEEN 1 AND 8388608),
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  -- A controlled code (native), or relayhistory's stored error (backfill, bounded).
  last_error text CHECK (char_length(last_error) BETWEEN 1 AND 1000),
  origin text NOT NULL DEFAULT 'native' CHECK (origin IN ('native', 'neon_backfill')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CONSTRAINT session_analysis_jobs_v2_lease CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
  CONSTRAINT session_analysis_jobs_v2_running_leased CHECK (status <> 'running' OR origin <> 'native' OR lease_token IS NOT NULL),
  CONSTRAINT session_analysis_jobs_v2_result_whole CHECK (
    (result_s3_key IS NULL) = (result_sha256 IS NULL) AND (result_s3_key IS NULL) = (result_bytes IS NULL)),
  CONSTRAINT session_analysis_jobs_v2_completed_result CHECK (status <> 'completed' OR result_s3_key IS NOT NULL),
  CONSTRAINT session_analysis_jobs_v2_native_error CHECK (
    origin <> 'native' OR last_error IS NULL OR last_error ~ '^[a-z][a-z0-9_]{0,63}$')
);
CREATE INDEX session_analysis_jobs_v2_due_idx ON sessions.session_analysis_jobs_v2 (available_at)
  WHERE status IN ('queued', 'running');
CREATE INDEX session_analysis_jobs_v2_scope_idx
  ON sessions.session_analysis_jobs_v2 (org_id, workspace_id, session_id, created_at DESC, id DESC);

CREATE TABLE sessions.session_analysis_requests_v2 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512 AND btrim(org_id) <> ''),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id) <> ''),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512 AND btrim(session_id) <> ''),
  requested_by_user_id text NOT NULL CHECK (char_length(requested_by_user_id) BETWEEN 1 AND 512),
  -- Native: 'resolved' once linked (the public status is the job's). Backfill: the public
  -- v1 status Neon served.
  status text NOT NULL CHECK (status IN ('queued', 'running', 'resolved', 'completed', 'failed')),
  analysis_job_id uuid REFERENCES sessions.session_analysis_jobs_v2 (id) ON DELETE SET NULL,
  -- Backfill: Neon's public v1 job id (md5('v1-job:' || request id) for brief-era requests).
  v1_job_id uuid,
  last_error text CHECK (char_length(last_error) BETWEEN 1 AND 1000),
  origin text NOT NULL DEFAULT 'native' CHECK (origin IN ('native', 'neon_backfill')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT session_analysis_requests_v2_native CHECK (
    origin <> 'native' OR (status = 'resolved' AND analysis_job_id IS NOT NULL AND v1_job_id IS NULL))
);
CREATE INDEX session_analysis_requests_v2_scope_idx
  ON sessions.session_analysis_requests_v2 (org_id, workspace_id, session_id, created_at DESC, id DESC);
CREATE INDEX session_analysis_requests_v2_day_idx
  ON sessions.session_analysis_requests_v2 (org_id, created_at);
CREATE INDEX session_analysis_requests_v2_job_idx
  ON sessions.session_analysis_requests_v2 (analysis_job_id);

ALTER TABLE sessions.session_analysis_jobs_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_analysis_requests_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_jobs_v2, sessions.session_analysis_requests_v2
  FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_analysis_jobs_v2, sessions.session_analysis_requests_v2 TO service_role;

-- Erase-safe publication tickets. Not deleted by erasure: they are how the sweeper finds
-- an object written for (or orphaned by) a forgotten session.
CREATE TABLE sessions.session_analysis_result_tickets (
  result_key text PRIMARY KEY CHECK (char_length(result_key) BETWEEN 1 AND 1024),
  job_id uuid NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  session_id text NOT NULL,
  reason text NOT NULL CHECK (reason IN ('publishing', 'forgotten')),
  created_at timestamptz NOT NULL DEFAULT now(),
  -- A writer's bounded lease on a 'publishing' ticket (the operator backfill heartbeats it):
  -- the sweeper never sweeps a ticket whose lease is live, and a writer whose lease lapsed
  -- must not write. NULL for the worker's tickets (its job lease fences them instead).
  lease_expires_at timestamptz,
  -- The backfill writer that holds the lease (one random id per run): a successor may take the
  -- ticket over only after the lease expired, and every fence names its own writer, so a
  -- stale writer can never mistake a successor's live lease for its own. NULL for the worker.
  writer_token uuid
);
CREATE INDEX session_analysis_result_tickets_created_idx ON sessions.session_analysis_result_tickets (created_at);
ALTER TABLE sessions.session_analysis_result_tickets ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_result_tickets FROM PUBLIC, anon, authenticated;
GRANT ALL ON sessions.session_analysis_result_tickets TO service_role;


-- C3 purge fence (purge_session_analysis_for_owned): a session whose analysis was purged by a
-- reset/disable stays fenced (admission answers not_found, the read hides it, and the fence moves
-- the source revision so an in-flight job can neither reserve nor complete) until a NEW pointer
-- for it is created after the purge (a fresh upload after re-enable). Owner-only.
CREATE TABLE sessions.session_analysis_purge_fences (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  session_id text NOT NULL,
  purged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (org_id, workspace_id, session_id)
);
ALTER TABLE sessions.session_analysis_purge_fences ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_analysis_purge_fences FROM PUBLIC, anon, authenticated, service_role;

-- Authoritative legacy coverage for the never-in-Neon proof. The frozen copy
-- (ingest_frozen_source_records) holds Neon DELIVERY records only: an older turns-only Neon
-- session (turns uploaded on the legacy path, no delivery record) is absent from it, and
-- session_index.created_at is the SP pointer's insert time, not the session's age, so a
-- not-yet-exported old session whose FIRST SP pointer lands after the seal (a continuation,
-- or a late first upload) passes both absence checks while its old Neon turns are
-- unaccounted for. No invariant in the source excludes that case, so the proof also needs a
-- Neon-side census: every (org, workspace, user, source, session id) Neon holds any
-- delivery record, turn, event, link, outcome, rollup or catalog row for, loaded by the owner
-- (scripts/neon-census-load.ts) AFTER Neon refuses legacy writes, and sealed with its row
-- count and digest. Until a census generation is sealed the proof is FALSE for every session
-- (the read API stays turns_unknown and analysis stays incomplete): fail closed, never
-- inferred. Columns Neon does not carry for a row (a rollup's workspace, a turn's user) are ''.
-- Generations: rows are loaded under a generation (the census digest), the seal names the one
-- in force, and a sealed generation is immutable (trigger below); a reload loads a NEW
-- generation and its seal replaces the old one, which is then dropped.
-- Owner-only: no grant to any runtime or read role; only the SECURITY DEFINER proof reads it.
CREATE TABLE sessions.session_neon_census (
  generation text NOT NULL CHECK (generation ~ '^[0-9a-f]{64}$'),
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL DEFAULT '' CHECK (char_length(workspace_id) <= 512),
  user_id text NOT NULL DEFAULT '' CHECK (char_length(user_id) <= 512),
  source text NOT NULL DEFAULT '' CHECK (char_length(source) <= 512),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512),
  PRIMARY KEY (generation, org_id, session_id, workspace_id, user_id, source)
);
CREATE TABLE sessions.session_neon_census_seal (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  generation text NOT NULL CHECK (generation ~ '^[0-9a-f]{64}$'),
  census_rows bigint NOT NULL CHECK (census_rows >= 0),
  census_sha256 text NOT NULL CHECK (census_sha256 ~ '^[0-9a-f]{64}$'),
  -- The Neon-side evidence the loader verified (counts per source table, snapshot time).
  neon_snapshot jsonb NOT NULL,
  sealed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE sessions.session_neon_census ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_neon_census_seal ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_neon_census, sessions.session_neon_census_seal FROM PUBLIC, anon, authenticated, service_role;

-- A sealed generation is immutable: no row of it is inserted, changed or deleted, and the
-- table is never truncated while a seal exists. Rows of an unsealed generation can be loaded
-- (resumably) and a superseded generation's rows are dropped by the next seal.
CREATE FUNCTION sessions.session_neon_census_immutable()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $census_immutable$
DECLARE
  v_sealed text := (SELECT generation FROM sessions.session_neon_census_seal WHERE singleton);
BEGIN
  IF TG_OP = 'TRUNCATE' THEN
    IF v_sealed IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'neon census: a sealed census is immutable';
    END IF;
    RETURN NULL;
  END IF;
  IF (TG_OP <> 'DELETE' AND NEW.generation = v_sealed) OR (TG_OP <> 'INSERT' AND OLD.generation = v_sealed) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'neon census: a sealed census is immutable';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$census_immutable$;
CREATE TRIGGER session_neon_census_immutable BEFORE INSERT OR UPDATE OR DELETE ON sessions.session_neon_census
  FOR EACH ROW EXECUTE FUNCTION sessions.session_neon_census_immutable();
CREATE TRIGGER session_neon_census_no_truncate BEFORE TRUNCATE ON sessions.session_neon_census
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.session_neon_census_immutable();

-- The loader's write path (owner-only; scripts/neon-census-load.ts --execute). Idempotent:
-- one batch of [org, workspace, user, source, session] rows into an UNSEALED generation,
-- ON CONFLICT DO NOTHING, so a rerun of the same generation resumes. Returns rows inserted.
CREATE FUNCTION sessions.session_neon_census_load(p_generation text, p_rows jsonb)
RETURNS integer LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $census_load$
DECLARE
  v_n integer;
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY[]::text[]);
  IF p_generation IS NULL OR p_generation !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'neon census: generation must be 64 hex';
  END IF;
  IF p_generation = (SELECT generation FROM sessions.session_neon_census_seal WHERE singleton) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'neon census: a sealed census is immutable';
  END IF;
  IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' OR jsonb_array_length(p_rows) > 10000 OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_rows) AS e(v)
     WHERE jsonb_typeof(e.v) <> 'array' OR jsonb_array_length(e.v) <> 5
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(e.v) AS x(f) WHERE jsonb_typeof(x.f) <> 'string')
        OR coalesce(e.v ->> 0, '') = '' OR coalesce(e.v ->> 4, '') = '') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'neon census: rows must be [org, workspace, user, source, session] strings';
  END IF;
  INSERT INTO sessions.session_neon_census (generation, org_id, workspace_id, user_id, source, session_id)
  SELECT p_generation, e.v ->> 0, e.v ->> 1, e.v ->> 2, e.v ->> 3, e.v ->> 4 FROM jsonb_array_elements(p_rows) AS e(v)
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END
$census_load$;

-- The census digest of one generation, recomputed from its rows (owner-only). Tenant-keyed:
-- every row's (org, workspace, user, source, session) as a length-prefixed line
-- (octet_length:value joined by '|'), each line's sha256 truncated to 60 bits, summed
-- (order-independent, streamable on the Neon side), then
-- sha256('neon-census-v1:' || rows || ':' || sum). src/migration/neon-census.ts computes the
-- identical value from Neon.
CREATE FUNCTION sessions.session_neon_census_digest(p_generation text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $census_digest$
DECLARE
  v_rows bigint;
  v_sum numeric;
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY[]::text[]);
  SELECT count(*), coalesce(sum(('x' || substr(encode(sha256(convert_to(
           octet_length(c.org_id) || ':' || c.org_id || '|' || octet_length(c.workspace_id) || ':' || c.workspace_id || '|' ||
           octet_length(c.user_id) || ':' || c.user_id || '|' || octet_length(c.source) || ':' || c.source || '|' ||
           octet_length(c.session_id) || ':' || c.session_id, 'UTF8')), 'hex'), 1, 15))::bit(60)::bigint::numeric), 0)
    INTO v_rows, v_sum
    FROM sessions.session_neon_census AS c WHERE c.generation = p_generation;
  RETURN jsonb_build_object('rows', v_rows,
    'sha256', encode(sha256(convert_to('neon-census-v1:' || v_rows || ':' || v_sum::text, 'UTF8')), 'hex'));
END
$census_digest$;

-- Seal a fully loaded generation (owner-only). The seal RECOMPUTES the generation's row count
-- and digest from its rows and refuses unless both equal what the loader computed from Neon
-- (p_rows, p_sha256); only then does it flip, atomically, and every other generation's rows
-- are dropped. Since a sealed generation is immutable (trigger above), its stored digest keeps
-- matching its rows, and neon_free admits only against that sealed generation: a partial or
-- in-progress load (unsealed) never admits. p_snapshot records the Neon snapshot identity
-- (snapshot, LSN, read time), the source identity and per-table counts. Sealing the generation
-- already in force with the same count and digest is a no-op (resumable); anything else for it
-- is refused.
CREATE FUNCTION sessions.session_neon_census_seal(p_generation text, p_rows bigint, p_sha256 text, p_snapshot jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $census_seal$
DECLARE
  v_seal sessions.session_neon_census_seal%ROWTYPE;
  v_have bigint;
  v_digest jsonb;
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY[]::text[]);
  IF p_generation IS NULL OR p_generation !~ '^[0-9a-f]{64}$' OR p_sha256 IS NULL OR p_sha256 !~ '^[0-9a-f]{64}$'
     OR p_rows IS NULL OR p_rows < 0 OR jsonb_typeof(p_snapshot) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'neon census: invalid seal arguments';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('session-neon-census-seal', 0));
  SELECT * INTO v_seal FROM sessions.session_neon_census_seal WHERE singleton FOR UPDATE;
  IF v_seal.generation = p_generation THEN
    IF v_seal.census_rows = p_rows AND v_seal.census_sha256 = p_sha256 THEN
      RETURN jsonb_build_object('sealed', true, 'already', true);
    END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'neon census: a sealed census is immutable';
  END IF;
  IF NOT (p_snapshot ? 'snapshot' AND p_snapshot ? 'read_at' AND p_snapshot ? 'source_identity') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'neon census: the seal needs the Neon snapshot and source identity';
  END IF;
  v_digest := sessions.session_neon_census_digest(p_generation);
  v_have := (v_digest ->> 'rows')::bigint;
  IF v_have <> p_rows THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('neon census: generation holds %s rows, expected %s', v_have, p_rows);
  END IF;
  IF v_digest ->> 'sha256' <> p_sha256 THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'neon census: the recomputed digest does not match';
  END IF;
  INSERT INTO sessions.session_neon_census_seal (singleton, generation, census_rows, census_sha256, neon_snapshot)
  VALUES (true, p_generation, p_rows, p_sha256, p_snapshot)
  ON CONFLICT (singleton) DO UPDATE SET generation = excluded.generation, census_rows = excluded.census_rows,
    census_sha256 = excluded.census_sha256, neon_snapshot = excluded.neon_snapshot, sealed_at = clock_timestamp();
  DELETE FROM sessions.session_neon_census WHERE generation <> p_generation;
  RETURN jsonb_build_object('sealed', true, 'already', false);
END
$census_seal$;

-- Proof that a session never had Neon evidence, from sessions-pipeline's own records, so
-- its analysis is complete from S3 without the legacy export's coverage marker. True only
-- when ALL hold (any doubt is false, and false keeps the marker requirement):
--   1. the frozen Neon copy is sealed (20261105010000 ingest_frozen_source_completeness);
--   2. the sealed copy holds no delivery record for the session id in the org (any
--      workspace, physical or canonical id): it was never a Neon delivery session;
--   3. it has a live pointer, and every pointer (live or tombstoned) was created after the
--      seal (a necessary condition only: created_at is the SP insert time, not the session's age);
--   4. the Neon census (above) is sealed and holds no row for the session id in the org (any
--      workspace, physical or canonical id): this is the authoritative exclusion of an old
--      turns-only or not-yet-exported Neon session whose first SP pointer lands after the seal.
-- The worker additionally requires that no manifest carries a Neon export/backfill marker
-- (src/session-analysis/worker.ts), which catches a turns-only Neon session copied later.
-- Index for (2) on large frozen copies: supabase/pending/20261105046000_frozen_records_session_index.sql.
-- SECURITY DEFINER: the frozen custody tables stay owner-only (no SELECT for the runtime or
-- read role); this returns one boolean.
-- Caller pin for this lane's SECURITY DEFINER functions (the ingest_require_role pattern):
-- the session's effective role (SET ROLE, else the login; a definer does not change it) must
-- be one of p_roles, or the migration owner (operators, tests). The entry '<runtime>' names the
-- migrator's configured runtime role (HOOPDB_RUNTIME_ROLE), read from the role literal the
-- migrator writes into the verified function-boundary trigger (the bootstrap reads it the same
-- way). Without that trigger (the local/Hoop profile, which has no Supabase bootstrap) it names
-- a role holding an explicit EXECUTE grant on session_analysis_source_revision: only the
-- migrator grants that (RUNTIME_APPLICATION_FUNCTIONS) and its ACL drift check refuses any
-- other grantee. Not SECURITY DEFINER; owner-only.
CREATE FUNCTION sessions.session_analysis_require_caller(p_roles text[])
RETURNS void LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp AS $require_caller$
DECLARE
  v_caller text := coalesce(nullif(current_setting('role', true), 'none'), session_user::text);
  v_runtime text;
BEGIN
  IF '<runtime>' = ANY (p_roles) THEN
    IF to_regprocedure('hoopdb_boundary.sessions_function_boundary()') IS NOT NULL THEN
      SELECT substring(p.prosrc FROM 'v_runtime text := ''([a-z0-9_]+)'';') INTO v_runtime
        FROM pg_proc AS p WHERE p.oid = to_regprocedure('hoopdb_boundary.sessions_function_boundary()');
    ELSIF EXISTS (
      SELECT 1 FROM pg_proc AS p
       CROSS JOIN LATERAL aclexplode(p.proacl) AS a
       JOIN pg_roles AS g ON g.oid = a.grantee
      WHERE p.oid = 'sessions.session_analysis_source_revision(text,text,text)'::regprocedure
        AND a.privilege_type = 'EXECUTE' AND g.rolname = v_caller
        AND g.rolname NOT IN ('anon', 'authenticated', 'bulls96_read_api', 'bulls96_analysis_admit')) THEN
      v_runtime := v_caller;
    END IF;
  END IF;
  IF v_caller = ANY (p_roles) OR v_caller = v_runtime OR v_caller = (
    SELECT pg_get_userbyid(p.proowner) FROM pg_proc AS p
     WHERE p.oid = 'sessions.session_analysis_require_caller(text[])'::regprocedure) THEN
    RETURN;
  END IF;
  RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session analysis: caller is not a role this function serves';
END
$require_caller$;

-- Callers: the worker (service_role or the configured runtime role, directly and through
-- session_analysis_source_revision),
-- admission (bulls96_analysis_admit, through analysis_request_v1) and the read role (through
-- session_neon_free_for when present).
CREATE FUNCTION sessions.session_analysis_neon_free(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $neon_free$
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>', 'bulls96_analysis_admit', 'bulls96_read_api']);
  RETURN (
  WITH seal AS (SELECT sealed_at FROM sessions.ingest_frozen_source_completeness WHERE singleton),
  ptr AS (
    SELECT p.session_id, p.created_at, p.deleted_at FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
  )
  SELECT EXISTS (SELECT 1 FROM seal)
     AND EXISTS (SELECT 1 FROM ptr WHERE ptr.deleted_at IS NULL)
     AND NOT EXISTS (SELECT 1 FROM ptr, seal WHERE ptr.created_at <= seal.sealed_at)
     AND NOT EXISTS (
       SELECT 1 FROM sessions.ingest_frozen_source_records AS r
        WHERE r.org_id = p_org_id
          AND (r.session_id = p_session_id OR r.session_id IN (SELECT ptr.session_id FROM ptr)))
     AND EXISTS (SELECT 1 FROM sessions.session_neon_census_seal WHERE singleton)
     AND NOT EXISTS (
       SELECT 1 FROM sessions.session_neon_census AS c
        WHERE c.generation = (SELECT generation FROM sessions.session_neon_census_seal WHERE singleton)
          AND c.org_id = p_org_id
          AND (c.session_id = p_session_id OR c.session_id IN (SELECT ptr.session_id FROM ptr))));
END
$neon_free$;

-- The same proof for the read API's turns/metadata routes (src/shared/never-in-neon.ts), as
-- bulls96_read_api: the org is the transaction's bound tenant (session_directory_read_api_scope),
-- never a caller argument. SECURITY DEFINER only to read the frozen copy and seal, which the read
-- role cannot; it returns one boolean.
CREATE FUNCTION sessions.session_neon_free_for(p_workspace_id text, p_session_id text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $neon_free_for$
DECLARE
  v_org text := sessions.session_directory_read_api_scope(p_workspace_id);
BEGIN
  IF p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 THEN RETURN false; END IF;
  RETURN sessions.session_analysis_neon_free(v_org, p_workspace_id, p_session_id);
END
$neon_free_for$;

-- The source revision of a session: the live pointer set as the read API matches it
-- (canonical id, else raw id), signed over every field canonicalSourceGeneration
-- (src/session-read-api/canonical.ts) binds: the tenant and canonical session id, and per
-- pointer the harness, canonical and physical machine id, user id, storage provider,
-- bucket, manifest key and etag, revision, raw content digest and raw format; PLUS every
-- input of the never-in-Neon proof (session_analysis_neon_free above): each pointer's
-- created_at and deleted_at, tombstoned ones included, the frozen copy's seal (sealed_at and
-- manifest digest), whether a frozen delivery record names the session, the census seal (sealed_at
-- and digest), whether the census names the session, and the proof's verdict itself. (Export and
-- backfill markers live in the manifest, so its etag carries them.) And a C3 purge fence's
-- purged_at, so a purge refuses every in-flight reserve and completion. Any
-- change that can change the evidence or the proof moves it, so neither a reuse nor a
-- publication can outlive it. An unambiguous tuple (a JSON array; NULL kept), never a
-- separator join. NULL when no pointer is live.
-- SECURITY DEFINER (pinned like the proof) to read the owner-only proof inputs; returns one hash.
CREATE FUNCTION sessions.session_analysis_source_revision(p_org_id text, p_workspace_id text, p_session_id text)
RETURNS text LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $revision$
BEGIN
  PERFORM sessions.session_analysis_require_caller(ARRAY['service_role', '<runtime>', 'bulls96_analysis_admit']);
  RETURN (
  WITH ids AS (
    SELECT DISTINCT p.session_id FROM sessions.session_index AS p
     WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
       AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
    UNION SELECT p_session_id)
  SELECT CASE WHEN count(*) FILTER (WHERE p.deleted_at IS NULL) = 0 THEN NULL ELSE encode(sha256(convert_to(jsonb_build_array(
           p_org_id, p_workspace_id, p_session_id,
           sessions.session_analysis_neon_free(p_org_id, p_workspace_id, p_session_id),
           (SELECT jsonb_build_array(c.sealed_at, c.manifest_sha256) FROM sessions.ingest_frozen_source_completeness AS c WHERE c.singleton),
           EXISTS (SELECT 1 FROM sessions.ingest_frozen_source_records AS r WHERE r.org_id = p_org_id AND r.session_id IN (SELECT session_id FROM ids)),
           (SELECT jsonb_build_array(c.generation, c.sealed_at, c.census_sha256) FROM sessions.session_neon_census_seal AS c WHERE c.singleton),
           EXISTS (SELECT 1 FROM sessions.session_neon_census AS c
                    WHERE c.generation = (SELECT generation FROM sessions.session_neon_census_seal WHERE singleton)
                      AND c.org_id = p_org_id AND c.session_id IN (SELECT session_id FROM ids)),
           (SELECT f.purged_at FROM sessions.session_analysis_purge_fences AS f
             WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id AND f.session_id = p_session_id),
           jsonb_agg(jsonb_build_array(
             p.harness, coalesce(p.canonical_session_id, p.session_id),
             coalesce(p.canonical_machine_id, p.machine_id), p.machine_id, p.user_id,
             p.storage_provider, p.bucket, p.manifest_key, p.manifest_etag, p.revision,
             encode(p.content_sha256, 'hex'), p.raw_format, p.created_at, p.deleted_at)
             ORDER BY p.manifest_key, p.harness, p.machine_id, p.created_at))::text, 'UTF8')), 'hex') END
    FROM sessions.session_index AS p
   WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
     AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id));
END
$revision$;

-- The admission principal's tenant pin (session_directory_read_api_scope's pattern):
-- refuses 42501 unless the caller is bulls96_analysis_admit and the transaction's bound
-- tenant (the verified assertion) is exactly (p_org_id, p_workspace_id).
CREATE FUNCTION sessions.session_analysis_admit_scope(p_org_id text, p_workspace_id text)
RETURNS void LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $scope$
DECLARE
  v_org text := nullif(current_setting('bulls96_read.org_id', true), '');
  v_workspace text := nullif(current_setting('bulls96_read.workspace_id', true), '');
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'bulls96_analysis_admit' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session analysis: caller is not the admission role';
  END IF;
  IF v_org IS NULL OR v_workspace IS NULL
     OR p_org_id IS DISTINCT FROM v_org OR p_workspace_id IS DISTINCT FROM v_workspace THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session analysis: tenant binding missing or not this tenant';
  END IF;
END
$scope$;

-- Erasure: a tombstoned or deleted pointer or directory row forgets the session's analysis
-- rows, as session_briefs_forget does, and tickets every result object it orphans so the
-- sweeper deletes it even if the session's object erasure already ran.
CREATE FUNCTION sessions.session_analysis_v2_forget()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $forget$
DECLARE
  -- A request names the session as the read API does (canonical id, else raw id).
  v_ids text[] := array_remove(ARRAY[OLD.session_id, to_jsonb(OLD) ->> 'canonical_session_id'], NULL);
BEGIN
  INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason)
  SELECT j.result_s3_key, j.id, j.org_id, j.workspace_id, j.session_id, 'forgotten'
    FROM sessions.session_analysis_jobs_v2 AS j
   WHERE j.org_id = OLD.org_id AND j.workspace_id = OLD.workspace_id AND j.session_id = ANY (v_ids)
     AND j.result_s3_key IS NOT NULL
  ON CONFLICT (result_key) DO UPDATE SET reason = 'forgotten';
  DELETE FROM sessions.session_analysis_requests_v2 AS r
   WHERE r.org_id = OLD.org_id AND r.workspace_id = OLD.workspace_id AND r.session_id = ANY (v_ids);
  DELETE FROM sessions.session_analysis_jobs_v2 AS j
   WHERE j.org_id = OLD.org_id AND j.workspace_id = OLD.workspace_id AND j.session_id = ANY (v_ids);
  RETURN NULL;
END
$forget$;

CREATE TRIGGER session_analysis_v2_forget_tombstoned_pointer
  AFTER UPDATE OF deleted_at ON sessions.session_index
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_analysis_v2_forget();
CREATE TRIGGER session_analysis_v2_forget_deleted_pointer
  AFTER DELETE ON sessions.session_index
  FOR EACH ROW EXECUTE FUNCTION sessions.session_analysis_v2_forget();
CREATE TRIGGER session_analysis_v2_forget_tombstoned_directory
  AFTER UPDATE OF deleted_at ON sessions.session_directory
  FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
  EXECUTE FUNCTION sessions.session_analysis_v2_forget();
CREATE TRIGGER session_analysis_v2_forget_deleted_directory
  AFTER DELETE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_analysis_v2_forget();

-- C3 privacy purge for Neon-era sessions. Exported/backfilled sessions have no publication
-- head in SP, so a reset or disable tombstones no pointer and the forget triggers never fire
-- for them. Lane A's authority (041000) calls these, guarded, BEFORE it deletes the owned rows:
--   purge_session_analysis_for_owned(org, user)          reset: the user's whole owned set
--   purge_session_analysis_for_owned(org, user, sessions) disable: only the listed
--     [{"source": ..., "session_id": ...}] that are in the user's owned set
-- The owned set is ingest_frozen_source_owned_sessions UNION the user's frozen delivery records
-- with a session (exactly the set the reset fences). Rows are matched on the owned row's org,
-- workspace and session id, never on a bare session id: another user's or workspace's session
-- with the same id is untouched. Like the forget trigger, every result key is ticketed
-- 'forgotten' for the sweeper's every-version erase before the rows are deleted. A no-op (0)
-- when lane A's tables are absent. Returns the analysis jobs purged. Owner-only.
CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text, p_user text, p_sessions jsonb)
RETURNS integer LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $purge_owned$
DECLARE
  v_jobs integer;
BEGIN
  -- Caller pin: lane A's privacy authority (bulls96_session_ingest, inside its begin, which
  -- sets bulls96.privacy_authority_writer) or the migration owner (operator).
  PERFORM sessions.session_analysis_require_caller(ARRAY['bulls96_session_ingest']);
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text) = 'bulls96_session_ingest'
     AND coalesce(current_setting('bulls96.privacy_authority_writer', true), '') <> 'authority' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'purge_session_analysis_for_owned: outside the privacy authority';
  END IF;
  IF p_org IS NULL OR btrim(p_org) = '' OR p_user IS NULL OR btrim(p_user) = '' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'purge_session_analysis_for_owned: org and user are required';
  END IF;
  IF p_sessions IS NOT NULL AND jsonb_typeof(p_sessions) <> 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'purge_session_analysis_for_owned: sessions must be a json array';
  END IF;
  IF p_sessions IS NOT NULL AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_sessions) AS e(v)
     WHERE jsonb_typeof(e.v) <> 'object' OR coalesce(e.v ->> 'source', '') = '' OR coalesce(e.v ->> 'session_id', '') = ''
  ) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'purge_session_analysis_for_owned: each session needs source and session_id';
  END IF;
  IF to_regclass('sessions.ingest_frozen_source_owned_sessions') IS NULL THEN
    RETURN 0;
  END IF;
  CREATE TEMP TABLE IF NOT EXISTS pg_temp.analysis_purge_owned (org_id text, workspace_id text, session_id text) ON COMMIT DROP;
  TRUNCATE pg_temp.analysis_purge_owned;
  INSERT INTO pg_temp.analysis_purge_owned
  SELECT DISTINCT o.org_id, o.workspace_id, o.session_id
    FROM sessions.ingest_frozen_source_owned_sessions AS o
   WHERE o.org_id = p_org AND o.user_id = p_user
     AND (p_sessions IS NULL OR EXISTS (
       SELECT 1 FROM jsonb_array_elements(p_sessions) AS e(v)
        WHERE e.v ->> 'source' = o.source AND e.v ->> 'session_id' = o.session_id));
  IF to_regclass('sessions.ingest_frozen_source_records') IS NOT NULL THEN
    EXECUTE $records$
      INSERT INTO pg_temp.analysis_purge_owned
      SELECT DISTINCT r.org_id, r.workspace_id, r.session_id
        FROM sessions.ingest_frozen_source_records AS r
       WHERE r.org_id = $1 AND r.user_id = $2 AND r.session_id IS NOT NULL
         AND ($3::jsonb IS NULL OR EXISTS (
           SELECT 1 FROM jsonb_array_elements($3::jsonb) AS e(v)
            WHERE e.v ->> 'source' = r.source AND e.v ->> 'session_id' = r.session_id))
    $records$ USING p_org, p_user, p_sessions;
  END IF;
  -- Fence first (same transaction): reads hide, admission refuses and in-flight jobs cannot
  -- publish from here on, before any object is gone.
  INSERT INTO sessions.session_analysis_purge_fences (org_id, workspace_id, session_id)
  SELECT DISTINCT o.org_id, o.workspace_id, o.session_id FROM pg_temp.analysis_purge_owned AS o
  ON CONFLICT (org_id, workspace_id, session_id) DO UPDATE SET purged_at = clock_timestamp();
  INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason)
  SELECT j.result_s3_key, j.id, j.org_id, j.workspace_id, j.session_id, 'forgotten'
    FROM sessions.session_analysis_jobs_v2 AS j
    JOIN (SELECT DISTINCT * FROM pg_temp.analysis_purge_owned) AS o
      ON j.org_id = o.org_id AND j.workspace_id = o.workspace_id AND j.session_id = o.session_id
   WHERE j.result_s3_key IS NOT NULL
  ON CONFLICT (result_key) DO UPDATE SET reason = 'forgotten', lease_expires_at = NULL;
  -- In-flight publications of these sessions (reserved, not completed): forgotten too, so no
  -- writer can take them over and the sweeper erases whatever their bounded PUT left.
  UPDATE sessions.session_analysis_result_tickets AS t SET reason = 'forgotten', lease_expires_at = NULL
    FROM (SELECT DISTINCT * FROM pg_temp.analysis_purge_owned) AS o
   WHERE t.org_id = o.org_id AND t.workspace_id = o.workspace_id AND t.session_id = o.session_id;
  DELETE FROM sessions.session_analysis_requests_v2 AS r
   USING (SELECT DISTINCT * FROM pg_temp.analysis_purge_owned) AS o
   WHERE r.org_id = o.org_id AND r.workspace_id = o.workspace_id AND r.session_id = o.session_id;
  DELETE FROM sessions.session_analysis_jobs_v2 AS j
   USING (SELECT DISTINCT * FROM pg_temp.analysis_purge_owned) AS o
   WHERE j.org_id = o.org_id AND j.workspace_id = o.workspace_id AND j.session_id = o.session_id;
  GET DIAGNOSTICS v_jobs = ROW_COUNT;
  TRUNCATE pg_temp.analysis_purge_owned;
  RETURN v_jobs;
END
$purge_owned$;

CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text, p_user text)
RETURNS integer LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $purge_owned_all$
  SELECT sessions.purge_session_analysis_for_owned(p_org, p_user, NULL::jsonb);
$purge_owned_all$;

-- relayhistory request_session_analysis_v1 (0062:413) over the v2 tables. Same outcome
-- enum and request object: accepted {request} | not_found | rate_limited | invalid_request.
-- The engine identity below is src/session-analysis/versions.ts (a test pins them equal).
CREATE FUNCTION sessions.analysis_request_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request$
DECLARE
  c_analysis_version constant text := 'session-analysis-v2.4:96bulls-s3-v1';
  c_schema_version constant integer := 2;
  c_model_version constant text := 'gpt-5.4';
  c_summary_version constant text := 'session-summary-v3:workspace-output-v2';
  c_summary_model_version constant text := 'gpt-5.6-luna';
  v_revision text;
  v_day timestamptz := date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
  v_latest sessions.session_analysis_requests_v2%ROWTYPE;
  v_request sessions.session_analysis_requests_v2%ROWTYPE;
  v_job sessions.session_analysis_jobs_v2%ROWTYPE;
  v_job_id uuid;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  PERFORM sessions.session_analysis_admit_scope(p_org_id, p_workspace_id);
  -- Serializes admission per org, as relayhistory does; the evidence check follows it.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  IF NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                  WHERE p.org_id = p_org_id AND p.workspace_id = p_workspace_id
                    AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
                    AND p.deleted_at IS NULL) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  -- Purged by a C3 reset/disable and not re-uploaded since: as if erased.
  IF EXISTS (SELECT 1 FROM sessions.session_analysis_purge_fences AS f
                  WHERE f.org_id = p_org_id AND f.workspace_id = p_workspace_id AND f.session_id = p_session_id
                    AND NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                                     WHERE p.org_id = f.org_id AND p.workspace_id = f.workspace_id
                                       AND (coalesce(p.canonical_session_id, p.session_id) = f.session_id OR p.session_id = f.session_id)
                                       AND p.deleted_at IS NULL AND p.created_at > f.purged_at)) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  SELECT * INTO v_latest FROM sessions.session_analysis_requests_v2
   WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;

  -- An analysis already queued or running for this session answers the request.
  SELECT r.* INTO v_request FROM sessions.session_analysis_requests_v2 AS r
    JOIN sessions.session_analysis_jobs_v2 AS j ON j.id = r.analysis_job_id
   WHERE r.org_id = p_org_id AND r.workspace_id = p_workspace_id AND r.session_id = p_session_id
     AND j.origin = 'native' AND j.status IN ('queued', 'running')
   ORDER BY r.created_at DESC, r.id DESC LIMIT 1;
  IF v_request.id IS NULL THEN
    -- Throttled (a request in the last 2 minutes): the session's latest request.
    IF v_latest.id IS NOT NULL AND v_latest.created_at > now() - interval '2 minutes' THEN
      v_request := v_latest;
    END IF;
  END IF;
  IF v_request.id IS NOT NULL THEN
    RETURN jsonb_build_object('outcome', 'accepted',
      'request', sessions.analysis_request_json_v1(v_request));
  END IF;

  IF (SELECT count(*) FROM sessions.session_analysis_requests_v2
       WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND session_id = p_session_id
         AND created_at >= v_day) >= 30
     OR (SELECT count(DISTINCT (workspace_id, session_id)) FROM sessions.session_analysis_requests_v2
          WHERE org_id = p_org_id AND origin = 'native' AND created_at >= v_day
            AND NOT (workspace_id = p_workspace_id AND session_id = p_session_id)) >= 20 THEN
    RETURN jsonb_build_object('outcome', 'rate_limited');
  END IF;

  -- A native analysis of this engine completed in the last 30 minutes over the SAME live
  -- pointer set is reused (relayhistory reused by source fingerprint; this is its revision).
  v_revision := sessions.session_analysis_source_revision(p_org_id, p_workspace_id, p_session_id);
  SELECT id INTO v_job_id FROM sessions.session_analysis_jobs_v2
   WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND session_id = p_session_id
     AND origin = 'native' AND status = 'completed'
     AND source_revision IS NOT DISTINCT FROM v_revision AND v_revision IS NOT NULL
     AND analysis_version = c_analysis_version AND schema_version = c_schema_version
     AND model_version = c_model_version AND summary_version = c_summary_version
     AND summary_model_version = c_summary_model_version
     AND completed_at > now() - interval '30 minutes'
   ORDER BY completed_at DESC, id DESC LIMIT 1;
  IF v_job_id IS NULL THEN
    INSERT INTO sessions.session_analysis_jobs_v2
      (org_id, workspace_id, session_id, analysis_version, schema_version, model_version,
       summary_version, summary_model_version)
    VALUES (p_org_id, p_workspace_id, p_session_id, c_analysis_version, c_schema_version, c_model_version,
            c_summary_version, c_summary_model_version)
    RETURNING id INTO v_job_id;
  END IF;
  INSERT INTO sessions.session_analysis_requests_v2
    (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id)
  VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, 'resolved', v_job_id)
  RETURNING * INTO v_request;
  RETURN jsonb_build_object('outcome', 'accepted',
    'request', sessions.analysis_request_json_v1(v_request));
END
$request$;

-- One request as relayhistory's v1 request object (sessionAnalysisRequest), with the
-- public status/error/job id of the 0031 projection. Owner-only; called by the definer
-- functions as their owner.
CREATE FUNCTION sessions.analysis_request_json_v1(r sessions.session_analysis_requests_v2)
RETURNS jsonb LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $json$
  SELECT CASE WHEN r.origin = 'neon_backfill' THEN
      jsonb_build_object('id', r.id, 'sessionId', r.session_id, 'status', r.status,
        'analysisId', r.v1_job_id, 'error', r.last_error,
        'createdAt', r.created_at, 'updatedAt', r.updated_at)
    ELSE
      jsonb_build_object('id', r.id, 'sessionId', r.session_id,
        'status', COALESCE(j.status, 'failed'),
        'analysisId', j.id,
        'error', CASE WHEN j.id IS NULL THEN 'analysis_unavailable'
                      WHEN j.status = 'failed' THEN j.last_error END,
        'createdAt', r.created_at, 'updatedAt', GREATEST(r.updated_at, j.updated_at))
    END
    FROM (SELECT 1) AS one
    LEFT JOIN sessions.session_analysis_jobs_v2 AS j
      ON j.id = r.analysis_job_id AND j.org_id = r.org_id
     AND j.workspace_id = r.workspace_id AND j.session_id = r.session_id;
$json$;

-- relayhistory read_session_analysis_v1 (0024:537 over the 0031 projection) for the read
-- Worker. `analyses[].result` is replaced by `resultRef` {key, sha256, bytes} plus `origin`
-- and `harness`; the Worker reads and verifies the object and assembles the v1 `result`
-- (with the brief from session_briefs for native rows). 50-request window, as relayhistory.
CREATE FUNCTION sessions.analysis_read_v1(p_workspace_id text, p_session_id text)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $read$
DECLARE
  v_org text := sessions.session_directory_read_api_scope(p_workspace_id);
  v_requests jsonb;
  v_analyses jsonb;
BEGIN
  IF p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                  WHERE p.org_id = v_org AND p.workspace_id = p_workspace_id
                    AND (coalesce(p.canonical_session_id, p.session_id) = p_session_id OR p.session_id = p_session_id)
                    AND p.deleted_at IS NULL)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_requests_v2
                      WHERE org_id = v_org AND workspace_id = p_workspace_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  IF EXISTS (SELECT 1 FROM sessions.session_analysis_purge_fences AS f
                  WHERE f.org_id = v_org AND f.workspace_id = p_workspace_id AND f.session_id = p_session_id
                    AND NOT EXISTS (SELECT 1 FROM sessions.session_index AS p
                                     WHERE p.org_id = f.org_id AND p.workspace_id = f.workspace_id
                                       AND (coalesce(p.canonical_session_id, p.session_id) = f.session_id OR p.session_id = f.session_id)
                                       AND p.deleted_at IS NULL AND p.created_at > f.purged_at)) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  WITH recent AS (
    SELECT r FROM sessions.session_analysis_requests_v2 AS r
     WHERE r.org_id = v_org AND r.workspace_id = p_workspace_id AND r.session_id = p_session_id
     ORDER BY r.created_at DESC, r.id DESC LIMIT 50
  )
  SELECT COALESCE(jsonb_agg(sessions.analysis_request_json_v1((recent.r)::sessions.session_analysis_requests_v2)
           ORDER BY (recent.r).created_at DESC, (recent.r).id DESC), '[]'::jsonb)
    INTO v_requests FROM recent;

  WITH recent AS (
    SELECT * FROM sessions.session_analysis_requests_v2 AS r
     WHERE r.org_id = v_org AND r.workspace_id = p_workspace_id AND r.session_id = p_session_id
     ORDER BY r.created_at DESC, r.id DESC LIMIT 50
  ), linked AS (
    SELECT DISTINCT ON (j.id) j.*, COALESCE(recent.v1_job_id, j.id) AS public_id
      FROM recent JOIN sessions.session_analysis_jobs_v2 AS j
        ON j.id = recent.analysis_job_id AND j.org_id = recent.org_id
       AND j.workspace_id = recent.workspace_id AND j.session_id = recent.session_id
     ORDER BY j.id, recent.created_at DESC, recent.id DESC
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', l.public_id, 'sessionId', l.session_id, 'sourceProvider', COALESCE(l.harness, ''),
      'sourceFingerprint', COALESCE(l.source_fingerprint, ''), 'status', l.status,
      'attempts', l.attempts, 'analysisVersion', l.analysis_version,
      'schemaVersion', l.schema_version, 'summaryVersion', l.summary_version,
      'modelVersion', l.model_version, 'summaryModelVersion', l.summary_model_version,
      'resultRef', CASE WHEN l.result_s3_key IS NULL THEN NULL ELSE jsonb_build_object(
        'key', l.result_s3_key, 'sha256', l.result_sha256, 'bytes', l.result_bytes) END,
      'origin', l.origin, 'harness', l.harness,
      'error', CASE WHEN l.status = 'failed' THEN l.last_error END,
      'createdAt', l.created_at, 'updatedAt', l.updated_at, 'completedAt', l.completed_at
    ) ORDER BY l.created_at DESC, l.public_id DESC), '[]'::jsonb)
    INTO v_analyses FROM linked AS l;

  RETURN jsonb_build_object('outcome', 'ok', 'sessionId', p_session_id,
    'requests', v_requests, 'analyses', v_analyses);
END
$read$;

REVOKE ALL ON FUNCTION sessions.session_analysis_v2_forget() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_require_caller(text[]) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_neon_census_immutable() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_neon_census_load(text, jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_neon_census_digest(text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_neon_census_seal(text, bigint, text, jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.purge_session_analysis_for_owned(text, text, jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.purge_session_analysis_for_owned(text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_admit_scope(text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_source_revision(text, text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.session_analysis_neon_free(text, text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_analysis_neon_free(text, text, text) TO service_role;
REVOKE ALL ON FUNCTION sessions.session_neon_free_for(text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.session_neon_free_for(text, text) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.session_analysis_source_revision(text, text, text) TO service_role;
REVOKE ALL ON FUNCTION sessions.analysis_request_v1(text, text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.analysis_read_v1(text, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.analysis_read_v1(text, text) TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.analysis_request_v1(text, text, text, text) TO bulls96_analysis_admit;

-- As 034000: strip EXECUTE from every grantee present at creation (default privileges),
-- keeping only bulls96_read_api on analysis_read_v1.
DO $revoke$
DECLARE
  v_fn regprocedure;
  v_grantee text;
BEGIN
  FOREACH v_fn IN ARRAY ARRAY[
    'sessions.session_analysis_v2_forget()'::regprocedure,
    'sessions.session_analysis_require_caller(text[])'::regprocedure,
    'sessions.session_neon_census_immutable()'::regprocedure,
    'sessions.session_neon_census_load(text, jsonb)'::regprocedure,
    'sessions.session_neon_census_digest(text)'::regprocedure,
    'sessions.session_neon_census_seal(text, bigint, text, jsonb)'::regprocedure,
    'sessions.purge_session_analysis_for_owned(text, text, jsonb)'::regprocedure,
    'sessions.purge_session_analysis_for_owned(text, text)'::regprocedure,
    'sessions.session_analysis_admit_scope(text, text)'::regprocedure,
    'sessions.session_analysis_source_revision(text, text, text)'::regprocedure,
    'sessions.session_analysis_neon_free(text, text, text)'::regprocedure,
    'sessions.session_neon_free_for(text, text)'::regprocedure,
    'sessions.analysis_request_v1(text, text, text, text)'::regprocedure,
    'sessions.analysis_request_json_v1(sessions.session_analysis_requests_v2)'::regprocedure,
    'sessions.analysis_read_v1(text, text)'::regprocedure
  ] LOOP
    FOR v_grantee IN
      SELECT r.rolname
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
        JOIN pg_roles AS r ON r.oid = a.grantee
       WHERE p.oid = v_fn AND a.privilege_type = 'EXECUTE' AND a.grantee <> p.proowner
         AND NOT (r.rolname = 'bulls96_read_api' AND v_fn = 'sessions.analysis_read_v1(text, text)'::regprocedure)
         AND NOT (r.rolname = 'bulls96_analysis_admit' AND v_fn = 'sessions.analysis_request_v1(text, text, text, text)'::regprocedure)
         AND NOT (r.rolname = 'service_role' AND v_fn = 'sessions.session_analysis_source_revision(text, text, text)'::regprocedure)
         AND NOT (r.rolname = 'service_role' AND v_fn = 'sessions.session_analysis_neon_free(text, text, text)'::regprocedure)
         AND NOT (r.rolname = 'bulls96_read_api' AND v_fn = 'sessions.session_neon_free_for(text, text)'::regprocedure)
    LOOP
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', v_fn, v_grantee);
    END LOOP;
  END LOOP;
END
$revoke$;
