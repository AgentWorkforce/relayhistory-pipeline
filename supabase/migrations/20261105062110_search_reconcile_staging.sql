-- Staged reconcile for the self-host search indexer (Phase 3 large-session search, slice 2a;
-- registry slot 062110). The database half of src/session-pipeline/staged-reconcile.ts: a
-- streaming reconcile keeps its diff state here instead of in memory, commits it in batches and
-- resumes after a crash. Behind SEARCH_INDEX_STAGED_RECONCILE_ENABLED (default off); in this
-- slice no runtime path calls these functions.
--
--   1. sessions.search_reconcile_runs_v1: one row per scope (org, workspace, harness, session).
--      The source stamp it was started from (canonical id, directory pointer epoch, admitted
--      manifest fingerprint), its phase ('want' -> 'have' -> 'write' -> 'final'), the stream
--      pass and the chunk cursor reached in that phase, the regenerable kinds, the pass 4
--      counters, and the lease (lease_nonce, lease_ms, lease_expires_at, attempts, last_error).
--   2. sessions.search_reconcile_stage_v1: the staged keys, PK (run_id, side, key_hash).
--      side 'want' = the in-window index identities passes 1-3 produced (key_hash is the
--      stream index key, match_key equals it); side 'have' = the index scan, one row per index
--      identity (key_hash identifies the identity, match_key is its stream index key, payload
--      carries the identity a delete needs). 'fold' and 'directory_seen' are reserved for the
--      next slice. owner is the JSON array [workspace id, session id] on both sides.
--   Logged tables, RLS on, no policy, no table grant: reachable only through the functions.
--
-- Writer functions (SECURITY DEFINER, search_path pinned, EXECUTE to search_consumer_runtime
-- alone, which each checks itself, 42501 otherwise):
--   search_reconcile_claim_v1        take (or, once expired, take over) the scope's run lease;
--                                    a run started from another source stamp is reset (its
--                                    stage rows go) and restarts. No row: another live lease.
--   search_reconcile_stage_batch_v1  stage one batch on the run's side and move its cursor
--   search_reconcile_advance_v1      move the cursor, or the phase one step on; may release
--   search_reconcile_diff_page_v1    anti-join pages: 'insert' = want keys the index lacks
--                                    (identity or fingerprint differs), 'delete' = index
--                                    identities the reconcile removes
--   search_reconcile_finish_v1       the final step: the run and its stage rows go
--   search_reconcile_abandon_v1      the stage rows go, the run restarts from 'want'
--   search_reconcile_erase_scope_v1  erasure: every run (and stage row) of the scope goes,
--                                    leased or not
-- Every function but claim and erase_scope checks the lease nonce under the run's row lock and
-- extends the lease. Expiry only opens the run to a takeover: claim replaces the nonce once
-- lease_expires_at has passed, so the holder keeps the run through a long source read unless
-- someone took it over. A stale lease (taken over, finished, abandoned or erased) is SQLSTATE
-- RH062. Cursors only move forward within a phase and are never cleared mid-phase. A claim never
-- waits on a run row another transaction holds: it answers busy (no row).
--
-- Roles: never created or altered here; search_consumer_runtime (session_consumer_roles.sql) is
-- provisioned before migrate and checked here, fail closed (42704 absent, 55000 elevated).
-- New objects only: no existing function is replaced, no extension, no temp table.
--
-- Transactional (no CONCURRENTLY, no enum change): migrate.ts wraps it with its ledger row.
-- Code only; applying it to a real database needs Khaliq's explicit OK. Down path:
-- supabase/rollbacks/20261105062110_search_reconcile_staging.down.sql.

DO $roles$
DECLARE
  v_name text;
  r pg_roles%ROWTYPE;
BEGIN
  FOREACH v_name IN ARRAY ARRAY['search_consumer_runtime', 'bulls96_read_api'] LOOP
    SELECT * INTO r FROM pg_roles WHERE rolname = v_name;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE = '42704',
        MESSAGE = format('role %s does not exist; this migration does not create it', v_name),
        HINT = 'Run the role bootstrap (session_consumer_roles.sql, session_read_api_roles.sql) as a privileged role first, then rerun.';
    END IF;
    IF r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolinherit THEN
      RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = format('role %s exists elevated', v_name);
    END IF;
  END LOOP;
END
$roles$;

CREATE TABLE sessions.search_reconcile_runs_v1 (
  run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 255),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 255),
  harness text NOT NULL CHECK (char_length(harness) BETWEEN 1 AND 255),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255),
  canonical_id text NOT NULL CHECK (char_length(canonical_id) BETWEEN 1 AND 255),
  pointer_epoch bigint NOT NULL CHECK (pointer_epoch >= 0),
  manifest_fingerprint text NOT NULL CHECK (char_length(manifest_fingerprint) BETWEEN 1 AND 128),
  phase text NOT NULL DEFAULT 'want' CHECK (phase IN ('want', 'have', 'write', 'final')),
  pass smallint NOT NULL DEFAULT 0 CHECK (pass BETWEEN 0 AND 4),
  chunk_cursor text CHECK (char_length(chunk_cursor) <= 512),
  regenerable_kinds text[] NOT NULL DEFAULT '{}' CHECK (cardinality(regenerable_kinds) <= 256),
  write_wanted bigint NOT NULL DEFAULT 0 CHECK (write_wanted >= 0),
  write_done bigint NOT NULL DEFAULT 0 CHECK (write_done >= 0),
  lease_nonce uuid,
  lease_ms integer NOT NULL CHECK (lease_ms BETWEEN 1000 AND 3600000),
  lease_expires_at timestamptz NOT NULL,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error text CHECK (char_length(last_error) <= 200),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (org_id, workspace_id, harness, session_id)
);
CREATE INDEX search_reconcile_runs_v1_canonical ON sessions.search_reconcile_runs_v1 (org_id, workspace_id, harness, canonical_id);
ALTER TABLE sessions.search_reconcile_runs_v1 ENABLE ROW LEVEL SECURITY;

CREATE TABLE sessions.search_reconcile_stage_v1 (
  run_id bigint NOT NULL REFERENCES sessions.search_reconcile_runs_v1 (run_id) ON DELETE CASCADE,
  side text NOT NULL CHECK (side IN ('want', 'have', 'fold', 'directory_seen')),
  key_hash text COLLATE "C" NOT NULL CHECK (char_length(key_hash) BETWEEN 1 AND 128),
  match_key text COLLATE "C" NOT NULL CHECK (char_length(match_key) BETWEEN 1 AND 128),
  owner text NOT NULL CHECK (char_length(owner) BETWEEN 1 AND 600),
  ts_ms bigint NOT NULL,
  fingerprint text NOT NULL CHECK (char_length(fingerprint) BETWEEN 1 AND 256),
  payload jsonb CHECK (payload IS NULL OR (jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 4096)),
  PRIMARY KEY (run_id, side, key_hash),
  CHECK (side <> 'want' OR match_key = key_hash),
  CHECK (side <> 'have' OR jsonb_typeof(payload->'kind') = 'string')
);
CREATE INDEX search_reconcile_stage_v1_match ON sessions.search_reconcile_stage_v1 (run_id, side, match_key);
ALTER TABLE sessions.search_reconcile_stage_v1 ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON sessions.search_reconcile_runs_v1, sessions.search_reconcile_stage_v1
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;

-- The caller check shared by every writer: the consumer login, nothing else.
CREATE FUNCTION sessions.search_reconcile_caller_v1()
RETURNS void
LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp AS $caller$
BEGIN
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'search_consumer_runtime' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'search reconcile: caller is not the consumer runtime role';
  END IF;
END
$caller$;

-- The run held under this nonce, locked for the rest of the transaction, its lease extended by
-- its lease_ms. Anything else (no such run, another nonce) is RH062. An expired lease nobody
-- has taken over is still the holder's: claim, under the same row lock, is what ends it.
CREATE FUNCTION sessions.search_reconcile_lease_v1(p_run_id bigint, p_nonce uuid)
RETURNS sessions.search_reconcile_runs_v1
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp AS $lease$
DECLARE r sessions.search_reconcile_runs_v1%ROWTYPE;
BEGIN
  PERFORM sessions.search_reconcile_caller_v1();
  IF p_run_id IS NULL OR p_nonce IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: run and lease nonce required';
  END IF;
  SELECT * INTO r FROM sessions.search_reconcile_runs_v1 AS t WHERE t.run_id = p_run_id FOR UPDATE;
  IF NOT FOUND OR r.lease_nonce IS DISTINCT FROM p_nonce THEN
    RAISE EXCEPTION USING ERRCODE = 'RH062', MESSAGE = 'search reconcile: stale lease';
  END IF;
  UPDATE sessions.search_reconcile_runs_v1 AS t
     SET lease_expires_at = clock_timestamp() + t.lease_ms * interval '1 millisecond', updated_at = clock_timestamp()
   WHERE t.run_id = p_run_id
  RETURNING * INTO r;
  RETURN r;
END
$lease$;

CREATE FUNCTION sessions.search_reconcile_claim_v1(
  p_org_id text, p_workspace_id text, p_harness text, p_session_id text, p_canonical_id text,
  p_pointer_epoch bigint, p_manifest_fingerprint text, p_lease_ms integer
)
RETURNS TABLE (
  run_id bigint, lease_nonce uuid, phase text, pass smallint, chunk_cursor text,
  write_wanted bigint, write_done bigint, attempts integer, restarted boolean
)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp
SET lock_timeout = '2s' AS $claim$
#variable_conflict use_column
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_restarted boolean := false;
  v_try integer;
BEGIN
  PERFORM sessions.search_reconcile_caller_v1();
  IF coalesce(p_org_id, '') = '' OR coalesce(p_workspace_id, '') = '' OR coalesce(p_harness, '') = ''
     OR coalesce(p_session_id, '') = '' OR coalesce(p_canonical_id, '') = ''
     OR p_pointer_epoch IS NULL OR p_pointer_epoch < 0 OR coalesce(p_manifest_fingerprint, '') = ''
     OR p_lease_ms IS NULL OR p_lease_ms NOT BETWEEN 1000 AND 3600000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: invalid claim';
  END IF;
  -- A claim never waits behind the run's holder: a run row another transaction holds (a pass 4
  -- write, the final step, a finish or erase in flight) is skipped and the claim answers busy, so
  -- redelivered ticks of a hot scope do not each park a pool connection on the row lock.
  FOR v_try IN 1..3 LOOP
    SELECT * INTO r FROM sessions.search_reconcile_runs_v1 AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.harness = p_harness AND t.session_id = p_session_id
     FOR UPDATE SKIP LOCKED;
    EXIT WHEN FOUND;
    -- Visible but skipped: held by another transaction; busy, no row.
    IF EXISTS (SELECT 1 FROM sessions.search_reconcile_runs_v1 AS t
                WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.harness = p_harness AND t.session_id = p_session_id) THEN
      RETURN;
    END IF;
    -- No run: create it, then lock it on the next pass. Against another claim's uncommitted insert
    -- of the same scope, INSERT ... ON CONFLICT DO NOTHING waits for that transaction to end
    -- (speculative insertion waits on the conflicting in-progress row) and then inserts nothing if
    -- it committed. Only a claim inserts a run and the module commits each claim on its own, so the
    -- wait is one short claim; this function's lock_timeout bounds it, and a timeout is busy.
    BEGIN
      INSERT INTO sessions.search_reconcile_runs_v1 AS t
        (org_id, workspace_id, harness, session_id, canonical_id, pointer_epoch, manifest_fingerprint, lease_ms, lease_expires_at)
      VALUES (p_org_id, p_workspace_id, p_harness, p_session_id, p_canonical_id, p_pointer_epoch, p_manifest_fingerprint,
              p_lease_ms, '-infinity')
      ON CONFLICT (org_id, workspace_id, harness, session_id) DO NOTHING;
    EXCEPTION WHEN lock_not_available THEN
      RETURN;
    END;
  END LOOP;
  -- Still gone after three tries: a concurrent finish or erase keeps winning; busy, no row.
  IF r.run_id IS NULL THEN
    RETURN;
  END IF;
  -- Another live lease: busy, no row.
  IF r.lease_nonce IS NOT NULL AND r.lease_expires_at > clock_timestamp() THEN
    RETURN;
  END IF;
  -- Started from another source: abandoned, restarts from nothing.
  IF r.canonical_id IS DISTINCT FROM p_canonical_id OR r.pointer_epoch IS DISTINCT FROM p_pointer_epoch
     OR r.manifest_fingerprint IS DISTINCT FROM p_manifest_fingerprint THEN
    DELETE FROM sessions.search_reconcile_stage_v1 AS s WHERE s.run_id = r.run_id;
    UPDATE sessions.search_reconcile_runs_v1 AS t
       SET canonical_id = p_canonical_id, pointer_epoch = p_pointer_epoch, manifest_fingerprint = p_manifest_fingerprint,
           phase = 'want', pass = 0, chunk_cursor = NULL, regenerable_kinds = '{}', write_wanted = 0, write_done = 0,
           last_error = 'source_changed'
     WHERE t.run_id = r.run_id;
    v_restarted := true;
  END IF;
  UPDATE sessions.search_reconcile_runs_v1 AS t
     SET lease_nonce = gen_random_uuid(), lease_ms = p_lease_ms,
         lease_expires_at = clock_timestamp() + p_lease_ms * interval '1 millisecond',
         attempts = t.attempts + 1, updated_at = clock_timestamp()
   WHERE t.run_id = r.run_id
  RETURNING * INTO r;
  RETURN QUERY SELECT r.run_id, r.lease_nonce, r.phase, r.pass, r.chunk_cursor, r.write_wanted, r.write_done, r.attempts, v_restarted;
END
$claim$;

CREATE FUNCTION sessions.search_reconcile_stage_batch_v1(
  p_run_id bigint, p_nonce uuid, p_side text, p_pass smallint, p_entries jsonb, p_cursor text
)
RETURNS integer
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $stage$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_n integer;
BEGIN
  r := sessions.search_reconcile_lease_v1(p_run_id, p_nonce);
  IF NOT ((r.phase = 'want' AND p_side IN ('want', 'fold', 'directory_seen')) OR (r.phase = 'have' AND p_side = 'have')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search reconcile: side does not match the run phase';
  END IF;
  IF p_pass IS NULL OR p_pass NOT BETWEEN 0 AND 4 OR jsonb_typeof(p_entries) IS DISTINCT FROM 'array'
     OR jsonb_array_length(p_entries) > 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: invalid batch';
  END IF;
  -- Within a phase a cursor never moves back and, once set, is never cleared (only a phase change resets it).
  IF r.chunk_cursor IS NOT NULL AND (p_cursor IS NULL OR p_cursor < r.chunk_cursor COLLATE "C") THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search reconcile: cursor moved backwards';
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_to_recordset(p_entries) AS e(k text, o text, t bigint, f text)
              WHERE e.k IS NULL OR e.o IS NULL OR e.t IS NULL OR e.f IS NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: incomplete stage entry';
  END IF;
  INSERT INTO sessions.search_reconcile_stage_v1 (run_id, side, key_hash, match_key, owner, ts_ms, fingerprint, payload)
  SELECT p_run_id, p_side, e.k, coalesce(e.m, e.k), e.o, e.t, e.f, e.p
    FROM jsonb_to_recordset(p_entries) AS e(k text, m text, o text, t bigint, f text, p jsonb)
  ON CONFLICT (run_id, side, key_hash) DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  UPDATE sessions.search_reconcile_runs_v1 AS t SET pass = p_pass, chunk_cursor = p_cursor WHERE t.run_id = p_run_id;
  RETURN v_n;
END
$stage$;

CREATE FUNCTION sessions.search_reconcile_advance_v1(
  p_run_id bigint, p_nonce uuid, p_phase text, p_cursor text, p_regenerable_kinds text[],
  p_write_wanted bigint, p_write_done bigint, p_release boolean
)
RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $advance$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_next text;
BEGIN
  r := sessions.search_reconcile_lease_v1(p_run_id, p_nonce);
  v_next := CASE r.phase WHEN 'want' THEN 'have' WHEN 'have' THEN 'write' WHEN 'write' THEN 'final' END;
  IF p_phase IS NULL OR NOT (p_phase = r.phase OR p_phase = v_next) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search reconcile: phase out of order';
  END IF;
  IF coalesce(p_write_wanted, 0) < 0 OR coalesce(p_write_done, 0) < 0 OR p_release IS NULL
     OR (p_regenerable_kinds IS NOT NULL AND (r.phase <> 'want' OR p_phase <> 'have'
         OR array_position(p_regenerable_kinds, NULL) IS NOT NULL)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: invalid advance';
  END IF;
  IF p_phase = r.phase AND r.chunk_cursor IS NOT NULL AND (p_cursor IS NULL OR p_cursor < r.chunk_cursor COLLATE "C") THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search reconcile: cursor moved backwards';
  END IF;
  UPDATE sessions.search_reconcile_runs_v1 AS t
     SET phase = p_phase, chunk_cursor = p_cursor,
         regenerable_kinds = coalesce(p_regenerable_kinds, t.regenerable_kinds),
         write_wanted = t.write_wanted + coalesce(p_write_wanted, 0),
         write_done = t.write_done + coalesce(p_write_done, 0),
         lease_nonce = CASE WHEN p_release THEN NULL ELSE t.lease_nonce END,
         lease_expires_at = CASE WHEN p_release THEN clock_timestamp() ELSE t.lease_expires_at END
   WHERE t.run_id = p_run_id;
END
$advance$;

CREATE FUNCTION sessions.search_reconcile_diff_page_v1(
  p_run_id bigint, p_nonce uuid, p_mode text, p_after text, p_limit integer, p_keep_prefix text
)
RETURNS TABLE (key_hash text, payload jsonb)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $diff$
#variable_conflict use_column
DECLARE r sessions.search_reconcile_runs_v1%ROWTYPE;
BEGIN
  r := sessions.search_reconcile_lease_v1(p_run_id, p_nonce);
  IF p_mode IS NULL OR p_mode NOT IN ('insert', 'delete') OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000
     OR p_keep_prefix IS NULL OR char_length(p_keep_prefix) NOT BETWEEN 1 AND 16 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: invalid diff page';
  END IF;
  IF NOT (r.phase = 'final' OR (r.phase = 'write' AND p_mode = 'insert')) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search reconcile: diff before the stage is complete';
  END IF;
  IF p_mode = 'insert' THEN
    -- Wanted keys the index lacks: no index identity with the same key, owner, time and fingerprint.
    RETURN QUERY
      SELECT w.key_hash::text, NULL::jsonb
        FROM sessions.search_reconcile_stage_v1 AS w
       WHERE w.run_id = p_run_id AND w.side = 'want'
         AND (p_after IS NULL OR w.key_hash > p_after COLLATE "C")
         AND NOT EXISTS (SELECT 1 FROM sessions.search_reconcile_stage_v1 AS h
                          WHERE h.run_id = p_run_id AND h.side = 'have' AND h.match_key = w.key_hash
                            AND h.owner = w.owner AND h.ts_ms = w.ts_ms AND h.fingerprint = w.fingerprint)
       ORDER BY w.key_hash COLLATE "C"
       LIMIT p_limit;
  ELSE
    -- Index identities to remove, as the in-memory reconcile decides: only regenerable kinds; a
    -- row with the keep prefix (buffered origin) and no wanted key stays; otherwise any row whose
    -- key is not wanted, or wanted under another owner or time.
    RETURN QUERY
      SELECT h.key_hash::text, h.payload
        FROM sessions.search_reconcile_stage_v1 AS h
        LEFT JOIN sessions.search_reconcile_stage_v1 AS w
          ON w.run_id = h.run_id AND w.side = 'want' AND w.key_hash = h.match_key
       WHERE h.run_id = p_run_id AND h.side = 'have'
         AND (p_after IS NULL OR h.key_hash > p_after COLLATE "C")
         AND (h.payload->>'kind') = ANY (r.regenerable_kinds)
         AND NOT (w.key_hash IS NULL AND left(h.fingerprint, char_length(p_keep_prefix)) = p_keep_prefix)
         AND (w.key_hash IS NULL OR w.owner <> h.owner OR w.ts_ms <> h.ts_ms)
       ORDER BY h.key_hash COLLATE "C"
       LIMIT p_limit;
  END IF;
END
$diff$;

CREATE FUNCTION sessions.search_reconcile_finish_v1(p_run_id bigint, p_nonce uuid)
RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $finish$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_n bigint;
BEGIN
  r := sessions.search_reconcile_lease_v1(p_run_id, p_nonce);
  IF r.phase <> 'final' THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'search reconcile: finish before the final phase';
  END IF;
  DELETE FROM sessions.search_reconcile_stage_v1 AS s WHERE s.run_id = p_run_id;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  DELETE FROM sessions.search_reconcile_runs_v1 AS t WHERE t.run_id = p_run_id;
  RETURN v_n;
END
$finish$;

CREATE FUNCTION sessions.search_reconcile_abandon_v1(p_run_id bigint, p_nonce uuid, p_reason text)
RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $abandon$
DECLARE r sessions.search_reconcile_runs_v1%ROWTYPE;
BEGIN
  r := sessions.search_reconcile_lease_v1(p_run_id, p_nonce);
  IF coalesce(p_reason, '') = '' OR char_length(p_reason) > 200 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: invalid abandon reason';
  END IF;
  DELETE FROM sessions.search_reconcile_stage_v1 AS s WHERE s.run_id = p_run_id;
  UPDATE sessions.search_reconcile_runs_v1 AS t
     SET phase = 'want', pass = 0, chunk_cursor = NULL, regenerable_kinds = '{}', write_wanted = 0, write_done = 0,
         lease_nonce = NULL, lease_expires_at = clock_timestamp(), last_error = p_reason
   WHERE t.run_id = p_run_id;
END
$abandon$;

CREATE FUNCTION sessions.search_reconcile_erase_scope_v1(
  p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[]
)
RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $erase$
DECLARE v_n bigint;
BEGIN
  PERFORM sessions.search_reconcile_caller_v1();
  IF coalesce(p_org_id, '') = '' OR coalesce(p_workspace_id, '') = '' OR coalesce(p_harness, '') = ''
     OR p_session_ids IS NULL OR cardinality(p_session_ids) NOT BETWEEN 1 AND 1000
     OR array_position(p_session_ids, NULL) IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'search reconcile: invalid scope';
  END IF;
  DELETE FROM sessions.search_reconcile_stage_v1 AS s
   USING sessions.search_reconcile_runs_v1 AS t
   WHERE s.run_id = t.run_id AND t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.harness = p_harness
     AND (t.session_id = ANY (p_session_ids) OR t.canonical_id = ANY (p_session_ids));
  DELETE FROM sessions.search_reconcile_runs_v1 AS t
   WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.harness = p_harness
     AND (t.session_id = ANY (p_session_ids) OR t.canonical_id = ANY (p_session_ids));
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END
$erase$;

REVOKE ALL ON FUNCTION sessions.search_reconcile_caller_v1() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.search_reconcile_lease_v1(bigint, uuid) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.search_reconcile_claim_v1(text, text, text, text, text, bigint, text, integer)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.search_reconcile_stage_batch_v1(bigint, uuid, text, smallint, jsonb, text)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.search_reconcile_advance_v1(bigint, uuid, text, text, text[], bigint, bigint, boolean)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.search_reconcile_diff_page_v1(bigint, uuid, text, text, integer, text)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.search_reconcile_finish_v1(bigint, uuid) FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.search_reconcile_abandon_v1(bigint, uuid, text) FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.search_reconcile_erase_scope_v1(text, text, text, text[])
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_claim_v1(text, text, text, text, text, bigint, text, integer) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_stage_batch_v1(bigint, uuid, text, smallint, jsonb, text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_advance_v1(bigint, uuid, text, text, text[], bigint, bigint, boolean) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_diff_page_v1(bigint, uuid, text, text, integer, text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_finish_v1(bigint, uuid) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_abandon_v1(bigint, uuid, text) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.search_reconcile_erase_scope_v1(text, text, text, text[]) TO search_consumer_runtime;
