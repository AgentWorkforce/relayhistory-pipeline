-- Staged output facts for the staged search reconcile (Phase 3 large-session search, slice 2c;
-- registry slot 062140). The self-host output facts (20261105045040) are reconciled by
-- sessions.output_facts_reconcile, which takes the scope's whole fact set as one jsonb call; a
-- staged reconcile (20261105062110) never holds the whole scope in memory. Here a run's facts
-- are staged batch by batch under the 062110 run lease and reconciled from the stage table in the
-- run's final transaction, with the same version rule and the same result as
-- output_facts_reconcile. Behind SEARCH_INDEX_STAGED_RECONCILE_ENABLED together with
-- RELAYHISTORY_SELFHOST_OUTPUT_FACTS (both default off).
--
--   sessions.staged_facts_v1: one row per staged item of a 062110 run, PK (run_id, item_kind,
--   item_key). item_kind:
--     'event'      a session_event_facts row (body: the row as output_facts_reconcile's p_events
--                  element; item_key row_id:fingerprint, so two rows of one row_id resolve as
--                  output_facts_reconcile's DISTINCT ON (row_id) ORDER BY row_id, fingerprint does)
--     'fact'       a session_output_facts row (likewise, as a p_facts element)
--     'candidate' / 'call' / 'parent'
--                  the tool_result commit check's inputs (the result with its candidate facts, the
--                  session's tool calls and their parent session events), keyed for lookup by
--                  group_key (sha256 of [source, session id]) and match_key (sha256 of the tool use
--                  id or message id); decided at the end, when the whole session has been staged
--     'record'     a legacy outcome or turn record, folded at the end
--     'session'    a session id the scope's events carry (the delete filter and the state rows)
--     'note'       a reason a batch could not be read exactly (the pass is then incomplete)
--   Logged, RLS on, no policy, no table grant: reachable only through the functions.
--
--   Lifecycle: the stage rows go with the run, and no 062110 function is replaced for it. The
--   first stage call of a run adds one anchor row to sessions.search_reconcile_stage_v1 (side
--   'fold', key_hash '~output_facts': '~' is outside the base64 alphabet of a stream index key, so
--   it is never a real fold key, and the 062110 diff pages read only the 'want' and 'have'
--   sides), and every staged_facts_v1 row references that anchor ON DELETE CASCADE. Every 062110
--   path that drops a run's stage rows (search_reconcile_finish_v1, search_reconcile_abandon_v1,
--   a search_reconcile_claim_v1 restart on a changed source, and search_reconcile_erase_scope_v1)
--   deletes the anchor, and the facts stage goes with it in the same statement.
--
--   Wholeness: the anchor's payload {"whole", "seq", "done", "sealed"} says whether the stage holds
--   the run's entire facts input. The wiring delivers that input once per attempt, at the end of the
--   want phase, as numbered chunks (seq 0, 1, ...; the last one flagged), each in its own
--   transaction. Chunk 0 drops whatever the run had staged (an earlier attempt's partial pass) and
--   starts the chain whole; a later chunk keeps it whole only when it is the next number and the
--   chain is neither done nor sealed. The want -> have advance seals the anchor, whole only if the
--   last chunk was seen. A run whose facts input was not staged in one unbroken pass (facts turned
--   on after the pass, or mid-pass, or a gap) therefore never yields a whole stage. The reconcile
--   writes only a whole, sealed stage; anything else leaves the facts tables as they are (the
--   caller reports it and the run goes round again).
--
-- Writer functions (SECURITY DEFINER, search_path pinned, EXECUTE to search_consumer_runtime
-- alone; each takes the run id and its lease nonce and goes through search_reconcile_lease_v1,
-- so the caller check is 42501 and a lost lease is RH062, exactly as for the 062110 writers):
--   staged_facts_stage_v1      stage a chunk of items (phase 'want' with the chunk's number and
--                              last flag, or 'final' for the facts decided at the end); idempotent
--                              per (kind, key)
--   staged_facts_seal_v1       the want phase ended (phase 'want' or 'have')
--   staged_facts_read_v1       page one kind of the run's items, optionally by group and match
--                              keys, in staging order (phase 'final')
--   staged_facts_status_v1     whether the stage is whole, and the staged facts' time span
--                              (phase 'final')
--   staged_facts_reconcile_v1  output_facts_reconcile over the staged event and fact rows
--                              (phase 'final'): the run must be the scope's; a stage that is not
--                              whole writes nothing (applied = false); then the same stale
--                              source_version rule (applied = false, nothing written), the same
--                              out-of-scope refusal, upserts and complete-only deletes
-- Helpers (owner-only, not SECURITY DEFINER): staged_facts_run_v1 (the lease plus the phase check)
-- and staged_facts_whole_v1 (the anchor's verdict).
-- The 045040 helpers output_facts_writer_scope and output_facts_stored_version are reused as is.
--
-- Roles: never created or altered here; search_consumer_runtime and bulls96_read_api are
-- provisioned before migrate and checked here, fail closed (42704 absent, 55000 elevated).
-- New objects only: no existing function is replaced, no extension, no temp table. The foreign key
-- adds PostgreSQL's internal referential triggers to sessions.search_reconcile_stage_v1.
--
-- Transactional (no CONCURRENTLY, no enum change): migrate.ts wraps it with its ledger row.
-- Code only; applying it to a real database needs Khaliq's explicit OK. Down path:
-- supabase/rollbacks/20261105062140_staged_output_facts.down.sql.

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

CREATE TABLE sessions.staged_facts_v1 (
  run_id bigint NOT NULL,
  anchor_side text NOT NULL DEFAULT 'fold' CHECK (anchor_side = 'fold'),
  anchor_key text COLLATE "C" NOT NULL DEFAULT '~output_facts' CHECK (anchor_key = '~output_facts'),
  item_kind text NOT NULL CHECK (item_kind IN ('event', 'fact', 'candidate', 'call', 'parent', 'record', 'session', 'note')),
  item_key text COLLATE "C" NOT NULL CHECK (char_length(item_key) BETWEEN 1 AND 200),
  seq bigint GENERATED ALWAYS AS IDENTITY,
  group_key text COLLATE "C" CHECK (group_key ~ '^[0-9a-f]{64}$'),
  match_key text COLLATE "C" CHECK (match_key ~ '^[0-9a-f]{64}$'),
  row_id text COLLATE "C",
  ts timestamptz,
  body text NOT NULL,
  PRIMARY KEY (run_id, item_kind, item_key),
  FOREIGN KEY (run_id, anchor_side, anchor_key)
    REFERENCES sessions.search_reconcile_stage_v1 (run_id, side, key_hash) ON DELETE CASCADE,
  CHECK ((item_kind IN ('event', 'fact')) = (row_id IS NOT NULL AND ts IS NOT NULL))
);
-- The cascade probes this index for every 062110 stage row a finish, abandon or restart deletes.
CREATE INDEX staged_facts_v1_anchor ON sessions.staged_facts_v1 (run_id, anchor_side, anchor_key);
CREATE INDEX staged_facts_v1_match ON sessions.staged_facts_v1 (run_id, item_kind, group_key, match_key);
CREATE INDEX staged_facts_v1_row ON sessions.staged_facts_v1 (run_id, item_kind, row_id) WHERE row_id IS NOT NULL;
CREATE INDEX staged_facts_v1_seq ON sessions.staged_facts_v1 (run_id, item_kind, seq);
ALTER TABLE sessions.staged_facts_v1 ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON sessions.staged_facts_v1
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;

-- The run held under this nonce (search_reconcile_lease_v1: 42501 for another caller, RH062 for a
-- lost lease, the lease extended), in one of these phases (55000 otherwise).
CREATE FUNCTION sessions.staged_facts_run_v1(p_run_id bigint, p_nonce uuid, p_phases text[])
RETURNS sessions.search_reconcile_runs_v1
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp AS $run$
DECLARE r sessions.search_reconcile_runs_v1%ROWTYPE;
BEGIN
  r := sessions.search_reconcile_lease_v1(p_run_id, p_nonce);
  IF NOT (r.phase = ANY (p_phases)) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'staged facts: not allowed in this phase of the run';
  END IF;
  RETURN r;
END
$run$;

-- Whether the run's facts stage covers its whole want pass (see the header).
CREATE FUNCTION sessions.staged_facts_whole_v1(p_run_id bigint)
RETURNS boolean
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp AS $whole$
  SELECT coalesce((SELECT (s.payload->>'whole')::boolean AND (s.payload->>'sealed')::boolean
                     FROM sessions.search_reconcile_stage_v1 AS s
                    WHERE s.run_id = p_run_id AND s.side = 'fold' AND s.key_hash = '~output_facts'), false);
$whole$;

-- p_entries: [{"k": kind, "i": item key (all kinds but event and fact), "g": group key,
-- "m": match key, "b": the item as JSON text}]. An event or fact is keyed by its own row_id and
-- fingerprint and carries its ts. In the want phase p_seq / p_last are the chunk's number and whether
-- it is the last (one call per chunk, items or not), or both null for a further call of the same chunk
-- (a chunk larger than one call), which leaves the chain as it is; in the final phase both are null.
-- Returns the items newly staged.
CREATE FUNCTION sessions.staged_facts_stage_v1(p_run_id bigint, p_nonce uuid, p_entries jsonb, p_seq integer, p_last boolean)
RETURNS integer
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $stage$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_anchor jsonb;
  v_next jsonb;
  v_n integer;
BEGIN
  r := sessions.staged_facts_run_v1(p_run_id, p_nonce, ARRAY['want', 'final']);
  IF jsonb_typeof(p_entries) IS DISTINCT FROM 'array' OR jsonb_array_length(p_entries) > 10000
     OR (r.phase = 'want' AND ((p_seq IS NULL) <> (p_last IS NULL) OR p_seq < 0))
     OR (r.phase = 'final' AND (p_seq IS NOT NULL OR p_last IS NOT NULL)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'staged facts: invalid batch';
  END IF;
  IF EXISTS (
    SELECT 1 FROM jsonb_array_elements(p_entries) AS x(e)
     WHERE jsonb_typeof(x.e) IS DISTINCT FROM 'object'
        OR jsonb_typeof(x.e->'b') IS DISTINCT FROM 'string'
        OR jsonb_typeof((x.e->>'b')::jsonb) IS DISTINCT FROM 'object'
        OR x.e->>'k' IS NULL
        OR x.e->>'k' NOT IN ('event', 'fact', 'candidate', 'call', 'parent', 'record', 'session', 'note')
        OR (x.e->>'k' IN ('event', 'fact')
            AND ((x.e->>'b')::jsonb->>'row_id' IS NULL OR (x.e->>'b')::jsonb->>'ts' IS NULL))
        OR (x.e->>'k' NOT IN ('event', 'fact') AND coalesce(x.e->>'i', '') = '')) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'staged facts: incomplete stage entry';
  END IF;
  -- The anchor the run's facts stage hangs off (see the header): 062110 drops it with the run's stage
  -- rows. Chunk 0 starts the chain over; a later chunk extends it; an anchor first made by anything
  -- but chunk 0 is never whole.
  SELECT s.payload INTO v_anchor FROM sessions.search_reconcile_stage_v1 AS s
   WHERE s.run_id = p_run_id AND s.side = 'fold' AND s.key_hash = '~output_facts' FOR UPDATE;
  IF r.phase = 'want' AND p_seq = 0 THEN
    DELETE FROM sessions.staged_facts_v1 AS f WHERE f.run_id = p_run_id;
    v_next := jsonb_build_object('whole', true, 'seq', 0, 'done', p_last, 'sealed', false);
  ELSIF r.phase = 'want' AND p_seq IS NOT NULL THEN
    v_next := jsonb_build_object(
      'whole', v_anchor IS NOT NULL AND (v_anchor->>'whole')::boolean AND NOT (v_anchor->>'done')::boolean
               AND NOT (v_anchor->>'sealed')::boolean AND p_seq = (v_anchor->>'seq')::integer + 1,
      'seq', p_seq, 'done', p_last, 'sealed', false);
  ELSE
    v_next := coalesce(v_anchor, jsonb_build_object('whole', false, 'seq', NULL, 'done', false, 'sealed', false));
  END IF;
  IF v_anchor IS NULL THEN
    INSERT INTO sessions.search_reconcile_stage_v1 (run_id, side, key_hash, match_key, owner, ts_ms, fingerprint, payload)
    VALUES (p_run_id, 'fold', '~output_facts', '~output_facts', jsonb_build_array(r.workspace_id, r.session_id)::text, 0, 'output_facts', v_next);
  ELSIF v_next IS DISTINCT FROM v_anchor THEN
    UPDATE sessions.search_reconcile_stage_v1 AS s SET payload = v_next
     WHERE s.run_id = p_run_id AND s.side = 'fold' AND s.key_hash = '~output_facts';
  END IF;
  INSERT INTO sessions.staged_facts_v1 (run_id, item_kind, item_key, group_key, match_key, row_id, ts, body)
  SELECT p_run_id, x.k,
         CASE WHEN x.k IN ('event', 'fact') THEN (x.b->>'row_id') || ':' || coalesce(x.b->>'fingerprint', '') ELSE x.i END,
         x.g, x.m,
         CASE WHEN x.k IN ('event', 'fact') THEN x.b->>'row_id' END,
         CASE WHEN x.k IN ('event', 'fact') THEN (x.b->>'ts')::timestamptz END,
         x.body
    FROM (SELECT e->>'k' AS k, e->>'i' AS i, e->>'g' AS g, e->>'m' AS m, e->>'b' AS body, (e->>'b')::jsonb AS b
            FROM jsonb_array_elements(p_entries) AS a(e)) AS x
  ON CONFLICT (run_id, item_kind, item_key) DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END
$stage$;

CREATE FUNCTION sessions.staged_facts_read_v1(
  p_run_id bigint, p_nonce uuid, p_kind text, p_group text, p_match text[], p_after bigint, p_limit integer
)
RETURNS TABLE (seq bigint, body text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $read$
#variable_conflict use_column
DECLARE r sessions.search_reconcile_runs_v1%ROWTYPE;
BEGIN
  r := sessions.staged_facts_run_v1(p_run_id, p_nonce, ARRAY['final']);
  IF p_kind IS NULL OR p_kind NOT IN ('event', 'fact', 'candidate', 'call', 'parent', 'record', 'session', 'note')
     OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000
     OR (p_match IS NOT NULL AND (p_group IS NULL OR cardinality(p_match) > 10000)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'staged facts: invalid read';
  END IF;
  RETURN QUERY
    SELECT s.seq, s.body
      FROM sessions.staged_facts_v1 AS s
     WHERE s.run_id = p_run_id AND s.item_kind = p_kind
       AND (p_group IS NULL OR s.group_key = p_group)
       AND (p_match IS NULL OR s.match_key = ANY (p_match))
       AND (p_after IS NULL OR s.seq > p_after)
     ORDER BY s.seq
     LIMIT p_limit;
END
$read$;

-- The want phase ended (called in the transaction that ends it): the anchor is sealed, whole only if
-- the chain saw its last chunk. No anchor: the facts input was never staged on this run; sealed, not whole.
CREATE FUNCTION sessions.staged_facts_seal_v1(p_run_id bigint, p_nonce uuid)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $seal$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_anchor jsonb;
BEGIN
  r := sessions.staged_facts_run_v1(p_run_id, p_nonce, ARRAY['want', 'have']);
  SELECT s.payload INTO v_anchor FROM sessions.search_reconcile_stage_v1 AS s
   WHERE s.run_id = p_run_id AND s.side = 'fold' AND s.key_hash = '~output_facts' FOR UPDATE;
  IF NOT FOUND THEN
    INSERT INTO sessions.search_reconcile_stage_v1 (run_id, side, key_hash, match_key, owner, ts_ms, fingerprint, payload)
    VALUES (p_run_id, 'fold', '~output_facts', '~output_facts', jsonb_build_array(r.workspace_id, r.session_id)::text, 0, 'output_facts',
            jsonb_build_object('whole', false, 'seq', NULL, 'done', false, 'sealed', true));
  ELSE
    UPDATE sessions.search_reconcile_stage_v1 AS s
       SET payload = v_anchor || jsonb_build_object('whole', (v_anchor->>'whole')::boolean AND (v_anchor->>'done')::boolean, 'sealed', true)
     WHERE s.run_id = p_run_id AND s.side = 'fold' AND s.key_hash = '~output_facts';
  END IF;
  RETURN sessions.staged_facts_whole_v1(p_run_id);
END
$seal$;

CREATE FUNCTION sessions.staged_facts_status_v1(p_run_id bigint, p_nonce uuid)
RETURNS TABLE (whole boolean, min_ts timestamptz, max_ts timestamptz)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $status$
DECLARE r sessions.search_reconcile_runs_v1%ROWTYPE;
BEGIN
  r := sessions.staged_facts_run_v1(p_run_id, p_nonce, ARRAY['final']);
  RETURN QUERY
    SELECT sessions.staged_facts_whole_v1(p_run_id), min(s.ts), max(s.ts) FROM sessions.staged_facts_v1 AS s
     WHERE s.run_id = p_run_id AND s.item_kind IN ('event', 'fact');
END
$status$;

-- sessions.output_facts_reconcile (20261105045040) with the staged event and fact rows of the run
-- in place of p_events and p_facts: every rule and statement below is that function's.
CREATE FUNCTION sessions.staged_facts_reconcile_v1(
  p_run_id bigint, p_nonce uuid, p_org_id text, p_workspace_id text, p_harness text, p_session_ids text[],
  p_canonical_id text, p_complete boolean, p_source_version bigint
)
RETURNS TABLE (applied boolean, inserted bigint, deleted bigint)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $reconcile$
DECLARE
  r sessions.search_reconcile_runs_v1%ROWTYPE;
  v_stored bigint;
  v_n bigint;
  v_inserted bigint := 0;
  v_deleted bigint := 0;
BEGIN
  r := sessions.staged_facts_run_v1(p_run_id, p_nonce, ARRAY['final']);
  PERFORM sessions.output_facts_writer_scope(p_org_id, p_workspace_id, p_harness, p_session_ids, p_canonical_id);
  IF p_canonical_id IS NULL OR p_complete IS NULL OR p_source_version IS NULL OR p_source_version < 0 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: invalid reconcile arguments';
  END IF;
  -- The run's facts reconcile the run's own scope only.
  IF r.org_id IS DISTINCT FROM p_org_id OR r.workspace_id IS DISTINCT FROM p_workspace_id OR r.harness IS DISTINCT FROM p_harness
     OR r.canonical_id IS DISTINCT FROM p_canonical_id OR NOT (r.session_id = ANY (p_session_ids)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'staged facts: the run belongs to another scope';
  END IF;
  -- A stage that does not cover the whole want pass is never read as the scope's facts: nothing is
  -- written, so a partial stage can never delete what an earlier pass wrote.
  IF NOT sessions.staged_facts_whole_v1(p_run_id) THEN
    RETURN QUERY SELECT false, 0::bigint, 0::bigint;
    RETURN;
  END IF;
  v_stored := sessions.output_facts_stored_version(p_org_id, p_workspace_id, p_harness, p_canonical_id);
  IF v_stored IS NOT NULL AND v_stored > p_source_version THEN
    RETURN QUERY SELECT false, 0::bigint, 0::bigint;
    RETURN;
  END IF;

  -- Every row must belong to this scope: never a write outside the tenant, harness or sessions.
  IF EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
               CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS e(org_id text, workspace_id text, source text, session_id text)
              WHERE s.run_id = p_run_id AND s.item_kind = 'event'
                AND (e.org_id IS DISTINCT FROM p_org_id OR e.workspace_id IS DISTINCT FROM p_workspace_id
                 OR e.source IS DISTINCT FROM p_harness OR e.session_id IS NULL
                 OR NOT (e.session_id = ANY (p_session_ids))))
     OR EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
               CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS f(org_id text, workspace_id text, scope_source text, session_id text)
              WHERE s.run_id = p_run_id AND s.item_kind = 'fact'
                AND (f.org_id IS DISTINCT FROM p_org_id OR f.workspace_id IS DISTINCT FROM p_workspace_id
                 OR f.scope_source IS DISTINCT FROM p_harness OR f.session_id IS NULL
                 OR NOT (f.session_id = ANY (p_session_ids)))) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'output facts: a row lies outside the scope';
  END IF;

  INSERT INTO sessions.session_event_facts AS t
  SELECT DISTINCT ON (e.row_id)
      e.org_id, e.workspace_id, e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id,
      e.project_id, e.task_ref_id, coalesce(e.task_ref_empty, 0), coalesce(e.task_ref_json, '{}'), e.task_title,
      e.user_id, e.branch, e.model, e.provider, e.input_tokens, e.output_tokens, e.reasoning_tokens,
      e.cache_read_tokens, e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens,
      e.cost_usd_micros, coalesce(e.files_touched, '{}'), coalesce(e.retention_days, 0), e.row_id, e.fingerprint,
      p_source_version
    FROM sessions.staged_facts_v1 AS s
    CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS e(
      org_id text, workspace_id text, ts timestamptz, session_id text, source text, machine_id text, kind text,
      event_id text, project_id text, task_ref_id text, task_ref_empty smallint, task_ref_json text,
      task_title text, user_id text, branch text, model text, provider text, input_tokens bigint,
      output_tokens bigint, reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint,
      cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, cost_usd_micros bigint,
      files_touched text[], retention_days integer, row_id text, fingerprint text)
   WHERE s.run_id = p_run_id AND s.item_kind = 'event'
    ORDER BY e.row_id, e.fingerprint
  ON CONFLICT (org_id, workspace_id, row_id) DO UPDATE SET
    ts = EXCLUDED.ts, session_id = EXCLUDED.session_id, source = EXCLUDED.source,
    machine_id = EXCLUDED.machine_id, kind = EXCLUDED.kind, event_id = EXCLUDED.event_id,
    project_id = EXCLUDED.project_id, task_ref_id = EXCLUDED.task_ref_id,
    task_ref_empty = EXCLUDED.task_ref_empty, task_ref_json = EXCLUDED.task_ref_json,
    task_title = EXCLUDED.task_title, user_id = EXCLUDED.user_id, branch = EXCLUDED.branch,
    model = EXCLUDED.model, provider = EXCLUDED.provider, input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens, reasoning_tokens = EXCLUDED.reasoning_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens, cache_create_tokens = EXCLUDED.cache_create_tokens,
    cache_create_5m_tokens = EXCLUDED.cache_create_5m_tokens, cache_create_1h_tokens = EXCLUDED.cache_create_1h_tokens,
    cost_usd_micros = EXCLUDED.cost_usd_micros, files_touched = EXCLUDED.files_touched,
    retention_days = EXCLUDED.retention_days, fingerprint = EXCLUDED.fingerprint,
    source_version = EXCLUDED.source_version
  WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
    AND t.source = p_harness AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_inserted := v_inserted + v_n;

  INSERT INTO sessions.session_output_facts AS t
  SELECT DISTINCT ON (f.row_id)
      f.org_id, f.workspace_id, f.fact_kind, f.ts, f.scope_source, f.source, f.session_id, f.fact_key,
      f.origin, f.row_key, f.added, f.removed, coalesce(f.reverted, 0), coalesce(f.retention_days, 0),
      f.row_id, f.fingerprint, p_source_version
    FROM sessions.staged_facts_v1 AS s
    CROSS JOIN LATERAL jsonb_to_record(s.body::jsonb) AS f(
      org_id text, workspace_id text, fact_kind text, ts timestamptz, scope_source text, source text,
      session_id text, fact_key text, origin text, row_key text, added bigint, removed bigint,
      reverted smallint, retention_days integer, row_id text, fingerprint text)
   WHERE s.run_id = p_run_id AND s.item_kind = 'fact'
    ORDER BY f.row_id, f.fingerprint
  ON CONFLICT (org_id, workspace_id, row_id) DO UPDATE SET
    fact_kind = EXCLUDED.fact_kind, ts = EXCLUDED.ts, scope_source = EXCLUDED.scope_source,
    source = EXCLUDED.source, session_id = EXCLUDED.session_id, fact_key = EXCLUDED.fact_key,
    origin = EXCLUDED.origin, row_key = EXCLUDED.row_key, added = EXCLUDED.added,
    removed = EXCLUDED.removed, reverted = EXCLUDED.reverted, retention_days = EXCLUDED.retention_days,
    fingerprint = EXCLUDED.fingerprint, source_version = EXCLUDED.source_version
  WHERE t.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
    AND t.scope_source = p_harness AND t.session_id = ANY (p_session_ids);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  v_inserted := v_inserted + v_n;

  -- Only an exact pass deletes: the scope's rows it no longer carries.
  IF p_complete THEN
    DELETE FROM sessions.session_event_facts AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.source = p_harness
       AND t.session_id = ANY (p_session_ids)
       AND NOT EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
                        WHERE s.run_id = p_run_id AND s.item_kind = 'event' AND s.row_id = t.row_id);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_deleted := v_deleted + v_n;
    DELETE FROM sessions.session_output_facts AS t
     WHERE t.org_id = p_org_id AND t.workspace_id = p_workspace_id AND t.scope_source = p_harness
       AND t.session_id = ANY (p_session_ids)
       AND NOT EXISTS (SELECT 1 FROM sessions.staged_facts_v1 AS s
                        WHERE s.run_id = p_run_id AND s.item_kind = 'fact' AND s.row_id = t.row_id);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_deleted := v_deleted + v_n;
  END IF;
  RETURN QUERY SELECT true, v_inserted, v_deleted;
END
$reconcile$;

REVOKE ALL ON FUNCTION sessions.staged_facts_run_v1(bigint, uuid, text[]) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.staged_facts_whole_v1(bigint) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION sessions.staged_facts_stage_v1(bigint, uuid, jsonb, integer, boolean)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.staged_facts_seal_v1(bigint, uuid)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.staged_facts_read_v1(bigint, uuid, text, text, text[], bigint, integer)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.staged_facts_status_v1(bigint, uuid) FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.staged_facts_reconcile_v1(bigint, uuid, text, text, text, text[], text, boolean, bigint)
  FROM PUBLIC, anon, authenticated, service_role, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.staged_facts_stage_v1(bigint, uuid, jsonb, integer, boolean) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.staged_facts_seal_v1(bigint, uuid) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.staged_facts_read_v1(bigint, uuid, text, text, text[], bigint, integer) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.staged_facts_status_v1(bigint, uuid) TO search_consumer_runtime;
GRANT EXECUTE ON FUNCTION sessions.staged_facts_reconcile_v1(bigint, uuid, text, text, text, text[], text, boolean, bigint)
  TO search_consumer_runtime;
