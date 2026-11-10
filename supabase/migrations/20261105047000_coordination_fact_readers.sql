-- Slot 20261105047000 assigned by the migration registry owner. Sparse, tenant-scoped readers for
-- GET /v1/coordination/overlaps. Code only: applying this to a real database needs Khaliq's explicit OK.
--
-- The general 045040 readers page every event/message in hash order. A busy workspace therefore spends a
-- 10,000-row request budget on an effectively random prefix. These readers expose only the evidence used by
-- outcome-facts: non-empty file writes, and commit/outcome facts. They add no table, stored result, worker,
-- model call or backfill.
--
-- Both partial indexes use the 044020 adopt-or-create rule. The migrator is transactional, so an absent index
-- on a populated table is refused unless HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true. Production operators first
-- build the exact definitions out of band with CREATE INDEX CONCURRENTLY (packet in
-- docs/coordination-overlap-detection.md), then this migration adopts them without taking a table lock.
DO $coordination_fact_indexes$
DECLARE
 v_spec text[];
 v_tbl regclass;
 v_rel record;
 v_idx record;
 v_def text;
 v_rows boolean;
 v_allow boolean:=coalesce(current_setting('hoopdb.allow_blocking_index_build',true),'')='on';
 v_rls text:=current_setting('row_security');
BEGIN
 FOREACH v_spec SLICE 1 IN ARRAY ARRAY[
  ['session_event_facts_coordination_writes_idx','session_event_facts',
   'CREATE INDEX session_event_facts_coordination_writes_idx ON sessions.session_event_facts USING btree (org_id, workspace_id, ts, row_id) WHERE (cardinality(files_touched) > 0)'],
  ['session_output_facts_coordination_outcomes_idx','session_output_facts',
   'CREATE INDEX session_output_facts_coordination_outcomes_idx ON sessions.session_output_facts USING btree (org_id, workspace_id, ts, row_id) WHERE (fact_kind = ANY (ARRAY[''commit''::text, ''outcome''::text]))']
 ] LOOP
  v_tbl:=to_regclass(format('sessions.%I',v_spec[2]));
  IF v_tbl IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='42P01',
    MESSAGE=format('coordination fact readers: table sessions.%s does not exist',v_spec[2]);
  END IF;
  SELECT c.oid,c.relkind INTO v_rel FROM pg_class c
   WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  IF NOT FOUND THEN
   PERFORM set_config('row_security','off',true);
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
   IF v_rows AND NOT v_allow THEN
    RAISE EXCEPTION USING ERRCODE='55000',
     MESSAGE=format('coordination fact readers: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
     HINT=format('prebuild it out of band, CONCURRENTLY, with the coordination fact-reader index packet (docs/coordination-overlap-detection.md) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
   END IF;
   EXECUTE format('LOCK TABLE %s IN SHARE MODE',v_tbl);
   IF NOT v_allow THEN
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)',v_tbl) INTO v_rows;
    IF v_rows THEN
     RAISE EXCEPTION USING ERRCODE='55000',
      MESSAGE=format('coordination fact readers: index sessions.%s is absent and table sessions.%s is not empty; refusing a blocking CREATE INDEX on a populated table',v_spec[1],v_spec[2]),
      HINT=format('prebuild it out of band, CONCURRENTLY, with the coordination fact-reader index packet (docs/coordination-overlap-detection.md) and rerun, this migration then adopts it; or opt into a blocking build with HOOPDB_ALLOW_BLOCKING_INDEX_BUILD=true (expected: %s)',v_spec[3]);
    END IF;
   END IF;
   PERFORM set_config('row_security',v_rls,true);
   EXECUTE v_spec[3];
   SELECT c.oid,c.relkind INTO STRICT v_rel FROM pg_class c
    WHERE c.relnamespace='sessions'::regnamespace AND c.relname=v_spec[1];
  END IF;
  IF v_rel.relkind<>'i' THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('coordination fact readers: sessions.%s exists but is not a plain index (relkind %s); reconcile it by hand, nothing was changed',v_spec[1],v_rel.relkind);
  END IF;
  SELECT i.indrelid,i.indisvalid,i.indisready,i.indislive INTO STRICT v_idx FROM pg_index i WHERE i.indexrelid=v_rel.oid;
  v_def:=pg_get_indexdef(v_rel.oid);
  IF v_idx.indrelid<>v_tbl OR v_def IS DISTINCT FROM v_spec[3] THEN
   RAISE EXCEPTION USING ERRCODE='42P07',
    MESSAGE=format('coordination fact readers: index sessions.%s exists with a different definition; reconcile it (expected: %s)',v_spec[1],v_spec[3]),
    DETAIL=format('found: %s',v_def);
  END IF;
  IF NOT (v_idx.indisvalid AND v_idx.indisready AND v_idx.indislive) THEN
   RAISE EXCEPTION USING ERRCODE='55000',
    MESSAGE=format('coordination fact readers: index sessions.%s is INVALID or not ready (a failed CONCURRENTLY build); drop it with DROP INDEX CONCURRENTLY sessions.%s, rebuild it out of band or let this migration create it, and rerun',v_spec[1],v_spec[1]);
  END IF;
 END LOOP;
END
$coordination_fact_indexes$;

CREATE FUNCTION sessions.coordination_write_facts_for(
  p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
)
RETURNS TABLE (
  ts timestamptz, session_id text, source text, machine_id text, kind text, event_id text,
  project_id text, task_ref_id text, task_ref_empty smallint, task_ref_json text, task_title text,
  user_id text, branch text, model text, provider text, input_tokens bigint, output_tokens bigint,
  reasoning_tokens bigint, cache_read_tokens bigint, cache_create_tokens bigint,
  cache_create_5m_tokens bigint, cache_create_1h_tokens bigint, cost_usd_micros bigint,
  files_touched text[], row_id text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_writes$
DECLARE v_org text;
BEGIN
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination facts: invalid write window or limit';
  END IF;
  RETURN QUERY
    SELECT e.ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id, e.project_id, e.task_ref_id,
           e.task_ref_empty, e.task_ref_json, e.task_title, e.user_id, e.branch, e.model, e.provider,
           e.input_tokens, e.output_tokens, e.reasoning_tokens, e.cache_read_tokens, e.cache_create_tokens,
           e.cache_create_5m_tokens, e.cache_create_1h_tokens, e.cost_usd_micros, e.files_touched, e.row_id
      FROM sessions.session_event_facts AS e
     WHERE e.org_id = v_org AND e.workspace_id = p_workspace_id AND e.ts >= p_from AND e.ts < p_to
       AND cardinality(e.files_touched) > 0
       AND (p_after_row_id IS NULL OR e.row_id > p_after_row_id)
     ORDER BY e.row_id
     LIMIT p_limit;
END
$coordination_writes$;

CREATE FUNCTION sessions.coordination_output_facts_for(
  p_workspace_id text, p_from timestamptz, p_to timestamptz, p_after_row_id text, p_limit integer
)
RETURNS TABLE (
  fact_kind text, ts timestamptz, scope_source text, source text, session_id text, fact_key text,
  origin text, row_key text, added bigint, removed bigint, reverted smallint, row_id text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $coordination_outputs$
DECLARE v_org text;
BEGIN
  v_org := sessions.session_directory_read_api_scope(p_workspace_id);
  IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 10000 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'coordination facts: invalid output window or limit';
  END IF;
  RETURN QUERY
    SELECT f.fact_kind, f.ts, f.scope_source, f.source, f.session_id, f.fact_key, f.origin, f.row_key,
           f.added, f.removed, f.reverted, f.row_id
      FROM sessions.session_output_facts AS f
     WHERE f.org_id = v_org AND f.workspace_id = p_workspace_id AND f.ts >= p_from AND f.ts < p_to
       AND f.fact_kind = ANY (ARRAY['commit'::text, 'outcome'::text])
       AND (p_after_row_id IS NULL OR f.row_id > p_after_row_id)
     ORDER BY f.row_id
     LIMIT p_limit;
END
$coordination_outputs$;

REVOKE ALL ON FUNCTION sessions.coordination_write_facts_for(text, timestamptz, timestamptz, text, integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
REVOKE ALL ON FUNCTION sessions.coordination_output_facts_for(text, timestamptz, timestamptz, text, integer)
  FROM PUBLIC, anon, authenticated, service_role, search_consumer_runtime, bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.coordination_write_facts_for(text, timestamptz, timestamptz, text, integer)
  TO bulls96_read_api;
GRANT EXECUTE ON FUNCTION sessions.coordination_output_facts_for(text, timestamptz, timestamptz, text, integer)
  TO bulls96_read_api;
