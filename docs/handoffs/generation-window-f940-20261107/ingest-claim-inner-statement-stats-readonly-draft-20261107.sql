-- SOURCE ONLY. Root's existing Supabase owner route; no credential acquisition here.
-- No reset, configuration changes, EXPLAIN, workload invocation or application rows.
-- Precondition: installed pg_stat_statements is in schema extensions. Any other
-- namespace requires a separately qualified identifier substitution, never a fallback.
-- Statistics are cumulative since their last reset, not an incident-window delta.
-- Failed/timed-out executions may be absent: zero samples do not prove fast/absent work.
BEGIN READ ONLY;
SET LOCAL ROLE postgres;
SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL statement_timeout = '3s';
SET LOCAL lock_timeout = '250ms';

DO $preflight$
BEGIN
  IF current_user <> 'postgres' OR session_user <> 'cli_login_postgres'
     OR current_setting('transaction_read_only') <> 'on' THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='statistics owner route not qualified';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_extension e
    JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
    WHERE e.extname='pg_stat_statements' AND n.nspname='extensions'
  ) THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='pg_stat_statements unavailable in reviewed extensions schema';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='bulls96_session_ingest') THEN
    RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='ingest runtime role absent';
  END IF;
END;
$preflight$;

WITH settings AS MATERIALIZED (
  SELECT current_setting('pg_stat_statements.track', true) AS statement_track,
         current_setting('track_functions', true) AS function_track
), statements AS MATERIALIZED (
  SELECT s.toplevel, s.calls, s.mean_exec_time, s.max_exec_time,
         s.stddev_exec_time, s.rows, s.shared_blks_hit, s.shared_blks_read,
         s.temp_blks_read, s.temp_blks_written,
         left(s.query,300) AS normalized_query_first_300_chars,
         (s.query IS NOT NULL AND s.query <> '<insufficient privilege>') AS query_visible
  FROM extensions.pg_stat_statements s
  WHERE s.userid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname='bulls96_session_ingest')
    AND s.dbid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
), top_max AS (
  SELECT * FROM statements ORDER BY max_exec_time DESC, calls DESC LIMIT 20
), top_mean AS (
  SELECT * FROM statements ORDER BY mean_exec_time DESC, calls DESC LIMIT 20
), nested_max AS (
  SELECT * FROM statements WHERE toplevel IS FALSE
    AND (SELECT statement_track='all' FROM settings)
  ORDER BY max_exec_time DESC,calls DESC LIMIT 20
), nested_mean AS (
  SELECT * FROM statements WHERE toplevel IS FALSE
    AND (SELECT statement_track='all' FROM settings)
  ORDER BY mean_exec_time DESC,calls DESC LIMIT 20
), functions AS MATERIALIZED (
  -- Function counters are global per function, not attributable to the ingest role.
  SELECT f.schemaname, f.funcname, pg_catalog.pg_get_function_identity_arguments(f.funcid) AS argument_types,
         f.calls, f.total_time AS total_ms, f.self_time AS self_ms
  FROM pg_catalog.pg_stat_user_functions f
  WHERE f.schemaname='sessions' AND left(f.funcname,7)='ingest_'
    AND (SELECT function_track IN ('pl','all') FROM settings)
)
SELECT jsonb_build_object(
  'extension_exists',true,
  'extension_namespace','extensions',
  'pg_stat_statements_track',(SELECT statement_track FROM settings),
  'track_functions',(SELECT function_track FROM settings),
  'nested_statement_capture_enabled',(SELECT statement_track='all' FROM settings),
  'function_capture_enabled',(SELECT function_track IN ('pl','all') FROM settings),
  'top20_by_max_exec_ms',COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY max_exec_time DESC,calls DESC) FROM top_max t),'[]'::jsonb),
  'top20_by_mean_exec_ms',COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY mean_exec_time DESC,calls DESC) FROM top_mean t),'[]'::jsonb),
  'nested_top20_by_max_exec_ms',CASE WHEN (SELECT statement_track='all' FROM settings)
    THEN COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY max_exec_time DESC,calls DESC) FROM nested_max t),'[]'::jsonb) ELSE NULL END,
  'nested_top20_by_mean_exec_ms',CASE WHEN (SELECT statement_track='all' FROM settings)
    THEN COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY mean_exec_time DESC,calls DESC) FROM nested_mean t),'[]'::jsonb) ELSE NULL END,
  'ingest_function_statistics',CASE WHEN (SELECT function_track IN ('pl','all') FROM settings)
    THEN COALESCE((SELECT jsonb_agg(to_jsonb(f) ORDER BY total_ms DESC,funcname,argument_types) FROM functions f),'[]'::jsonb) ELSE NULL END,
  'statistics_are_cumulative_not_window_delta',true,
  'failed_or_timed_out_executions_may_be_unrecorded',true,
  'function_counters_are_not_role_scoped',true
) AS ingest_claim_statistics;
ROLLBACK;
