-- Aggregate reads for the cutover metrics (docs/cutover-observability.md).
--
-- pipeline_jobs, pipeline_dead_letters and pipeline_events have RLS on and no
-- policy, so a role without BYPASSRLS reads no rows and every metric looks
-- healthy. The sampler must not bypass RLS (helm-chart-chief, 2026-10-30), so it
-- gets these functions instead. They return aggregates and closed labels only:
-- no ids, no attrs, no messages, no error text.
--
-- SECURITY DEFINER, so the owner's view of the tables applies. Each one first
-- refuses to answer if RLS would filter it (an owner that lost the exemption, or
-- FORCE ROW LEVEL SECURITY): a blind read raises CM001 and never returns zero.
--
-- cutover_metrics_reader is NOLOGIN, NOBYPASSRLS and has no table privileges. A
-- login role for the sampler is a separate approved step.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cutover_metrics_reader') THEN
    CREATE ROLE cutover_metrics_reader NOLOGIN NOINHERIT NOBYPASSRLS;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA sessions TO cutover_metrics_reader;

CREATE FUNCTION sessions.cutover_metrics_assert_visible(p_table regclass)
RETURNS void LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
  IF row_security_active(p_table) THEN
    RAISE EXCEPTION 'row security filters this metric read' USING ERRCODE = 'CM001';
  END IF;
END
$fn$;

-- Lag is the oldest ready job. A leased job has visible_at ahead, so a dead
-- consumer shows in oldest_seconds instead.
CREATE FUNCTION sessions.cutover_metrics_jobs()
RETURNS TABLE (stage text, depth bigint, ready bigint, leased bigint, dlq_pending bigint,
               lag_seconds double precision, oldest_seconds double precision)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
#variable_conflict use_column
BEGIN
  PERFORM sessions.cutover_metrics_assert_visible('sessions.pipeline_jobs');
  RETURN QUERY
  SELECT j.stage,
         count(*) FILTER (WHERE NOT j.dlq),
         count(*) FILTER (WHERE NOT j.dlq AND j.visible_at <= now()),
         count(*) FILTER (WHERE NOT j.dlq AND j.visible_at > now() AND j.lease_id IS NOT NULL),
         count(*) FILTER (WHERE j.dlq),
         coalesce(extract(epoch FROM now() - min(j.visible_at) FILTER (WHERE NOT j.dlq AND j.visible_at <= now())), 0)::double precision,
         coalesce(extract(epoch FROM now() - min(j.enqueued_at) FILTER (WHERE NOT j.dlq)), 0)::double precision
    FROM sessions.pipeline_jobs AS j
   GROUP BY j.stage;
END
$fn$;

CREATE FUNCTION sessions.cutover_metrics_dead_letters()
RETURNS TABLE (stage text, state text, depth bigint, oldest_seconds double precision)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
#variable_conflict use_column
BEGIN
  PERFORM sessions.cutover_metrics_assert_visible('sessions.pipeline_dead_letters');
  RETURN QUERY
  SELECT d.stage, d.state, count(*),
         extract(epoch FROM now() - min(d.first_dead_at))::double precision
    FROM sessions.pipeline_dead_letters AS d
   WHERE d.state <> 'resolved'
   GROUP BY d.stage, d.state;
END
$fn$;

-- Redelivery can repeat a row, so transitions are counted by idempotency key.
-- at <= now(): a client clock may run up to 5 minutes ahead.
CREATE FUNCTION sessions.cutover_metrics_events(p_window_seconds integer)
RETURNS TABLE (stage text, status text, events bigint, p95_ms double precision,
               last_seconds double precision)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
#variable_conflict use_column
BEGIN
  PERFORM sessions.cutover_metrics_assert_visible('sessions.pipeline_events');
  IF p_window_seconds IS NULL OR p_window_seconds NOT BETWEEN 1 AND 3600 THEN
    RAISE EXCEPTION 'window must be 1 to 3600 seconds' USING ERRCODE = '22023';
  END IF;
  RETURN QUERY
  SELECT e.stage, e.status,
         count(DISTINCT e.idempotency_key) + count(*) FILTER (WHERE e.idempotency_key IS NULL),
         percentile_cont(0.95) WITHIN GROUP (ORDER BY e.duration_ms),
         extract(epoch FROM now() - max(e.at))::double precision
    FROM sessions.pipeline_events AS e
   WHERE e.at > now() - make_interval(secs => p_window_seconds) AND e.at <= now()
   GROUP BY e.stage, e.status;
END
$fn$;

-- Clients report some of these rows, so a code that isn't code-shaped comes back
-- as 'other': this function can't carry free text out of the error column.
CREATE FUNCTION sessions.cutover_metrics_errors(p_window_seconds integer)
RETURNS TABLE (stage text, status text, code text, events bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $fn$
#variable_conflict use_column
BEGIN
  PERFORM sessions.cutover_metrics_assert_visible('sessions.pipeline_events');
  IF p_window_seconds IS NULL OR p_window_seconds NOT BETWEEN 1 AND 3600 THEN
    RAISE EXCEPTION 'window must be 1 to 3600 seconds' USING ERRCODE = '22023';
  END IF;
  RETURN QUERY
  SELECT f.stage, f.status, f.code, count(*)
    FROM (SELECT e.stage, e.status,
                 CASE WHEN e.error->>'code' ~ '^[a-z][a-z0-9_.-]{0,63}$'
                      THEN e.error->>'code' ELSE 'other' END AS code
            FROM sessions.pipeline_events AS e
           WHERE e.status IN ('error', 'retrying', 'dead_lettered')
             AND e.at > now() - make_interval(secs => p_window_seconds)
             AND e.at <= now()) AS f
   GROUP BY f.stage, f.status, f.code
   ORDER BY count(*) DESC
   LIMIT 50;
END
$fn$;

REVOKE ALL ON FUNCTION sessions.cutover_metrics_assert_visible(regclass),
  sessions.cutover_metrics_jobs(), sessions.cutover_metrics_dead_letters(),
  sessions.cutover_metrics_events(integer), sessions.cutover_metrics_errors(integer)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION sessions.cutover_metrics_jobs(), sessions.cutover_metrics_dead_letters(),
  sessions.cutover_metrics_events(integer), sessions.cutover_metrics_errors(integer)
  TO cutover_metrics_reader;
