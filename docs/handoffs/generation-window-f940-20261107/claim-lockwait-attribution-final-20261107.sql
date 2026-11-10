-- Read-only lock-wait attribution for ingest claim 55P03/57014 (pipeline-lead, 2026-11-07).
-- Catalog/stat views only: no application-row reads, no identifiers emitted beyond
-- role/state/wait class and a truncated md5 of the query text (no query text, PID or tenant values).
-- Run as a role with pg_read_all_stats (or pg_monitor). Repeat 5x, ~10 s apart, capture client-side.
BEGIN READ ONLY;
SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '1s';
SELECT 1 / CASE WHEN current_setting('transaction_read_only') = 'on' THEN 1 ELSE 0 END AS read_only_guard;

-- 1. Who is waiting, on what, and who blocks them (aggregated).
SELECT
  w.usename                                  AS waiter_role,
  w.wait_event_type, w.wait_event,
  b.usename                                  AS blocker_role,
  b.state                                    AS blocker_state,
  left(md5(coalesce(b.query, '')), 12)       AS blocker_query_md5,
  count(DISTINCT w.pid)                      AS waiters,
  count(*)                                   AS blocking_edges,
  max(now() - w.query_start)                 AS max_waiter_query_age,
  max(now() - b.xact_start)                  AS blocker_xact_age
FROM pg_stat_activity w
CROSS JOIN LATERAL unnest(pg_blocking_pids(w.pid)) AS bp(pid)
JOIN pg_stat_activity b ON b.pid = bp.pid
WHERE w.datname = current_database()
GROUP BY 1,2,3,4,5,6
ORDER BY waiters DESC, max_waiter_query_age DESC
LIMIT 50;

-- 2. Long-open transactions (idle in transaction holds row locks on fence owners).
SELECT usename, state, wait_event_type, wait_event,
       now() - xact_start AS xact_age,
       left(md5(coalesce(query, '')), 12) AS query_md5
FROM pg_stat_activity
WHERE datname = current_database() AND xact_start IS NOT NULL AND now() - xact_start > interval '2 seconds'
  AND pid <> pg_backend_pid()
ORDER BY xact_age DESC
LIMIT 30;

-- 3. CONTEXT ONLY (not proof of row contention): lock counts by sessions.* relation.
--    Ungranted 'tuple' locks name the relation whose row a waiter is queued on;
--    transactionid waits carry no relation and are covered by q1.
SELECT l.relation::regclass AS relation, l.mode, l.granted, count(*) AS n
FROM pg_locks l
WHERE l.locktype IN ('relation','tuple','transactionid') AND l.relation IS NOT NULL
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND l.relation::regclass::text LIKE 'sessions.%'
GROUP BY 1,2,3
HAVING NOT bool_and(l.granted) OR count(*) > 5
ORDER BY n DESC
LIMIT 30;
ROLLBACK;
