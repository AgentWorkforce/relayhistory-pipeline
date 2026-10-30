import pg from "pg";
const c = new pg.Client({ connectionString: process.env.URL, application_name: "96bulls-load-check" , statement_timeout: 30000});
await c.connect();
await c.query("SET default_transaction_read_only = on");
const q = async (sql) => (await c.query(sql)).rows;
const out = {};
out.settings = await q(`SELECT name, setting FROM pg_settings WHERE name IN ('max_connections','shared_buffers','work_mem','neon.max_cluster_size','effective_cache_size')`);
out.activity = await q(`SELECT state, wait_event_type, count(*)::int AS n, max(extract(epoch FROM now() - query_start))::int AS oldest_s
  FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() GROUP BY 1,2 ORDER BY n DESC`);
out.longRunning = await q(`SELECT usename, state, extract(epoch FROM now() - xact_start)::int AS xact_s, left(regexp_replace(query, '\\s+', ' ', 'g'), 90) AS query
  FROM pg_stat_activity WHERE datname = current_database() AND xact_start < now() - interval '30 seconds' AND pid <> pg_backend_pid() ORDER BY xact_start LIMIT 8`);
out.hasStatements = (await q(`SELECT count(*)::int AS n FROM pg_extension WHERE extname = 'pg_stat_statements'`))[0].n;
if (out.hasStatements) out.topQueries = await q(`SELECT calls, round(total_exec_time/1000)::bigint AS total_s, round(mean_exec_time)::bigint AS mean_ms, left(regexp_replace(query, '\\s+', ' ', 'g'), 100) AS query
  FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10`);
out.deadTuples = await q(`SELECT relname, n_live_tup, n_dead_tup, last_autovacuum::text, last_autoanalyze::text FROM pg_stat_user_tables WHERE schemaname='sessions' ORDER BY n_dead_tup DESC LIMIT 6`);
out.dbStats = (await q(`SELECT xact_commit, xact_rollback, blks_read, blks_hit, round(100.0*blks_hit/nullif(blks_hit+blks_read,0),2) AS hit_pct, deadlocks, temp_bytes, stats_reset::text FROM pg_stat_database WHERE datname = current_database()`))[0];
console.log(JSON.stringify(out, null, 1));
await c.end();
