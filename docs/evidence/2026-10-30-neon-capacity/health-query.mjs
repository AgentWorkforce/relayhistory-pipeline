import pg from "pg";
const c = new pg.Client({ connectionString: process.env.URL, application_name: "96bulls-health-check", statement_timeout: 20000 });
const t0 = Date.now(); await c.connect();
await c.query("SET default_transaction_read_only = on");
const q = async (sql) => (await c.query(sql)).rows;
console.log(JSON.stringify({
  connectMs: Date.now() - t0,
  postmasterStart: (await q("SELECT pg_postmaster_start_time()::text AS t"))[0].t,
  sharedBuffers: (await q("SHOW shared_buffers"))[0].shared_buffers,
  activity: await q(`SELECT state, count(*)::int AS n FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() GROUP BY 1 ORDER BY 2 DESC`),
  pgss: (await q("SELECT extversion FROM pg_extension WHERE extname='pg_stat_statements'"))[0]?.extversion ?? null,
  recentErrorsSinceStart: (await q(`SELECT xact_commit, xact_rollback, deadlocks FROM pg_stat_database WHERE datname = current_database()`))[0],
}, null, 1));
await c.end();
