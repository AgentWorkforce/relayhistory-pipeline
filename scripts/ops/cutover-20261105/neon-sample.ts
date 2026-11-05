// stdin: {ownerUrl}. READ ONLY sampler: every 2 s for N seconds, delivery-writer transactions and their ages.
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const seconds = Number(process.argv[2] ?? 60);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-sampler", statement_timeout: 5000 });
ownerUrl = "";
await c.connect();
const rows: unknown[] = [];
const until = Date.now() + seconds * 1000;
while (Date.now() < until) {
  const r = (await c.query(`SELECT count(*)::int AS n, coalesce(max(extract(epoch FROM now()-xact_start)),0)::numeric(6,1) AS max_s,
     coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM now()-xact_start)),0)::numeric(6,1) AS p50_s,
     count(*) FILTER (WHERE query ILIKE '%apply_neon_replica%')::int AS replica,
     count(*) FILTER (WHERE now()-xact_start > interval '10 s')::int AS over10
     FROM pg_stat_activity WHERE datname='neondb' AND pid<>pg_backend_pid() AND xact_start IS NOT NULL
     AND (query ILIKE '%accept_delivery_batch%' OR query ILIKE '%apply_neon_replica%' OR query ILIKE '%set_session_uploads%' OR query ILIKE '%reset_uploaded_sessions%')`)).rows[0];
  rows.push({ t: new Date().toISOString().slice(11, 19), ...r });
  await new Promise((res) => setTimeout(res, 2000));
}
await c.end();
const clear = rows.filter((r: any) => r.over10 === 0).length;
console.log(JSON.stringify({ samples: rows.length, samplesWithNoWriterOver10s: clear, rows }));
