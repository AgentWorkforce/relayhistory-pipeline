// stdin: {ownerUrl}. READ ONLY: per-function call rate and mean exec time over argv[2] seconds (pg_stat_statements deltas).
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const secs = Number(process.argv[2] ?? 180);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-rates", statement_timeout: 15000 });
ownerUrl = "";
await c.connect();
const snap = async () => Object.fromEntries((await c.query(`SELECT CASE WHEN query ILIKE '%accept_delivery_batch%' THEN 'accept' ELSE 'replica' END AS k,
   sum(calls)::float8 AS calls, sum(total_exec_time)::float8 AS ms FROM pg_stat_statements
   WHERE query ILIKE 'SELECT sessions.accept_delivery_batch%' OR query ILIKE 'SELECT sessions.apply_neon_replica_v1%' GROUP BY 1`)).rows.map((r: any) => [r.k, r]));
const a = await snap();
await new Promise((r) => setTimeout(r, secs * 1000));
const b = await snap();
await c.end();
const out: Record<string, unknown> = { seconds: secs };
for (const k of ["accept", "replica"]) {
  const dc = (b[k]?.calls ?? 0) - (a[k]?.calls ?? 0), dm = (b[k]?.ms ?? 0) - (a[k]?.ms ?? 0);
  out[k] = { perSec: +(dc / secs).toFixed(3), meanS: dc ? +(dm / dc / 1000).toFixed(2) : null, calls: dc };
}
console.log(JSON.stringify(out));
