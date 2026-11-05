// stdin: {ownerUrl}. READ ONLY: 60 s live sample of runtime-role active statements >1 s, grouped, with wait events.
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-live", statement_timeout: 5000 });
ownerUrl = "";
await c.connect();
const agg = new Map<string, { n: number; max: number; waits: Record<string, number> }>();
const seen = new Set<string>();
const end = Date.now() + 60000;
while (Date.now() < end) {
  const rows = (await c.query(`SELECT pid, query_start, round(extract(epoch FROM now()-query_start)*1000)::int AS ms, coalesce(wait_event_type,'cpu')||':'||coalesce(wait_event,'') AS w,
      left(regexp_replace(query,'\\s+',' ','g'),70) AS q FROM pg_stat_activity
      WHERE usename='relayhistory_app_runtime' AND state='active' AND now()-query_start > interval '1 second'`)).rows;
  for (const r of rows) {
    const a = agg.get(r.q) ?? { n: 0, max: 0, waits: {} };
    const key = r.pid + "|" + r.query_start;
    if (!seen.has(key)) { seen.add(key); a.n++; }
    a.max = Math.max(a.max, r.ms); a.waits[r.w] = (a.waits[r.w] ?? 0) + 1;
    agg.set(r.q, a);
  }
  await new Promise((res) => setTimeout(res, 1000));
}
await c.end();
for (const [q, a] of [...agg].sort((x, y) => y[1].max - x[1].max)) console.log(JSON.stringify({ q, ...a }));
