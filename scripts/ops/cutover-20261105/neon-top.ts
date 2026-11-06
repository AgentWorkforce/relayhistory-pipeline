// stdin: {ownerUrl}. READ ONLY: top statements by exec time added over 60 s, all roles (pg_stat_statements delta).
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-top", statement_timeout: 20000 });
ownerUrl = "";
await c.connect();
const snap = async () => new Map((await c.query(`SELECT s.userid::text||':'||s.queryid::text AS k, r.rolname, s.calls::float8 AS calls, s.total_exec_time::float8 AS ms,
   left(regexp_replace(s.query,'\\s+',' ','g'),110) AS q FROM pg_stat_statements s JOIN pg_roles r ON r.oid=s.userid`)).rows.map((r: any) => [r.k, r]));
const a = await snap();
await new Promise((r) => setTimeout(r, 60000));
const b = await snap();
await c.end();
const d = [...b].map(([k, r]: any) => ({ role: r.rolname, calls: r.calls - (a.get(k)?.calls ?? 0), secs: +((r.ms - (a.get(k)?.ms ?? 0)) / 1000).toFixed(1), q: r.q }))
  .filter((x) => x.secs > 0).sort((x, y) => y.secs - x.secs).slice(0, 12);
for (const x of d) console.log(JSON.stringify(x));
