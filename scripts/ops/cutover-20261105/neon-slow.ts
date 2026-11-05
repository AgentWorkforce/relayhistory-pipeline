// stdin: {ownerUrl}. READ ONLY: the slowest runtime-role statements by max time (pg_stat_statements).
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-slow", statement_timeout: 15000 });
ownerUrl = "";
await c.connect();
const rows = (await c.query(`SELECT r.rolname, s.calls::int, round(s.mean_exec_time)::int AS mean_ms, round(s.max_exec_time)::int AS max_ms,
   round(s.stddev_exec_time)::int AS sd_ms, left(regexp_replace(s.query,'\\s+',' ','g'),220) AS q
  FROM pg_stat_statements s JOIN pg_roles r ON r.oid=s.userid
  WHERE r.rolname='relayhistory_app_runtime' AND s.calls>3 ORDER BY s.mean_exec_time DESC LIMIT 12`)).rows;
await c.end();
for (const r of rows) console.log(JSON.stringify(r));
