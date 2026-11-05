// stdin: {ownerUrl}. READ ONLY: which roles are running delivery/replica calls now, plus error-ish signals.
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-who", statement_timeout: 10000 });
ownerUrl = "";
await c.connect();
const samples: unknown[] = [];
for (let i = 0; i < 10; i++) {
  samples.push((await c.query(`SELECT usename, left(regexp_replace(query,'\\s+',' ','g'),45) AS q, count(*)::int AS n FROM pg_stat_activity
    WHERE datname='neondb' AND pid<>pg_backend_pid() AND state='active' AND usename IN ('app_blue','relayhistory_app_runtime') GROUP BY 1,2 ORDER BY 3 DESC LIMIT 6`)).rows);
  await new Promise((r) => setTimeout(r, 3000));
}
const conns = (await c.query("SELECT usename, count(*)::int AS n FROM pg_stat_activity WHERE datname='neondb' AND usename IN ('app_blue','relayhistory_app_runtime') GROUP BY 1")).rows;
await c.end();
console.log(JSON.stringify({ conns, samples }));
