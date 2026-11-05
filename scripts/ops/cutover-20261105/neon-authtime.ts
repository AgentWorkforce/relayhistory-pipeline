// stdin: {ownerUrl}. READ ONLY: time the custody AUTHORITY subquery (run on every forwarded batch) 20 times.
import { readFileSync } from "node:fs";
const RHC = "/home/khaliqgant/Projects/AgentWorkforce/relayhistory-cloud-worktrees/delivery-cutover-0078-release-20261104";
const pg = (await import(RHC + "/node_modules/pg/lib/index.js")).default;
const { CURRENT_CUSTODY_AUTHORITY_SQL } = await import(RHC + "/packages/relayhistory/src/lib/delivery-custody-witness.ts");
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-authtime", statement_timeout: 20000 });
ownerUrl = "";
await c.connect();
await c.query("BEGIN READ ONLY");
const q = "WITH authority AS(" + CURRENT_CUSTODY_AUTHORITY_SQL + ") SELECT encode(sha256(convert_to(value::text,'UTF8')),'hex') AS h FROM authority";
const ms: number[] = [];
for (let i = 0; i < 20; i++) { const t = performance.now(); await c.query(q, ["ep-floral-pond-apkvat38-pooler.c-7.us-east-1.aws.neon.tech"]); ms.push(Math.round(performance.now() - t)); }
const plan = (await c.query("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + q, ["ep-floral-pond-apkvat38-pooler.c-7.us-east-1.aws.neon.tech"])).rows[0]["QUERY PLAN"][0];
await c.query("ROLLBACK");
await c.end();
const walk = (n: any, out: any[] = []) => { out.push({ t: n["Node Type"], rel: n["Relation Name"], ms: n["Actual Total Time"], loops: n["Actual Loops"], rows: n["Actual Rows"] }); for (const k of n.Plans ?? []) walk(k, out); return out; };
const nodes = walk(plan.Plan).sort((a, b) => b.ms * b.loops - a.ms * a.loops).slice(0, 8);
console.log(JSON.stringify({ ms: ms.sort((a, b) => a - b), execMs: plan["Execution Time"], planMs: plan["Planning Time"], top: nodes }));
