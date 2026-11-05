// stdin: {ownerUrl}. READ ONLY post-0078 checks: traffic resumed, cutover status, runtime-role private capability.
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-post0078", statement_timeout: 10000 });
ownerUrl = "";
await c.connect();
await c.query("BEGIN READ ONLY");
const R = "relayhistory_app_runtime";
const out: Record<string, unknown> = {};
out.status = (await c.query("SELECT sessions.delivery_cutover_status() AS s")).rows[0].s;
out.legacyOpen = (await c.query("SELECT sessions.delivery_cutover_legacy_open() AS o")).rows[0].o;
out.writersNow = (await c.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE query ILIKE '%accept_delivery_batch%' AND pid<>pg_backend_pid()")).rows[0].n;
// (receipt-age column check removed: aborted the read-only txn)
for (const role of [R, "app_blue"]) out[role] = (await c.query(
  `SELECT has_table_privilege($1,'sessions.delivery_cutover_state','SELECT,INSERT,UPDATE,DELETE') AS state,
          has_table_privilege($1,'sessions.delivery_cutover_permits','SELECT,INSERT,UPDATE,DELETE') AS permits,
          has_function_privilege($1,'sessions.owner_delivery_cutover_v1(text,text,text)','EXECUTE') AS owner_fn,
          has_function_privilege($1,'sessions.delivery_cutover_status()','EXECUTE') AS status_fn,
          has_function_privilege($1,'sessions.accept_delivery_batch(text,text,text,text,text,text,jsonb,jsonb)','EXECUTE') AS accept_fn,
          has_table_privilege($1,'sessions.auth_sessions','SELECT') AS auth`, [role])).rows[0];
await c.query("ROLLBACK");
await c.end();
console.log(JSON.stringify(out));
