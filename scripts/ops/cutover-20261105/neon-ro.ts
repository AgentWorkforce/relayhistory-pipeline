// stdin: {ownerUrl}. READ ONLY: was 0078 recorded, and what holds/waits on the delivery tables right now.
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-readonly", statement_timeout: 20000 });
ownerUrl = "";
await c.connect();
const out: Record<string, unknown> = { at: new Date().toISOString() };
try {
  await c.query("BEGIN READ ONLY");
  out.cutoverTable = (await c.query("SELECT to_regclass('sessions.delivery_cutover_state') IS NOT NULL AS present")).rows[0].present;
  const ledger = (await c.query("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name ILIKE '%migration%' AND table_schema NOT IN ('pg_catalog','information_schema')")).rows;
  out.ledgerTables = ledger.map((r: any) => r.table_schema + "." + r.table_name);
  out.activity = (await c.query(`SELECT a.usename, a.application_name, a.state, a.wait_event_type,
      round(extract(epoch FROM now()-a.xact_start))::int AS xact_s, left(regexp_replace(a.query,'\\s+',' ','g'),90) AS q
    FROM pg_stat_activity a WHERE a.datname='neondb' AND a.pid<>pg_backend_pid() AND a.xact_start IS NOT NULL ORDER BY a.xact_start LIMIT 25`)).rows;
  out.deliveryLocks = (await c.query(`SELECT c.relname, l.mode, l.granted, count(*)::int AS n FROM pg_locks l JOIN pg_class c ON c.oid=l.relation
    JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND c.relname IN ('delivery_origins','delivery_receipts','delivery_records','deleted_upload_sessions','session_resets','uploaded_support_identities','delivery_replay_divergence','convergence_events','delivery_catalog_acceptance','delivery_projection_acceptance')
    GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 30`)).rows;
  out.xactAgeHistogram = (await c.query(`SELECT count(*) FILTER (WHERE now()-xact_start < interval '1s')::int AS lt1s, count(*) FILTER (WHERE now()-xact_start BETWEEN interval '1s' AND interval '10s')::int AS s1_10, count(*) FILTER (WHERE now()-xact_start > interval '10s')::int AS gt10s FROM pg_stat_activity WHERE datname='neondb' AND xact_start IS NOT NULL AND pid<>pg_backend_pid()`)).rows[0];
  await c.query("ROLLBACK");
} catch (e: any) { out.error = e?.code; out.message = String(e?.message).slice(0, 200); }
finally { await c.end(); }
console.log(JSON.stringify(out));
