// stdin: {ownerUrl}. READ ONLY: who holds RowExclusive (or stronger) on 0078's 10 tables, plus the ACLs of the entrypoints.
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const c = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-holders", statement_timeout: 10000 });
ownerUrl = "";
await c.connect();
const T = "('delivery_origins','delivery_receipts','delivery_records','deleted_upload_sessions','session_resets','uploaded_support_identities','delivery_replay_divergence','convergence_events','delivery_catalog_acceptance','delivery_projection_acceptance')";
const holders = (await c.query(`SELECT a.pid, a.usename, round(extract(epoch FROM now()-a.xact_start))::int AS xact_s, a.state,
   left(regexp_replace(a.query,'\\s+',' ','g'),160) AS q, array_agg(DISTINCT c.relname) AS tables
  FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_stat_activity a ON a.pid=l.pid
  WHERE n.nspname='sessions' AND c.relname IN ${T} AND l.mode IN ('RowExclusiveLock','ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock') AND l.granted
  GROUP BY 1,2,3,4,5 ORDER BY 3 DESC`)).rows;
const owners = (await c.query(`SELECT left(regexp_replace(query,'\\s+',' ','g'),400) AS q, round(extract(epoch FROM now()-xact_start))::int AS xact_s FROM pg_stat_activity WHERE usename='neondb_owner' AND pid<>pg_backend_pid() AND xact_start IS NOT NULL ORDER BY xact_start LIMIT 3`)).rows;
const acl = (await c.query(`SELECT p.oid::regprocedure::text AS fn, p.proacl::text AS acl FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
  WHERE n.nspname='sessions' AND p.proname IN ('accept_delivery_batch','apply_neon_replica_v1','set_session_uploads','reset_uploaded_sessions','set_session_uploads_v1','reset_uploaded_sessions_v1') ORDER BY 1`)).rows;
const callers = (await c.query(`SELECT usename, count(*)::int AS n, left(regexp_replace(query,'\\s+',' ','g'),60) AS q FROM pg_stat_activity
  WHERE datname='neondb' AND pid<>pg_backend_pid() AND xact_start IS NOT NULL GROUP BY 1,3 ORDER BY 2 DESC LIMIT 15`)).rows;
await c.end();
console.log(JSON.stringify({ holders, owners, acl, callers }, null, 1));
