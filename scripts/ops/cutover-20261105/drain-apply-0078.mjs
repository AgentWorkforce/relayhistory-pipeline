// SF side. stdin: one JSON line {ownerUrl, appUrl}. Drains Neon delivery writers, applies the
// canonical relayhistory migrations (0078) exactly as Deploy Production does, and ALWAYS restores
// the entrypoint EXECUTE grants. The replica queue is paused/resumed by the caller (Mac).
import { readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
const RHC = "/home/khaliqgant/Projects/AgentWorkforce/relayhistory-cloud-worktrees/delivery-cutover-0078-release-20261104";
const LOG = process.env.HOME + "/relay-briefs/bulls-resume-20261104/drain-apply-0078.log";
const pg = (await import(RHC + "/node_modules/pg/lib/index.js")).default;
process.on("SIGHUP", () => {}); // an ssh drop must not skip the restore below
const { ownerUrl, appUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const lines = [];
const log = (m) => { const l = new Date().toISOString() + " " + m; lines.push(l); console.log(l); writeFileSync(LOG, lines.join("\n") + "\n"); };
const FNS = [
  "sessions.accept_delivery_batch(text,text,text,text,text,text,jsonb,jsonb)",
  "sessions.set_session_uploads(text,text,text,text,jsonb,boolean)",
  "sessions.reset_uploaded_sessions(text,text,text,text)",
];
const TABLES = "('delivery_origins','delivery_receipts','delivery_records','deleted_upload_sessions','session_resets','uploaded_support_identities','delivery_replay_divergence','convergence_events','delivery_catalog_acceptance','delivery_projection_acceptance')";
const owner = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-drain", statement_timeout: 15000 });
await owner.connect();
const before = (await owner.query("SELECT p.oid::regprocedure::text AS fn, p.proacl::text AS acl FROM pg_proc p WHERE p.oid = ANY($1::regprocedure[])", [FNS])).rows;
log("acl before " + JSON.stringify(before));
if (before.length !== 3 || before.some((r) => r.acl !== null && r.acl !== "{neondb_owner=X/neondb_owner,=X/neondb_owner}")) { log("ABORT unexpected ACL; nothing changed"); await owner.end(); process.exit(2); }
const state0 = (await owner.query("SELECT to_regclass('sessions.delivery_cutover_state') IS NOT NULL AS present")).rows[0].present;
if (state0) { log("ABORT 0078 already present; nothing changed"); await owner.end(); process.exit(3); }
let revoked = false, result = "not_run";
try {
  await owner.query("SET lock_timeout='5s'");
  for (const fn of FNS) await owner.query(`REVOKE EXECUTE ON FUNCTION ${fn} FROM PUBLIC`);
  revoked = true;
  log("revoked EXECUTE from PUBLIC on 3 entrypoints");
  const deadline = Date.now() + 180000;
  let holders = -1;
  while (Date.now() < deadline) {
    holders = (await owner.query(`SELECT count(DISTINCT l.pid)::int AS n FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='sessions' AND c.relname IN ${TABLES} AND l.granted AND l.pid<>pg_backend_pid()
      AND l.mode IN ('RowExclusiveLock','ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock')`)).rows[0].n;
    if (holders === 0) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  log("writer holders at apply: " + holders);
  if (holders !== 0) { result = "drain_timeout"; throw new Error("drain_timeout"); }
  const run = spawnSync("/home/khaliqgant/.local/share/mise/installs/node/22.22.3/bin/node", ["scripts/apply-neon-migration.mjs"], {
    cwd: RHC + "/packages/relayhistory", encoding: "utf8", timeout: 600000,
    env: { PATH: "/home/khaliqgant/.local/share/mise/installs/node/22.22.3/bin:/usr/bin:/bin", HOME: process.env.HOME,
      DATABASE_MIGRATIONS_URL: ownerUrl, DATABASE_URL: appUrl, RELAYHISTORY_LEGACY_RETRACT_INDEX: "1" },
  });
  const tail = (run.stdout + "\n" + run.stderr).split("\n").filter((l) => !/postgres(ql)?:\/\//i.test(l)).slice(-12).join(" | ");
  log("helper exit " + run.status + " :: " + tail);
  result = run.status === 0 ? "applied" : "helper_failed";
} catch (e) {
  log("error " + (e?.code ?? e?.message));
} finally {
  if (revoked) {
    for (let i = 0; i < 5; i++) {
      try {
        for (const fn of FNS) await owner.query(`GRANT EXECUTE ON FUNCTION ${fn} TO PUBLIC`);
        log("restored EXECUTE to PUBLIC");
        break;
      } catch (e) { log("restore retry " + (e?.code ?? e?.message)); await new Promise((r) => setTimeout(r, 2000)); }
    }
  }
  const after = (await owner.query("SELECT p.oid::regprocedure::text AS fn, has_function_privilege('app_blue', p.oid, 'EXECUTE') AS app_blue_exec FROM pg_proc p WHERE p.oid = ANY($1::regprocedure[])", [FNS])).rows;
  const st = (await owner.query("SELECT to_regclass('sessions.delivery_cutover_state') IS NOT NULL AS present")).rows[0].present
    ? (await owner.query("SELECT sessions.delivery_cutover_status() AS s")).rows[0].s : null;
  log("final result=" + result + " app_blue_exec=" + JSON.stringify(after) + " cutover=" + JSON.stringify(st));
  await owner.end();
}
