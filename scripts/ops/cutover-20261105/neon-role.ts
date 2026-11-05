// stdin: one JSON line {ownerUrl, appPassword}. Creates relayhistory_app_runtime and runs ONLY the reviewed runtime_grants DO block.
// argv[2]: "create" (role must be absent) or "refresh" (role must exist; rerun grants after 0078).
import { readFileSync } from "node:fs";
import { createHash, createHmac, pbkdf2Sync, randomBytes } from "node:crypto";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
const ROLE = "relayhistory_app_runtime";
const SQL_PATH = process.env.HOME + "/relay-briefs/migration-fast-20261103/role-plan-source1545-20261104/runtime_grants.sql";
const grants = readFileSync(SQL_PATH, "utf8");
if (createHash("sha256").update(grants).digest("hex") !== "3ead7380be1a19b8976f076e081d677d5679bef3b8246ec1a121ba4dd0806732") throw new Error("grants_sha");
const mode = process.argv[2];
if (mode !== "create" && mode !== "refresh") throw new Error("mode");
let { ownerUrl, appPassword } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const u = new URL(ownerUrl);
const out: Record<string, unknown> = { at: new Date().toISOString(), mode, host: u.hostname, db: u.pathname.slice(1) };
const owner = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-runtime-role", statement_timeout: 60000 });
ownerUrl = "";
const fail = (code: string) => Object.assign(new Error(code), { code });
await owner.connect();
const flags = "SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication, rolbypassrls FROM pg_roles WHERE rolname=$1";
try {
  const id = (await owner.query("SELECT current_user AS u, current_database() AS d")).rows[0];
  if (id.u !== "neondb_owner" || id.d !== "neondb") throw fail("owner_identity");
  await owner.query("BEGIN");
  const exists = (await owner.query("SELECT 1 FROM pg_roles WHERE rolname=$1", [ROLE])).rows.length === 1;
  if (mode === "create") {
    if (exists) throw fail("role_exists");
    if (!appPassword || appPassword.length < 32) throw fail("password_shape");
    // Neon's control plane accepts only plaintext role passwords (it refuses SCRAM verifiers).
    // Built in RAM, standard-quoted; never logged or written.
    const literal = (await owner.query("SELECT quote_literal($1::text) AS q", [appPassword])).rows[0].q;
    await owner.query("CREATE ROLE " + ROLE + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD " + literal);
  } else if (!exists) throw fail("role_missing");
  await owner.query(grants);
  await owner.query("COMMIT");
  out.flags = (await owner.query(flags, [ROLE])).rows[0];
  out.memberOf = (await owner.query("SELECT g.rolname FROM pg_auth_members a JOIN pg_roles g ON g.oid=a.roleid JOIN pg_roles m ON m.oid=a.member WHERE m.rolname=$1", [ROLE])).rows.map((r: any) => r.rolname);
  out.owns = (await owner.query("SELECT count(*)::int AS n FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE r.rolname=$1", [ROLE])).rows[0].n;
  out.privs = (await owner.query(
    "SELECT has_schema_privilege($1,'sessions','USAGE') AS sessions_usage, has_schema_privilege($1,'sessions','CREATE') AS sessions_create," +
    " has_table_privilege($1,'sessions.auth_sessions','SELECT') AS auth_select, has_table_privilege($1,'sessions.auth_sessions','UPDATE') AS auth_update," +
    " (SELECT count(*)::int FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND c.relkind IN ('r','p') AND has_table_privilege($1,c.oid,'SELECT')) AS readable_tables," +
    " (SELECT count(*)::int FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND c.relname LIKE 'delivery_cutover%' AND has_table_privilege($1,c.oid,'SELECT,INSERT,UPDATE,DELETE')) AS cutover_private_access," +
    " (SELECT count(*)::int FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname LIKE 'owner_%' AND has_function_privilege($1,p.oid,'EXECUTE')) AS owner_fn_execute",
    [ROLE])).rows[0];
} catch (e: any) {
  await owner.query("ROLLBACK").catch(() => {});
  out.error = e?.code ?? "error";
  out.message = String(e?.message ?? "").slice(0, 200);
} finally {
  await owner.end();
}
if (!out.error && appPassword) {
  const app = new pg.Client({ host: u.hostname, port: 5432, database: "neondb", user: ROLE, password: appPassword, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-runtime-proof" });
  try {
    await app.connect();
    out.login = (await app.query("SELECT current_user AS u, (SELECT count(*)::int FROM sessions.auth_sessions WHERE false) AS auth_probe")).rows[0];
    try { await app.query("CREATE TABLE sessions.x_probe(i int)"); out.ddl = "ALLOWED"; } catch (e: any) { out.ddl = e?.code; }
  } catch (e: any) {
    out.loginError = e?.code ?? String(e?.message).slice(0, 120);
  } finally {
    await app.end().catch(() => {});
  }
}
appPassword = "";
console.log(JSON.stringify(out));
