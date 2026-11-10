// Activates bulls96_session_ingest LOGIN exactly as supabase/bootstrap/session_ingest_role.sql,
// sets its password as a client-computed SCRAM verifier (plaintext never sent or logged),
// runs session_ingest_role_verify.sql's checks, then proves the login positively and negatively.
// Password: stdin only.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { createHash, createHmac, pbkdf2Sync, randomBytes } from "node:crypto";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
const { parseCliLogin } = await import(WT + "/scripts/schema-release-owner-operator-plan.ts");
const { SUPABASE_PROD_CA_2021 } = await import(WT + "/src/session-pipeline/supabase-ca.ts");
const HOST = "aws-0-us-east-1.pooler.supabase.com", REF = "pyamkrhoyszgzirqqffq", ROLE = "bulls96_session_ingest";
let pw = readFileSync(0, "utf8").replace(/\r?\n$/, "");
if (pw.length < 32 || /\s/.test(pw)) throw new Error("password_shape");
const salt = randomBytes(16), salted = pbkdf2Sync(pw.normalize("NFKC"), salt, 4096, 32, "sha256");
const stored = createHash("sha256").update(createHmac("sha256", salted).update("Client Key").digest()).digest();
const server = createHmac("sha256", salted).update("Server Key").digest();
const verifier = `SCRAM-SHA-256$4096:${salt.toString("base64")}$${stored.toString("base64")}:${server.toString("base64")}`;
const tls = { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST };
const body = (f: string) => readFileSync(`${WT}/supabase/bootstrap/${f}`, "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n");
const roleSql = body("session_ingest_role.sql").replace(/^BEGIN;$/m, "").replace(/^COMMIT;$/m, "");
const verifySql = body("session_ingest_role_verify.sql") + "\n" + body("session_ingest_verify.sql");
const raw = execFileSync("/home/khaliqgant/.local/bin/supabase", ["db","dump","--linked","--project-ref",REF,"--dry-run"], { cwd: WT, encoding: "utf8", stdio: ["ignore","pipe","pipe"], timeout: 30000 });
const login = parseCliLogin(raw);
const owner = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: `cli_login_postgres.${REF}`, password: login.password, ssl: tls, connectionTimeoutMillis: 10000, options: "-c statement_timeout=30000 -c lock_timeout=5000", application_name: "96bulls-bulls-migrator-ingest-login" });
login.password = "";
const out: Record<string, unknown> = { at: new Date().toISOString() };
await owner.connect();
try {
  const before = (await owner.query(`SELECT rolcanlogin, rolinherit, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls, rolconfig FROM pg_roles WHERE rolname=$1`, [ROLE])).rows[0];
  out.before = before;
  await owner.query("BEGIN"); await owner.query("SET LOCAL ROLE postgres");
  await owner.query(roleSql);
  await owner.query(`ALTER ROLE ${ROLE} PASSWORD '${verifier}'`);
  await owner.query("COMMIT");
  await owner.query("BEGIN"); await owner.query("SET LOCAL ROLE postgres");
  await owner.query("SELECT set_config('hoopdb.db_mode', 'supabase', true)");
  await owner.query(verifySql);
  await owner.query("COMMIT");
  out.verify = "session_ingest_role_verify+session_ingest_verify passed";
  out.after = (await owner.query(`SELECT rolcanlogin, rolinherit, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls, rolconfig FROM pg_roles WHERE rolname=$1`, [ROLE])).rows[0];
} catch (e: any) { await owner.query("ROLLBACK").catch(() => {}); out.error = e?.code ?? "error"; out.message = String(e?.message ?? "").slice(0, 200); }
finally { await owner.end(); }
if (!out.error) {
  const ing = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: `${ROLE}.${REF}`, password: pw, ssl: tls, connectionTimeoutMillis: 10000, application_name: "96bulls-bulls-migrator-ingest-login-proof" });
  try {
    await ing.connect();
    out.loginAs = (await ing.query("SELECT current_user AS u, current_setting('statement_timeout') AS st, current_setting('lock_timeout') AS lt")).rows[0];
    const denied: Record<string,string> = {};
    for (const q of ["SELECT 1 FROM sessions.ingest_batches LIMIT 1", "SELECT 1 FROM sessions.session_index LIMIT 1", "CREATE TABLE sessions.x_probe(i int)"]) {
      try { await ing.query(q); denied[q.slice(0,40)] = "ALLOWED"; } catch (e: any) { denied[q.slice(0,40)] = e?.code ?? "err"; }
    }
    out.negative = denied;
  } catch (e: any) { out.loginError = e?.code ?? String(e?.message).slice(0, 120); }
  finally { await ing.end().catch(() => {}); }
}
pw = "";
console.log(JSON.stringify(out));
