// SF side. stdin: {appUrl} (relayhistory_app_runtime, the exact URL that becomes NEON_APP_DATABASE_URL).
// Captures DELIVERY_CUSTODY_SOURCE on the real fresh login after the final schema, using the
// canonical CURRENT_CUSTODY_AUTHORITY_SQL from relayhistory-cloud main (e6bf926 == worktree bytes).
import { readFileSync } from "node:fs";
const RHC = "/home/khaliqgant/Projects/AgentWorkforce/relayhistory-cloud-worktrees/delivery-cutover-0078-release-20261104";
const pg = (await import(RHC + "/node_modules/pg/lib/index.js")).default;
// Import the evaluated constant (it interpolates helpers) exactly as the Worker builds it.
const { CURRENT_CUSTODY_AUTHORITY_SQL: AUTH_SQL } = await import(RHC + "/packages/relayhistory/src/lib/delivery-custody-witness.ts");
if (typeof AUTH_SQL !== "string" || AUTH_SQL.length < 100) throw new Error("authority_sql_import");
let { appUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const u = new URL(appUrl);
if (decodeURIComponent(u.username) !== "relayhistory_app_runtime") throw new Error("not_runtime_role");
const c = new pg.Client({ connectionString: appUrl, ssl: { rejectUnauthorized: true }, application_name: "bulls-migrator-custody-capture", statement_timeout: 10000 });
appUrl = "";
await c.connect();
const q = "WITH authority AS(" + AUTH_SQL + ") SELECT value AS authority, encode(sha256(convert_to(value::text,'UTF8')),'hex') AS source_identity FROM authority";
// The app binds $1 = hostname (bindCustodyQuery); count placeholders to bind exactly what the app binds.
const n = new Set(AUTH_SQL.match(/\$\d+/g) ?? []).size;
if (n !== 1) throw new Error("unexpected_placeholder_count_" + n);
const row = (await c.query(q, [u.hostname])).rows[0];
const who = (await c.query("SELECT current_user AS u, session_user AS s")).rows[0];
await c.end();
console.log(JSON.stringify({
  custodySource: { version: 1, hostname: u.hostname, sourceIdentitySha256: row.source_identity },
  login: who, authorityKeys: Object.keys(row.authority ?? {}),
}));
