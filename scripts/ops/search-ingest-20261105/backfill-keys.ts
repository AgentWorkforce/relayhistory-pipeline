// READ ONLY. stdin: {hoopdbUrl}. Prints live manifest keys indexed since --since, one per line, deduped per
// session scope (one key per org/workspace/harness/session is enough: a pass reconciles the whole scope).
import { readFileSync } from "node:fs";
const pg = (await import(process.cwd() + "/node_modules/pg/lib/index.js")).default;
const since = process.argv[2] ?? "2026-11-04T09:00:00Z";
let { hoopdbUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]);
const ca = readFileSync(process.cwd() + "/certs/supabase-prod-ca-2021.crt", "utf8");
const u = new URL(hoopdbUrl); u.searchParams.delete("sslmode");
const c = new pg.Client({ connectionString: u.toString(), ssl: { ca, rejectUnauthorized: true }, statement_timeout: 60000, application_name: "search-backfill-keys-readonly" });
hoopdbUrl = "";
await c.connect();
await c.query("BEGIN READ ONLY");
const rows = (await c.query(
  `SELECT DISTINCT ON (org_id, workspace_id, harness, session_id) manifest_key
     FROM sessions.session_index WHERE deleted_at IS NULL AND indexed_at >= $1::timestamptz
     ORDER BY org_id, workspace_id, harness, session_id, indexed_at DESC`, [since])).rows;
const total = (await c.query("SELECT count(*)::int n FROM sessions.session_index WHERE deleted_at IS NULL AND indexed_at >= $1::timestamptz", [since])).rows[0].n;
await c.query("ROLLBACK"); await c.end();
console.error(JSON.stringify({ since, liveKeys: total, scopes: rows.length }));
for (const r of rows) console.log(r.manifest_key);
