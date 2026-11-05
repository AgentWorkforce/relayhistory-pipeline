import { afterAll, beforeAll, expect, test, setDefaultTimeout } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { sqlHistoryMaskStore, updateHistoryMask } from "../../shared/history-mask.ts";
setDefaultTimeout(120000);
// Real PostgreSQL races for the production scope creator (PGlite has one connection).
const url = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64);
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
let admin: pg.Client, pool: pg.Pool, name = "";
beforeAll(async () => {
  if (!url) return;
  admin = new pg.Client({ connectionString: url }); await admin.connect(); name = "scope_creator_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name); const u = new URL(url); u.pathname = "/" + name;
  pool = new pg.Pool({ connectionString: u.href, max: 16 });
  await pool.query(read("supabase/tests/supabase_shim.sql")); await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
  for (const f of ["session_directory_search_worker.sql", "session_token_verifier.sql", "session_ingest_roles.sql"]) await pool.query(read("supabase/bootstrap/" + f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pool.query(read("supabase/migrations/" + f));
  await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
  await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')", [gen]);
});
afterAll(async () => { if (!url) return; await pool?.end(); await admin.query("DROP DATABASE IF EXISTS " + name); await admin.end(); });
async function asIngest<T>(c: pg.PoolClient, work: () => Promise<T>, commit = true): Promise<T> {
  await c.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await work(); if (commit) await c.query("COMMIT"); return r; } catch (e) { await c.query("ROLLBACK"); throw e; }
}
const call = async (c: pg.PoolClient | pg.Pool, fn: string, p: unknown) => (await c.query("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0].r;
const runner = () => ({ query: async (sql: string, args: unknown[]) => { const c = await pool.connect(); try { return await asIngest(c, () => c.query(sql, args as never)); } finally { c.release(); } } });
let n = 0;
async function scope() {
  const i = ++n, org = "race-org-" + i, ws = "race-ws", identity = { orgId: org, workspaceId: ws, source: "claude", sessionId: "race-session-" + i };
  const row = { org_id: org, workspace_id: ws, user_id: "race-user", harness: "claude", session_id: identity.sessionId, machine_id: "srv_origin", created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "race-fixture", manifest_key: "96bulls/" + org + "/v1/ws=" + ws + "/m=srv_origin/h=claude/d=2026/11/01/s=" + identity.sessionId + "/manifest.json", content_sha256: "\\x" + "a".repeat(64), raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await pool.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await pool.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,$3,$4,now(),now())", [org, ws, "claude", identity.sessionId]);
  const mask = await updateHistoryMask(sqlHistoryMaskStore(runner() as any, gen, org, ws), { orgId: org, workspaceId: ws }, null, [{ recordId: "k1", winner: null }]);
  const anchor = (g: string) => "96bulls/" + org + "/upload-generations/g=" + g + "/v1/ws=" + ws + "/m=history_scope/h=claude/d=2026/11/01/s=" + identity.sessionId + "/publications/" + g + "/manifest.json";
  return { identity, mask, anchor };
}
async function captured(s: Awaited<ReturnType<typeof scope>>) {
  const c = await pool.connect();
  try {
    const b = await asIngest(c, () => call(c, "ingest_history_scope_begin", { generation: gen, identity: s.identity }));
    await asIngest(c, () => call(c, "ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [] }));
    await asIngest(c, () => call(c, "ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken }));
    return b;
  } finally { c.release(); }
}
const installArgs = (b: any, s: Awaited<ReturnType<typeof scope>>, g = "12345678-1234-1234-1234-123456789abc") => ({ generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count, derivedAnchorKey: s.anchor(g), bucket: "race-fixture", region: "us-east-1", keyRoot: "96bulls" });

test.skipIf(!url)("install never blocks behind an in-flight invalidation and refuses once it commits", async () => {
  const s = await scope(), b = await captured(s);
  const holder = await pool.connect(), installer = await pool.connect();
  try {
    await holder.query("BEGIN");
    await holder.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
    const t0 = Date.now();
    await expect(asIngest(installer, () => call(installer, "ingest_history_scope_install", installArgs(b, s)))).rejects.toThrow("logical scope busy");
    expect(Date.now() - t0).toBeLessThan(5000);
    await holder.query("COMMIT");
    await expect(asIngest(installer, () => call(installer, "ingest_history_scope_install", installArgs(b, s)))).rejects.toThrow("scope source changed since capture");
  } finally { holder.release(); installer.release(); }
});

test.skipIf(!url)("a build begun during an uncommitted install binds the pre-install authority and can never overwrite it", async () => {
  const s = await scope(), a = await captured(s);
  const ca = await pool.connect(), cb = await pool.connect();
  try {
    await ca.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    const installed = await call(ca, "ingest_history_scope_install", installArgs(a, s)); // holds A's build row + current row
    const late = asIngest(cb, () => call(cb, "ingest_history_scope_begin", { generation: gen, identity: s.identity })); // blocks on A's build row
    await new Promise((r) => setTimeout(r, 300));
    await ca.query("COMMIT");
    const b = await late;
    expect(b.expectedAuthorityId).toBeNull(); // read before A committed
    await asIngest(cb, () => call(cb, "ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: ["k1"], owners: [] }));
    await asIngest(cb, () => call(cb, "ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken }));
    await expect(asIngest(cb, () => call(cb, "ingest_history_scope_install", installArgs(b, s, "22222222-2222-2222-2222-222222222222")))).rejects.toThrow("scope source changed since capture");
    const cur = (await pool.query("SELECT authority_id::text a FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].a;
    expect(cur).toBe(installed.authorityId);
  } finally { ca.release(); cb.release(); }
});

test.skipIf(!url)("parallel installs of the same captured build: exactly one wins", async () => {
  const s = await scope(), b = await captured(s);
  const cs = await Promise.all([0, 1, 2, 3].map(() => pool.connect()));
  try {
    const results = await Promise.allSettled(cs.map((c) => asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(b, s)))));
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
    for (const r of results) if (r.status === "rejected") expect(String((r.reason as Error).message)).toMatch(/scope build lease lost|logical scope busy/);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.ingest_history_scope_versions WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(1);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.canonical_feature_journal WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(1);
  } finally { for (const c of cs) c.release(); }
});

test.skipIf(!url)("parallel installs of different scopes get distinct, gap-free journal revisions", async () => {
  const scopes = await Promise.all([0, 1, 2, 3, 4, 5].map(() => scope()));
  const builds = await Promise.all(scopes.map((s) => captured(s)));
  const before = (await pool.query("SELECT coalesce(max(revision),0)::int m FROM sessions.canonical_feature_journal")).rows[0].m;
  const cs = await Promise.all(scopes.map(() => pool.connect()));
  try {
    const results = await Promise.allSettled(cs.map((c, i) => asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(builds[i], scopes[i])))));
    expect(results.every((r) => r.status === "fulfilled")).toBe(true);
    const revs = (await pool.query("SELECT revision::int r FROM sessions.canonical_feature_journal WHERE revision>$1 ORDER BY revision", [before])).rows.map((x) => x.r);
    expect(revs).toEqual(Array.from({ length: 6 }, (_, i) => before + 1 + i));
  } finally { for (const c of cs) c.release(); }
});
