import { afterAll, beforeAll, expect, test, setDefaultTimeout } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

// Minimal SQL-only mask builder (history-mask-v1): leaves sorted by sha256(recordId); a 16-way
// root node above 500 records. Mirrors ingest_history_mask_register's own validation.
import { createHash } from "node:crypto";
const shaHex = (s: string) => createHash("sha256").update(s).digest("hex");
async function registerMask(reg: (doc: unknown) => Promise<any>, orgId: string, workspaceId: string, recordIds: string[]) {
  const sorted = [...recordIds].sort((a, b) => (shaHex(a) < shaHex(b) ? -1 : 1));
  const leaf = (prefix: string, ids: string[]) => reg({ protocol: "history-mask-v1", orgId, workspaceId, prefix, kind: "leaf", count: String(ids.length), items: ids.map((recordId) => ({ recordId, winner: null })) });
  if (sorted.length <= 500) { const r = await leaf("", sorted); return { sha256: r.sha256 as string, count: String(sorted.length) }; }
  const items: { digit: string; sha256: string }[] = [];
  for (const d of "0123456789abcdef") { const ids = sorted.filter((x) => shaHex(x)[0] === d); if (ids.length) items.push({ digit: d, sha256: (await leaf(d, ids)).sha256 }); }
  const r = await reg({ protocol: "history-mask-v1", orgId, workspaceId, prefix: "", kind: "node", count: String(sorted.length), items });
  return { sha256: r.sha256 as string, count: String(sorted.length) };
}
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
  await pool.query("SELECT sessions.canonical_feature_journal_seed(1000,$1)", [gen]);
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
  const mask = await registerMask((doc) => runner().query("SELECT sessions.ingest_history_mask_register($1::jsonb) r", [JSON.stringify({ generation: gen, document: doc })]).then((x: any) => x.rows[0].r), org, ws, ["k1"]);
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

const kinds = async (org: string) => (await pool.query("SELECT kind FROM sessions.canonical_feature_journal WHERE org_id=$1 ORDER BY revision", [org])).rows.map((x) => x.kind);
const installNow = async (s: Awaited<ReturnType<typeof scope>>, g?: string) => { const b = await captured(s); const c = await pool.connect(); try { return await asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(b, s, g))); } finally { c.release(); } };

test.skipIf(!url)("an uncommitted replacement exposes neither half of the pair; commit exposes both, in order", async () => {
  const s = await scope();
  await installNow(s);
  const b = await captured(s), c = await pool.connect();
  try {
    await c.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    await call(c, "ingest_history_scope_install", installArgs(b, s, "22222222-2222-2222-2222-222222222222"));
    expect(await kinds(s.identity.orgId)).toEqual(["publish"]);       // another session sees no partial pair
    await c.query("COMMIT");
  } finally { c.release(); }
  expect(await kinds(s.identity.orgId)).toEqual(["publish", "retire", "publish"]);
});

test.skipIf(!url)("parallel installs of one replacement build: exactly one wins and exactly one retire/publish pair is written", async () => {
  const s = await scope();
  await installNow(s);
  const b = await captured(s), cs = await Promise.all([0, 1, 2, 3].map(() => pool.connect()));
  try {
    const results = await Promise.allSettled(cs.map((c, i) => asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(b, s, "3333333" + i + "-3333-3333-3333-333333333333")))));
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
  } finally { for (const c of cs) c.release(); }
  expect(await kinds(s.identity.orgId)).toEqual(["publish", "retire", "publish"]);
});

test.skipIf(!url)("erase then re-enable: the stale build refuses with no rows; a fresh build replaces with one ordered pair", async () => {
  const s = await scope();
  await installNow(s);
  const stale = await captured(s);
  await pool.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
  const c = await pool.connect();
  try { await expect(asIngest(c, () => call(c, "ingest_history_scope_install", installArgs(stale, s)))).rejects.toThrow(); } finally { c.release(); }
  expect(await kinds(s.identity.orgId)).toEqual(["publish"]);
  await installNow(s, "44444444-4444-4444-4444-444444444444");
  expect(await kinds(s.identity.orgId)).toEqual(["publish", "retire", "publish"]);
});

// Exactly the ingest_lazy_native_slots writes the production ACK makes (slot init, then winner change).
const sha = (x: string) => createHash("sha256").update(x).digest("hex");
async function ackOn(c: pg.PoolClient | pg.Pool, s: Awaited<ReturnType<typeof scope>>, rec: string, revision = 1) {
  const key = sha("slot:" + s.identity.orgId + ":" + rec);
  await c.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES($1,$2::jsonb,$3,$4,'srv_origin',$5,$6,$7,$8) ON CONFLICT DO NOTHING",
    [key, JSON.stringify(["global-native-custody-v1", s.identity.orgId, s.identity.workspaceId, "srv_origin", rec]), s.identity.orgId, s.identity.workspaceId, rec, sha("src"), sha("ctl"), sha("wit")]);
  await c.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1,current_owner='u1',current_source='claude',current_session=$2,current_revision=$3,current_digest=$4,current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=$1",
    [key, s.identity.sessionId, revision, sha("digest:" + revision)]);
}

test.skipIf(!url)("concurrent ACKs on one managed scope both commit: the second waits for the scope lock, never 40001", async () => {
  const s = await scope();
  await installNow(s);
  const version = async () => (await pool.query("SELECT coalesce(max(request_version),0)::int v FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId])).rows[0].v;
  const before = await version();
  const c1 = await pool.connect(), c2 = await pool.connect();
  try {
    await c1.query("BEGIN"); await ackOn(c1, s, "k1");                      // holds the scope lock, uncommitted
    let done = false;
    const second = (async () => { await c2.query("BEGIN"); await ackOn(c2, s, "k2"); await c2.query("COMMIT"); done = true; })();
    await new Promise((r) => setTimeout(r, 300));
    expect(done).toBe(false);                                                 // waiting, not failed
    await c1.query("COMMIT");
    await second;
    expect(done).toBe(true);
  } finally { c1.release(); c2.release(); }
  expect(await version()).toBeGreaterThan(before + 1);                      // both ACKs invalidated the scope
});

test.skipIf(!url)("an install racing an uncommitted ACK fails fast (busy) and never blocks the ACK", async () => {
  const s = await scope();
  await installNow(s);
  const b = await captured(s);
  const c1 = await pool.connect(), c2 = await pool.connect();
  try {
    await c1.query("BEGIN"); await ackOn(c1, s, "k1");
    await expect(asIngest(c2, () => call(c2, "ingest_history_scope_install", installArgs(b, s, "55555555-5555-5555-5555-555555555555")))).rejects.toThrow("logical scope busy");
    await c1.query("COMMIT");
  } finally { c1.release(); c2.release(); }
});
