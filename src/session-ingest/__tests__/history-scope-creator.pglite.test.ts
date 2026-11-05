import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { captureHistorySource } from "../../shared/history-source-authority.ts";
import { sqlHistoryMaskStore, updateHistoryMask } from "../../shared/history-mask.ts";
setDefaultTimeout(60000);
// Production scope-version creator + final install CAS (sessions.ingest_history_scope_*):
// the fixture INSERTs other suites use are replaced here by the real protocol.
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64);
let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + gen + "','pass')");
});
afterAll(async () => { await db?.close(); });
const run: any = { query: async (sql: string, args: unknown[]) => {
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await db.query(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
} };
const rpc = async (fn: string, p: unknown) => (await run.query("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0].r;
let n = 0;
async function scope(records: string[]) {
  const i = ++n, org = "creator-org-" + i, ws = "creator-ws", identity = { orgId: org, workspaceId: ws, source: "claude", sessionId: "creator-session-" + i };
  const row = { org_id: org, workspace_id: ws, user_id: "creator-user", harness: "claude", session_id: identity.sessionId, machine_id: "srv_origin", created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "creator-fixture", manifest_key: "96bulls/" + org + "/v1/ws=" + ws + "/m=srv_origin/h=claude/d=2026/11/01/s=" + identity.sessionId + "/manifest.json", content_sha256: "\\x" + "a".repeat(64), raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,$3,$4,now(),now())", [org, ws, "claude", identity.sessionId]);
  const mask = await updateHistoryMask(sqlHistoryMaskStore(run, gen, org, ws), { orgId: org, workspaceId: ws }, null, records.map((recordId) => ({ recordId, winner: null })));
  const anchor = (g: string) => "96bulls/" + org + "/upload-generations/g=" + g + "/v1/ws=" + ws + "/m=history_scope/h=claude/d=2026/11/01/s=" + identity.sessionId + "/publications/" + g + "/manifest.json";
  return { identity, mask, anchor, row };
}
const install = (b: any, s: Awaited<ReturnType<typeof scope>>, g = "12345678-1234-1234-1234-123456789abc") => rpc("ingest_history_scope_install", {
  generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count,
  derivedAnchorKey: s.anchor(g), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" });
async function build(s: Awaited<ReturnType<typeof scope>>, keys: string[]) {
  const b = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  for (let i = 0; i < keys.length; i += 500) await rpc("ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: keys.slice(i, i + 500), owners: [] });
  const c = await rpc("ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken });
  return { b, c };
}

test("the production creator installs a ready scope that the existing capture/permit path accepts", async () => {
  const keys = Array.from({ length: 1200 }, (_, i) => "native-" + i);
  const s = await scope(keys), { b, c } = await build(s, keys);
  expect(c.epoch).toMatch(/^[1-9][0-9]*$/);
  const r = await install(b, s);
  expect(r).toMatchObject({ installed: true, retiredAuthorityId: null, maskSha256: s.mask.sha256, recordCount: "1200" });
  const captured = await captureHistorySource(run, s.identity, { schemaGeneration: gen });
  expect(captured.authorityId).toBe(r.authorityId);
  expect(captured.source).toEqual(r.source);
  expect(captured.mask.winners.size).toBe(1200);
  expect((await db.query("SELECT state FROM sessions.ingest_history_scope_builds WHERE build_id=$1", [b.buildId])).rows[0]).toEqual({ state: "installed" });
});

test("a mask record that was never registered as a scope key cannot be installed (coverage)", async () => {
  const s = await scope(["covered", "uncovered"]), { b } = await build(s, ["covered"]);
  await expect(install(b, s)).rejects.toThrow("mask record not registered as a scope key");
  expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0]!.n).toBe(0);
});

test("invalidation between capture and install refuses the stale build", async () => {
  const s = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await db.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId]);
  await expect(install(b, s)).rejects.toThrow("scope source changed since capture");
});

test("a pointer-set change between capture and install refuses", async () => {
  const s = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...s.row, revision: 2, content_sha256: "\\x" + "b".repeat(64) }])]);
  await expect(install(b, s)).rejects.toThrow("scope source changed since capture");
});

test("a newer build supersedes an older one: the older lease can never capture or install", async () => {
  const s = await scope(["k1"]);
  const old = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  const { b } = await build(s, ["k1"]);
  await expect(rpc("ingest_history_scope_capture", { generation: gen, buildId: old.buildId, leaseToken: old.leaseToken })).rejects.toThrow("scope build lease lost");
  await install(b, s);
  await expect(install(b, s)).rejects.toThrow("scope build lease lost");
});

test("an expired lease refuses at install", async () => {
  const s = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await db.query("UPDATE sessions.ingest_history_scope_builds SET lease_until=clock_timestamp()-interval'1 second' WHERE build_id=$1", [b.buildId]);
  await expect(install(b, s)).rejects.toThrow("scope build lease lost");
});

test("reinstall retires the prior version and its live derived objects atomically", async () => {
  const s = await scope(["k1"]);
  const first = await install((await build(s, ["k1"])).b, s);
  await db.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES($1,$2,repeat('c',64),10)", [s.anchor("12345678-1234-1234-1234-123456789abc").replace("manifest.json", "derived/summary-short/11111111-1111-1111-1111-111111111111.bin"), first.authorityId]);
  const second = await install((await build(s, ["k1"])).b, s, "22222222-2222-2222-2222-222222222222");
  expect(second.retiredAuthorityId).toBe(first.authorityId);
  expect((await db.query("SELECT state FROM sessions.ingest_history_scope_versions WHERE authority_id=$1", [first.authorityId])).rows[0]).toEqual({ state: "retired" });
  expect((await db.query("SELECT cleanup_state FROM sessions.ingest_history_derived_objects WHERE authority_id=$1", [first.authorityId])).rows[0]).toEqual({ cleanup_state: "retired" });
  expect((await db.query("SELECT authority_id::text FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0]).toEqual({ authority_id: second.authorityId });
});

test("a foreign-tenant mask root or an anchor outside the tenant prefix is refused", async () => {
  const s = await scope(["k1"]), other = await scope(["k1"]), { b } = await build(s, ["k1"]);
  await expect(rpc("ingest_history_scope_install", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: other.mask.sha256, recordCount: other.mask.count, derivedAnchorKey: s.anchor("12345678-1234-1234-1234-123456789abc"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })).rejects.toThrow("mask root does not match scope");
  await expect(rpc("ingest_history_scope_install", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count, derivedAnchorKey: other.anchor("12345678-1234-1234-1234-123456789abc"), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" })).rejects.toThrow("derived anchor outside tenant");
});

test("only the ingest role may run the creator", async () => {
  const s = await scope(["k1"]);
  await db.exec("BEGIN; SET LOCAL ROLE session_directory_search_worker");
  try { await expect(db.query("SELECT sessions.ingest_history_scope_begin($1::jsonb)", [JSON.stringify({ generation: gen, identity: s.identity })])).rejects.toThrow(); }
  finally { await db.exec("ROLLBACK"); }
});
