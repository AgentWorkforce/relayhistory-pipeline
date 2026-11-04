/** Worker protocol tests: real baseline SQL/encryption/object-store modules,
 * with a controlled Native authority PORT. Not a proof of unfinished 040600 SQL.
 */
import { afterEach, expect, it } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { publicationManifestKey, uploadGenerationPrefix, rawChunkKey } from "../../../packages/object-store/src/index.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { PipelineError, type ManifestV1, type ObjectStore, type StageMessage } from "../../shared/vendor.ts";
import { bobCostasHandler } from "../handler.ts";
import { refereeHandler } from "../../referee/handler.ts";
import { extractiveSummarizer } from "../summarizer.ts";
import { readSummaryObject } from "../summary-objects.ts";

const root = join(import.meta.dir, "../../..");
const generation = "11111111-1111-1111-1111-111111111111", schemaGeneration = "a".repeat(64);
const loc = { prefix: uploadGenerationPrefix("96bulls/o", generation), workspaceId: "w", machineId: "srv_m", harness: "claude", sessionId: "s1", startDate: "2026-11-04" };
const key = (publication: string) => String(publicationManifestKey(loc, publication));
const oldKey = key("22222222-2222-2222-2222-222222222222"), newKey = key("33333333-3333-3333-3333-333333333333");
const message = (manifestKey = oldKey): StageMessage => ({ payload: { manifestKey }, ctx: { orgId: "o", workspaceId: "w", harness: "claude", sessionId: "s1", machineId: "srv_m" } }) as StageMessage;
const recorder = { log: { info() {} } } as never;
const manifest = (revision = 2, scrubbed = true): ManifestV1 => ({
  revision, trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b",
  session: { workspace_id: "w", machine_id: "srv_m", harness: "claude", session_id: "s1", started_at: "2026-11-04T00:00:00Z" },
  raw: { format: "jsonl", content_sha256: "c".repeat(64), raw_bytes: 60, state: "closed", updated_at: "2026-11-04T00:00:00Z",
    chunks: [{ seq: 0, start_offset: 0, sha256_8: "abcdef12" }], ...(scrubbed ? { scrub: { ruleset: "fixture", version: 1 } } : {}) },
}) as unknown as ManifestV1;
const opened: PGlite[] = [], dirs: string[] = [];
afterEach(async () => { for (const db of opened.splice(0)) await db.close(); for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

async function fixture() {
  const pg = new PGlite({ extensions: { btree_gin, pg_trgm } }); opened.push(pg);
  for (const f of ["supabase/tests/supabase_shim.sql", "supabase/bootstrap/session_directory_search_worker.sql", "supabase/bootstrap/session_token_verifier.sql", "supabase/bootstrap/session_ingest_roles.sql"])
    await pg.exec(readFileSync(join(root, f), "utf8"));
  for (const dir of ["supabase/migrations", "src/hoopdb/migrations"])
    // This is a controlled authority PORT test. The real 040600 pointer trigger
    // requires real registry/head setup and belongs to the separate SQL E2E.
    for (const file of readdirSync(join(root, dir)).filter(f => f.endsWith(".sql") && f !== "20261104060000_ingest_canonical_publication.sql").sort())
      await pg.exec(readFileSync(join(root, dir, file), "utf8"));
  await pg.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
    storage_provider,bucket,manifest_key,trace_id,revision,content_sha256)
    VALUES('o','w','claude','s1','srv_m','u',now(),now(),'s3','fixture',$1,$2,2,decode($3,'hex'))`, [oldKey, manifest().trace_id, "c".repeat(64)]);
  const active = { key: oldKey, revision: "2", current: true, ready: true, registrationAllowed: true };
  const events: string[] = [], registered = new Map<string, { sha256: string; bytes: number }>();
  const hooks: { beforePut?: () => Promise<void>; beforeRead?: () => Promise<void>; beforeConnect?: () => Promise<void> } = {};
  let inTx = false;
  const authority = (p: Record<string, unknown>) => {
    if (!active.ready || p.schemaGeneration !== schemaGeneration) throw Object.assign(new Error("fixture generation gate closed"), { code: "55000" });
    expect([p.orgId, p.workspaceId, p.harness, p.sessionId, p.machineId]).toEqual(["o", "w", "claude", "s1", "srv_m"]);
    return { managed: true, current: active.current && p.manifestKey === active.key && p.revision === active.revision, generation, revision: active.revision };
  };
  const q = async (sql: string, params: unknown[] = []) => {
    if (sql === "BEGIN") { expect(inTx).toBe(false); inTx = true; events.push("begin"); }
    if (sql === "COMMIT" || sql === "ROLLBACK") { events.push(sql.toLowerCase()); inTx = false; }
    if (sql.includes("sessions.session_publication_probe") || sql.includes("sessions.session_publication_lock")) {
      const lock = sql.includes("_lock"); events.push(lock ? "lock" : "probe");
      expect(inTx).toBe(lock);
      return { rows: [{ result: authority(JSON.parse(String(params[0]))) }] };
    }
    if (sql.includes("session_publication_register_object")) {
      expect(inTx).toBe(false); const p = JSON.parse(String(params[0]));
      if (!active.registrationAllowed || !authority(p.pointer).current) throw Object.assign(new Error("fixture registration refused"), { code: "40001" });
      registered.set(p.key, { sha256: p.sha256, bytes: p.bytes }); events.push("register");
      return { rows: [{ result: { registered: true } }] };
    }
    if (sql.includes("set_session_state_for_publication")) {
      expect(inTx).toBe(true); expect(params[9]).toBe(schemaGeneration); events.push("qualified-state");
      const applied = active.current && params[5] === active.key && String(params[6]) === active.revision;
      if (applied) await pg.query("UPDATE sessions.session_index SET state=$1,state_classified_at=$2", [params[7], params[8]]);
      return { rows: [{ applied }] };
    }
    if (sql.includes("sessions.set_session_state(")) throw new Error("unqualified managed state write");
    if (sql.includes("FOR UPDATE")) { events.push("index-lock"); expect(events.lastIndexOf("lock")).toBeGreaterThan(events.lastIndexOf("begin")); }
    if (sql.includes("INSERT INTO sessions.session_summaries")) { expect(inTx).toBe(true); events.push("summary-sql"); }
    return { rows: (await pg.query(sql, params as never)).rows };
  };
  const db = { query: q, pool: { connect: async () => { await hooks.beforeConnect?.(); return { query: q, release() {} }; } }, end: async () => {} } as never;
  const outputRoot = mkdtempSync(join(tmpdir(), "bob-managed-port-")); dirs.push(outputRoot);
  const base = createFsStore({ root: outputRoot });
  const store = new Proxy(base, { get(target, property) {
    if (property === "put") return async (...args: Parameters<ObjectStore["put"]>) => {
      const [k, body, options] = args;
      expect(inTx).toBe(false); expect(registered.get(String(k))).toEqual({ sha256: options?.contentSha256, bytes: (body as Uint8Array).byteLength });
      expect(options?.ifNoneMatch).toBe(true); expect(options?.ifMatch).toBeUndefined();
      await hooks.beforePut?.(); events.push("put"); return target.put(...args);
    };
    if (property === "getObject") return async (...args: Parameters<ObjectStore["getObject"]>) => { expect(inTx).toBe(false); await hooks.beforeRead?.(); return target.getObject(...args); };
    const value = Reflect.get(target, property, target); return typeof value === "function" ? value.bind(target) : value;
  } }) as ObjectStore;
  await base.put(rawChunkKey(loc, 0, 0, "abcdef12", "jsonl"), zstdCompressSync(Buffer.from('{"role":"user","content":"synthetic managed question"}\n')));
  const keyring = await masterKeyring(parseMasterKeys(`fixture:${"a".repeat(64)}`));
  const deps = { db, store, keyring, schemaGeneration, summarizer: extractiveSummarizer };
  const run = (m = manifest(), k = oldKey, extra = {}) => bobCostasHandler("short", { ...deps, ...extra, reader: { read: async () => ({ manifest: m }) } as never })(message(k), recorder);
  const activate = async (m: ManifestV1, k: string) => {
    active.key = k; active.revision = String(m.revision); active.current = true;
    await pg.query("UPDATE sessions.session_index SET manifest_key=$1,revision=$2,content_sha256=decode($3,'hex'),deleted_at=NULL", [k, m.revision, m.raw.content_sha256]);
  };
  const pointer = async () => (await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_summaries WHERE org_id='o'")).rows[0];
  return { pg, db, store, base, keyring, deps, run, activate, pointer, active, registered, events, hooks };
}
function barrier() { let enter!: () => void, release!: () => void; return { started: new Promise<void>(r => enter = r), wait: new Promise<void>(r => release = r), enter: () => enter(), release: () => release() }; }

it("registers exact encrypted bytes before immutable PUT; final lock/write share SQL only; replay does not rewrite", async () => {
  const f = await fixture(); await f.run();
  const p = (await f.pointer())!;
  expect(String(p.object_key)).toContain("/publications/22222222-2222-2222-2222-222222222222/derived/summary-short/");
  expect(f.events.indexOf("register")).toBeLessThan(f.events.indexOf("put"));
  expect(f.events.indexOf("put")).toBeLessThan(f.events.indexOf("lock"));
  const read = await readSummaryObject({ store: f.base, keyring: f.keyring, ids: { orgId: "o", workspaceId: "w" }, key: String(p.object_key), sha256: String(p.object_sha256), kind: "short", revision: 2 });
  expect(read?.summary).toContain("synthetic managed question");
  expect(await f.run()).toEqual({ status: "skipped", reason: "summary_current" });
  expect((await f.pointer())!.object_sha256).toBe(p.object_sha256);
  expect(f.events.filter(e => e === "put")).toHaveLength(1);
}, 30_000);

for (const ready of [true, false]) it(`old ${ready ? "model result" : "nonready clear"} cannot replace a fresh publication`, async () => {
  const f = await fixture(), b = barrier(); let waitOnce = true;
  const slow = { ...f.keyring, currentKey: async (ids: { orgId: string; workspaceId: string }) => { if (waitOnce) { waitOnce = false; b.enter(); await b.wait; } return f.keyring.currentKey(ids); } };
  const slowModel = { ...extractiveSummarizer, summarize: async (...args: Parameters<typeof extractiveSummarizer.summarize>) => {
    b.enter(); await b.wait; return extractiveSummarizer.summarize(...args);
  } };
  const pending = f.run(manifest(2, ready), oldKey, ready ? { summarizer: slowModel } : { keyring: slow }); await b.started;
  await f.activate(manifest(3), newKey); await f.run(manifest(3), newKey); const fresh = (await f.pointer())!;
  b.release(); expect(await pending).toEqual({ status: "skipped", reason: "publication_not_current" });
  expect(await f.pointer()).toEqual(fresh); expect(f.registered.size).toBe(1);
}, 30_000);

it("Native reset authority prevents recreation even when the old index key/revision/hash remain live", async () => {
  const f = await fixture(); f.active.current = false;
  expect(await f.run()).toEqual({ status: "skipped", reason: "publication_not_current" });
  expect(await f.pointer()).toBeUndefined(); expect(f.registered.size).toBe(0); expect(f.events).toEqual(["probe"]);
  expect((await f.pg.query("SELECT deleted_at,revision,manifest_key FROM sessions.session_index")).rows[0])
    .toMatchObject({ deleted_at: null, revision: 2, manifest_key: oldKey });
}, 30_000);

it("old decrypt/read-error path leaves fresh pointer and immutable object intact", async () => {
  const f = await fixture(); await f.run(); const b = barrier(); let once = true;
  f.hooks.beforeRead = async () => { if (once) { once = false; b.enter(); await b.wait; throw new PipelineError("fixture_read_error", "fixture read error", true); } };
  const pending = f.run(); await b.started;
  await f.activate(manifest(3), newKey); await f.run(manifest(3), newKey); const fresh = (await f.pointer())!;
  b.release(); await expect(pending).rejects.toMatchObject({ code: "fixture_read_error" });
  expect(await f.pointer()).toEqual(fresh);
  expect(await f.base.head(String(fresh.object_key) as never)).not.toBeNull();
}, 30_000);

it("same revision/hash under a new publication cannot reuse the old qualified object", async () => {
  const f = await fixture(); await f.run(); const before = (await f.pointer())!;
  await f.activate(manifest(), newKey); await f.run(manifest(), newKey);
  expect((await f.pointer())!.object_key).not.toBe(before.object_key); expect(f.registered.size).toBe(2);
}, 30_000);

it("registration refusal prevents PUT; closed compiled generation prevents all downstream work", async () => {
  const f = await fixture(); f.active.registrationAllowed = false;
  await expect(f.run()).rejects.toMatchObject({ code: "40001" }); expect(f.events).not.toContain("put"); expect(await f.pointer()).toBeUndefined();
  f.active.ready = false; const before = f.events.length;
  await expect(f.run()).rejects.toMatchObject({ code: "55000" }); expect(f.events.slice(before)).toEqual(["probe"]);
  const last = f.events.length;
  await expect(f.run(manifest(), oldKey, { schemaGeneration: undefined })).rejects.toMatchObject({ code: "derived_managed_authority_unavailable" });
  expect(f.events).toHaveLength(last);
}, 30_000);

it("a current nonready result clears SQL pointers without rewriting previous immutable bytes", async () => {
  const f = await fixture(); await f.run(); const old = (await f.pointer())!;
  const oldHead = await f.base.head(String(old.object_key) as never);
  await f.activate(manifest(3, false), newKey); await f.run(manifest(3, false), newKey);
  expect(await f.pointer()).toMatchObject({ revision: 3, status: "quarantined", object_key: null, object_sha256: null, key_id: null });
  expect(await f.base.head(String(old.object_key) as never)).toEqual(oldHead);
  expect(f.registered.size).toBe(1);
}, 30_000);

it("reset between registration and PUT cannot recreate SQL metadata; private-object GC remains Native's proof", async () => {
  const f = await fixture(); f.hooks.beforePut = async () => {
    f.active.current = false; await f.pg.query("UPDATE sessions.session_index SET deleted_at=now()");
  };
  expect(await f.run()).toEqual({ status: "skipped", reason: "publication_not_current" });
  expect(f.registered.size).toBe(1); expect(f.events).toContain("put"); expect(await f.pointer()).toBeUndefined();
}, 30_000);

it("Referee uses qualified state and verdict in one fenced transaction; a delayed old verdict does neither", async () => {
  const f = await fixture(); const b = barrier(); let once = true;
  f.hooks.beforeConnect = async () => { if (once) { once = false; b.enter(); await b.wait; } };
  const run = (m: ManifestV1, k: string) => refereeHandler({ db: f.db, schemaGeneration,
    reader: { read: async () => ({ manifest: m }) } as never })(message(k), recorder);
  const old = run(manifest(), oldKey); await b.started;
  await f.activate(manifest(3), newKey); await run(manifest(3), newKey);
  const before = (await f.pg.query("SELECT revision,state FROM sessions.session_verdicts")).rows;
  b.release(); expect(await old).toEqual({ status: "skipped", reason: "publication_or_index_not_current" });
  expect((await f.pg.query("SELECT revision,state FROM sessions.session_verdicts")).rows).toEqual(before);
  expect(before).toEqual([{ revision: 3, state: "done" }]); expect(f.events.filter(e => e === "qualified-state")).toHaveLength(1);
}, 30_000);
