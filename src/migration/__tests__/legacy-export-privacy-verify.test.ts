/**
 * Independent adversarial verification of the neonexit-reviewer privacy
 * findings (a)-(e) on review/turns-gates-legacy-export. Synthetic only:
 * PGlite stands in for Neon and for the Supabase privacy authority (two
 * SEPARATE databases, so a fence read from the wrong database is caught), and
 * a local fs store stands in for S3. Nothing here reaches a real database or
 * bucket.
 *
 * The tests assert behaviour (nothing exported / erased / readable), not the
 * fix's internals, so they run unchanged against any head of the branch.
 */
import { afterAll as __modeAfterAll, beforeEach as __modeBeforeEach } from "bun:test";
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey, parseManifest, readAll, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { LEGACY_EXPORT_ORIGIN_ID, legacyMachineId, legacyUnit, legacyUnitSource, type LegacyScope } from "../neon-s3-sync/legacy-source.ts";
import { exportUnit } from "../neon-s3-sync/export-session.ts";
import { Checkpoint, newCounters, runOrg, withLegacyPrivacyFence } from "../legacy-export/runner.ts";
import { storeEraser } from "../neon-s3-erasure/eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import { liveObjectPrefix, livePointerPrefix, liveTargets } from "../neon-s3-erasure/live.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { eraseDiscoveredSessionBytes } from "../../session-erase/selfhost-eraser.ts";
import { canonicalSnapshot, CanonicalReadError, type CanonicalPointer } from "../../session-read-api/canonical.ts";
import { canonicalLegacy } from "../../session-read-api/canonical-legacy.ts";
import type { Queryable } from "../relayhistory-export.ts";

const OBLIGATIONS_MODE: "on" | "off" = "on";
const __modeBefore = process.env.S3X_OBLIGATIONS;
process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE;
__modeBeforeEach(() => void (process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE));
__modeAfterAll(() => { if (__modeBefore === undefined) delete process.env.S3X_OBLIGATIONS; else process.env.S3X_OBLIGATIONS = __modeBefore; });
setDefaultTimeout(60_000);

const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const tmp = (p: string) => mkdtemp(join(process.env.TMPDIR ?? tmpdir(), `lep-${p}-`));
const TURN = "22222222-2222-4222-8222-222222222222";
const OWNER = "person";
const OTHER = "member_b";
/** A legacy-only scope (no rollup, no catalog, no events): only the legacy exporter can put it in S3. */
const S: LegacyScope = { orgId: "org_1", workspaceId: "ws_1", source: "claude", sessionId: "sess:1" };
/** The same session under the token-without-workspace spelling: turns say `default`, fences say `""`. */
/** A layout-safe session id for the discovery eraser. */
const SR: LegacyScope = { ...S, sessionId: "sess_r" };
const SD: LegacyScope = { ...S, workspaceId: "default", sessionId: "sess:d" };

/** The scope's real user (a turn's session_owner is not accepted as erasure identity since 5b0fa79a). */
const outcome = (pg: PGlite, s: LegacyScope, sha = "c0ffee") =>
  pg.query("INSERT INTO sessions.session_outcomes VALUES($1,$2,'claude',$3,$4,$5,false,NULL,'2026-11-01T10:00:00Z','2026-11-01T10:00:00Z')", [s.orgId, s.workspaceId, s.sessionId, sha, OWNER]);

/** Neon: legacy tables, the vendored deleted_upload_sessions, and 0074's conversation_turn_fences. */
async function neon() {
  const sql = await readFile(new URL("../../../vendor/relayhistory-cloud/migrations/0038_session_upload_deletion.sql", import.meta.url), "utf8");
  const ddl = /CREATE TABLE sessions\.deleted_upload_sessions \([\s\S]*?\n\);/.exec(sql)?.[0];
  if (!ddl) throw new Error("vendored 0038 no longer creates deleted_upload_sessions");
  const pg = new PGlite();
  await pg.exec(`CREATE SCHEMA sessions; ${ddl}
 CREATE TABLE sessions.conversation_turn_fences(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,source,session_id));
 CREATE TABLE sessions.session_resets(org_id text,workspace_id text,user_id text,request_id text,deleted bigint);
 CREATE TABLE sessions.delivery_records(org_id text,workspace_id text,origin_id text,record_id text,kind text,source text,session_id text,user_id text);
 CREATE TABLE sessions.session_rollups(org_id text,session_id text,workspace_id text,source text,first_ts timestamptz,last_ts timestamptz,machine_ids text[],models text[],event_count bigint,input_tokens bigint,output_tokens bigint,reasoning_tokens bigint,cache_read_tokens bigint,cache_create_tokens bigint,cost_usd_micros bigint);
 CREATE TABLE sessions.session_catalog(org_id text,session_id text,workspace_id text,source text,first_activity_at timestamptz,last_activity_at timestamptz,received_at timestamptz);
 CREATE TABLE sessions.convergence_events(org_id text,workspace_id text,source text,session_id text,user_id text NOT NULL DEFAULT 'u_event');
 CREATE TABLE sessions.conversation_turns(id uuid PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,session_owner text,turn_index int,role text,content text,actor_name text,actor_role text,metadata jsonb,ts timestamptz,UNIQUE(org_id,workspace_id,source,session_id,turn_index));
 CREATE TABLE sessions.session_links(id bigint PRIMARY KEY,org_id text,workspace_id text,source text,session_id text,link_kind text,link_ref text,link_url text,metadata jsonb,confidence_basis_points int,link_ts timestamptz,updated_at timestamptz,created_at timestamptz);
 CREATE TABLE sessions.session_outcomes(org_id text,workspace_id text,source text,session_id text,commit_sha text,user_id text,reverted boolean,shipped_at timestamptz,ingested_at timestamptz,created_at timestamptz);`);
  // Stored source `owner`, attributed source `claude` (nativeCli), owner `person`.
  await pg.query(
    `INSERT INTO sessions.conversation_turns VALUES($1,$2,$3,'owner',$4,$5,0,'assistant','erase me','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00Z')`,
    [TURN, S.orgId, S.workspaceId, S.sessionId, OWNER],
  );
  await outcome(pg, S);
  return { pg, db: pg as unknown as Queryable };
}

/** Supabase privacy authority: the real column sets of the ledgers the exporter may read. */
async function supabase() {
  const pg = new PGlite();
  await pg.exec(`CREATE SCHEMA sessions;
 CREATE TABLE sessions.session_erasure_receipts(org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,request_id text NOT NULL DEFAULT 'r',status text NOT NULL DEFAULT 'in_progress');
 CREATE TABLE sessions.upload_deletions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,deleted_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
 CREATE TABLE sessions.upload_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,reset_at timestamptz NOT NULL DEFAULT now(),deleted bigint NOT NULL DEFAULT 0);
 CREATE TABLE sessions.operator_user_erasures(request_id text PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,state text NOT NULL,origins text[] NOT NULL DEFAULT '{}');
 CREATE TABLE sessions.turn_session_fences(org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,first_fenced_at timestamptz NOT NULL DEFAULT clock_timestamp(),fence_basis text NOT NULL CHECK (fence_basis IN ('live','legacy_import')),PRIMARY KEY(org_id,workspace_id,source,session_id));`);
  // Transaction control and advisory locks are stood in (PGlite is single-session); the fence SQL runs for real.
  const db: Queryable = {
    async query<T>(sql: string, params?: unknown[]) {
      if (/^\s*(BEGIN|COMMIT|ROLLBACK|SET LOCAL|SET SESSION)/u.test(sql) || sql.includes("pg_advisory")) return { rows: [] as T[] };
      return (await pg.query<T>(sql, params)) as { rows: T[] };
    },
  };
  return { pg, db };
}

/** The store with a hook before every put; keeps the class's prototype methods (a spread would drop them). */
function withPutHook(store: ObjectStore, before: () => Promise<void>): ObjectStore {
  return new Proxy(store, {
    get(target, prop, recv) {
      if (prop === "put") return async (...a: unknown[]) => { await before(); return (target.put as (...x: unknown[]) => unknown).apply(target, a); };
      const v = Reflect.get(target, prop, recv);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
}

async function allKeys(store: ObjectStore) {
  const out: string[] = [];
  let c: string | undefined;
  do {
    const p = await store.list(objectKey("96bulls"), c);
    out.push(...p.items.map((i) => i.key));
    c = p.nextCursor;
  } while (c);
  return out.sort();
}

async function run(n: Queryable, p: Queryable, store: ObjectStore, name: string, opts: { export: boolean } = { export: true }) {
  const checkpoint = new Checkpoint(join(await tmp(name), "c.sqlite"));
  const counters = newCounters();
  const logs: Record<string, unknown>[] = [];
  // A privacy race may either fail the scope and continue, or halt the run (5b0fa79a halts): both are accepted.
  let halted: string | undefined;
  await runOrg({ db: n, privacyDb: p, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: (f) => void logs.push(f) }, S.orgId, opts, counters)
    .catch((e: unknown) => {
      const code = (e as { code?: string }).code;
      if (code !== "legacy_privacy_race") throw e;
      halted = code;
    });
  return { counters, checkpoint, logs, halted };
}

async function pointerFor(store: ObjectStore, key: string, s: LegacyScope = S): Promise<CanonicalPointer> {
  const manifest = await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(key)))), key);
  const ext = manifest.extensions?.["96bulls"] as { user_id: string; canonical_machine_id: string };
  return {
    org_id: s.orgId, workspace_id: manifest.session.workspace_id, harness: manifest.session.harness, session_id: s.sessionId,
    machine_id: ext.canonical_machine_id, user_id: ext.user_id, storage_provider: "s3", bucket: "bucket", manifest_key: key,
    content_sha256: manifest.raw.content_sha256, raw_format: "ndjson", revision: manifest.revision, deleted_at: null,
  };
}
const snapshot = (store: ObjectStore, pointers: CanonicalPointer[], s: LegacyScope = S) =>
  canonicalSnapshot(
    { store, provider: "s3", bucket: "bucket", prefix: { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" }, pointers: async () => pointers, decompress },
    s,
  );
/** Rows of legacy content the reader would serve; a refused read serves none. */
const servedLegacy = async (store: ObjectStore, pointers: CanonicalPointer[]) =>
  snapshot(store, pointers).then(
    (snap) => snap.rows.filter((r) => ["conversation_turns", "session_links", "session_outcomes"].includes(r.table) || JSON.stringify(r).includes("erase me")),
    () => [],
  );

// ---------------------------------------------------------------------------
describe("(a) turn-specific cross-member fences block a legacy-only export", () => {
  test("control: an unfenced legacy-only scope IS exported (the fixture is live)", async () => {
    const n = await neon(), p = await supabase();
    const store = createFsStore({ root: await tmp("a0") });
    const { counters } = await run(n.db, p.db, store, "a0");
    expect(counters).toMatchObject({ exported: 1, fenced: 0, failed: 0 });
    expect((await allKeys(store)).length).toBeGreaterThan(0);
  });

  test("coverage: a turns-only legacy scope (no events, no outcomes) is exported, not left behind as legacy_user_unbound", async () => {
    // Pending a product decision: if turns-only scopes are intentionally not exported, replace this
    // with a test that they are counted and documented as data left behind. Until then it gates.
    const n = await neon(), p = await supabase();
    await n.pg.query("DELETE FROM sessions.session_outcomes");
    const store = createFsStore({ root: await tmp("a0t") });
    const { counters } = await run(n.db, p.db, store, "a0t");
    expect(counters).toMatchObject({ exported: 1, fenced: 0, failed: 0 });
  });

  test("Supabase turn_session_fences (legacy_import, no user) fences the scope: nothing reaches S3", async () => {
    for (const basis of ["legacy_import", "live"]) {
      const n = await neon(), p = await supabase();
      await p.pg.query("INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,$2,$3,$4,$5)", [S.orgId, S.workspaceId, S.source, S.sessionId, basis]);
      const store = createFsStore({ root: await tmp("a1") });
      const { counters } = await run(n.db, p.db, store, "a1");
      expect({ basis, ...counters }).toMatchObject({ basis, exported: 0, fenced: 1 });
      expect(await allKeys(store)).toEqual([]);
      // --plan must report it fenced as well.
      const plan = await run(n.db, p.db, store, "a1p", { export: false });
      expect(plan.counters.fenced).toBe(1);
    }
  });

  test("Neon conversation_turn_fences written by ANOTHER member's erasure fences the owner's legacy scope", async () => {
    const n = await neon(), p = await supabase();
    await n.pg.query("INSERT INTO sessions.conversation_turn_fences VALUES($1,$2,$3,$4,$5)", [S.orgId, S.workspaceId, OTHER, S.source, S.sessionId]);
    const store = createFsStore({ root: await tmp("a2") });
    const { counters } = await run(n.db, p.db, store, "a2");
    expect(counters).toMatchObject({ exported: 0, fenced: 1 });
    expect(await allKeys(store)).toEqual([]);
  });

  test("conversation_turn_fences under the delivery spelling '' fences turns stored under 'default'", async () => {
    const n = await neon(), p = await supabase();
    await n.pg.query("DELETE FROM sessions.conversation_turns");
    await n.pg.query(
      `INSERT INTO sessions.conversation_turns VALUES($1,$2,'default','owner',$3,$4,0,'assistant','erase me','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00Z')`,
      [TURN, SD.orgId, SD.sessionId, OWNER],
    );
    await n.pg.query("DELETE FROM sessions.session_outcomes");
    await outcome(n.pg, SD);
    await n.pg.query("INSERT INTO sessions.conversation_turn_fences VALUES($1,'',$2,$3,$4)", [SD.orgId, OTHER, SD.source, SD.sessionId]);
    const store = createFsStore({ root: await tmp("a3") });
    const { counters } = await run(n.db, p.db, store, "a3");
    expect(counters).toMatchObject({ exported: 0, fenced: 1 });
    expect(await allKeys(store)).toEqual([]);
  });

  test("Supabase ledgers written under the delivery spelling '' fence turns stored under 'default'", async () => {
    const ledgers: [string, string][] = [
      ["turn_session_fences", "INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,'',$2,$3,'live')"],
      ["session_erasure_receipts", "INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id) VALUES($1,'',$2,$3)"],
      ["upload_deletions", `INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,'','${OTHER}','o',$2,$3)`],
    ];
    for (const [ledger, insert] of ledgers) {
      const n = await neon(), p = await supabase();
      await n.pg.query("DELETE FROM sessions.conversation_turns");
      await n.pg.query(
        `INSERT INTO sessions.conversation_turns VALUES($1,$2,'default','owner',$3,$4,0,'assistant','erase me','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00Z')`,
        [TURN, SD.orgId, SD.sessionId, OWNER],
      );
      await n.pg.query("DELETE FROM sessions.session_outcomes");
      await outcome(n.pg, SD);
      await p.pg.query(insert, [SD.orgId, SD.source, SD.sessionId]);
      const store = createFsStore({ root: await tmp("a3s") });
      const { counters } = await run(n.db, p.db, store, "a3s");
      expect({ ledger, ...counters }).toMatchObject({ ledger, exported: 0, fenced: 1 });
      expect(await allKeys(store)).toEqual([]);
    }
  });

  test("an already-exported, unchanged scope fenced later is reported fenced on the rerun (checkpoint skip path)", async () => {
    const n = await neon(), p = await supabase();
    const store = createFsStore({ root: await tmp("a4") });
    const checkpoint = new Checkpoint(join(await tmp("a4c"), "c.sqlite"));
    const deps = { db: n.db, privacyDb: p.db, target: { store, keyRoot: "96bulls", grantId: "g" }, checkpoint, hook: async () => {}, log: () => {} };
    const first = newCounters();
    await runOrg(deps, S.orgId, { export: true }, first);
    expect(first.exported).toBe(1);
    await p.pg.query("INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,$2,$3,$4,'live')", [S.orgId, S.workspaceId, S.source, S.sessionId]);
    const second = newCounters();
    await runOrg(deps, S.orgId, { export: true, refresh: true }, second);
    expect(second).toMatchObject({ fenced: 1, skippedCheckpoint: 0, exported: 0 });
    checkpoint.close();
  });

  test("no over-fencing: fences on a neighbouring session, source or org do not fence this scope", async () => {
    const n = await neon(), p = await supabase();
    await p.pg.query(
      "INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,$2,$3,'sess:10','live'),($1,$2,'codex',$4,'live'),('org_2',$2,$3,$4,'live')",
      [S.orgId, S.workspaceId, S.source, S.sessionId],
    );
    await n.pg.query("INSERT INTO sessions.conversation_turn_fences VALUES($1,$2,$3,$4,'sess:10')", [S.orgId, S.workspaceId, OTHER, S.source]);
    const { counters } = await run(n.db, p.db, createFsStore({ root: await tmp("a5") }), "a5");
    expect(counters).toMatchObject({ exported: 1, fenced: 0 });
  });
});

// ---------------------------------------------------------------------------
describe("(b) the race-loser recheck sees every ledger the pre-check sees", () => {
  const race = async (insert: (p: PGlite) => Promise<unknown>) => {
    const p = await supabase();
    let wrote = false;
    const outcome = await withLegacyPrivacyFence(p.db, S, OWNER, async () => {
      await insert(p.pg); // the erasure commits while the S3 write is in flight
      wrote = true;
      return "written";
    }).then((v) => ({ ok: v }), (e: unknown) => ({ code: (e as { code?: string }).code }));
    return { outcome, wrote };
  };

  test("control: upload_deletions inserted mid-write fails the export", async () => {
    const r = await race((pg) => pg.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'o',$4,$5)", [S.orgId, S.workspaceId, OTHER, S.source, S.sessionId]));
    expect(r.wrote).toBe(true);
    expect(r.outcome).toEqual({ code: "legacy_privacy_race" });
  });

  test("session_erasure_receipts inserted mid-write fails the export", async () => {
    const r = await race((pg) => pg.query("INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id) VALUES($1,$2,$3,$4)", [S.orgId, S.workspaceId, S.source, S.sessionId]));
    expect(r.outcome).toEqual({ code: "legacy_privacy_race" });
  });

  test("turn_session_fences inserted mid-write fails the export (if the pre-check reads it, the recheck must too)", async () => {
    const r = await race((pg) => pg.query("INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,$2,$3,$4,'live')", [S.orgId, S.workspaceId, S.source, S.sessionId]));
    expect(r.outcome).toEqual({ code: "legacy_privacy_race" });
  });

  test("a race fails the scope in runOrg: it is never checkpointed exported", async () => {
    const n = await neon(), p = await supabase();
    const store = createFsStore({ root: await tmp("b4") });
    // Inject the receipt at the first S3 write (inside the fence).
    let puts = 0;
    const racing = withPutHook(store, async () => {
      puts += 1;
      await p.pg.query("INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id) SELECT $1,$2,$3,$4 WHERE NOT EXISTS(SELECT 1 FROM sessions.session_erasure_receipts)", [S.orgId, S.workspaceId, S.source, S.sessionId]);
    });
    const { counters, checkpoint, logs } = await run(n.db, p.db, racing, "b4");
    expect(puts).toBeGreaterThan(0); // the write really started inside the fence
    expect(logs.some((l) => l.error_code === "legacy_privacy_race")).toBe(true);
    expect(counters.exported).toBe(0);
    expect(counters.failed + counters.fenced).toBe(1);
    checkpoint.close();
  });
});

// ---------------------------------------------------------------------------
describe("(c) a session erased AFTER export loses its legacy objects and pointer", () => {
  /** The erasure job's other reads are stood in; the live fence SQL runs for real against Neon. */
  const jobDb = (pg: PGlite): Queryable => ({
    async query<T>(text: string, params?: unknown[]) {
      if (text.includes("pg_locks")) return { rows: [{ total: "0", non_advisory: "0" }] as T[] };
      if (text.includes("sessions.session_catalog")) return { rows: [] as T[] };
      if (text.includes("FROM sessions.session_rollups"))
        return { rows: (text.includes("(org_id, session_id) >") || text.startsWith("SELECT 1") ? [] : [{ org_id: "org_1", session_id: "keep", last_ms: "1", events: "1" }]) as T[] };
      return (await pg.query<T>(text, params)) as { rows: T[] };
    },
  });
  const later = () => Date.now() + 10 * 60 * 1000;
  const legacyGroup = (user: string, s: LegacyScope = S) => ({ org_id: s.orgId, workspace_id: s.workspaceId, user_id: user, origin_id: LEGACY_EXPORT_ORIGIN_ID, source: s.source, session_id: s.sessionId });

  async function exportThenErase(tombstoneUser: string, opts: { neighbour?: boolean } = {}) {
    const n = await neon(), p = await supabase();
    const store = createFsStore({ root: await tmp("c") });
    if (opts.neighbour)
      await n.pg.query(
        `INSERT INTO sessions.conversation_turns VALUES('33333333-3333-4333-8333-333333333333',$1,$2,'owner','sess:keep',$3,0,'assistant','keep me','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00Z')`,
        [S.orgId, S.workspaceId, OWNER],
      );
    if (opts.neighbour) await outcome(n.pg, { ...S, sessionId: "sess:keep" }, "beef");
    const { counters } = await run(n.db, p.db, store, "c-export");
    expect(counters.exported).toBe(opts.neighbour ? 2 : 1);
    const exported = await allKeys(store);
    const objectRoot = liveObjectPrefix("96bulls", legacyGroup(OWNER));
    const manifestKey = exported.find((k) => k.startsWith(objectRoot) && k.includes("/s=sess_1/") && k.endsWith("manifest.json"));
    expect(manifestKey).toBeDefined();
    expect(objectRoot).toContain(`/m=${legacyMachineId(S.workspaceId, OWNER)}/`);
    const pointer = await pointerFor(store, manifestKey!);
    expect((await servedLegacy(store, [pointer])).length).toBeGreaterThan(0); // readable before erasure
    // A location pointer under the legacy machine, as an indexer/live writer layout would hold it.
    const ptrKey = liveTargets("96bulls", legacyGroup(OWNER)).pointerKey;
    await store.put(objectKey(ptrKey), new TextEncoder().encode("{}"));
    // The session is erased afterwards, through an ORDINARY origin (never legacy:tables:v1).
    await n.pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'relayhistory:delivery:v1',$4,$5)", [S.orgId, S.workspaceId, tombstoneUser, S.source, S.sessionId]);
    const state = new SyncState(join(await tmp("c-state"), "s.sqlite"), { obligations: OBLIGATIONS_MODE === "on" });
    const job = new ErasureJob(jobDb(n.pg), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    await job.sweep();
    const left = await allKeys(store);
    state.close();
    return { left, objectRoot, ptrKey, pointer, store, pointerRoot: livePointerPrefix("96bulls", legacyGroup(OWNER)) };
  }

  test("the owner's own erasure removes every legacy object of the session, its pointer, and the reader serves nothing", async () => {
    const r = await exportThenErase(OWNER, { neighbour: true });
    expect(r.left.filter((k) => k.startsWith(r.objectRoot) && k.includes("/s=sess_1/"))).toEqual([]);
    expect(r.left).not.toContain(r.ptrKey);
    expect(r.left.filter((k) => k.startsWith(r.pointerRoot) && k.includes("s=sess_1."))).toEqual([]);
    expect(await servedLegacy(r.store, [r.pointer])).toEqual([]);
    // Precision: the same user's other legacy session stays.
    expect(r.left.some((k) => k.startsWith(r.objectRoot) && k.includes("/s=sess_keep/"))).toBe(true);
  });

  test("ANOTHER member's erasure of the session also removes the owner's legacy copy (turns are fenced for every member)", async () => {
    const r = await exportThenErase(OTHER);
    expect(r.left.filter((k) => k.startsWith(r.objectRoot) && k.includes("/s=sess_1/"))).toEqual([]);
    expect(r.left).not.toContain(r.ptrKey);
    expect(await servedLegacy(r.store, [r.pointer])).toEqual([]);
  });

  /**
   * Race losers (reviewer request): the export is fenced mid-write by a ledger that writes NO
   * deleted_upload_sessions/upload_deletions tombstone, so the objects already put stay in S3 (the
   * exporter key is write-only by design). The eraser that owns that fence type must remove them.
   */
  async function raceLoser(ledger: "turn_session_fences" | "session_erasure_receipts") {
    const n = await neon(), p = await supabase();
    // A layout-safe id: the discovery eraser refuses lossy ids such as `sess:1` by design.
    await n.pg.query("UPDATE sessions.conversation_turns SET session_id=$1", [SR.sessionId]);
    await n.pg.query("UPDATE sessions.session_outcomes SET session_id=$1", [SR.sessionId]);
    const store = createFsStore({ root: await tmp(`race-${ledger}`) });
    const insert = ledger === "turn_session_fences"
      ? "INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) SELECT $1,$2,$3,$4,'legacy_import' WHERE NOT EXISTS(SELECT 1 FROM sessions.turn_session_fences)"
      : "INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id) SELECT $1,$2,$3,$4 WHERE NOT EXISTS(SELECT 1 FROM sessions.session_erasure_receipts)";
    const racing = withPutHook(store, async () => void (await p.pg.query(insert, [SR.orgId, SR.workspaceId, SR.source, SR.sessionId])));
    const { counters, checkpoint } = await run(n.db, p.db, racing, `race-${ledger}`);
    checkpoint.close();
    expect(counters.exported).toBe(0);
    const objectRoot = liveObjectPrefix("96bulls", legacyGroup(OWNER));
    const leftover = (await allKeys(store)).filter((k) => k.startsWith(objectRoot) && k.includes("/s=sess_r/"));
    expect(leftover.length).toBeGreaterThan(0); // the loser did write before the recheck (fixture is live)
    return { n, p, store, objectRoot };
  }

  test("race loser fenced by session_erasure_receipts: the receipt-owned session eraser removes the legacy manifest and raw object", async () => {
    const { store, objectRoot } = await raceLoser("session_erasure_receipts");
    // The operator session-erase byte stage (selfhost-runtime.ts) is what consumes a receipt.
    await eraseDiscoveredSessionBytes(storeEraser(store), { orgId: SR.orgId, workspaceId: SR.workspaceId, source: SR.source, sessionId: SR.sessionId }, { keyRoot: "96bulls", bucket: "bucket" });
    expect((await allKeys(store)).filter((k) => k.startsWith(objectRoot) && k.includes("/s=sess_r/"))).toEqual([]);
  });

  test("race loser fenced by turn_session_fences alone (no tombstone): the ordinary deletion job removes the legacy manifest and raw object", async () => {
    const { n, store, objectRoot } = await raceLoser("turn_session_fences");
    // A legacy-only session has no registered turn writer, so the fence materializes no upload_deletions row.
    const state = new SyncState(join(await tmp("race-state"), "s.sqlite"), { obligations: OBLIGATIONS_MODE === "on" });
    const job = new ErasureJob(jobDb(n.pg), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    await job.sweep();
    state.close();
    expect((await allKeys(store)).filter((k) => k.startsWith(objectRoot) && k.includes("/s=sess_r/"))).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
describe("(d) forged/stale legacy parts are dropped visibly; invalid evidence is a 503", () => {
  test("a forged legacy part keeps the read available and surfaces a provenance reason or counter", async () => {
    const n = await neon();
    const store = createFsStore({ root: await tmp("d") });
    const unit = (await legacyUnit(n.db, S))!;
    const forged = await exportUnit({ store, keyRoot: "96bulls", grantId: "g" }, unit, {
      fenced: async () => false,
      async *lines(u) {
        for await (const line of legacyUnitSource(n.db, async () => {}, 500, undefined, OWNER).lines(u)) yield { ...line, machine: "srv_forged" };
      },
    });
    const snap = await snapshot(store, [await pointerFor(store, forged.manifests[0]!.manifestKey)]);
    expect(snap.rows.filter((r) => r.table === "conversation_turns")).toEqual([]);
    expect(snap.legacyCoverage).toBeFalsy();
    const { rows: _rows, ...meta } = snap as unknown as Record<string, unknown>;
    // Implementation-agnostic: some non-row field names the dropped legacy provenance.
    expect(JSON.stringify(meta)).toMatch(/legacy_provenance/u);
  });

  test("canonical-legacy fail() is CanonicalReadError legacy_evidence_invalid with status 503 (never a bare Error / 500)", () => {
    const outOfScope = [{ kind: "legacy_conversation_turn", origin_id: "legacy:conversation_turns", record_id: `legacy:conversation_turns:${TURN}`, revision: "1", operation: "upsert", received_at: "2026-11-01T10:00:00Z", payload: { org_id: "org_2", workspace_id: S.workspaceId, source: S.source, session_id: S.sessionId } }];
    let thrown: unknown;
    try {
      canonicalLegacy(outOfScope as never, { orgId: S.orgId, workspaceId: S.workspaceId, source: S.source, sessionId: S.sessionId } as never);
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(CanonicalReadError);
    expect((thrown as CanonicalReadError).reason).toBe("legacy_evidence_invalid");
    expect((thrown as CanonicalReadError).status).toBe(503);
  });
});

// ---------------------------------------------------------------------------
describe("(e) the exporter's fatal log carries an error code only", () => {
  test("an unparseable URL carrying a credential is never echoed into the exit log", () => {
    const secret = "S3CR3T_PASSWORD_canary";
    const r = spawnSync(process.execPath, [new URL("../legacy-export/main.ts", import.meta.url).pathname, "--plan"], {
      input: `postgres://u:${secret}@[bad\npostgres://u:${secret}@[bad\n`,
      encoding: "utf8",
      env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", TMPDIR: process.env.TMPDIR ?? "" },
      timeout: 30_000,
    });
    expect(r.status).toBe(1);
    const out = `${r.stdout}\n${r.stderr}`;
    expect(out).not.toContain(secret);
    const line = r.stdout.trim().split("\n").map((l) => JSON.parse(l) as Record<string, unknown>).find((l) => l.message === "exit on error");
    expect(line).toBeDefined();
    expect(String(line!.error_code)).toMatch(/^[A-Za-z][A-Za-z0-9_]{0,63}$/u);
  });

  test("an invalid --since value (operator free text) is not echoed either", () => {
    const canary = "free_text_canary_value";
    const r = spawnSync(process.execPath, [new URL("../legacy-export/main.ts", import.meta.url).pathname, "--plan", `--since=${canary}`], {
      input: "postgres://u:p@127.0.0.1:1/db\npostgres://u:p@127.0.0.1:1/db\n",
      encoding: "utf8",
      env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", TMPDIR: process.env.TMPDIR ?? "" },
      timeout: 30_000,
    });
    expect(r.status).toBe(1);
    expect(`${r.stdout}${r.stderr}`).not.toContain(canary);
  });
});
