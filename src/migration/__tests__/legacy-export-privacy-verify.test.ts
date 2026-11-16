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
import { existsSync, readdirSync, readFileSync } from "node:fs";
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
import { supabaseAuthority } from "../neon-s3-erasure/authority.ts";
import { authorityFixture, epoch } from "../../session-ingest/__tests__/privacy-authority-support.ts";
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

/** The owner-registration migration, found by name so a renumbered slot (e.g. 062020 -> 062030) needs no suite change. */
const AUTHORITY_MIGRATION = (() => {
  const dir = new URL("../../../supabase/migrations/", import.meta.url);
  const hits = readdirSync(dir).filter((f) => /^\d+_legacy_export_scope_authority\.sql$/u.test(f));
  if (hits.length > 1) throw new Error(`more than one legacy_export_scope_authority migration: ${hits.join(",")}`);
  return new URL(hits[0] ?? "__absent_legacy_export_scope_authority.sql", dir);
})();
/** The statements a plain ledger fixture needs to run the real register RPC; empty on heads without the migration. */
function legacyAuthoritySql(): string[] {
  if (!existsSync(AUTHORITY_MIGRATION)) return [];
  const pick = (text: string, re: RegExp, what: string) => {
    const m = re.exec(text);
    if (!m) throw new Error(`migration no longer defines ${what}`);
    return m[0];
  };
  const ledger = readFileSync(new URL("../../../supabase/migrations/20261104022000_session_ingest_ledger.sql", import.meta.url), "utf8");
  const authority = readFileSync(AUTHORITY_MIGRATION, "utf8");
  return [
    pick(ledger, /CREATE FUNCTION sessions\.ingest_exact_keys\([\s\S]*?\$keys\$;/u, "ingest_exact_keys"),
    pick(authority, /CREATE TABLE sessions\.legacy_export_scope_authority\([\s\S]*?\n\);/u, "legacy_export_scope_authority"),
    pick(authority, /CREATE FUNCTION sessions\.ingest_legacy_export_scope_authority_register\([\s\S]*?\$register\$;/u, "the register RPC"),
  ];
}

/** Supabase privacy authority: the real column sets of the ledgers the exporter may read. */
async function supabase() {
  const pg = new PGlite();
  await pg.exec(`CREATE SCHEMA sessions;
 CREATE TABLE sessions.session_erasure_receipts(org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,request_id text NOT NULL DEFAULT 'r',status text NOT NULL DEFAULT 'in_progress');
 CREATE TABLE sessions.upload_deletions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,deleted_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
 CREATE TABLE sessions.ingest_frozen_source_deletions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
 CREATE TABLE sessions.upload_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,reset_at timestamptz NOT NULL DEFAULT now(),deleted bigint NOT NULL DEFAULT 0);
 CREATE TABLE sessions.operator_user_erasures(request_id text PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,state text NOT NULL,origins text[] NOT NULL DEFAULT '{}');
 CREATE TABLE sessions.turn_session_fences(org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,first_fenced_at timestamptz NOT NULL DEFAULT clock_timestamp(),fence_basis text NOT NULL CHECK (fence_basis IN ('live','legacy_import')),PRIMARY KEY(org_id,workspace_id,source,session_id));`);
  // The exporter's owner registration (20261105062020), loaded verbatim from the repo when the head has it: the table,
  // the SECURITY DEFINER register RPC, and the helper it calls. Nothing is hand-written.
  for (const sql of legacyAuthoritySql()) await pg.exec(sql);
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

  test("owner rule: a turns-only scope whose session_owner is unverified free text is not exported, writes nothing, and is counted unbound_turns_only (not failed)", async () => {
    // Lead's rule: events user, then outcomes user, then session_owner ONLY if it is a verified Cloud user id. A label
    // that appears in no authenticated user evidence is never an erasure identity; the scope stays on the frozen copy.
    const n = await neon(), p = await supabase();
    await n.pg.query("DELETE FROM sessions.session_outcomes");
    await n.pg.query("UPDATE sessions.conversation_turns SET session_owner='Agent Smith <agent@example.invalid>'");
    const store = createFsStore({ root: await tmp("unbound") });
    const { counters } = await run(n.db, p.db, store, "unbound");
    expect(counters).toMatchObject({ exported: 0, fenced: 0, failed: 0 });
    expect(await allKeys(store)).toEqual([]);
    const flat = Object.entries(counters as unknown as Record<string, unknown>);
    expect(flat.filter(([k, v]) => /unbound_?turns_?only/iu.test(k) && v === 1).length).toBe(1);
  });

  test("owner rule: two distinct event users, or two distinct outcome users, are not exported, write nothing, and are counted unbound_multi_owner (not failed)", async () => {
    for (const evidence of ["events", "outcomes"] as const) {
      const n = await neon(), p = await supabase();
      if (evidence === "events")
        await n.pg.query("INSERT INTO sessions.convergence_events VALUES($1,$2,$3,$4,'u_a'),($1,$2,$3,$4,'u_b')", [S.orgId, S.workspaceId, S.source, S.sessionId]);
      else await outcome(n.pg, S, "f00d").then(() => n.pg.query("UPDATE sessions.session_outcomes SET user_id='u_b' WHERE commit_sha='f00d'"));
      const store = createFsStore({ root: await tmp(`multi-${evidence}`) });
      const { counters } = await run(n.db, p.db, store, `multi-${evidence}`);
      expect({ evidence, ...counters }).toMatchObject({ evidence, exported: 0, fenced: 0, failed: 0 });
      expect(await allKeys(store)).toEqual([]);
      const flat = Object.entries(counters as unknown as Record<string, unknown>);
      expect({ evidence, hits: flat.filter(([k, v]) => /unbound_?multi_?owner/iu.test(k) && v === 1).length }).toEqual({ evidence, hits: 1 });
    }
  });

  test("owner rule: a turns-only session_owner that is a verified Cloud user id in the SAME tenant is exported under that owner; evidence from another org does not verify it", async () => {
    for (const where of ["same-tenant", "other-org"] as const) {
      const n = await neon(), p = await supabase();
      await n.pg.query("DELETE FROM sessions.session_outcomes");
      await n.pg.query("UPDATE sessions.conversation_turns SET session_owner='u_real'");
      // Authenticated evidence for 'u_real' from a DIFFERENT session (this scope itself stays turns-only).
      await n.pg.query("INSERT INTO sessions.convergence_events VALUES($1,$2,'claude','sess:elsewhere','u_real')", [where === "same-tenant" ? S.orgId : "org_2", S.workspaceId]);
      const store = createFsStore({ root: await tmp(`verified-${where}`) });
      const { counters } = await run(n.db, p.db, store, `verified-${where}`);
      const keys = (await allKeys(store)).filter((k) => k.includes("/s=sess_1/"));
      if (where === "same-tenant") {
        expect({ where, ...counters }).toMatchObject({ where, exported: 1, failed: 0 });
        expect(keys.some((k) => k.includes(`/m=${legacyMachineId(S.workspaceId, "u_real")}/`))).toBe(true);
      } else {
        expect({ where, ...counters }).toMatchObject({ where, exported: 0, failed: 0 });
        expect(keys).toEqual([]);
        const flat = Object.entries(counters as unknown as Record<string, unknown>);
        expect(flat.filter(([k, v]) => /unbound_?turns_?only/iu.test(k) && v === 1).length).toBe(1);
      }
    }
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

  test("Supabase ingest_frozen_source_deletions (a Neon-era deletion copied at freeze) fences the scope", async () => {
    // Under the Supabase privacy authority the frozen copy is the record of Neon-era deletions.
    const n = await neon(), p = await supabase();
    await p.pg.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,'relayhistory:delivery:v1',$4,$5)", [S.orgId, S.workspaceId, OTHER, S.source, S.sessionId]);
    const store = createFsStore({ root: await tmp("a6") });
    const { counters } = await run(n.db, p.db, store, "a6");
    expect(counters).toMatchObject({ exported: 0, fenced: 1 });
    expect(await allKeys(store)).toEqual([]);
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

  test("race loser fenced by turn_session_fences alone (no tombstone): the HOSTED deletion job under the Supabase authority removes the legacy manifest and raw object", async () => {
    // Production shape end to end, on the repo's real Supabase migration stack (authorityFixture, activated as in
    // neon-s3-erasure-authority.test.ts):
    //  - the exporter runs through runOrg on a restricted privacy role holding only the runbook's grants, using the
    //    exporter's own privacy-session initializer and preflight (main.ts), with NO transaction-control stubbing;
    //  - the export loses after its PUT (the object is written, then the scope's transaction fails), which is the
    //    race loser's end state on a single-connection database;
    //  - the session is then fenced ONLY by turn_session_fences (no upload_deletions / frozen-deletion tombstone);
    //  - the hosted ErasureJob runs on the real supabaseAuthority and must leave no legacy object.
    const main = (await import("../legacy-export/main.ts")) as Record<string, unknown>;
    const initPrivacy = main.legacyPrivacySession as ((q: Queryable) => Promise<Queryable>) | undefined;
    const preflight = main.assertPrivacySelectOnly as (q: Queryable) => Promise<void>;
    expect(typeof initPrivacy).toBe("function"); // the exporter must export its real privacy-connection initializer
    expect(existsSync(AUTHORITY_MIGRATION)).toBe(true); // the durable owner registration must exist
    const n = await neon();
    await n.pg.query("UPDATE sessions.conversation_turns SET session_id=$1", [SR.sessionId]);
    await n.pg.query("UPDATE sessions.session_outcomes SET session_id=$1", [SR.sessionId]);
    const f = await authorityFixture();
    try {
      await f.seal();
      await f.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: epoch });
      const fp = (await f.db.query("SELECT sessions.ingest_privacy_authority_packet('[]','[]','[]') AS f")).rows[0] as { f: string };
      const owned = (await f.ownerRpc("ingest_privacy_authority_owned_digest")).r;
      await f.ownerRpc("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: fp.f, heads: [], deletions: [], resets: [], neonOwned: owned });
      // turn_session_fences comes from the turns-forward migration (20261105062010, pipeline-ingest branch); when the
      // stack under test lacks it, create it with that migration's exact shape.
      const hadTurnFences = ((await f.db.query("SELECT to_regclass('sessions.turn_session_fences') IS NOT NULL AS ok")).rows[0] as { ok: boolean }).ok;
      await f.db.exec(`CREATE TABLE IF NOT EXISTS sessions.turn_session_fences(org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
        first_fenced_at timestamptz NOT NULL DEFAULT clock_timestamp(),fence_basis text NOT NULL CHECK (fence_basis IN ('live','legacy_import')),PRIMARY KEY(org_id,workspace_id,source,session_id))`);
      const LEDGERS = ["session_erasure_receipts", "upload_deletions", "ingest_frozen_source_deletions", "upload_resets", "turn_session_fences", "operator_user_erasures"];
      // The production privacy role, bootstrapped and granted by the repo's own bootstrap and migrations (never by this test).
      const ROLE = "bulls96_legacy_export_privacy";
      expect((await f.db.query("SELECT 1 FROM pg_roles WHERE rolname=$1", [ROLE])).rows.length).toBe(1);
      // Production order is 062010 (turn_session_fences) THEN 062020. When this stack lacks 062010, the fixture created the
      // table above in 062010's shape; give it 062010's RLS and re-run 062020's own conditional grant/policy block verbatim
      // (it skipped that block at migration time because the table did not exist yet). Nothing here is hand-granted.
      if (!hadTurnFences) {
        await f.db.exec("ALTER TABLE sessions.turn_session_fences ENABLE ROW LEVEL SECURITY");
        const block = /DO \$turn_fence_grant\$[\s\S]*?END \$turn_fence_grant\$;/u.exec(readFileSync(AUTHORITY_MIGRATION, "utf8"))?.[0];
        if (!block) throw new Error("062020 no longer carries its conditional turn_session_fences grant block");
        await f.db.exec(block);
      }
      // One session-level SET ROLE for the exporter phase: its BEGIN/COMMIT, advisory locks and RPC run for real.
      // Like node-postgres: a parameterless query uses the simple protocol (multi-statement allowed, last result).
      const raw: Queryable = {
        query: async <T,>(t: string, p?: unknown[]) => {
          if (p?.length) return (await f.db.query<T>(t, p as never)) as { rows: T[] };
          const all = await f.db.exec(t);
          return { rows: (all.at(-1)?.rows ?? []) as T[] };
        },
      };
      await f.db.exec(`SET ROLE ${ROLE}`);
      let privacyDb: Queryable;
      try {
        privacyDb = await initPrivacy!(raw);
        await preflight(privacyDb);
        for (const t of [...LEDGERS, "legacy_export_scope_authority"]) // the role still writes no ledger and not the authority table
          expect(await f.db.query(`DELETE FROM sessions.${t}`).then(() => "wrote", (e) => String((e as Error).message))).toContain("permission denied");
        const store = createFsStore({ root: await tmp("race-hosted") });
        let puts = 0;
        // The PUT lands, then the scope fails (as a race loser does after its recheck): the transaction rolls back.
        const losing = new Proxy(store, {
          get(target, prop, recv) {
            if (prop === "put") return async (...a: unknown[]) => {
              const r = await (target.put as (...x: unknown[]) => unknown).apply(target, a);
              puts += 1;
              throw Object.assign(new Error("simulated loss after PUT"), { code: "legacy_privacy_race" });
              return r;
            };
            const v = Reflect.get(target, prop, recv);
            return typeof v === "function" ? v.bind(target) : v;
          },
        }) as ObjectStore;
        const { counters, checkpoint } = await run(n.db, privacyDb, losing, "race-hosted");
        checkpoint.close();
        expect(puts).toBeGreaterThan(0);
        expect(counters.exported).toBe(0);
        await f.db.exec("RESET ROLE");
        const objectRoot = liveObjectPrefix("96bulls", legacyGroup(OWNER));
        const written = (await allKeys(store)).filter((k) => k.startsWith(objectRoot) && k.includes("/s=sess_r/"));
        expect(written.length).toBeGreaterThan(0); // the loser's object is really in S3
        // The owner row must survive the loser's rollback: committed BEFORE the PUT, or the eraser can never find it.
        const row = (await f.db.query("SELECT legacy_user_id,legacy_machine_id FROM sessions.legacy_export_scope_authority WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4", [SR.orgId, SR.workspaceId, SR.source, SR.sessionId])).rows;
        expect(row).toEqual([{ legacy_user_id: OWNER, legacy_machine_id: legacyMachineId(SR.workspaceId, OWNER) }]);
        // Fenced only by turn_session_fences.
        await f.db.query("INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,$2,$3,$4,'legacy_import') ON CONFLICT DO NOTHING", [SR.orgId, SR.workspaceId, SR.source, SR.sessionId]);
        const tomb = async (t: string) => ((await f.db.query(`SELECT count(*)::int AS n FROM sessions.${t} WHERE session_id=$1`, [SR.sessionId])).rows[0] as { n: number }).n;
        expect(await tomb("upload_deletions")).toBe(0);
        expect(await tomb("ingest_frozen_source_deletions")).toBe(0);
        const state = new SyncState(join(await tmp("race-hosted-state"), "s.sqlite"), { obligations: OBLIGATIONS_MODE === "on" });
        const logs: Record<string, unknown>[] = [];
        const job = new ErasureJob(supabaseAuthority(f.queryable), state, storeEraser(store), (x) => void logs.push(x), DEFAULT_ERASURE_CONFIG, later, async () => {});
        await job.sweep();
        await job.sweep();
        state.close();
        expect(logs.some((l) => l.message === "erasure sweep" && l.authority === "supabase")).toBe(true); // the authority was live
        expect((await allKeys(store)).filter((k) => k.startsWith(objectRoot) && k.includes("/s=sess_r/"))).toEqual([]);
      } finally {
        await f.db.exec("RESET ROLE").catch(() => {});
      }
    } finally {
      await f.close();
    }
  });

  /**
   * The production-shaped hosted world shared by the F1-fix checks: the repo's real Supabase migration stack, activated,
   * with 062010's turn_session_fences (062010 then 062020's own grant block when the stack lacks it), the provisioned
   * bulls96_legacy_export_privacy role, and the exporter's own privacy-session initializer and preflight.
   */
  async function hostedWorld() {
    const main = (await import("../legacy-export/main.ts")) as Record<string, unknown>;
    const initPrivacy = main.legacyPrivacySession as ((q: Queryable) => Promise<Queryable>) | undefined;
    const preflight = main.assertPrivacySelectOnly as (q: Queryable) => Promise<void>;
    expect(typeof initPrivacy).toBe("function");
    expect(existsSync(AUTHORITY_MIGRATION)).toBe(true);
    const f = await authorityFixture();
    await f.seal();
    await f.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: epoch });
    const fp = (await f.db.query("SELECT sessions.ingest_privacy_authority_packet('[]','[]','[]') AS f")).rows[0] as { f: string };
    const owned = (await f.ownerRpc("ingest_privacy_authority_owned_digest")).r;
    await f.ownerRpc("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: fp.f, heads: [], deletions: [], resets: [], neonOwned: owned });
    const had = ((await f.db.query("SELECT to_regclass('sessions.turn_session_fences') IS NOT NULL AS ok")).rows[0] as { ok: boolean }).ok;
    if (!had) {
      await f.db.exec(`CREATE TABLE sessions.turn_session_fences(org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
        first_fenced_at timestamptz NOT NULL DEFAULT clock_timestamp(),fence_basis text NOT NULL CHECK (fence_basis IN ('live','legacy_import')),PRIMARY KEY(org_id,workspace_id,source,session_id));
        ALTER TABLE sessions.turn_session_fences ENABLE ROW LEVEL SECURITY`);
      const block = /DO \$turn_fence_grant\$[\s\S]*?END \$turn_fence_grant\$;/u.exec(readFileSync(AUTHORITY_MIGRATION, "utf8"))?.[0];
      if (!block) throw new Error("062020 no longer carries its conditional turn_session_fences grant block");
      await f.db.exec(block);
    }
    const raw: Queryable = {
      query: async <T,>(t: string, p?: unknown[]) => {
        if (p?.length) return (await f.db.query<T>(t, p as never)) as { rows: T[] };
        const all = await f.db.exec(t);
        return { rows: (all.at(-1)?.rows ?? []) as T[] };
      },
    };
    const asExporter = async <T,>(body: (privacyDb: Queryable) => Promise<T>): Promise<T> => {
      await f.db.exec("SET ROLE bulls96_legacy_export_privacy");
      try {
        const privacyDb = await initPrivacy!(raw);
        await preflight(privacyDb);
        return await body(privacyDb);
      } finally {
        await f.db.exec("RESET ROLE");
      }
    };
    const authorityRow = async (sc: LegacyScope) =>
      (await f.db.query("SELECT legacy_user_id,legacy_machine_id FROM sessions.legacy_export_scope_authority WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND session_id=$4", [sc.orgId, sc.workspaceId, sc.source, sc.sessionId])).rows;
    const sweep = async (store: ObjectStore) => {
      const state = new SyncState(join(await tmp("hosted-state"), "s.sqlite"), { obligations: OBLIGATIONS_MODE === "on" });
      const logs: Record<string, unknown>[] = [];
      const job = new ErasureJob(supabaseAuthority(f.queryable), state, storeEraser(store), (x) => void logs.push(x), DEFAULT_ERASURE_CONFIG, later, async () => {});
      try {
        await job.sweep();
        await job.sweep();
      } finally {
        state.close();
      }
      expect(logs.some((l) => l.message === "erasure sweep" && l.authority === "supabase")).toBe(true);
      return logs;
    };
    return { f, asExporter, authorityRow, sweep };
  }
  /** A Neon fixture holding two layout-safe legacy-only scopes owned by OWNER. */
  async function twoScopes() {
    const n = await neon();
    await n.pg.query("UPDATE sessions.conversation_turns SET session_id=$1", [SR.sessionId]);
    await n.pg.query("UPDATE sessions.session_outcomes SET session_id=$1", [SR.sessionId]);
    await n.pg.query(
      `INSERT INTO sessions.conversation_turns VALUES('44444444-4444-4444-8444-444444444444',$1,$2,'owner','sess_q',$3,0,'assistant','erase me too','Agent','owner','{"nativeCli":"claude"}','2026-11-01T10:00:00Z')`,
      [S.orgId, S.workspaceId, OWNER],
    );
    await outcome(n.pg, { ...S, sessionId: "sess_q" }, "dead");
    return n;
  }
  const legacyKeys = async (store: ObjectStore, session: string) =>
    (await allKeys(store)).filter((k) => k.startsWith(liveObjectPrefix("96bulls", legacyGroup(OWNER))) && k.includes(`/s=${session}/`));

  test("owner rule (Neon authority, today's default): the eraser targets the owner bound at export, even after Neon later re-derives a different one", async () => {
    const n = await neon(), p = await supabase();
    const store = createFsStore({ root: await tmp("drift-neon") });
    const { counters } = await run(n.db, p.db, store, "drift-neon");
    expect(counters.exported).toBe(1);
    const objectRoot = liveObjectPrefix("96bulls", legacyGroup(OWNER));
    expect((await allKeys(store)).some((k) => k.startsWith(objectRoot) && k.includes("/s=sess_1/"))).toBe(true);
    // A later delivery event by another user: re-deriving from live Neon would now bind 'u_drift' (events outrank outcomes).
    await n.pg.query("INSERT INTO sessions.convergence_events VALUES($1,$2,$3,$4,'u_drift')", [S.orgId, S.workspaceId, S.source, S.sessionId]);
    await n.pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,'relayhistory:delivery:v1',$4,$5)", [S.orgId, S.workspaceId, OTHER, S.source, S.sessionId]);
    const state = new SyncState(join(await tmp("drift-neon-state"), "s.sqlite"), { obligations: OBLIGATIONS_MODE === "on" });
    const job = new ErasureJob(jobDb(n.pg), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, later, async () => {});
    await job.sweep();
    await job.sweep();
    state.close();
    expect((await allKeys(store)).filter((k) => k.startsWith(objectRoot) && k.includes("/s=sess_1/"))).toEqual([]);
  });

  test("owner rule: the hosted eraser targets the owner registered at export, even after Neon later re-derives a different one", async () => {
    const n = await neon();
    await n.pg.query("UPDATE sessions.conversation_turns SET session_id=$1", [SR.sessionId]);
    await n.pg.query("UPDATE sessions.session_outcomes SET session_id=$1", [SR.sessionId]);
    const w = await hostedWorld();
    try {
      const store = createFsStore({ root: await tmp("drift") });
      const counters = await w.asExporter(async (privacyDb) => {
        const r = await run(n.db, privacyDb, store, "drift");
        r.checkpoint.close();
        return r.counters;
      });
      expect(counters.exported).toBe(1);
      expect(await w.authorityRow(SR)).toEqual([{ legacy_user_id: OWNER, legacy_machine_id: legacyMachineId(SR.workspaceId, OWNER) }]);
      expect((await legacyKeys(store, "sess_r")).length).toBeGreaterThan(0);
      // Neon now carries a delivery event by another user: re-deriving would pick 'u_drift' (events outrank outcomes).
      await n.pg.query("INSERT INTO sessions.convergence_events VALUES($1,$2,$3,$4,'u_drift')", [SR.orgId, SR.workspaceId, SR.source, SR.sessionId]);
      await w.f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'relayhistory:delivery:v1',$4,$5)", [SR.orgId, SR.workspaceId, OTHER, SR.source, SR.sessionId]);
      await w.sweep(store);
      expect(await legacyKeys(store, "sess_r")).toEqual([]); // the registered owner's machine, not a re-derived one
      expect(await w.authorityRow(SR)).toEqual([{ legacy_user_id: OWNER, legacy_machine_id: legacyMachineId(SR.workspaceId, OWNER) }]);
    } finally {
      await w.f.close();
    }
  });

  test("F1-fix (1): a registered scope whose PUT never landed does not wedge the hosted sweep; a fenced neighbour in the same sweep is still erased", async () => {
    const n = await twoScopes();
    const w = await hostedWorld();
    try {
      const store = createFsStore({ root: await tmp("absent") });
      let failed = 0;
      // sess_r: registration may commit, then the PUT itself fails before writing anything. sess_q: exported normally.
      const flaky = new Proxy(store, {
        get(target, prop, recv) {
          if (prop === "put") return async (...a: unknown[]) => {
            if (String(a[0]).includes("/s=sess_r/")) { failed += 1; throw Object.assign(new Error("PUT failed"), { code: "ECONNRESET_SIM" }); }
            return (target.put as (...x: unknown[]) => unknown).apply(target, a);
          };
          const v = Reflect.get(target, prop, recv);
          return typeof v === "function" ? v.bind(target) : v;
        },
      }) as ObjectStore;
      await w.asExporter(async (privacyDb) => {
        const r = await run(n.db, privacyDb, flaky, "absent");
        r.checkpoint.close();
      });
      expect(failed).toBeGreaterThan(0);
      expect(await legacyKeys(store, "sess_r")).toEqual([]);
      expect((await legacyKeys(store, "sess_q")).length).toBeGreaterThan(0);
      for (const sid of ["sess_r", "sess_q"])
        await w.f.db.query("INSERT INTO sessions.turn_session_fences(org_id,workspace_id,source,session_id,fence_basis) VALUES($1,$2,$3,$4,'live') ON CONFLICT DO NOTHING", [S.orgId, S.workspaceId, S.source, sid]);
      const logs = await w.sweep(store);
      // A missing object counts as erased: no failure logged, and the sweep reaches the fenced neighbour.
      expect(logs.filter((l) => /failed|refused/u.test(String(l.message)) && /legacy/u.test(String(l.message)))).toEqual([]);
      expect(await legacyKeys(store, "sess_q")).toEqual([]);
    } finally {
      await w.f.close();
    }
  });

  test("F1-fix (2): an erasure committed between owner registration and the PUT leaves no legacy object", async () => {
    const n = await twoScopes();
    await n.pg.query("DELETE FROM sessions.conversation_turns WHERE session_id='sess_q'");
    await n.pg.query("DELETE FROM sessions.session_outcomes WHERE session_id='sess_q'");
    const w = await hostedWorld();
    const erase = () =>
      w.f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'relayhistory:delivery:v1',$4,$5) ON CONFLICT DO NOTHING", [SR.orgId, SR.workspaceId, OTHER, SR.source, SR.sessionId]);
    try {
      const store = createFsStore({ root: await tmp("window") });
      let fired = 0;
      // The erasure commits after registration and before the object lands. On a one-connection database the insert at
      // the PUT is what READ COMMITTED shows the exporter's recheck; it is durable, so it is re-asserted after any rollback.
      const racing = new Proxy(store, {
        get(target, prop, recv) {
          if (prop === "put") return async (...a: unknown[]) => {
            if (!fired) {
              fired += 1;
              await w.f.db.exec("RESET ROLE");
              await erase();
              await w.f.db.exec("SET ROLE bulls96_legacy_export_privacy");
            }
            return (target.put as (...x: unknown[]) => unknown).apply(target, a);
          };
          const v = Reflect.get(target, prop, recv);
          return typeof v === "function" ? v.bind(target) : v;
        },
      }) as ObjectStore;
      const counters = await w.asExporter(async (privacyDb) => {
        const r = await run(n.db, privacyDb, racing, "window");
        r.checkpoint.close();
        return r.counters;
      });
      await erase(); // the concurrent erasure committed: it survives whatever the exporter rolled back
      expect(fired).toBe(1);
      expect(counters.exported).toBe(0); // never checkpointed as exported over an erasure
      await w.sweep(store);
      expect(await legacyKeys(store, "sess_r")).toEqual([]);
    } finally {
      await w.f.close();
    }
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
