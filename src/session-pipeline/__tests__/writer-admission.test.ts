/** Writer assembly admission (SQL-owner M5 + storage admission): the newly opened pool
 * is closed on any refusal, the primary refusal is rethrown unchanged, success keeps the
 * pool, and the storage verifier runs on the SAME transaction with the identity bound. */
import { expect, test } from "bun:test";
import { PORTABLE_LEDGER_TABLE, PORTABLE_MARKER_NAME, PORTABLE_MARKER_TABLE, admitWriterRuntime, gatedSearchTransact, portableWorkGate } from "../lambda.ts";
import { MIGRATION_LEDGER } from "../../hoopdb/migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../../shared/db.ts";
import { PortableConnectionRefused, admitPortableConnection } from "../../shared/portable-connection-identity.ts";
import type { SearchComposition } from "../../shared/search-composition.ts";

function fakeRuntime(rows: Record<string, unknown> | ((text: string) => unknown) = {}) {
  const statements: { client: number; text: string; params?: unknown[] }[] = [];
  let clients = 0;
  let ended = 0;
  let released = 0;
  const reply = typeof rows === "function" ? rows : () => rows;
  const pool = {
    async connect() {
      const id = ++clients;
      return {
        async query(text: string, params?: unknown[]) {
          statements.push({ client: id, text, params });
          const r = reply(text);
          if (r instanceof Error) throw r;
          return { rows: r ? [r] : [] };
        },
        release() { released++; },
      };
    },
  };
  const db = { pool, query: async () => ({ rows: [] }), end: async () => { ended++; } };
  return { rt: { deps: { pointer: { db } } } as never, statements, stats: () => ({ ended, released, clients }) };
}

const PORTABLE = { searchEnabled: true, searchBackend: "opensearch" } as never;
const composition = (admit: SearchComposition["admit"]): SearchComposition => ({ registry: {} as never, state: () => ({}) as never, admit });

test("refusal closes the newly opened pool and rethrows the primary refusal unchanged; repeated refusals each close their own pool", async () => {
  const refusal = new PortableConnectionRefused("superuser");
  for (let attempt = 0; attempt < 3; attempt++) {
    const f = fakeRuntime();
    const error = await admitWriterRuntime(f.rt, PORTABLE, composition(async () => { throw refusal; }), "SELECT 1", "rds").catch(e => e);
    expect(error).toBe(refusal);
    expect(f.stats()).toEqual({ ended: 1, released: 1, clients: 1 });
    expect(f.statements.map(s => s.text)).toContain("ROLLBACK");
  }
});

test("success keeps the pool; identity checks and the storage verifier share one transaction with the identity bound", async () => {
  const f = fakeRuntime((text) => text.includes("rolsuper")
    ? { present: true, super: false, bypassrls: false, owner_member: false, can_write: true }
    : {});
  const VERIFY = "DO $verify$ BEGIN RAISE NOTICE 'ok'; END $verify$";
  await admitWriterRuntime(f.rt, PORTABLE, composition((sql, role, options) => admitPortableConnection(sql, role, options)), VERIFY, "rds");
  expect(f.stats()).toEqual({ ended: 0, released: 1, clients: 1 });
  const texts = f.statements.map(s => s.text);
  const bind = texts.findIndex(t => t.includes("set_config('hoopdb.runtime_role', current_user, true)"));
  expect(texts[0]).toBe("BEGIN");
  expect(bind).toBeGreaterThan(texts.findIndex(t => t.includes("rolsuper")));
  expect(texts[bind + 1]).toBe(VERIFY);
  expect(texts.at(-1)).toBe("COMMIT");
  expect(new Set(f.statements.map(s => s.client))).toEqual(new Set([1]));
  expect(f.statements[bind]!.params).toEqual(["rds"]);
});

test("a failing storage verifier is a stable refusal and closes the pool; non-portable selections skip admission", async () => {
  const f = fakeRuntime((text) => text.includes("rolsuper")
    ? { present: true, super: false, bypassrls: false, owner_member: false, can_write: true }
    : text.startsWith("DO") ? Object.assign(new Error("portable search storage is not in the approved shape: x"), { code: "55000" }) : {});
  const error = await admitWriterRuntime(f.rt, PORTABLE, composition((sql, role, options) => admitPortableConnection(sql, role, options)), "DO $v$ BEGIN END $v$", "rds").catch(e => e);
  expect(error).toBeInstanceOf(PortableConnectionRefused);
  expect((error as PortableConnectionRefused).code).toBe("storage_unverified_sqlstate_55000");
  expect(f.stats().ended).toBe(1);
  for (const env of [{ searchEnabled: false, searchBackend: "opensearch" }, { searchEnabled: true, searchBackend: "clickhouse" }, { searchEnabled: true }]) {
    const g = fakeRuntime();
    await admitWriterRuntime(g.rt, env as never, composition(async () => { throw new Error("must not run"); }), "SELECT 1", "rds");
    expect(g.stats()).toEqual({ ended: 0, released: 0, clients: 0 });
  }
  const h = fakeRuntime();
  await admitWriterRuntime(h.rt, PORTABLE, { registry: {} as never, state: () => ({}) as never }, "SELECT 1", "rds"); // v1 composition: no admit
  expect(h.stats()).toEqual({ ended: 0, released: 0, clients: 0 });
});

test("the reader may not run the storage verifier", async () => {
  const f = fakeRuntime((text) => text.includes("rolsuper") ? { present: true, super: false, bypassrls: false, owner_member: false, can_write: false } : {});
  const client = await (f.rt as any).deps.pointer.db.pool.connect();
  await expect(admitPortableConnection(client, "reader", { storageVerifySql: "SELECT 1" })).rejects.toThrow("storage_verify_writer_only");
});

test("a declared admission with a missing or empty storage verifier body refuses (never skips) and closes the pool", async () => {
  for (const body of [undefined, "", "   "]) {
    const f = fakeRuntime();
    const error = await admitWriterRuntime(f.rt, PORTABLE, composition(async () => undefined), body, "rds").catch(e => e);
    expect((error as PortableConnectionRefused).code).toBe("storage_verifier_missing");
    expect(f.stats().ended).toBe(1);
  }
});

/** A database whose marker row, ledger and verifier outcome can change between passes,
 * or DURING a pass (onVerify runs when the storage verifier statement executes). */
const NAMES = ["upstream:a.sql", "upstream:b.sql", "96bulls:c.sql"];
function liveDb() {
  const state = {
    marker: [{ status: "pass", generation: "gen-a" }] as { status: string; generation: string }[],
    ledger: new Set(NAMES), verifierFails: false, onVerify: undefined as undefined | (() => void),
  };
  const statements: { client: number; text: string }[] = [];
  let clients = 0, released = 0;
  const pool = {
    async connect() {
      const id = ++clients;
      return {
        async query(text: string, params?: unknown[]) {
          statements.push({ client: id, text });
          if (text.includes("__96bulls_role_validation")) return { rows: state.marker.map(r => ({ ...r })) };
          if (text.includes("__96bulls_migrations")) return { rows: [{ n: (params![0] as string[]).filter(n => state.ledger.has(n)).length }] };
          if (text.includes("rolsuper")) return { rows: [{ present: true, super: false, bypassrls: false, owner_member: false, can_write: true }] };
          if (text.startsWith("DO")) { state.onVerify?.(); if (state.verifierFails) throw Object.assign(new Error("portable search storage is not in the approved shape: x"), { code: "55000" }); }
          return { rows: [] };
        },
        release() { released++; },
      };
    },
  };
  return { db: { pool, query: async () => ({ rows: [] }), end: async () => {} } as never, state, statements, released: () => released };
}

test("current-work gate: warm success runs the gate then the work on one transaction; later drift, pending marker or a newer generation refuse BEFORE work", async () => {
  const live = liveDb();
  const comp = composition((sql, role, options) => admitPortableConnection(sql, role, options));
  const VERIFY = "DO $v$ BEGIN END $v$";
  const gate = (tx: never) => portableWorkGate(tx, "gen-a", comp, { storageVerifySql: VERIFY, dbMode: "rds", expectedNames: NAMES });
  let worked = 0;
  const pass = () => gatedSearchTransact(live.db, gate, async () => { worked++; return "done"; });
  // Warm success.
  expect(await pass()).toBe("done");
  expect(worked).toBe(1);
  const first = live.statements.filter(s => s.client === 1).map(s => s.text);
  expect(first[0]).toBe("BEGIN");
  expect(first.findIndex(t => t.includes("__96bulls_role_validation"))).toBeLessThan(first.indexOf(VERIFY));
  expect(first.at(-1)).toBe("COMMIT");
  // Drift after the warm start: the storage verifier now fails.
  live.state.verifierFails = true;
  expect(((await pass().catch(e => e)) as PortableConnectionRefused).code).toBe("storage_unverified_sqlstate_55000");
  live.state.verifierFails = false;
  // A migrator flipped the marker to pending.
  live.state.marker = [{ status: "pending", generation: "gen-a" }];
  expect(((await pass().catch(e => e)) as PortableConnectionRefused).code).toBe("work_gate_marker_pending");
  // A newer generation was migrated (this build is now stale).
  live.state.marker = [{ status: "pass", generation: "gen-b" }];
  expect(((await pass().catch(e => e)) as PortableConnectionRefused).code).toBe("work_gate_generation_mismatch");
  // Marker missing.
  live.state.marker = [];
  expect(((await pass().catch(e => e)) as PortableConnectionRefused).code).toBe("work_gate_marker_missing");
  // None of the refused passes ran any work, and each rolled back.
  expect(worked).toBe(1);
  expect(live.statements.filter(s => s.text === "ROLLBACK").length).toBe(4);
  // Back to a valid state: work resumes.
  live.state.marker = [{ status: "pass", generation: "gen-a" }];
  expect(await pass()).toBe("done");
  expect(worked).toBe(2);
});

test("current-work gate refuses a missing verifier body; ungated (v1/ClickHouse) passes run without it", async () => {
  const live = liveDb();
  const comp = composition((sql, role, options) => admitPortableConnection(sql, role, options));
  for (const admission of [undefined, {}, { storageVerifySql: " " }]) {
    let worked = 0;
    const error = await gatedSearchTransact(live.db, (tx) => portableWorkGate(tx, "gen-a", comp, admission && { ...admission, expectedNames: NAMES }), async () => { worked++; }).catch(e => e);
    expect((error as PortableConnectionRefused).code).toBe("storage_verifier_missing");
    expect(worked).toBe(0);
  }
  let worked = 0;
  await gatedSearchTransact(live.db, undefined, async () => { worked++; });
  expect(worked).toBe(1);
});

test("the work gate reads exactly the shared readiness marker", () => {
  expect(PORTABLE_MARKER_TABLE).toBe(ROLE_VALIDATION_TABLE);
  expect(PORTABLE_MARKER_NAME).toBe(ROLE_VALIDATION_NAME);
});

test("transition window: a marker flip committed DURING verification is refused by the final read before work; READ COMMITTED is pinned first", async () => {
  const live = liveDb();
  const comp = composition((sql, role, options) => admitPortableConnection(sql, role, options));
  const VERIFY = "DO $v$ BEGIN END $v$";
  const gate = (tx: never) => portableWorkGate(tx, "gen-a", comp, { storageVerifySql: VERIFY, expectedNames: NAMES });
  let worked = 0;
  const pass = () => gatedSearchTransact(live.db, gate, async () => { worked++; });
  await pass();
  const texts = live.statements.filter(s => s.client === 1).map(s => s.text);
  const iso = texts.indexOf("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
  expect(iso).toBeGreaterThan(0);
  expect(iso).toBeLessThan(texts.findIndex(t => t.includes("__96bulls_migrations")));
  const markers = texts.map((t, i) => t.includes("__96bulls_role_validation") ? i : -1).filter(i => i >= 0);
  expect(markers.length).toBe(2); // initial and FINAL
  expect(markers[1]).toBeGreaterThan(texts.indexOf(VERIFY));
  for (const flip of [{ status: "pending", generation: "gen-a" }, { status: "pass", generation: "gen-b" }]) {
    live.state.marker = [{ status: "pass", generation: "gen-a" }];
    live.state.onVerify = () => { live.state.marker = [flip]; };
    const releasedBefore = live.released();
    const error = await pass().catch(e => e);
    expect((error as PortableConnectionRefused).code).toBe(flip.status === "pending" ? "work_gate_marker_pending" : "work_gate_generation_mismatch");
    expect(live.released()).toBe(releasedBefore + 1);
  }
  expect(worked).toBe(1);
  expect(live.statements.filter(s => s.text === "ROLLBACK").length).toBe(2);
});

test("readiness ledger membership: same PASS/generation and valid storage but a missing expected ledger row refuses before the verifier and before work", async () => {
  const live = liveDb();
  const comp = composition((sql, role, options) => admitPortableConnection(sql, role, options));
  const VERIFY = "DO $v$ BEGIN END $v$";
  live.state.ledger.delete("upstream:b.sql");
  let worked = 0;
  const error = await gatedSearchTransact(live.db, (tx) => portableWorkGate(tx, "gen-a", comp, { storageVerifySql: VERIFY, expectedNames: NAMES }), async () => { worked++; }).catch(e => e);
  expect((error as PortableConnectionRefused).code).toBe("work_gate_schema_pending");
  expect(worked).toBe(0);
  expect(live.statements.some(s => s.text === VERIFY)).toBe(false);
  // Expected names missing from the admission refuse too (never an empty requirement).
  const none = await gatedSearchTransact(live.db, (tx) => portableWorkGate(tx, "gen-a", comp, { storageVerifySql: VERIFY, expectedNames: ["verify:x"] }), async () => { worked++; }).catch(e => e);
  expect((none as PortableConnectionRefused).code).toBe("work_gate_expected_names_missing");
  expect(PORTABLE_LEDGER_TABLE).toBe(MIGRATION_LEDGER);
});
