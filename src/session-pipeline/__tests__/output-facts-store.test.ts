/**
 * OutputFactsStore with fakes: the ClickHouse store issues exactly the calls today's functions
 * do (same order, same statements, same rows), and the Postgres store maps the interface onto
 * the 20261105045040 functions (row shape, timestamps, versions, stale refusals). The SQL
 * functions themselves run on PGlite in src/hoopdb/__tests__/selfhost-output-facts.pglite.test.ts.
 */
import { describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import {
  eraseOutputFacts,
  outputFactsFromScope,
  outputFactsSchema,
  previousOutputFactsState,
  reconcileOutputFacts,
  recordOutputFactsState,
} from "../output-facts.ts";
import { clickhouseOutputFactsStore, pgInstant, postgresOutputFactsStore } from "../output-facts-store.ts";
import { OUTPUT_FIXTURE_SCOPE, outputFixture } from "./output-facts-fixture.ts";

const SCOPE = OUTPUT_FIXTURE_SCOPE;
const AT = new Date("2026-11-09T12:00:00.123Z");
const LIMITS = { maxRows: 50_000, insertBatch: 2 };

/** A ClickHouse client that records every call and answers each select from `answer`. */
function recordingCh(answer: (query: string) => unknown[] = () => []) {
  const log: unknown[] = [];
  const ch: ClickHouseClient = {
    database: "rh",
    async command(query, options) { log.push(["command", query, options?.params ?? null]); },
    async select<T>(query: string, options?: { params?: unknown }) { log.push(["select", query, options?.params ?? null]); return answer(query) as T[]; },
    async insert(table, rows) { log.push(["insert", table, structuredClone(rows)]); },
  };
  return { ch, log };
}

describe("clickhouseOutputFactsStore", () => {
  test("every method issues exactly the calls of today's functions", async () => {
    const facts = outputFactsFromScope(SCOPE, outputFixture(), 42);
    const range = facts.range ?? null;
    const direct = recordingCh();
    const viaStore = recordingCh();
    const store = clickhouseOutputFactsStore(viaStore.ch, LIMITS);

    expect(await store.schemaReady()).toEqual(await outputFactsSchema(direct.ch));
    expect(await store.previousState(SCOPE)).toEqual(await previousOutputFactsState(direct.ch, SCOPE));
    await recordOutputFactsState(direct.ch, SCOPE, false, AT, range, "pending", 42);
    await store.recordState(SCOPE, { complete: false, at: AT, range, phase: "pending", sourceVersion: 42 });
    const d = await reconcileOutputFacts(direct.ch, SCOPE, facts, LIMITS);
    const s = await store.reconcile(SCOPE, { eventFacts: facts.events, outputFacts: facts.facts, complete: facts.complete, reasons: facts.reasons }, 42);
    expect(s).toEqual({ ...d, applied: true });
    await recordOutputFactsState(direct.ch, SCOPE, true, AT, range, "final", 42);
    expect(await store.recordState(SCOPE, { complete: true, at: AT, range, sourceVersion: 42 })).toBe(true);
    await eraseOutputFacts(direct.ch, SCOPE);
    await store.erase(SCOPE);

    expect(viaStore.log.length).toBeGreaterThan(5);
    expect(viaStore.log).toEqual(direct.log);
  });
});

/** A SqlExecutor that records every statement and answers from `answer`. */
function fakeTx(answer: (text: string) => Record<string, unknown>[] = () => []) {
  const calls: Array<{ text: string; params: unknown[] }> = [];
  const tx: SqlExecutor = {
    async query<R>(text: string, params: unknown[] = []) {
      calls.push({ text: text.replace(/\s+/g, " ").trim(), params });
      return { rows: answer(text) as R[] };
    },
  };
  return { tx, calls };
}

describe("postgresOutputFactsStore", () => {
  test("pgInstant turns DateTime64(6) text into a zoned ISO instant and refuses anything else", () => {
    expect(pgInstant("2026-11-05 10:00:01.000001")).toBe("2026-11-05T10:00:01.000001Z");
    expect(pgInstant("2026-11-05 10:00:01")).toBe("2026-11-05T10:00:01Z");
    for (const bad of ["", "2026-11-05T10:00:01Z", "2026-11-05 10:00:01+02", "2026-11-05 10:00:01.0000001", "now"])
      expect(() => pgInstant(bad)).toThrow("output_facts_invalid_time");
  });

  test("reconcile sends the builder's rows without the ClickHouse version, with zoned timestamps", async () => {
    const facts = outputFactsFromScope(SCOPE, outputFixture(), 7);
    const { tx, calls } = fakeTx(() => [{ applied: true, inserted: "3", deleted: 1 }]);
    const r = await postgresOutputFactsStore(tx).reconcile(SCOPE, { eventFacts: facts.events, outputFacts: facts.facts, complete: true, reasons: ["a", "a"] }, 7);
    expect(r).toEqual({ inserted: 3, deleted: 1, complete: true, reasons: ["a"], applied: true });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.text).toContain("sessions.output_facts_reconcile(");
    const [org, ws, harness, ids, canonical, events, outs, complete, version] = calls[0]!.params as [string, string, string, string[], string, string, string, boolean, number];
    expect([org, ws, harness, ids, canonical, complete, version]).toEqual([SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionIds, SCOPE.canonicalId, true, 7]);
    const ev = JSON.parse(events) as Array<Record<string, unknown>>;
    const of = JSON.parse(outs) as Array<Record<string, unknown>>;
    expect(ev).toHaveLength(facts.events.length);
    expect(of).toHaveLength(facts.facts.length);
    for (const row of [...ev, ...of]) {
      expect("version" in row).toBe(false);
      expect(row.ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/);
    }
    expect(ev[0]!.row_id).toBe(facts.events[0]!.row_id);
    expect(ev[0]!.files_touched).toEqual(facts.events[0]!.files_touched);
  });

  test("a stale refusal changes nothing and says so; an incomplete pass is never complete", async () => {
    const stale = fakeTx(() => [{ applied: false, inserted: 0, deleted: 0 }]);
    expect(await postgresOutputFactsStore(stale.tx).reconcile(SCOPE, { eventFacts: [], outputFacts: [], complete: true, reasons: [] }, 1)).toEqual({
      inserted: 0, deleted: 0, complete: false, reasons: ["output_facts_stale_source_version"], applied: false,
    });
    const partial = fakeTx(() => [{ applied: true, inserted: 0, deleted: 0 }]);
    expect(await postgresOutputFactsStore(partial.tx).reconcile(SCOPE, { eventFacts: [], outputFacts: [], complete: false, reasons: ["x"] }, 1)).toMatchObject({
      complete: false, applied: true, reasons: ["x"] });
  });

  test("recordState: pending is never complete, ranges are zoned, a refusal is false", async () => {
    const { tx, calls } = fakeTx((t) => [{ ok: !t.includes("NEVER") }]);
    const store = postgresOutputFactsStore(tx);
    const range = { min: "2026-11-05 08:00:00.000000", max: "2026-11-05 12:00:00.000000" };
    expect(await store.recordState(SCOPE, { complete: true, at: AT, range, phase: "pending", sourceVersion: 4 })).toBe(true);
    expect(await store.recordState(SCOPE, { complete: true, at: AT, range: null, sourceVersion: 4 })).toBe(true);
    expect(calls[0]!.text).toContain("sessions.output_facts_record_state(");
    expect(calls[0]!.params.slice(5)).toEqual([false, "2026-11-05T08:00:00.000000Z", "2026-11-05T12:00:00.000000Z", 4, AT.toISOString()]);
    expect(calls[1]!.params.slice(5)).toEqual([true, null, null, 4, AT.toISOString()]);
    const refused = fakeTx(() => [{ ok: false }]);
    expect(await postgresOutputFactsStore(refused.tx).recordState(SCOPE, { complete: true, at: AT, range: null, sourceVersion: 1 })).toBe(false);
  });

  test("source versions must be safe non-negative integers (nothing is sent otherwise)", async () => {
    const { tx, calls } = fakeTx();
    const store = postgresOutputFactsStore(tx);
    for (const bad of [-1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) {
      await expect(store.reconcile(SCOPE, { eventFacts: [], outputFacts: [], complete: true, reasons: [] }, bad)).rejects.toThrow("output_facts_invalid_source_version");
      await expect(store.recordState(SCOPE, { complete: true, at: AT, range: null, sourceVersion: bad })).rejects.toThrow("output_facts_invalid_source_version");
    }
    expect(calls).toHaveLength(0);
  });

  test("previousState maps the row (or none) and erase sends the scope", async () => {
    const some = fakeTx(() => [{ lo: "2026-11-05 08:00:00.000000", hi: "2026-11-05 12:00:00.000000", v: "9" }]);
    expect(await postgresOutputFactsStore(some.tx).previousState(SCOPE)).toEqual({ range: { min: "2026-11-05 08:00:00.000000", max: "2026-11-05 12:00:00.000000" }, version: 9 });
    expect(some.calls[0]!.text).toContain("sessions.output_facts_state(");
    const none = fakeTx(() => [{ lo: null, hi: null, v: null }]);
    expect(await postgresOutputFactsStore(none.tx).previousState(SCOPE)).toEqual({ range: null, version: 0 });
    const erase = fakeTx();
    await postgresOutputFactsStore(erase.tx).erase(SCOPE);
    expect(erase.calls).toEqual([{ text: "SELECT sessions.output_facts_erase($1, $2, $3, $4::text[])", params: [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionIds] }]);
    expect(await postgresOutputFactsStore(erase.tx).schemaReady()).toEqual({ ready: true, present: true });
  });
});
