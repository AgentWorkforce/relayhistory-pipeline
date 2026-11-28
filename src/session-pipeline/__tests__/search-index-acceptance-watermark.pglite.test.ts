/**
 * SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED on the hosted path (with SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED):
 * the acceptance refusal holds across new revisions of an unaccepted session, at the cost of the added chunks
 * only, and ends the moment a record with a refused record id arrives, in any sibling manifest, from any origin.
 *
 * Real: a migrated PGlite with the session directory and its triggers, the real pointer stage, the real
 * exporter writing manifests and chunks to a filesystem object store, the real reader, fold and search stage.
 * Mocked: ClickHouse (the hosted harness's in-memory fake; statefulClickHouse answers the stamp reads from what
 * the pass inserted) and the clock. Synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import {
  ACCEPTANCE_MISSING,
  ACCEPTANCE_WATERMARK_VERSION,
  SEARCH_INDEX_DEFAULT_LIMITS,
  acceptanceWatermarkBase,
  SKIPPED_BEFORE_READ,
  SKIP_STATE_MAX_AGE_MS,
  acceptanceStampName,
  indexSearchScope,
  type SearchIndexDeps,
} from "../search-index.ts";
import { lambdaEnv, runtimeFromSecret, searchIndexAcceptanceWatermarkFromEnv } from "../lambda.ts";
import { INGEST_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { ID, SCOPE, T0, USER, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { HOSTED_NOW, hostedRig } from "./search-index-hosted-harness.ts";
import { captureLogs, realHandler, statefulClickHouse } from "./search-index-skip-flags-harness.ts";

const SKIP: Partial<SearchIndexDeps> = { skipAcceptanceMissing: true };
const ON: Partial<SearchIndexDeps> = { skipAcceptanceMissing: true, acceptanceWatermark: true };
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const HELD_REASONS = [ACCEPTANCE_MISSING, SKIPPED_BEFORE_READ];
const CONTENDED_ID = "unseated";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

/** Origin A and origin B contend for one record id, each in its own manifest; `seats` grants acceptance in place. */
function contenders(seats = false): Line[] {
  return [
    activity("machine:contend-a", "capacity:contend-a", "session_event", CONTENDED_ID, Date.parse("2026-11-01T10:00:00.600Z"), { role: "user", text: "a" }, "1", seats ? 1_000_001 : undefined),
    activity("machine:contend-b", "capacity:contend-b", "session_event", CONTENDED_ID, Date.parse("2026-11-01T10:00:00.700Z"), { role: "user", text: "b" }, "1", seats ? 1_000_002 : undefined),
  ];
}
/** `n` more events of the live, appending session on machine:0 (never the contended record id). */
function appended(n: number): Line[] {
  return Array.from({ length: n }, (_, i) =>
    activity("machine:0", "capacity:0", "session_event", `appended-${i}`, T0 + 30_000 + i * 10, { role: "user", text: `appended ${i} ${"x".repeat(200)}` }));
}
const base = (): Line[] => [...generate(8), ...appended(4), ...contenders()];

/** Publish `lines` as the scope's new content: every changed manifest moves to its next revision. */
async function publish(x: ExportedScope, lines: Line[]) {
  const encoder = new TextEncoder();
  await exportUnit({ store: x.store, keyRoot: "96bulls", grantId: "stream_parity", maxChunkBytes: 1200 },
    { orgId: ID.orgId, workspaceId: ID.workspaceId, source: ID.source, sessionId: ID.sessionId, firstMs: T0, lastMs: T0 + 60_000, machineIds: [], totals: {} },
    {
      fenced: async () => false,
      async *lines() {
        for (const { machine, record, userId } of lines) yield { machine, userId: userId ?? USER, bytes: encoder.encode(JSON.stringify(record) + "\n") };
      },
    });
}

async function rig(lines: Line[] = base()) {
  const x = await exportStreamScope(lines, 1200);
  cleanup.push(x.dispose);
  const r = await hostedRig(x, cleanup);
  const state = statefulClickHouse(r.fake);
  await r.tick({ ch: state.ch });
  const clock = { ms: HOSTED_NOW.getTime() };
  // What each transaction saw: object reads and ClickHouse calls made while it was open, and whether it took a lock.
  const transactions: Array<{ locked: boolean; reads: number; ch: number }> = [];
  // Runs once, right after the next lock-free transaction commits (the probe's pointer snapshot): a racing writer.
  const race: { afterProbe?: () => Promise<unknown> } = {};
  const transact: SearchIndexDeps["transact"] = async (work) => {
    const reads = r.reads.count, ch = r.fake.calls.length, sql = r.sql.length;
    let locked = false;
    try {
      return await r.transact(work);
    } finally {
      locked = r.sql.slice(sql).some((s) => String(s.sql).includes("pg_try_advisory_xact_lock"));
      transactions.push({ locked, reads: r.reads.count - reads, ch: r.fake.calls.length - ch });
      if (!locked && race.afterProbe) {
        const run = race.afterProbe;
        race.afterProbe = undefined;
        await run();
      }
    }
  };
  const pass = async (extra: Partial<SearchIndexDeps> = ON) => {
    const reads = r.reads.count, sql = r.sql.length;
    r.fake.calls.length = 0;
    transactions.length = 0;
    clock.ms += 1_000;
    const result = await indexSearchScope(SCOPE, r.search({ ch: state.ch, now: () => new Date(clock.ms), transact, ...extra }));
    return { result, reads: r.reads.count - reads, calls: [...r.fake.calls], statements: r.sql.slice(sql).map((s) => String(s.sql)), transactions: [...transactions] };
  };
  /** The pointer stage takes what was published (search off for the tick: refused for size, so only the pointers move). */
  const pointers = () => r.tick({ ch: state.ch, limits: TINY });
  const stamps = () => state.ingest.filter((s) => s.name === acceptanceStampName(SCOPE));
  const latest = () => JSON.parse(stamps().at(-1)!.value) as { digest: string; w?: { v: number; fullAt: number; ids: string[]; manifests: Array<{ k: string; n: number }> } };
  return { x, r, state, pass, pointers, stamps, latest, clock, race };
}
const sqlWrites = (statements: string[]) => statements.filter((s) => /^(UPDATE|INSERT|DELETE)\b/i.test(s) || /\b(invalidate|UPDATE sessions)\b/i.test(s));
const chWrites = (calls: Array<Record<string, unknown>>) => calls.filter((c) => c.ch === "insert" || c.ch === "command");

describe("repeated revisions with acceptance missing", () => {
  test("the first pass reads in full and stamps a watermark; three new revisions are each held on the added chunks alone", async () => {
    const { x, pass, pointers, stamps, latest } = await rig();
    const first = await pass();
    expect(first.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(first.result.skippedBeforeRead).toBeUndefined();
    const fullReads = first.reads;
    expect(fullReads).toBeGreaterThan(3);
    expect(stamps()).toHaveLength(1);
    expect(latest().w).toMatchObject({ v: ACCEPTANCE_WATERMARK_VERSION });
    expect(latest().w!.ids).toHaveLength(1);
    // Only hashes and counts: never a record id, a manifest key, a session id or content.
    expect(stamps()[0]!.value).not.toContain(CONTENDED_ID);
    for (const key of x.keys) expect(stamps()[0]!.value).not.toContain(key);
    expect(stamps()[0]!.value).not.toContain(SCOPE.sessionId);
    expect(latest().w!.manifests).toHaveLength(x.keys.length);
    const fullAt = latest().w!.fullAt;

    let size = 4;
    for (let revision = 0; revision < 3; revision++) {
      size += 6;
      await publish(x, [...generate(8), ...appended(size), ...contenders()]);
      await pointers();
      const before = latest().digest;
      const p = await pass();
      // The acceptance outcome again, without the full read.
      expect(p.result).toEqual({ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, skippedBeforeRead: true, acceptanceHold: { deltaChunks: expect.any(Number) }, reasons: HELD_REASONS });
      expect(p.result.acceptanceHold!.deltaChunks).toBeGreaterThan(0);
      expect(p.reads).toBe(p.result.acceptanceHold!.deltaChunks);
      expect(p.reads).toBeLessThan(fullReads);
      // Nothing written in Postgres; in ClickHouse only the advanced stamp.
      expect(sqlWrites(p.statements)).toEqual([]);
      expect(chWrites(p.calls)).toEqual([expect.objectContaining({ ch: "insert", table: INGEST_STATE_TABLE })]);
      expect(latest().digest).not.toBe(before);
      // The hold keeps the time of the full read that found the refusal.
      expect(latest().w!.fullAt).toBe(fullAt);
      // A repeat at the same revision is the plain skip: no object read, no write at all.
      const repeat = await pass();
      expect(repeat.result).toEqual({ manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, skippedBeforeRead: true, reasons: HELD_REASONS });
      expect(repeat.reads).toBe(0);
      expect(chWrites(repeat.calls)).toEqual([]);
    }
  }, 240_000);

  test("no Postgres lock is held across the probe's object and ClickHouse reads, nor across the stamp write", async () => {
    const { x, pass, pointers } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const p = await pass();
    expect(p.result.acceptanceHold).toBeDefined();
    expect(p.reads).toBeGreaterThan(0);
    // Two transactions: the probe's lock-free pointer snapshot, then the locked pass. Neither saw an object read
    // or a ClickHouse call while open: every read ran before the locked one, the stamp write after it.
    expect(p.transactions).toEqual([{ locked: false, reads: 0, ch: 0 }, { locked: true, reads: 0, ch: 0 }]);
  }, 120_000);

  test("the cached outcome is the fresh one: with both flags off the same revision still refuses for acceptance", async () => {
    const { x, pass, pointers } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const cached = await pass();
    const fresh = await pass({});
    expect(cached.result.acceptanceHold).toBeDefined();
    expect(fresh.result).toMatchObject({ complete: false, inserted: 0, deleted: 0, reasons: [ACCEPTANCE_MISSING] });
    expect(fresh.result.skippedBeforeRead).toBeUndefined();
    expect(cached.result.complete).toBe(fresh.result.complete);
    expect(cached.result.inserted).toBe(fresh.result.inserted);
    expect(cached.result.reasons[0]).toBe(fresh.result.reasons[0]!);
  }, 120_000);

  test("the hosted streaming configuration (stream budgets and the invocation's stream slot): held the same way", async () => {
    const { x, pass, pointers } = await rig();
    const hosted = () => ({ ...ON, stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 }, streamSlot: { used: false } });
    const first = await pass(hosted());
    expect(first.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const p = await pass(hosted());
    expect(p.result).toMatchObject({ complete: false, skippedBeforeRead: true, reasons: HELD_REASONS });
    expect(p.result.acceptanceHold).toBeDefined();
    expect(p.reads).toBeLessThan(first.reads);
    expect(chWrites(p.calls)).toEqual([expect.objectContaining({ ch: "insert", table: INGEST_STATE_TABLE })]);
    // Acceptance arrives: one full pass indexes it.
    await publish(x, [...generate(8), ...appended(12), ...contenders(true)]);
    await pointers();
    expect((await pass(hosted())).result).toMatchObject({ complete: true, reasons: [] });
  }, 120_000);

  test("without the watermark flag the acceptance skip is as before: a plain stamp, and a new revision reads in full", async () => {
    const { x, pass, pointers, latest } = await rig();
    const first = await pass(SKIP);
    expect(latest().w).toBeUndefined();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const p = await pass(SKIP);
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(p.result.skippedBeforeRead).toBeUndefined();
    expect(p.reads).toBeGreaterThanOrEqual(first.reads);
    // A plain stamp never licenses a hold once the watermark flag is on either.
    await publish(x, [...generate(8), ...appended(20), ...contenders()]);
    await pointers();
    expect((await pass()).result.acceptanceHold).toBeUndefined();
  }, 120_000);
});

describe("acceptance arrives", () => {
  test("seats on the contenders at the next revision: exactly one full pass indexes the scope and the stamp clears", async () => {
    const { x, pass, pointers, stamps } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    expect((await pass()).result.acceptanceHold).toBeDefined();

    await publish(x, [...generate(8), ...appended(12), ...contenders(true)]);
    await pointers();
    const indexed = await pass();
    expect(indexed.result).toMatchObject({ complete: true, reasons: [] });
    expect(indexed.result.skippedBeforeRead).toBeUndefined();
    expect(indexed.result.inserted).toBeGreaterThan(0);
    expect(stamps().at(-1)!.value).toBe("");
    // Exactly one index: the pass after it finds every row present and holds nothing.
    const after = await pass();
    expect(after.result).toMatchObject({ complete: true, inserted: 0, deleted: 0 });
    expect(after.result.acceptanceHold).toBeUndefined();
    expect(stamps().at(-1)!.value).toBe("");
  }, 120_000);

  test("the deciding seats arrive in ANOTHER manifest (origin A's and B's own manifests unchanged): not held, indexed, same as a fresh evaluation", async () => {
    const { x, r, pass, pointers, stamps } = await rig();
    await pass();
    const contendKeys = x.keys.filter((k) => k.includes("contend-"));
    expect(contendKeys).toHaveLength(2);
    const before = await Promise.all(contendKeys.map(async (k) => (await x.reader.read(k))!.manifest.revision));
    // machine:0's manifest gains the accepted copies of both contenders' records (same origin, record id and
    // revision, now seated): the fold merges the seats and the group resolves. A's and B's manifests are untouched.
    const seated = contenders(true).map((line) => ({ ...line, machine: "machine:0" }));
    await publish(x, [...generate(8), ...appended(4), ...seated, ...contenders()]);
    await pointers();
    expect(await Promise.all(contendKeys.map(async (k) => (await x.reader.read(k))!.manifest.revision))).toEqual(before);

    const cached = await pass();
    expect(cached.result.acceptanceHold).toBeUndefined();
    expect(cached.result.skippedBeforeRead).toBeUndefined();
    expect(cached.result).toMatchObject({ complete: true, reasons: [] });
    expect(cached.result.inserted).toBeGreaterThan(0);
    expect(stamps().at(-1)!.value).toBe("");
    // Fresh evaluation of the same content on a rig that never held anything, both flags off.
    const fresh = await rig([...generate(8), ...appended(4), ...seated, ...contenders()]);
    const evaluated = await fresh.pass({});
    expect(evaluated.result).toMatchObject({ complete: true, reasons: [] });
    expect(cached.result.rows).toBe(evaluated.result.rows);
    // The same index, row for row (the fresh rig indexed it on its first tick).
    expect(r.fake.dump().length).toBeGreaterThan(0);
    expect(r.fake.dump()).toEqual(fresh.r.fake.dump());
  }, 240_000);

  test("a new contender for the refused id from a third origin, still unseated: not held (the pass reads in full and re-stamps)", async () => {
    const { x, pass, pointers, latest } = await rig();
    const first = await pass();
    const third = activity("machine:0", "capacity:third", "session_event", CONTENDED_ID, Date.parse("2026-11-01T10:00:00.800Z"), { role: "user", text: "c" });
    await publish(x, [...generate(8), ...appended(4), third, ...contenders()]);
    await pointers();
    const p = await pass();
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(p.reads).toBeGreaterThanOrEqual(first.reads);
    expect(latest().w).toBeDefined();
  }, 120_000);
});

describe("what never holds", () => {
  test("erase: every pointer tombstoned after a hold; the pass erases and clears the stamp", async () => {
    const { x, r, pass, pointers, stamps } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    expect((await pass()).result.acceptanceHold).toBeDefined();
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE org_id = $1 AND workspace_id = $2", [SCOPE.orgId, SCOPE.workspaceId]);
    const erased = await pass();
    expect(erased.result).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(stamps().at(-1)!.value).toBe("");
  }, 120_000);

  test("a sibling manifest gone, a pointer not read as listed, or other limits", async () => {
    const { x, r, pass, pointers } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    // Other limits: another watermark base.
    expect((await pass({ ...ON, limits: { maxScopeBytes: 50_000_000, maxScopeEvents: 123_456 } })).result.acceptanceHold).toBeUndefined();
    await pass();
    await publish(x, [...generate(8), ...appended(18), ...contenders()]);
    await pointers();
    // The pointer row names a revision the manifest read does not have.
    const moved = x.keys.find((k) => !k.includes("contend-"))!;
    await r.db.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [moved]);
    const unlisted = await pass();
    expect(unlisted.result.acceptanceHold).toBeUndefined();
    expect(unlisted.result.skippedBeforeRead).toBeUndefined();
    await r.db.pg.query("UPDATE sessions.session_index SET revision = revision - 1 WHERE manifest_key = $1", [moved]);
    await pass();
    // A sibling tombstoned: another manifest set.
    const contendB = x.keys.find((k) => k.includes("contend-b"))!;
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE manifest_key = $1", [contendB]);
    const without = await pass();
    expect(without.result.acceptanceHold).toBeUndefined();
    expect(without.result.skippedBeforeRead).toBeUndefined();
  }, 240_000);

  test("a pointer row changes between the probe and the locked pass: the hold is dropped under the locks and the pass reads in full", async () => {
    const { x, r, pass, pointers, race } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const moved = x.keys.find((k) => !k.includes("contend-"))!;
    let raced = false;
    race.afterProbe = async () => {
      raced = true;
      await r.db.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [moved]);
    };
    const p = await pass();
    expect(raced).toBe(true);
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result.skippedBeforeRead).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    // The probe's delta read, then the whole scope under the pass.
    expect(p.transactions.filter((t) => t.locked)).toHaveLength(1);
    expect(p.transactions.find((t) => t.locked)!.reads).toBeGreaterThan(0);
  }, 120_000);

  test("an erase lands between the probe and the locked pass: the pass erases, holds nothing, and leaves no live stamp", async () => {
    const { x, r, pass, pointers, stamps, race } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    race.afterProbe = () => r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE org_id = $1 AND workspace_id = $2", [SCOPE.orgId, SCOPE.workspaceId]);
    const p = await pass();
    expect(p.result).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(stamps().at(-1)!.value).toBe("");
    // And the erased scope stays unstamped: the next pass erases again, never holds.
    expect((await pass()).result).toMatchObject({ reasons: ["erased"] });
    expect(stamps().at(-1)!.value).toBe("");
  }, 120_000);

  test("the scope grows past the whole-read bound: the pass sizes it itself (refused for size here), never held", async () => {
    const { x, pass, pointers } = await rig();
    let bytes = 0;
    for (const k of x.keys) for (const c of (await x.reader.read(k))!.manifest.raw.chunks) bytes += Math.max(c.bytes, c.stored_raw_bytes ?? 0);
    const limits = { maxScopeBytes: bytes + 2_000, maxScopeEvents: 1_000_000 };
    expect((await pass({ ...ON, limits })).result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    await publish(x, [...generate(8), ...appended(8), ...contenders()]);
    await pointers();
    expect((await pass({ ...ON, limits })).result.acceptanceHold).toBeDefined();
    await publish(x, [...generate(8), ...appended(60), ...contenders()]);
    await pointers();
    const p = await pass({ ...ON, limits });
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large"] });
  }, 120_000);

  test("an unverifiable chunk prefix: an EARLIER chunk rewritten, or a stored chain that does not match, runs the ordinary pass", async () => {
    const { x, state, pass, pointers, stamps, latest } = await rig();
    const first = await pass();
    // machine:0's FIRST appended event changes: an early chunk of its manifest is rewritten, not just its tail.
    const rewritten = appended(12).map((line, i) => (i === 0 ? { ...line, record: { ...line.record, payload: { ...(line.record as { payload: object }).payload, text: `rewritten ${"y".repeat(200)}` } } } : line));
    await publish(x, [...generate(8), ...rewritten, ...contenders()]);
    await pointers();
    const p = await pass();
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result.skippedBeforeRead).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(p.reads).toBeGreaterThanOrEqual(first.reads);
    // A stamp whose stored chains match nothing (both the full and the all-but-last chain): never trusted.
    const value = latest();
    const multi = (value.w!.manifests as Array<{ n: number; c: string; p: string }>).find((m) => m.n > 1)!;
    expect(multi).toBeDefined();
    multi.c = "0".repeat(64);
    multi.p = "1".repeat(64);
    state.ingest.push({ ...stamps().at(-1)!, value: JSON.stringify(value), updated_at: stamps().at(-1)!.updated_at.replace(/\d$/, "9") });
    await publish(x, [...generate(8), ...rewritten, ...appended(20).slice(12), ...contenders()]);
    await pointers();
    const tampered = await pass();
    expect(tampered.result.acceptanceHold).toBeUndefined();
    expect(tampered.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(tampered.reads).toBeGreaterThanOrEqual(first.reads);
  }, 120_000);

  test("one workspace cannot suppress another: the stamp is named and bound per org, workspace, harness and session", async () => {
    const { x, state, pass, pointers, stamps, latest } = await rig();
    const other = { ...SCOPE, workspaceId: `${SCOPE.workspaceId}-other` };
    // Another name per workspace and per org, and another settings binding for the very same session.
    expect(acceptanceStampName(other)).not.toBe(acceptanceStampName(SCOPE));
    expect(acceptanceStampName({ ...SCOPE, orgId: `${SCOPE.orgId}-other` })).not.toBe(acceptanceStampName(SCOPE));
    const baseOf = (scope: typeof SCOPE) => acceptanceWatermarkBase(scope, x.canonicalId, "default:14d", SEARCH_INDEX_DEFAULT_LIMITS, undefined, undefined);
    expect(baseOf(other)).not.toBe(baseOf(SCOPE));
    await pass();
    const genuine = stamps().at(-1)!;
    // The other workspace's stamp row for the same session id sits beside ours: our passes never read it.
    state.ingest.push({ ...genuine, name: acceptanceStampName(other) });
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const held = await pass();
    expect(held.result.acceptanceHold).toBeDefined();
    const names = held.calls.filter((c) => JSON.stringify(c).includes("acceptance_skip/")).map((c) => JSON.stringify(c));
    expect(names.length).toBeGreaterThan(0);
    for (const call of names) expect(call).not.toContain(acceptanceStampName(other));
    // A watermark bound to the other workspace, even under OUR name with every chain and id intact: never held.
    const foreign = latest();
    (foreign.w as unknown as { base: string }).base = baseOf(other);
    state.ingest.push({ ...stamps().at(-1)!, value: JSON.stringify(foreign), updated_at: stamps().at(-1)!.updated_at.replace(/\d$/, "9") });
    await publish(x, [...generate(8), ...appended(18), ...contenders()]);
    await pointers();
    const p = await pass();
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result.skippedBeforeRead).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
  }, 120_000);

  test("the age bound: a hold never outlives SKIP_STATE_MAX_AGE_MS from the full read, however often it advanced", async () => {
    const { x, pass, pointers, clock, latest } = await rig();
    await pass();
    const fullAt = latest().w!.fullAt;
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    clock.ms = fullAt + SKIP_STATE_MAX_AGE_MS - 60_000;
    expect((await pass()).result.acceptanceHold).toBeDefined();
    await publish(x, [...generate(8), ...appended(18), ...contenders()]);
    await pointers();
    // The stamp was advanced a minute ago, but the full read is now at the bound: read in full, stamp anew.
    clock.ms = fullAt + SKIP_STATE_MAX_AGE_MS;
    const p = await pass();
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    expect(latest().w!.fullAt).toBeGreaterThan(fullAt);
  }, 120_000);

  test("a failing stamp read or manifest read fails open: the pass reads in full", async () => {
    const { x, state, pass, pointers } = await rig();
    await pass();
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    state.switches.failStampRead = true;
    const p = await pass();
    expect(p.result.acceptanceHold).toBeUndefined();
    expect(p.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
    state.switches.failStampRead = false;
    await publish(x, [...generate(8), ...appended(18), ...contenders()]);
    await pointers();
    // The probe's first manifest read throws; the pass's own reads are served.
    let failed = 0;
    const flaky = { provider: x.reader.provider, read: async (key: string) => { if (failed++ === 0) throw new Error("synthetic manifest read failure"); return x.reader.read(key); } };
    const broken = await pass({ ...ON, reader: flaky as never });
    expect(failed).toBeGreaterThan(1);
    expect(broken.result.acceptanceHold).toBeUndefined();
    expect(broken.result).toMatchObject({ complete: false, reasons: [ACCEPTANCE_MISSING] });
  }, 120_000);
});

describe("the real exported Lambda handler", () => {
  test("a new revision of an unaccepted session is acknowledged as held, with acceptance_hold on the log line", async () => {
    const { x, r, state, pointers } = await rig();
    const send = realHandler(r.consumer({ ch: state.ch, ...ON }));
    await send(r.messages);
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    await pointers();
    const { result, lines } = await captureLogs(() => send(r.messages));
    expect(result).toEqual({ batchItemFailures: [] });
    const reconciled = lines.filter((l) => l.message === "search index reconciled");
    expect(reconciled.at(-1)).toMatchObject({ ok: false, reason: HELD_REASONS.join(","), acceptance_hold: "watermark" });
    expect(reconciled.at(-1)!.delta_chunks).toBeGreaterThan(0);
  }, 120_000);
});

describe("what wakes a held scope", () => {
  test("the seat's own manifest event: ONE delivery through the real handler moves the pointers and indexes the scope, no sweep needed", async () => {
    const { x, r, state, stamps } = await rig();
    const send = realHandler(r.consumer({ ch: state.ch, ...ON }));
    await send(r.messages);
    // A new revision without acceptance: its delivery is held.
    await publish(x, [...generate(8), ...appended(12), ...contenders()]);
    const heldLines = (await captureLogs(() => send(r.messages))).lines.filter((l) => l.message === "search index reconciled");
    expect(heldLines.at(-1)).toMatchObject({ ok: false, acceptance_hold: "watermark" });
    // The seats are published: the writer's manifest PUTs are the only event. That one delivery runs the pointer
    // stage (the rows move to the new revisions) and then the search stage, which reads in full and indexes.
    await publish(x, [...generate(8), ...appended(12), ...contenders(true)]);
    const { result, lines } = await captureLogs(() => send(r.messages));
    expect(result).toEqual({ batchItemFailures: [] });
    expect(lines.filter((l) => l.message === "pointer indexed" && l.revision === 2).length).toBeGreaterThanOrEqual(2);
    const reconciled = lines.filter((l) => l.message === "search index reconciled");
    expect(reconciled.at(-1)).toMatchObject({ ok: true });
    expect(reconciled.some((l) => l.acceptance_hold !== undefined)).toBe(false);
    expect(stamps().at(-1)!.value).toBe("");
  }, 120_000);
});

describe("SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED in the Lambda", () => {
  const BASE = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q", SEARCH_INDEX_ENABLED: "1",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });
  test("the grammar: only true and false; true needs the acceptance skip", () => {
    for (const off of [undefined, "", "false"]) expect(searchIndexAcceptanceWatermarkFromEnv({ SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED: off })).toBe(false);
    expect(searchIndexAcceptanceWatermarkFromEnv({ SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED: "true" })).toBe(true);
    for (const bad of ["1", "TRUE", "on", " true"])
      expect(() => searchIndexAcceptanceWatermarkFromEnv({ SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED: bad })).toThrow("SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED must be true or false");
  });
  test("lambdaEnv and the search deps: off is exactly the acceptance skip alone; on alone refuses; on with the skip arms it", async () => {
    const skip = { ...BASE, SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: "true" };
    const { adapterEnv: _plainAdapter, ...plainEnv } = lambdaEnv(skip);
    expect(() => lambdaEnv({ ...BASE, SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED: "true" })).toThrow("SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED=true needs SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true");
    for (const [value, armed] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const { adapterEnv, ...env } = lambdaEnv({ ...skip, ...(value === undefined ? {} : { SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED: value }) });
      expect(env).toEqual(armed ? { ...plainEnv, searchAcceptanceWatermark: true } : plainEnv);
      const rt = runtimeFromSecret({ ...env, adapterEnv }, secret, "synthetic-generation");
      try {
        expect("acceptanceWatermark" in rt.deps.search!).toBe(armed);
        expect("skipAcceptanceMissing" in rt.deps.search!).toBe(true);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
  });
});
