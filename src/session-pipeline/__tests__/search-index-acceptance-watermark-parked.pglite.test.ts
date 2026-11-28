/**
 * SEARCH_INDEX_ACCEPTANCE_WATERMARK_ENABLED: a manifest replaced with accepted content at the SAME revision while
 * a pass is parked between its probe and its lock (pointer rows fixed throughout). Post-review fixture for T125
 * on core 974388c9; tests only.
 *
 * Phases, each asserted: (1) warm a refusal; (2) a delivery through the real exported handler runs the probe to
 * the end, successfully; the pass is parked right before its locked transaction; (3) the manifests are replaced
 * and the pointer rows proved unchanged; (4) the pass resumes: its outcome, index writes and stamp effects;
 * (5) the replacement's manifest events are delivered (SYNTHETIC delivery into the ordinary handler); (6) the
 * final index rows and stamp are compared with a fresh evaluation of the same final content, both skip flags
 * off, on a rig that never cached anything.
 *
 * Real: PGlite with the migrations and triggers, the pointer stage, the exporter, an on-disk object store, the
 * reader, the fold, the search stage, the exported Lambda handler. Mocked: ClickHouse (in-memory fake), the
 * clock, and event delivery (synthetic batches). Synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { ACCEPTANCE_MISSING, SKIPPED_BEFORE_READ, acceptanceStampName, type SearchIndexDeps } from "../search-index.ts";
import { INGEST_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { ID, SCOPE, T0, USER, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { HOSTED_NOW, hostedRig } from "./search-index-hosted-harness.ts";
import { captureLogs, realHandler, statefulClickHouse } from "./search-index-skip-flags-harness.ts";

const ON: Partial<SearchIndexDeps> = { skipAcceptanceMissing: true, acceptanceWatermark: true };
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const HELD = `${ACCEPTANCE_MISSING},${SKIPPED_BEFORE_READ}`;

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

function contenders(seats = false): Line[] {
  return [
    activity("machine:contend-a", "capacity:contend-a", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.600Z"), { role: "user", text: "a" }, "1", seats ? 1_000_001 : undefined),
    activity("machine:contend-b", "capacity:contend-b", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.700Z"), { role: "user", text: "b" }, "1", seats ? 1_000_002 : undefined),
  ];
}
const appended = (n: number): Line[] =>
  Array.from({ length: n }, (_, i) => activity("machine:0", "capacity:0", "session_event", `appended-${i}`, T0 + 30_000 + i * 10, { role: "user", text: `appended ${i} ${"x".repeat(200)}` }));
const content = (n: number, seats = false): Line[] => [...generate(8), ...appended(n), ...contenders(seats)];

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

/** Replace every manifest that `lines` changes with its new content at its CURRENT revision. Returns how many. */
async function replaceAtSameRevision(x: ExportedScope, lines: Line[]): Promise<number> {
  const prior = new Map<string, number>();
  for (const k of x.keys) prior.set(k, (await x.reader.read(k))!.manifest.revision);
  await publish(x, lines);
  let replaced = 0;
  for (const k of x.keys) {
    const now = (await x.reader.read(k))!.manifest;
    if (now.revision === prior.get(k)) continue;
    await x.store.put(objectKey(k as never), new TextEncoder().encode(JSON.stringify({ ...now, revision: prior.get(k) })), { contentType: "application/json" });
    replaced += 1;
  }
  return replaced;
}

async function rig(lines: Line[]) {
  const x = await exportStreamScope(lines, 1200);
  cleanup.push(x.dispose);
  const r = await hostedRig(x, cleanup);
  const state = statefulClickHouse(r.fake);
  await r.tick({ ch: state.ch });
  const clock = { ms: HOSTED_NOW.getTime() };
  // The search stage's manifest reads since the delivery began, and the hook that parks it: run once, right
  // before the first transaction that opens AFTER the search stage has read every sibling manifest. With the
  // watermark flag on those reads are the probe's, so that transaction is the locked pass.
  const park: { manifestReads: number; beforeLocked?: () => Promise<unknown>; parkedAfterReads?: number } = { manifestReads: 0 };
  const reader = { provider: x.reader.provider, read: async (key: string) => { park.manifestReads += 1; return x.reader.read(key); } };
  const transact: SearchIndexDeps["transact"] = async (work) => {
    if (park.beforeLocked && park.manifestReads >= x.keys.length) {
      const run = park.beforeLocked;
      park.beforeLocked = undefined;
      park.parkedAfterReads = park.manifestReads;
      await run();
    }
    return r.transact(work);
  };
  /** One synthetic delivery of the scope's manifest events into the real exported handler. */
  const deliver = async (flags: Partial<SearchIndexDeps>) => {
    clock.ms += 1_000;
    park.manifestReads = 0;
    const chunkReads = r.reads.count, sql = r.sql.length;
    r.fake.calls.length = 0;
    const send = realHandler(r.consumer({ ch: state.ch, now: () => new Date(clock.ms), reader: reader as never, transact, ...flags }));
    const { result, lines: logged } = await captureLogs(() => send(r.messages));
    const searchSql = r.sql.slice(sql).map((s) => String(s.sql));
    return {
      result,
      reconciled: logged.filter((l) => l.message === "search index reconciled"),
      chunkReads: r.reads.count - chunkReads,
      stampWrites: r.fake.calls.filter((c) => c.ch === "insert" && c.table === INGEST_STATE_TABLE).length,
      directoryWrites: searchSql.filter((s) => /session_directory/i.test(s) && /^(UPDATE|INSERT|DELETE)\b/i.test(s)).length,
    };
  };
  const pointers = () => r.tick({ ch: state.ch, limits: TINY });
  const pointerRows = async () => JSON.stringify((await r.db.pg.query(
    "SELECT manifest_key, revision::text AS revision, encode(content_sha256, 'hex') AS sha, manifest_etag FROM sessions.session_index ORDER BY manifest_key")).rows);
  const stamps = () => state.ingest.filter((s) => s.name === acceptanceStampName(SCOPE));
  return { x, r, state, deliver, pointers, pointerRows, stamps, park };
}

/** Fresh evaluation: the final content on a rig that never cached anything, both skip flags off. */
async function freshEvaluation(lines: Line[]) {
  const fresh = await rig(lines);
  const delivered = await fresh.deliver({});
  return { index: fresh.r.fake.dump(), stamps: fresh.stamps(), last: delivered.reconciled.at(-1)! };
}

describe("a manifest replaced at the same revision while the handler is parked between probe and lock", () => {
  test("parked after a successful VERIFICATION (same pointer set): the parked pass repeats the refusal and publishes nothing; the replacement's delivery re-evaluates; index and stamp equal fresh", async () => {
    const { x, r, deliver, pointerRows, stamps, park } = await rig(content(4));
    // Phase 1: warm. A full read refuses for acceptance and stamps a watermark.
    const warm = await deliver(ON);
    expect(warm.reconciled.at(-1)).toMatchObject({ ok: false, reason: ACCEPTANCE_MISSING });
    expect(JSON.parse(stamps().at(-1)!.value).w).toBeDefined();
    const pointersBefore = await pointerRows();
    const indexBefore = r.fake.dump();
    const stampsBefore = stamps().length;
    // Phases 2-4: the next delivery's probe verifies every manifest against the stamp; parked before the lock,
    // both contenders' manifests are replaced with the accepted content at their current revision.
    let replaced = 0;
    park.beforeLocked = async () => { replaced = await replaceAtSameRevision(x, content(4, true)); };
    const parked = await deliver(ON);
    expect(park.parkedAfterReads).toBe(x.keys.length);
    expect(replaced).toBe(2);
    expect(await pointerRows()).toBe(pointersBefore);
    // The parked pass: the refusal it verified, repeated. No chunk read, no index row, no directory write, no stamp write.
    expect(parked.result).toEqual({ batchItemFailures: [] });
    expect(parked.reconciled.at(-1)).toMatchObject({ ok: false, reason: HELD, inserted: 0, deleted: 0 });
    expect(parked.chunkReads).toBe(0);
    expect(r.fake.dump()).toEqual(indexBefore);
    expect(parked.directoryWrites).toBe(0);
    expect(parked.stampWrites).toBe(0);
    expect(stamps()).toHaveLength(stampsBefore);
    // Phase 5: the replacement's own manifest events, delivered synthetically. Pointer rows still fixed.
    const woken = await deliver(ON);
    expect(await pointerRows()).toBe(pointersBefore);
    expect(woken.result).toEqual({ batchItemFailures: [] });
    expect(woken.reconciled.at(-1)).toMatchObject({ ok: true });
    expect(String(woken.reconciled.at(-1)!.reason ?? "")).not.toContain(SKIPPED_BEFORE_READ);
    expect(woken.chunkReads).toBeGreaterThan(0);
    // Phase 6: final index rows and stamp against a fresh evaluation.
    const fresh = await freshEvaluation(content(4, true));
    expect(fresh.last).toMatchObject({ ok: true });
    expect(r.fake.dump().length).toBeGreaterThan(indexBefore.length);
    expect(r.fake.dump()).toEqual(fresh.index);
    expect(stamps().at(-1)!.value).toBe("");
    expect(fresh.stamps).toEqual([]);
  }, 240_000);

  test("parked after a successful HOLD probe (a new unaccepted revision): the parked pass holds and publishes nothing but its advanced stamp; the replacement's delivery re-evaluates; index and stamp equal fresh", async () => {
    const { x, r, deliver, pointers, pointerRows, stamps, park } = await rig(content(4));
    // Phase 1: warm, then a new unaccepted revision whose pointer rows have moved (so the next pass probes for a hold).
    await deliver(ON);
    await publish(x, content(12));
    await pointers();
    const pointersBefore = await pointerRows();
    const indexBefore = r.fake.dump();
    const stampsBefore = stamps().length;
    let replaced = 0;
    park.beforeLocked = async () => { replaced = await replaceAtSameRevision(x, content(12, true)); };
    const parked = await deliver(ON);
    expect(park.parkedAfterReads).toBe(x.keys.length);
    expect(replaced).toBe(2);
    expect(await pointerRows()).toBe(pointersBefore);
    // The parked pass: held on what its probe read. No index row, no directory write; ONE stamp write (the advance).
    expect(parked.reconciled.at(-1)).toMatchObject({ ok: false, reason: HELD, acceptance_hold: "watermark", inserted: 0, deleted: 0 });
    expect(r.fake.dump()).toEqual(indexBefore);
    expect(parked.directoryWrites).toBe(0);
    expect(parked.stampWrites).toBe(1);
    expect(stamps()).toHaveLength(stampsBefore + 1);
    expect(JSON.parse(stamps().at(-1)!.value).w).toBeDefined();
    // Phase 5: the replacement's delivery (synthetic). The advanced stamp matches the fixed pointer rows, but its
    // watermark is of the manifests the probe read, not the replaced ones: no verification, a full read.
    const woken = await deliver(ON);
    expect(await pointerRows()).toBe(pointersBefore);
    expect(woken.reconciled.at(-1)).toMatchObject({ ok: true });
    expect(woken.reconciled.at(-1)!.acceptance_hold).toBeUndefined();
    expect(woken.chunkReads).toBeGreaterThan(0);
    // Phase 6.
    const fresh = await freshEvaluation(content(12, true));
    expect(fresh.last).toMatchObject({ ok: true });
    expect(r.fake.dump()).toEqual(fresh.index);
    expect(stamps().at(-1)!.value).toBe("");
    expect(fresh.stamps).toEqual([]);
  }, 240_000);
});
