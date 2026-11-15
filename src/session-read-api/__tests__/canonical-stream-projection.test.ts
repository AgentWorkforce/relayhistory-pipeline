/**
 * Phase 2 (issue 610): the streamed reconcile reads a too-large scope in passes
 * and must reproduce exactly what the whole read (readCanonicalScope) derives
 * from the same manifests: index rows, directory events, catalog and sources.
 * Synthetic, secret-free fixtures in a temporary fs object store; no database.
 */
import { afterEach, expect, test } from "bun:test";
import type { ManifestV1 } from "../../../packages/object-store/src/index.ts";
import { deliveryMachine } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { readCanonicalScope, type ScopeRead } from "../canonical-projection.ts";
import {
  STREAM_READ_BYTES_BUDGET,
  STREAM_READ_EVENTS_BUDGET,
  STREAM_SOURCE_CHANGED,
  StreamBudgetError,
  StreamReadMeter,
  streamCanonicalScope,
  streamIndexKey,
  type StreamDeps,
  type StreamedScope,
} from "../canonical-stream-projection.ts";
import type { IndexRow } from "../../session-pipeline/vendor/search-index/rows.ts";
import { SCOPE, T0, activity, decompress, direct, exportStreamScope, generate, prefixFor, ID, type Line } from "./stream-fixture.ts";

const VERSION = 7;
const BIG = { maxScopeBytes: 1 << 30, maxScopeEvents: 1 << 24, maxRows: 1 << 24, insertBatch: 100 };
const TINY = { ...BIG, maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0)) await c();
});

interface Fixture {
  keys: string[];
  canonicalId: string;
  readDeps: Parameters<typeof readCanonicalScope>[2];
  streamDeps: StreamDeps;
}

async function fixture(lines: Line[], maxChunkBytes = 1500, wrap?: (d: StreamDeps["decompress"]) => StreamDeps["decompress"], id: typeof ID = ID): Promise<Fixture> {
  const x = await exportStreamScope(lines, maxChunkBytes, id);
  cleanup.push(x.dispose);
  return {
    keys: x.keys,
    canonicalId: x.canonicalId,
    readDeps: { reader: x.reader, store: x.store, prefixFor, decompress },
    streamDeps: { store: x.store, prefixFor, decompress: wrap ? wrap(decompress) : decompress },
  };
}

async function whole(f: Fixture, scope = SCOPE): Promise<ScopeRead> {
  return readCanonicalScope(scope, f.keys, f.readDeps, BIG, VERSION, f.canonicalId);
}

async function admitted(f: Fixture, scope = SCOPE): Promise<NonNullable<ScopeRead["admitted"]>> {
  const read = await readCanonicalScope(scope, f.keys, f.readDeps, TINY, VERSION, f.canonicalId);
  expect(read).toMatchObject({ partial: true, complete: false, reasons: ["scope_too_large"] });
  return read.admitted!;
}

const indexKey = (r: IndexRow) => streamIndexKey({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id });
const tsMs = (r: IndexRow) => Date.parse(`${r.ts.replace(" ", "T")}Z`);

async function regenerateAll(s: StreamedScope, batch = 7): Promise<IndexRow[]> {
  const out: IndexRow[] = [];
  await s.regenerate(new Set(s.wanted.keys()), batch, async (rows) => {
    expect(rows.length).toBeLessThanOrEqual(batch);
    out.push(...rows);
  });
  return out;
}

/** The streamed read equals the whole read: what it would write and what it would record. */
async function expectParity(f: Fixture, fromMs = 0, scope = SCOPE): Promise<{ w: ScopeRead; s: StreamedScope }> {
  const w = await whole(f, scope);
  const s = await streamCanonicalScope(scope, await admitted(f, scope), f.streamDeps, UNLIMITED, VERSION, fromMs);
  expect({ complete: s.complete, reasons: s.reasons }).toEqual({ complete: w.complete, reasons: w.reasons });
  expect(s.partial).toBe(false);
  expect(s.manifests).toBe(w.manifests);
  expect(s.sources).toEqual(w.sources);
  expect(s.directorySessionId).toBe(w.directorySessionId);
  expect(s.catalog).toEqual(w.catalog);
  expect(s.events).toEqual(w.events);
  if (!w.complete) return { w, s };
  // The whole read's wanted map: the last row of a key wins.
  const expected = new Map<string, IndexRow>();
  for (const r of w.rows.filter((r) => tsMs(r) >= fromMs)) expected.set(indexKey(r), r);
  expect(s.inWindow).toBe(w.rows.filter((r) => tsMs(r) >= fromMs).length);
  expect([...s.wanted.keys()].sort()).toEqual([...expected.keys()].sort());
  for (const [key, r] of expected) {
    const got = s.wanted.get(key)!;
    expect({ ...s.owners[got.owner], tsMs: got.tsMs, fingerprint: got.fingerprint }).toEqual({
      sessionId: r.session_id, workspaceId: r.workspace_id, tsMs: tsMs(r), fingerprint: r.fingerprint,
    });
  }
  expect([...s.kinds].sort()).toEqual([...new Set([...expected.values()].map((r) => r.kind))].sort());
  const rows = await regenerateAll(s);
  expect(new Map(rows.map((r) => [indexKey(r), r]))).toEqual(expected);
  expect(rows.length).toBe(expected.size);
  return { w, s };
}

test("streamed read equals the whole read over generated sessions (many seeds, chunk boundaries)", async () => {
  let rows = 0, chunks = 0;
  for (let seed = 1; seed <= 40; seed++) {
    const f = await fixture(generate(seed), seed % 2 ? 900 : 4000);
    const { w, s } = await expectParity(f);
    rows += w.rows.length;
    chunks += s.meter.chunks;
    // Passes 1 to 3 read every chunk once; regenerating reads them once more (none to regenerate: no read).
    expect(s.meter.passes).toBe(s.wanted.size ? 4 : 3);
    await cleanup.splice(0)[0]!();
  }
  expect(rows).toBeGreaterThan(500);
  expect(chunks).toBeGreaterThan(100);
}, 120_000);

test("a window keeps only in-window rows wanted, and regenerates exactly those", async () => {
  const f = await fixture(generate(101));
  await expectParity(f, T0 + 4000);
}, 30_000);

test("lead note 4: a cross-group (ts, event_id) tie orders by group, not by fold position", async () => {
  // machine:1's manifest owns the earliest direct line, so its group projects
  // first, although machine:0's tied line comes first in fold order.
  const lines = [
    direct("machine:1", "early", T0, { content: "not an opening event" , type: "assistant", actor_role: "assistant" }),
    direct("machine:0", "tied", T0 + 1000, { type: "user", actor_role: "user", content: "opening from machine 0" }),
    direct("machine:1", "tied", T0 + 1000, { type: "user", actor_role: "user", content: "opening from machine 1" }),
    activity("machine:0", "capacity:a", "session_event", "later", T0 + 2000, { role: "assistant", text: "reply" }, "1", 1),
  ];
  // Make "early" fold first and tie the other two (same ts and event id).
  const f = await fixture(lines);
  const { w, s } = await expectParity(f);
  expect(w.complete).toBe(true);
  expect(s.events!.openingEvent).toMatchObject({ machine_id: "machine:1", event_id: "tied" });
}, 30_000);

test("an activity line that replaces a direct line with the same key keeps parity", async () => {
  const origin = "capacity:shadow";
  const machine = deliveryMachine(ID.workspaceId, origin);
  const lines = [
    direct(machine, `${machine}:shared`, T0, { kind: "session_event", content: "direct copy" }),
    activity(machine, origin, "session_event", "shared", T0 + 500, { role: "user", text: "activity copy" }, "1", 1),
    direct(machine, `${machine}:other`, T0 + 1000),
  ];
  const f = await fixture(lines);
  const { w } = await expectParity(f);
  expect(w.complete).toBe(true);
}, 30_000);

test("edge records keep parity: codex tool captures, legacy, cross-origin and missing accepted_seq", async () => {
  const CODEX = { ...ID, source: "codex" };
  const scope = { ...SCOPE, harness: "codex" };
  const origin = "capacity:codex";
  const machine = deliveryMachine(ID.workspaceId, origin);
  const codex = { source: "codex", session_id: "codex-native", message_id: "call-msg" };
  const lines: Line[] = [
    // A codex tool call and the assistant tool_use event it names: the activity row takes the tool name.
    activity(machine, origin, "tool_call", "call-1", T0, { ...codex, name: "Bash", input: { cmd: "ls" } }, "1", 1),
    activity(machine, origin, "session_event", "use-1", T0 + 100, { ...codex, kind: "tool_use", role: "assistant", text: "run" }, "1", 2),
    // A direct line whose delivery_record_id names that event: the whole read never applies a
    // capture to a direct group, so neither may the stream (lead P3-1).
    direct(machine, `${machine}:direct-1`, T0 + 200, { source: "codex", kind: "session_event", delivery_record_id: "use-1" }),
    // One record id from two origins: the later accepted contender wins.
    activity(machine, origin, "session_event", "shared-id", T0 + 300, { role: "user", text: "first origin" }, "1", 3),
    activity(machine, "capacity:other", "session_event", "shared-id", T0 + 400, { role: "user", text: "second origin" }, "1", 4),
    // No accepted_seq and no contender: kept.
    activity(machine, origin, "session_event", "unsequenced", T0 + 500, { role: "assistant", text: "no seat" }),
    // A legacy record with a direct line's seat: a legacy kind never contends.
    { machine, record: { kind: "legacy_scope", origin_id: `machine:${machine}`, record_id: `${machine}:direct-1`, revision: "1", operation: "upsert", received_at: new Date(T0).toISOString(), payload: { note: "legacy" } } },
  ].map((l) => (l.record.kind === "convergence_event" ? { ...l, record: { ...l.record, payload: { ...l.record.payload, source: "codex" } } } : l));
  const f = await fixture(lines, 1500, undefined, CODEX);
  const { w, s } = await expectParity(f, 0, scope);
  expect(w.complete).toBe(true);
  const rows = await regenerateAll(s);
  expect(rows.length).toBe(5);
  // The capture reached the activity tool_use row and not the direct row naming the same event.
  const byId = (suffix: string) => w.rows.find((r) => r.event_id.endsWith(`:${suffix}`)) as unknown as Record<string, unknown>;
  expect(byId("use-1")).toMatchObject({ type: "tool_use", tool_name: "Bash" });
  expect(byId("direct-1")).toMatchObject({ type: "assistant", tool_name: "" });
  // The later accepted of the two origins wins; the legacy record projects no row.
  expect(w.rows.filter((r) => r.event_id.endsWith(":shared-id")).length).toBe(1);
  expect(byId("unsequenced")).toBeDefined();
  const missing = await fixture([
    ...lines.slice(0, 3),
    activity(machine, origin, "session_event", "unseated", T0 + 600, { role: "user", text: "a" }),
    activity(machine, "capacity:other", "session_event", "unseated", T0 + 700, { role: "user", text: "b" }),
  ], 1500, undefined, CODEX);
  // Contending ids without acceptance seats: both reads stop incomplete on the same code (the
  // whole read goes on and may list more codes; the stream stops at the first, as documented).
  const mw = await whole(missing, scope);
  const ms = await streamCanonicalScope(scope, await admitted(missing, scope), missing.streamDeps, UNLIMITED, VERSION, 0);
  expect(mw.complete).toBe(false);
  expect(ms).toMatchObject({ complete: false, partial: false });
  expect(mw.reasons).toContain("relayhistory_acceptance_missing");
  expect(ms.reasons).toContain("relayhistory_acceptance_missing");
}, 30_000);

test("the up-front budget check refuses before reading: both codes", async () => {
  const f = await fixture(generate(5));
  const a = await admitted(f);
  const bytes = await streamCanonicalScope(SCOPE, a, f.streamDeps, { maxReadBytes: 1, maxReadEvents: 1 << 24 }, VERSION, 0);
  expect(bytes).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large", STREAM_READ_BYTES_BUDGET] });
  expect(bytes.meter.chunks).toBe(0);
  expect(bytes.meter.passes).toBe(0);
  const events = await streamCanonicalScope(SCOPE, a, f.streamDeps, { maxReadBytes: 1 << 30, maxReadEvents: 1 }, VERSION, 0);
  expect(events).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large", STREAM_READ_EVENTS_BUDGET] });
  expect(events.meter.passes).toBe(0);
  await expect(bytes.regenerate(new Set(), 1, async () => {})).rejects.toThrow();
}, 30_000);

test("undeclared record counts: the events budget stops the stream mid-read, and after pass 1", async () => {
  const lines = generate(6);
  const f = await fixture(lines);
  const a = (await admitted(f)).map(({ manifest, key }) => ({
    key,
    manifest: { ...manifest, raw: { ...manifest.raw, chunks: manifest.raw.chunks.map(({ records: _r, ...c }) => c) } } as ManifestV1,
  }));
  const total = lines.length;
  const mid = await streamCanonicalScope(SCOPE, a, f.streamDeps, { maxReadBytes: 1 << 30, maxReadEvents: 3 }, VERSION, 0);
  expect(mid).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large", STREAM_READ_EVENTS_BUDGET] });
  expect(mid.meter.passes).toBe(1);
  expect(mid.meter.events).toBe(4);
  // Pass 1 reads everything, then its actual count times four is over.
  const after = await streamCanonicalScope(SCOPE, a, f.streamDeps, { maxReadBytes: 1 << 30, maxReadEvents: total * 4 - 1 }, VERSION, 0);
  expect(after).toMatchObject({ partial: true, reasons: ["scope_too_large", STREAM_READ_EVENTS_BUDGET] });
  expect(after.meter.passes).toBe(1);
  expect(after.meter.events).toBe(total);
  // Exactly four reads fit.
  const fits = await streamCanonicalScope(SCOPE, a, f.streamDeps, { maxReadBytes: 1 << 30, maxReadEvents: total * 4 }, VERSION, 0);
  expect(fits.complete).toBe(true);
  await regenerateAll(fits);
  expect(fits.meter.events).toBe(total * 4);
}, 30_000);

test("a chunk declared larger than it reads is budgeted for pass 4 after pass 1 (lead P3-3)", async () => {
  const sizes: number[] = [];
  const f = await fixture(generate(12), 900, (d) => (stored, max) => {
    const raw = d(stored, max);
    sizes.push(raw.length);
    return raw;
  });
  const a = await admitted(f);
  const n = a.reduce((k, { manifest }) => k + manifest.raw.chunks.length, 0);
  const probe = await streamCanonicalScope(SCOPE, a, f.streamDeps, UNLIMITED, VERSION, 0);
  expect(probe.complete).toBe(true);
  expect(n).toBeGreaterThan(3);
  const actual = sizes.slice(0, n);
  const A = actual.reduce((x, y) => x + y, 0);
  const last = actual[n - 1]!;
  // Every chunk declares 1 byte but the last, which declares `gap` more than it reads: the
  // declared total still fits four reads, and so do four actual reads.
  const gap = A - last - (n - 1);
  expect(gap).toBeGreaterThan(0);
  let seen = 0;
  const skewed = a.map(({ manifest, key }) => ({
    key,
    manifest: {
      ...manifest,
      raw: {
        ...manifest.raw,
        chunks: manifest.raw.chunks.map(({ stored_raw_bytes: _s, ...c }) => ({ ...c, bytes: ++seen === n ? last + gap : 1 })),
      },
    } as ManifestV1,
  }));
  const budget = { maxReadBytes: 4 * A, maxReadEvents: 1 << 24 };
  const s = await streamCanonicalScope(SCOPE, skewed, f.streamDeps, budget, VERSION, 0);
  // Pass 4's check of the last chunk would ask for 3A + (A - last) + last + gap > 4A: refused now.
  expect(s).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large", STREAM_READ_BYTES_BUDGET] });
  expect(s.meter.passes).toBe(1);
  expect(s.meter.peak).toBe(A + gap);
  // Declared exactly as read, the last pass fits the budget to the byte.
  seen = 0;
  const fits = a.map(({ manifest, key }) => ({
    key,
    manifest: {
      ...manifest,
      raw: { ...manifest.raw, chunks: manifest.raw.chunks.map(({ stored_raw_bytes: _s, ...c }) => ({ ...c, bytes: ++seen === n ? last : 1 })) },
    } as ManifestV1,
  }));
  const ok = await streamCanonicalScope(SCOPE, fits, f.streamDeps, budget, VERSION, 0);
  expect(ok.complete).toBe(true);
  await regenerateAll(ok);
  expect(ok.meter.passes).toBe(4);
}, 30_000);

test("the read meter checks a chunk before reading it and its decompressed size after", () => {
  const meter = new StreamReadMeter({ maxReadBytes: 100, maxReadEvents: 2 });
  meter.chunk({ bytes: 60 });
  meter.decompressed(60);
  expect(() => meter.chunk({ bytes: 41 })).toThrow(StreamBudgetError);
  meter.chunk({ bytes: 10, stored_raw_bytes: 40 });
  expect(() => meter.decompressed(41)).toThrow(expect.objectContaining({ budget: STREAM_READ_BYTES_BUDGET, code: "scope_too_large" }));
  meter.record();
  meter.record();
  expect(() => meter.record()).toThrow(expect.objectContaining({ budget: STREAM_READ_EVENTS_BUDGET }));
});

/** A decompressor that rewrites a marker once `after` chunks were read: the source changed between passes. */
function changing(after: () => number) {
  let calls = 0;
  return (d: StreamDeps["decompress"]): StreamDeps["decompress"] => (stored, max) => {
    const raw = d(stored, max);
    calls += 1;
    if (calls <= after()) return raw;
    const text = new TextDecoder().decode(raw);
    return text.includes("MARKER-A") ? new TextEncoder().encode(text.replace("MARKER-A", "MARKER-B")) : raw;
  };
}

function markerLines(): Line[] {
  return [
    ...generate(9),
    activity("machine:0", "capacity:marker", "session_event", "marked", T0 + 3000, { role: "user", text: "MARKER-A" }, "1", 999),
  ];
}

const withoutRawDigests = (a: NonNullable<ScopeRead["admitted"]>) =>
  a.map(({ manifest, key }) => ({
    key,
    manifest: { ...manifest, raw: { ...manifest.raw, chunks: manifest.raw.chunks.map(({ raw_sha256: _d, ...c }) => c) } } as ManifestV1,
  }));

test("a record that changes between passes stops the stream with stream_source_changed", async () => {
  let chunks = Number.POSITIVE_INFINITY;
  const f = await fixture(markerLines(), 1500, changing(() => chunks));
  const a = withoutRawDigests(await admitted(f));
  chunks = a.reduce((n, { manifest }) => n + manifest.raw.chunks.length, 0); // change after pass 1
  const s = await streamCanonicalScope(SCOPE, a, f.streamDeps, UNLIMITED, VERSION, 0);
  expect(s).toMatchObject({ complete: false, partial: false, reasons: [STREAM_SOURCE_CHANGED] });
  expect(s.wanted.size).toBe(0);
  await expect(s.regenerate(new Set(["x"]), 1, async () => {})).rejects.toThrow();
}, 30_000);

test("a record that changes before pass 4 refuses the regenerate with stream_source_changed", async () => {
  let chunks = Number.POSITIVE_INFINITY;
  const f = await fixture(markerLines(), 1500, changing(() => chunks));
  const a = withoutRawDigests(await admitted(f));
  chunks = 3 * a.reduce((n, { manifest }) => n + manifest.raw.chunks.length, 0); // change after pass 3
  const s = await streamCanonicalScope(SCOPE, a, f.streamDeps, UNLIMITED, VERSION, 0);
  expect(s.complete).toBe(true);
  const written: IndexRow[] = [];
  await expect(
    s.regenerate(new Set(s.wanted.keys()), 1_000_000, async (rows) => void written.push(...rows)),
  ).rejects.toMatchObject({ code: STREAM_SOURCE_CHANGED });
  expect(written).toEqual([]);
}, 30_000);

test("a chunk that disappears between passes stops the stream with the read's code", async () => {
  const f = await fixture(generate(11));
  const a = await admitted(f);
  let calls = 0;
  const total = a.reduce((n, { manifest }) => n + manifest.raw.chunks.length, 0);
  const deps: StreamDeps = {
    ...f.streamDeps,
    decompress: (stored, max) => {
      calls += 1;
      if (calls > total) throw new Error("not zstd");
      return decompress(stored, max);
    },
  };
  const s = await streamCanonicalScope(SCOPE, a, deps, UNLIMITED, VERSION, 0);
  expect(s).toMatchObject({ complete: false, partial: false, reasons: ["chunk_decompress_failed"] });
}, 30_000);
