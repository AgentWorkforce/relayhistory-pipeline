/**
 * The exported fold and settle pieces (issue 610 Phase 2) against the whole-read
 * functions built on them. A streaming reader cannot hold a session's records:
 * it folds payload-free seat summaries in one pass, then re-reads the records
 * and keeps the chosen copies, and it settles activity events from facts
 * gathered in a first pass. Both are spelled out here exactly as such a reader
 * would use the exports, over generated sessions with competing revisions,
 * origins, tombstones, tool pairs and requests, and must give the same records
 * and rows as `normalizeRelayhistoryRecords` and `projectRelayhistorySession`,
 * and fail on the same inputs.
 */
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  CONVERGENCE_EVENT_KIND,
  ProjectionSettlement,
  RELAYHISTORY_ACTIVITY_KINDS,
  compareProjectedEvents,
  projectRelayhistorySession,
  relayhistoryActivityEvent,
  relayhistoryCatalogKey,
  relayhistoryCatalogRows,
  relayhistoryDirectEvent,
  type ProjectionContext,
  type ProjectionRow,
} from "../src/lib/pipeline/relayhistory-projection.js";
import {
  chooseRelayhistoryRevision,
  compareRelayhistoryOrder,
  lastAccepted,
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  relayhistoryEventTime,
  relayhistoryNewestAcrossOrigins,
  relayhistoryRecordKey,
  relayhistoryRevisionContent,
  type RelayhistoryStreamRecord,
} from "../src/lib/pipeline/relayhistory-transform.js";

type Json = Record<string, unknown>;

function prng(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const MACHINE = "m1";
const context = (source: string): ProjectionContext => ({
  workspaceId: "ws-a",
  source,
  sessionId: "s1",
  machineId: MACHINE,
  userId: "u1",
});
const T = 1_793_000_000_000;
const USAGE = JSON.stringify({ input_tokens: 10, output_tokens: 4, cache_read_input_tokens: 2 });

/** A session's raw stream: several copies per record, out of order. */
function generate(seed: number, faults: boolean): RelayhistoryStreamRecord[] {
  const random = prng(seed);
  const pick = <T,>(values: readonly T[]): T => values[Math.floor(random() * values.length)]!;
  const chance = (p: number) => random() < p;
  const out: RelayhistoryStreamRecord[] = [];
  let seq = 1;
  // Without faults a seat keeps one shape, and each of its revisions one seat in the ledger.
  const shapes = new Map<string, { kind: string; origin: string; payload: Json }>();
  const seats = new Map<string, string>();
  const size = 1 + Math.floor(random() * 30);
  for (let i = 0; i < size; i++) {
    const recordId = pick([`r${i}`, `r${i}`, "shared-1", "shared-2"]);
    const kind = pick([
      "session_event", "session_event", "tool_call", "file_edit", "history",
      "session_marker", "presence", CONVERGENCE_EVENT_KIND,
    ]);
    const origin = kind === CONVERGENCE_EVENT_KIND ? `machine:${MACHINE}` : pick(["origin-a", "origin-b", `machine:${MACHINE}`]);
    const toolUse = pick(["t1", "t2", "", null]);
    const payload: Json =
      kind === "session_event"
        ? pick([
            { role: "assistant", message_id: `m${i}`, request_id: pick(["q1", "q2", ""]), token_json: pick([USAGE, USAGE, '{"input_tokens":1}', null]), ts_ms: T + i },
            { role: "assistant", message_id: `m${i}`, provider_message_id: "pm", token_json: USAGE },
            { role: "assistant", message_id: `m${i}`, token_json: USAGE, ts_ms: T + i },
            { role: "user", text: "hi", tool_use_id: toolUse, ts_ms: T + pick([0, 5, 9]) },
            { role: "user", is_sidechain: true, text: "side" },
          ])
        : kind === "tool_call"
          ? { name: "Read", tool_use_id: toolUse, ...(chance(0.5) ? { ts_ms: T + i } : {}) }
          : kind === "file_edit"
            ? { file_path: "src/a.ts", tool_use_id: toolUse }
            : kind === "history"
              ? { text: "prompt", ts_ms: T + i }
              : kind === "session_marker"
                ? { marker_uid: pick(["mk1", "mk2"]), kind: "note", ts_ms: T + i }
                : kind === CONVERGENCE_EVENT_KIND
                  ? {
                      source: "claude", session_id: "s1", machine_id: MACHINE, user_id: "u1",
                      event_id: recordId, kind: "assistant", ts: `2026-10-30T12:00:0${i % 10}Z`, content: "c",
                    }
                  : { state: "on" };
    const seatKey = JSON.stringify([origin, recordId]);
    const shape = (!faults && shapes.get(seatKey)) || { kind, origin, payload };
    shapes.set(seatKey, shape);
    const copies = 1 + Math.floor(random() * 3);
    for (let c = 0; c < copies; c++) {
      const tombstone = chance(0.1);
      // A tombstone and an upsert of one revision are a conflict: only faults make them.
      const revision = tombstone && !faults ? "9" : String(1 + Math.floor(random() * 3));
      const ledgerKey = JSON.stringify([seatKey, revision, tombstone]);
      const accepted = faults ? String(seq++) : (seats.get(ledgerKey) ?? String(seq++));
      seats.set(ledgerKey, accepted);
      out.push({
        kind: shape.kind,
        origin_id: shape.origin,
        record_id: recordId,
        // Copies of a revision carry one content unless faults are wanted.
        revision,
        operation: tombstone ? "delete" : "upsert",
        received_at: pick(["2026-10-30T12:00:00.000Z", "2026-10-30T12:00:00Z", "2026-10-30T12:00:01.5Z"]),
        payload: tombstone ? null : { ...shape.payload, rev: faults && chance(0.2) ? `x${c}` : revision },
        ...(faults && chance(0.05) ? {} : { accepted_seq: accepted }),
      });
    }
  }
  // Arrival order is not the fold's order.
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

/** Pass 1 keeps a summary per seat; pass 2 re-reads and keeps the chosen copies. */
function streamingFold(chunks: () => Iterable<readonly RelayhistoryStreamRecord[]>): RelayhistoryStreamRecord[] {
  interface Seat {
    kind: string;
    origin_id: string;
    record_id: string;
    revision: string;
    received_at: string;
    accepted_seq?: string;
    digest: string;
    live: boolean;
    time: number;
  }
  const seats = new Map<string, Seat>();
  for (const chunk of chunks())
    for (const record of chunk) {
      const key = relayhistoryRecordKey(record);
      const seat: Seat = {
        kind: record.kind,
        origin_id: record.origin_id,
        record_id: record.record_id,
        revision: record.revision,
        received_at: record.received_at,
        ...(record.accepted_seq === undefined ? {} : { accepted_seq: record.accepted_seq }),
        digest: createHash("sha256").update(relayhistoryRevisionContent(record)).digest("hex"),
        live: record.operation === "upsert" && record.payload !== null,
        time: relayhistoryEventTime(record),
      };
      seats.set(key, chooseRelayhistoryRevision(seats.get(key), seat, (a, b) => a.digest === b.digest));
    }
  const winners = new Map(
    relayhistoryNewestAcrossOrigins(seats.values())
      .filter((seat) => seat.live)
      .map((seat) => [relayhistoryRecordKey(seat), seat]),
  );
  const kept = new Map<string, RelayhistoryStreamRecord>();
  for (const chunk of chunks())
    for (const record of chunk) {
      const key = relayhistoryRecordKey(record);
      const seat = winners.get(key);
      if (!seat || kept.has(key) || record.revision !== seat.revision || record.received_at !== seat.received_at) continue;
      const { accepted_seq: _seq, ...rest } = record;
      kept.set(key, seat.accepted_seq === undefined ? rest : { ...rest, accepted_seq: seat.accepted_seq });
    }
  return [...kept.entries()]
    .sort(([a], [b]) => {
      const left = winners.get(a)!;
      const right = winners.get(b)!;
      return compareRelayhistoryOrder(left.time, left, right.time, right);
    })
    .map(([, record]) => record);
}

/** Pass 1 gathers settle facts; pass 2 projects each record again and settles it. */
function streamingProjection(records: readonly RelayhistoryStreamRecord[], ctx: ProjectionContext) {
  const settlement = new ProjectionSettlement(ctx.source);
  for (const record of records)
    if (RELAYHISTORY_ACTIVITY_KINDS.has(record.kind)) {
      const event = relayhistoryActivityEvent(record, ctx);
      if (event) settlement.add(event);
    }
  const events: Json[] = [];
  const catalog = new Map<string, RelayhistoryStreamRecord>();
  let ordinal = 0;
  let unprojected = 0;
  for (const record of records) {
    if (record.kind === CONVERGENCE_EVENT_KIND) events.push(relayhistoryDirectEvent(record, ctx));
    else if (RELAYHISTORY_ACTIVITY_KINDS.has(record.kind)) {
      const event = relayhistoryActivityEvent(record, ctx);
      if (event) {
        settlement.settle(event, ordinal++);
        events.push(event);
      }
    } else {
      const key = relayhistoryCatalogKey(record);
      if (key === null) unprojected += 1;
      else {
        const held = catalog.get(key);
        catalog.set(key, held ? lastAccepted(held, record) : record);
      }
    }
  }
  return {
    rows: [
      ...[...catalog.values()].flatMap((record) => relayhistoryCatalogRows(record, ctx)),
      ...events.sort(compareProjectedEvents).map((row) => ({ table: "convergence_events" as const, row })),
    ] satisfies ProjectionRow[],
    unprojected,
  };
}

type Outcome = { ok: unknown } | { failed: string };
function outcome(run: () => unknown): Outcome {
  try {
    return { ok: run() };
  } catch (error) {
    return { failed: (error as { code?: string }).code ?? String(error) };
  }
}
/** The stream as a reader sees it: chunks of a few records each. */
const chunked = (records: readonly RelayhistoryStreamRecord[], size: number) => () => {
  const out: RelayhistoryStreamRecord[][] = [];
  for (let i = 0; i < records.length; i += size) out.push(records.slice(i, i + size));
  return out;
};

describe("streaming fold parity", () => {
  for (const faults of [false, true])
    it(`folds to the same records and fails alike, ${faults ? "with" : "without"} conflicting copies`, () => {
      let folded = 0;
      let failed = 0;
      for (let seed = 1; seed <= 400; seed++) {
        const raw = generate(seed, faults);
        const expected = outcome(() => normalizeRelayhistoryRecords(raw));
        for (const size of [1, 3, 1000])
          expect({ seed, size, ...outcome(() => streamingFold(chunked(raw, size))) }).toEqual({ seed, size, ...expected });
        if ("failed" in expected) failed += 1;
        else folded += 1;
      }
      expect(folded).toBeGreaterThan(faults ? 20 : 300);
      if (faults) expect(failed).toBeGreaterThan(50);
    });

  it("keeps a seat's earliest-received copy with the seat's acceptance", () => {
    const base = { kind: "history", origin_id: "o", record_id: "r", revision: "2", operation: "upsert" as const, payload: { text: "x" } };
    const raw = [
      { ...base, received_at: "2026-10-30T12:00:02Z", accepted_seq: "7" },
      { ...base, received_at: "2026-10-30T12:00:01Z" },
    ];
    expect(streamingFold(chunked(raw, 1))).toEqual(normalizeRelayhistoryRecords(raw));
    expect(streamingFold(chunked(raw, 1))).toEqual([{ ...base, received_at: "2026-10-30T12:00:01Z", accepted_seq: "7" }]);
  });
});

describe("two-pass settlement parity", () => {
  for (const source of ["claude", "codex"])
    it(`projects the same rows as projectRelayhistorySession (${source})`, () => {
      let compared = 0;
      for (let seed = 1; seed <= 400; seed++) {
        const folded = outcome(() => normalizeRelayhistoryRecords(generate(seed, false)));
        if ("failed" in folded) continue;
        const records = folded.ok as RelayhistoryStreamRecord[];
        // Every convergence line must be the session's own for the whole read too.
        const ctx = context(source);
        const fixed = records.map((record) =>
          record.kind === CONVERGENCE_EVENT_KIND ? { ...record, payload: { ...record.payload, source } } : record,
        );
        const expected = outcome(() => projectRelayhistorySession(fixed, ctx));
        expect({ seed, ...outcome(() => streamingProjection(fixed, ctx)) }).toEqual({ seed, ...expected });
        if ("ok" in expected) compared += 1;
      }
      expect(compared).toBeGreaterThan(300);
    });

  it("settles tool times and request usage from facts, not neighbours", () => {
    const line = (record_id: string, kind: string, payload: Json) =>
      parseRelayhistoryRecord(JSON.stringify({
        kind, origin_id: "origin-a", record_id, revision: "1", received_at: "2026-10-30T12:00:00Z", payload,
      }));
    const records = normalizeRelayhistoryRecords([
      line("tool", "tool_call", { name: "Read", tool_use_id: "t1" }),
      line("late", "session_event", { role: "user", tool_use_id: "t1", ts_ms: T + 9 }),
      line("early", "session_event", { role: "user", tool_use_id: "t1", ts_ms: T + 1 }),
      line("b", "session_event", { role: "assistant", message_id: "m", request_id: "q", token_json: USAGE, ts_ms: T }),
      line("a", "session_event", { role: "assistant", message_id: "m", request_id: "q", token_json: USAGE, ts_ms: T }),
    ]);
    const ctx = context("claude");
    const whole = projectRelayhistorySession(records, ctx);
    expect(streamingProjection(records, ctx)).toEqual(whole);
    const events = whole.rows.map((row) => row.row);
    const tool = events.find((row) => row.kind === "tool_call")!;
    const early = events.find((row) => row.delivery_record_id === "early")!;
    expect(tool.ts).toBe(early.ts);
    // The smallest record id carries the request's usage, once.
    expect(events.find((row) => row.delivery_record_id === "a")!.input_tokens).toBe(10);
    expect(events.find((row) => row.delivery_record_id === "b")!.input_tokens).toBe(0);
  });

  it("numbers events by the order they were added", () => {
    const settlement = new ProjectionSettlement("codex");
    const event = (id: string) => ({
      kind: "session_event", delivery_record_id: id, request_key: "request-id:q", ts: "2026-10-30T12:00:00.000000Z",
      ingested_at: "2026-10-30T12:00:00.000000Z", input_tokens: 0, record: { payload: { token_json: USAGE } },
    });
    expect([settlement.add(event("b")), settlement.add(event("a"))]).toEqual([0, 1]);
    const [b, a] = [event("b"), event("a")];
    settlement.settle(b, 0);
    settlement.settle(a, 1);
    expect([a.input_tokens, b.input_tokens]).toEqual([10, 0]);
  });
});
