/**
 * Owner-buffered EVENTS (READ_API_OWNER_BUFFERED_EVENTS): the flag parser, readOwnerBufferedEvents
 * (parity with the canonical projection readCanonicalScope runs, per-record origin/machine/owner,
 * revision and tombstone folding across stages, scrub before emit, fences, bounds, tenant checks)
 * and readBufferedEventPage (full-tuple order, ASCII and Unicode, both directions, byte-budget
 * cuts equal to readAdapterPage, the marked cursor). Scratch fs stores only; synthetic values.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { comparePositions, type SearchHit, type SearchIndexAdapter, type SearchQuery } from "@rhc/search-index/contracts.ts";
import { objectStoreManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { deliveryMachine } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import { readCanonicalScope } from "../canonical-projection.ts";
import { CursorError, decodeBackendCursor, decodeCursor, encodeBufferedCursor, encodeCursor, type EventPosition } from "../cursor.ts";
import {
  BufferedFenced,
  BufferedProjectionFailed,
  OwnerBufferedEventsFlagError,
  eventRowFromIndex,
  ownerBufferedEventsEnabled,
  readOwnerBufferedEvents,
  type BufferedScope,
  type BufferedStageRow,
} from "../owner-buffered.ts";
import { TenantMismatchError, readBufferedEventPage, readEventPage, type Liveness } from "../page.ts";
import type { EventRow } from "../queries.ts";
import { prepareDelivery } from "../../session-ingest/vendor/delivery/prepare.ts";
import { groupRecords, writeBatchObjects } from "../../session-ingest/writer.ts";
import { prepareBufferedStage, putBufferedStage } from "../../session-ingest/staging.ts";
import { TENANT, batch, record } from "../../session-ingest/__tests__/support.ts";
import { TURN_ORIGIN_ID } from "../../session-ingest/turns.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { E1, SCOPE, SESSION, authority, deps, fencedGate, putStage, scratch, scriptedGate, scriptedReads, turn, unzip } from "./owner-buffered-support.ts";

const cleanup: Array<() => void> = [];
afterEach(() => {
  for (const done of cleanup.splice(0)) done();
});
function store() {
  const s = scratch();
  cleanup.push(s.done);
  return s.store;
}
const reason = async (p: Promise<unknown>) => {
  try {
    await p;
  } catch (error) {
    return error instanceof BufferedFenced ? error.reason : `other:${(error as Error).name}`;
  }
  return "resolved";
};

describe("READ_API_OWNER_BUFFERED_EVENTS flag", () => {
  test("unset or empty is off, exactly 'on' enables, anything else refuses (names the flag, never the value)", () => {
    expect(ownerBufferedEventsEnabled(undefined)).toBe(false);
    expect(ownerBufferedEventsEnabled("")).toBe(false);
    expect(ownerBufferedEventsEnabled("on")).toBe(true);
    for (const v of ["1", "true", "ON", " on", "on ", "off", "yes", "0"]) {
      let thrown: unknown;
      try {
        ownerBufferedEventsEnabled(v);
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toBeInstanceOf(OwnerBufferedEventsFlagError);
      expect((thrown as Error).message).toContain("READ_API_OWNER_BUFFERED_EVENTS");
      if (v.trim()) expect((thrown as Error).message).not.toContain(`"${v}"`);
    }
  });
});

/** The ingest TENANT as a buffered caller (same org, workspace and user). */
const CALLER: BufferedScope = { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId };
const AT = new Date("2026-11-05T12:00:00.000Z");
const TS = 1_793_000_000_000;

type Rec = Parameters<typeof record>[0];
interface Batch { origin?: string; batchId?: string; records: Rec[] }

/** The canonical answer: each batch written as the ingest writer writes it, then readCanonicalScope. */
async function canonicalRows(s: ObjectStore, batches: Batch[], version = 7): Promise<EventRow[]> {
  const keys = new Set<string>();
  for (const b of batches) {
    const origin = b.origin ?? "origin-1";
    const p = await prepareDelivery(TENANT, await batch(b.records.map((r) => ({ origin_id: origin, ...r }) as Rec).map((r) => record(r)), { origin_id: origin, batch_id: b.batchId ?? "batch-1" }));
    const w = await writeBatchObjects({ store: s, keyRoot: "96bulls", now: () => AT, sleep: async () => {} }, { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId, originId: p.originId, batchDigest: p.batchDigest, records: p.records });
    for (const m of w.manifests) keys.add(m.key);
  }
  const r = await readCanonicalScope(
    { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, harness: "claude", sessionId: "sess-1" },
    [...keys],
    { store: s, reader: objectStoreManifestReader(s, { provider: "s3", bucket: "b" }), prefixFor: (o: string) => `96bulls/${o}`, decompress: (raw: Uint8Array, max: number) => new Uint8Array(zstdDecompressSync(raw, { maxOutputLength: max })) } as never,
    { maxScopeBytes: 16 * 1024 * 1024, maxScopeEvents: 10_000, maxRows: 500_000, insertBatch: 5_000 },
    version,
  );
  expect(r.complete).toBe(true);
  return (r.rows as Parameters<typeof eventRowFromIndex>[0][]).map(eventRowFromIndex);
}

/** The buffered answer: the same batches as buffered stages (the admission writer's prepareBufferedStage), gated read. */
async function bufferedStages(s: ObjectStore, batches: Batch[]): Promise<BufferedStageRow[]> {
  const rows: BufferedStageRow[] = [];
  let ordinal = 0;
  for (const [n, b] of batches.entries()) {
    const origin = b.origin ?? "origin-1";
    const p = await prepareDelivery(TENANT, await batch(b.records.map((r) => record({ origin_id: origin, ...r })), { origin_id: origin, batch_id: b.batchId ?? "batch-1" }));
    const { groups } = groupRecords(p.records, AT);
    const ordinals = groups.map((g) => g.lines.map(() => ordinal++));
    const prepared = prepareBufferedStage("96bulls", { claim_token: `11111111-1111-4111-8111-${String(n + 1).padStart(12, "0")}`, batch_digest: p.batchDigest, received_at: AT.toISOString(), org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, origin_id: p.originId, user_id: TENANT.userId }, groups, ordinals);
    await putBufferedStage(s, prepared);
    rows.push({
      batch_id: `b-${n + 1}`, stage_key: prepared.spec.key, stage_sha256: prepared.spec.sha256, stage_bytes: prepared.spec.bytes,
      group_indexes: groups.map((_, i) => i), group_ordinals: prepared.spec.groupOrdinals, kept_ordinals: prepared.spec.groupOrdinals.flat(),
      reset_epoch: "0", accepted_at: `2026-11-05T12:0${n + 1}:00.000Z`, admission_epoch: E1, admission_version: "7",
    });
  }
  return rows;
}

async function bufferedRows(s: ObjectStore, batches: Batch[]): Promise<EventRow[]> {
  const stages = await bufferedStages(s, batches);
  const out = await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [stages] }).reads, gate: scriptedGate(authority("7")) }), CALLER, "claude", "sess-1");
  if (out === null || "unverifiedBeforeCandidate" in out || out.erased) throw new Error(`unexpected ${JSON.stringify(out)}`);
  return out.rows;
}

const sortKey = (r: EventRow) => JSON.stringify([r.machine_id, r.kind, r.event_id]);
const sorted = (rows: EventRow[]) => [...rows].sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0));

describe("readOwnerBufferedEvents: parity with the canonical projection", () => {
  const sameMs: Rec[] = [
    { record_id: "r-ascii", payload: { text: "hello one", ts_ms: TS, role: "user" } },
    { record_id: "r-unicode", payload: { text: "héllo ✓ 雪 two 🚀", ts_ms: TS, role: "assistant" } },
    { record_id: "r-tool", kind: "tool_call", payload: { tool_name: "Bash", ts_ms: TS + 1000 } },
  ];

  test("one origin, ASCII and Unicode text, same-millisecond records: rows identical to readCanonicalScope's", async () => {
    const canonical = await canonicalRows(store(), [{ records: sameMs }]);
    const buffered = await bufferedRows(store(), [{ records: sameMs }]);
    expect(canonical.length).toBeGreaterThanOrEqual(2);
    expect(sorted(buffered)).toEqual(sorted(canonical));
    expect(buffered.some((r) => r.content.includes("雪"))).toBe(true);
  });

  test("two origins keep their own per-record machine (deliveryMachine per origin) and owner", async () => {
    const batches: Batch[] = [
      { origin: "origin-1", batchId: "batch-1", records: [sameMs[0]!] },
      { origin: "origin-2", batchId: "batch-2", records: [sameMs[1]!] },
    ];
    const canonical = await canonicalRows(store(), batches);
    const buffered = await bufferedRows(store(), batches);
    expect(sorted(buffered)).toEqual(sorted(canonical));
    const machines = new Set(buffered.map((r) => r.machine_id));
    expect(machines).toEqual(new Set([deliveryMachine(TENANT.workspaceId, "origin-1"), deliveryMachine(TENANT.workspaceId, "origin-2")]));
    expect(buffered.every((r) => r.user_id === TENANT.userId && r.session_id === "sess-1" && r.source === "claude")).toBe(true);
  });

  test("revisions and tombstones fold across stages exactly as the canonical read folds them", async () => {
    const batches: Batch[] = [
      { batchId: "batch-1", records: [
        { record_id: "r1", payload: { text: "first draft", ts_ms: TS, role: "user" } },
        { record_id: "r2", payload: { text: "to be deleted", ts_ms: TS, role: "user" } },
        { record_id: "r3", payload: { text: "kept as is", ts_ms: TS + 5, role: "user" } },
      ] },
      { batchId: "batch-2", records: [
        { record_id: "r1", revision: 2, payload: { text: "second draft", ts_ms: TS, role: "user" } },
        { record_id: "r2", revision: 2, operation: "delete", payload: null as never },
      ] },
    ];
    const canonical = await canonicalRows(store(), batches);
    const buffered = await bufferedRows(store(), batches);
    expect(sorted(buffered)).toEqual(sorted(canonical));
    const text = buffered.map((r) => r.content).join("|");
    expect(text).toContain("second draft");
    expect(text).not.toContain("first draft");
    expect(text).not.toContain("to be deleted");
    expect(text).toContain("kept as is");
  });

  test("the admission version is the projection version (stage admission, not a constant)", async () => {
    const s = store();
    const stages = await bufferedStages(s, [{ records: [sameMs[0]!] }]);
    const at9 = stages.map((r) => ({ ...r, admission_version: "9" }));
    const out = await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [at9] }).reads, gate: scriptedGate(authority("9")) }), CALLER, "claude", "sess-1");
    const canonical9 = await canonicalRows(store(), [{ records: [sameMs[0]!] }], 9);
    expect(out && !("unverifiedBeforeCandidate" in out) && !out.erased && sorted(out.rows)).toEqual(sorted(canonical9));
  });
});

const ev = (id: string, over: Record<string, unknown> = {}, kind = "session_event") => ({ kind, origin_id: TURN_ORIGIN_ID, record_id: id, revision: "1", operation: "upsert", received_at: "2026-11-05T12:00:00Z", payload: { text: `t ${id}`, ts_ms: TS, role: "user", ...over } });

describe("readOwnerBufferedEvents: gate, scrub, bounds, tenant", () => {
  const SUFFIX = "zzTAILzz9q8w7e6r5t4y";
  test("a pre-v4 stage's over-cap secret is scrubbed before emit; rows carry no stage key, ordinal or raw line", async () => {
    const s = store();
    const st = await putStage(s, 1, [ev("e1", { text: `config {"api_key": "${"A".repeat(5000)}${SUFFIX}"} done` })], { header: { scrub_ruleset: 3 } });
    const out = await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }), SCOPE, "claude", SESSION);
    const json = JSON.stringify(out);
    expect(out && !("unverifiedBeforeCandidate" in out) && !out.erased && out.rows).toHaveLength(1);
    expect(json).toContain("[REDACTED]");
    expect(json).not.toContain(SUFFIX);
    for (const leak of [st.key, "buffered-batches", "stage_key", "kept_ordinals", "group_ordinals", '"l":', '"m":', "claim_token"]) expect(json).not.toContain(leak);
  });

  test("a payload naming another session is pinned to the gated session (identity comes only from the gated scope)", async () => {
    const s = store();
    const st = await putStage(s, 1, [ev("e1", { session_id: "someone-elses" })]);
    const out = await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }), SCOPE, "claude", SESSION);
    expect(out && !("unverifiedBeforeCandidate" in out) && !out.erased && out.rows.map((r) => [r.session_id, r.user_id, r.org_id, r.workspace_id])).toEqual([[SESSION, SCOPE.userId, SCOPE.orgId, SCOPE.workspaceId]]);
  });

  test("turn records project to no event row; a convergence_event line is refused (BufferedProjectionFailed, hidden by the route)", async () => {
    const s = store();
    const turns = await putStage(s, 1, [turn(0), turn(1)]);
    expect(await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[turns.row]] }).reads }), SCOPE, "claude", SESSION)).toEqual({ erased: false, rows: [] });
    const conv = await putStage(s, 2, [{ ...ev("c1"), kind: "convergence_event", payload: { source: "claude", session_id: SESSION, machine_id: "x", user_id: SCOPE.userId, event_id: "c1", kind: "prompt", ts: "2026-11-05T12:00:00.000Z", content: "direct" } }]);
    expect(await reason(readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[conv.row]] }).reads }), SCOPE, "claude", SESSION))).toBe("other:BufferedProjectionFailed");
  });

  test("first observation fenced: unverified before candidate, no RPC, no S3", async () => {
    const { reads, calls } = scriptedReads({ stages: [[]] });
    expect(await readOwnerBufferedEvents(deps({ store: store(), reads, gate: fencedGate("gate_timeout") }), SCOPE, "claude", SESSION)).toEqual({ unverifiedBeforeCandidate: "gate_timeout" });
    expect(calls).toHaveLength(0);
  });

  test("the final observation after all I/O: pending or a moved head is fenced (no rows)", async () => {
    const s = store();
    const st = await putStage(s, 1, [ev("e1")]);
    expect(await reason(readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate: scriptedGate(authority("7"), new BufferedFenced("gate_pending")) }), SCOPE, "claude", SESSION))).toBe("gate_pending");
    expect(await reason(readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate: scriptedGate(authority("7"), authority("8")) }), SCOPE, "claude", SESSION))).toBe("gate_moved");
  });

  test("recheck: a changed stage set hides, an empty one is erased, an old admission hides", async () => {
    const s = store();
    const a = await putStage(s, 1, [ev("e1")]);
    const b = await putStage(s, 2, [ev("e2")]);
    expect(await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[a.row, b.row], [b.row]] }).reads }), SCOPE, "claude", SESSION)).toBeNull();
    expect(await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[a.row], []] }).reads }), SCOPE, "claude", SESSION)).toEqual({ erased: true, rows: [] });
    const old = await putStage(s, 3, [ev("e3")], { admission: { epoch: E1, version: "5" } });
    expect(await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[old.row]] }).reads }), SCOPE, "claude", SESSION)).toBeNull();
  });

  test("a stage object missing (404) while still listed: hidden, never partial", async () => {
    const s = store();
    const a = await putStage(s, 1, [ev("e1")]);
    const b = await putStage(s, 2, [ev("e2")]);
    await s.delete(objectKey(b.key));
    expect(await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[a.row, b.row]] }).reads }), SCOPE, "claude", SESSION)).toBeNull();
  });

  test("over the decompressed bound: BufferedTooLarge; a corrupt body with a matching sha: refused, no rows", async () => {
    const s = store();
    const st = await putStage(s, 1, [ev("e1", { text: "x".repeat(4096) })]);
    const tiny = (b: Uint8Array) => unzip(b, 64);
    expect(await reason(readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, decompress: tiny }), SCOPE, "claude", SESSION))).toBe("other:BufferedTooLarge");
    const junk = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 1, 2, 3, 4, 5, 6]);
    const sha = createHash("sha256").update(junk).digest("hex");
    const key = st.key.replace(/b=1-/u, "b=9-");
    await s.put(objectKey(key), junk, { contentType: "application/zstd" });
    const bad = { ...st.row, stage_key: key, stage_sha256: sha, stage_bytes: junk.byteLength };
    const out = await reason(readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[bad]] }).reads }), SCOPE, "claude", SESSION));
    expect(out).not.toBe("resolved");
    expect(out.startsWith("other:")).toBe(true);
  });

  test("an admission version past Number's safe range: compared exactly by the gate, served (the version is not emitted)", async () => {
    const s = store();
    const exact = await putStage(s, 1, [ev("e1")], { admission: { epoch: E1, version: "9007199254740993" } });
    const out = await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[exact.row]] }).reads, gate: scriptedGate(authority("9007199254740993")) }), SCOPE, "claude", SESSION);
    expect(out && !("unverifiedBeforeCandidate" in out) && !out.erased && out.rows.map((r) => r.content)).toEqual(["t e1"]);
    const near = await putStage(s, 2, [ev("e1")], { admission: { epoch: E1, version: "9007199254740992" } });
    expect(await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[near.row]] }).reads, gate: scriptedGate(authority("9007199254740993")) }), SCOPE, "claude", SESSION)).toBeNull();
  });

  test("a stage whose header names another user: TenantMismatchError, no rows", async () => {
    const s = store();
    const st = await putStage(s, 1, [ev("e1")], { header: { user_id: "u-2" } });
    let thrown: unknown;
    try {
      await readOwnerBufferedEvents(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }), SCOPE, "claude", SESSION);
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(TenantMismatchError);
  });
});

// ---- readBufferedEventPage: full-tuple order, both directions, budget cuts, the marked cursor ----

const tenant = { orgId: "org-a", workspaceId: "ws-a" };
const liveness: Liveness = { async erased() { return new Set(); }, async sessionStart() { return { pointers: 1, live: 1, createdAtMs: 1 }; }, async livePointerSessions(_o, _w, ids) { return new Set(ids); } };
const hit = (tsMs: number, machineId: string, kind: string, eventId: string, content = "c", sessionId = "s-ä"): SearchHit => ({ ...tenant, tsMs, sessionId, source: "claude", machineId, eventId, kind, type: kind, userId: "u", actorName: "", actorRole: "", subagentId: "", projectId: "", taskTitle: "", taskRef: "{}", tags: [], filesTouched: [], toolName: "", toolStatus: "", content });
const toRow = (h: SearchHit): EventRow => ({
  org_id: h.orgId, workspace_id: h.workspaceId, ts_ms: h.tsMs, session_id: h.sessionId, source: h.source, machine_id: h.machineId, kind: h.kind, event_id: h.eventId,
  type: h.type, user_id: h.userId, actor_name: h.actorName, actor_role: h.actorRole, subagent_id: h.subagentId, project_id: h.projectId, task_title: h.taskTitle,
  task_ref: h.taskRef, tags: [...h.tags], files_touched: [...h.filesTouched], tool_name: h.toolName, tool_status: h.toolStatus, content: h.content, in_budget: 1,
});
/** A portable adapter returning the hits in `order` strictly after the cursor, `limit` at a time. */
function binding(all: SearchHit[]): SearchServiceBinding {
  const query: SearchIndexAdapter["query"] = async (q: SearchQuery) => {
    const sign = q.order === "desc" ? -1 : 1;
    const after = all.filter((h) => !q.cursor || sign * comparePositions(h, q.cursor) > 0).sort((a, b) => sign * comparePositions(a, b));
    const hits = after.slice(0, q.limit);
    return after.length > hits.length ? { hits, next: hits[hits.length - 1] } : { hits };
  };
  return { index: { query } as SearchIndexAdapter, state: { async fromMs() { return 0; }, async record() {}, async partials() { return { sessions: [], overflow: false }; } } };
}
/** Event ids are unique in the fixtures, so the id finds the row (the shaped event carries no machine). */
const posOf = (e: { eventId: string }, rows: EventRow[]) => rows.find((r) => r.event_id === e.eventId)!;

// Same millisecond everywhere; ties broken by machine, kind, event id by code point (ASCII,
// Latin-1, CJK, astral). The canonical order is comparePositions; no locale collation.
const FIXTURE: SearchHit[] = [
  hit(TS, "delivery:b", "message", "e-Z"),
  hit(TS, "delivery:b", "message", "e-a"),
  hit(TS, "delivery:a", "tool", "e-1"),
  hit(TS, "delivery:a", "message", "é-1"),
  hit(TS, "delivery:a", "message", "雪-1"),
  hit(TS, "delivery:a", "message", "🚀-1"),
  hit(TS, "delivery:ä", "message", "e-0-umlaut"),
  hit(TS, "delivery:A", "message", "e-0"),
  hit(TS - 1, "delivery:z", "message", "early"),
  hit(TS + 1, "delivery:0", "message", "late"),
];

describe("readBufferedEventPage", () => {
  test("full traversal in both directions at every limit: comparePositions order, no duplicate, no gap", () => {
    const rows = FIXTURE.map(toRow);
    for (const order of ["asc", "desc"] as const) {
      const expected = [...FIXTURE].sort((a, b) => (order === "asc" ? 1 : -1) * comparePositions(a, b)).map((h) => h.eventId + h.machineId);
      for (let limit = 1; limit <= rows.length + 1; limit++) {
        const seen: string[] = [];
        let cursor: EventPosition | undefined;
        for (let guard = 0; guard < 50; guard++) {
          const page = readBufferedEventPage(rows, { order, limit, cursor });
          seen.push(...page.events.map((e) => e.eventId + posOf(e, rows).machine_id));
          if (page.nextCursor === null) break;
          const decoded = decodeBackendCursor(page.nextCursor);
          expect(decoded?.backend).toBe("buffered");
          cursor = decoded!.position;
        }
        expect(seen).toEqual(expected);
      }
    }
  });

  test("pages equal readAdapterPage's (events, budget cut and cursor position), ASCII and Unicode, both directions", async () => {
    const big = (n: number) => "ü".repeat(n); // two UTF-8 bytes per unit: the budget counts bytes, not units
    const hits = [
      ...FIXTURE.slice(0, 6).map((h, i) => ({ ...h, content: big(300_000 + i) })),
      ...FIXTURE.slice(6),
      hit(TS, "delivery:a", "message", "ascii-big", "x".repeat(1_500_000)),
    ];
    const rows = hits.map(toRow);
    const adapter = binding(hits);
    for (const order of ["asc", "desc"] as const) {
      for (const limit of [1, 3, 5, 20]) {
        let cCursor: EventPosition | undefined;
        let bCursor: EventPosition | undefined;
        let cuts = 0;
        for (let guard = 0; guard < 50; guard++) {
          const canonical = await readEventPage({ adapter, liveness }, { ...tenant, order, limit, cursor: cCursor });
          const buffered = readBufferedEventPage(rows, { order, limit, cursor: bCursor });
          expect(buffered.events).toEqual(canonical.events);
          if (canonical.events.length < limit && canonical.nextCursor !== null) cuts++;
          expect(buffered.nextCursor === null).toBe(canonical.nextCursor === null);
          if (canonical.nextCursor === null) break;
          cCursor = decodeCursor(canonical.nextCursor);
          bCursor = decodeBackendCursor(buffered.nextCursor)!.position;
          expect(bCursor).toEqual(cCursor);
        }
        if (limit === 20) expect(cuts).toBeGreaterThan(0);
      }
    }
  });

  test("maxContent shapes the same way as the canonical page", async () => {
    const rows = FIXTURE.map(toRow);
    const canonical = await readEventPage({ adapter: binding(FIXTURE), liveness }, { ...tenant, order: "asc", limit: 4, maxContent: 1 });
    expect(readBufferedEventPage(rows, { order: "asc", limit: 4, maxContent: 1 }).events).toEqual(canonical.events);
  });

  test("the buffered cursor: canonical six-field position plus the ob1 marker; decodeCursor refuses it; tampering is refused", () => {
    const p: EventPosition = { tsMs: TS, sessionId: "s-ä", source: "claude", machineId: "delivery:雪", kind: "message", eventId: "🚀" };
    const raw = encodeBufferedCursor(p);
    expect(raw.startsWith("b96e1:")).toBe(true);
    expect(JSON.parse(Buffer.from(raw.slice(6), "base64url").toString("utf8"))).toEqual([TS, "s-ä", "claude", "delivery:雪", "message", "🚀", "ob1"]);
    expect(decodeBackendCursor(raw)).toEqual({ backend: "buffered", position: p });
    expect(decodeBackendCursor(encodeCursor(p))).toEqual({ backend: "canonical", position: p });
    expect(() => decodeCursor(raw)).toThrow(CursorError);
    const enc = (v: unknown) => `b96e1:${Buffer.from(JSON.stringify(v), "utf8").toString("base64url")}`;
    for (const bad of [
      enc([TS, "s", "claude", "m", "k", "e", "ob2"]),
      enc([TS, "s", "claude", "m", "k", "e", "ob1", "x"]),
      enc([-1, "s", "claude", "m", "k", "e", "ob1"]),
      enc([TS, "", "claude", "m", "k", "e", "ob1"]),
      raw.slice(0, -2),
      enc([TS, "s", "claude", "m", "k", "e".repeat(1025), "ob1"]),
      enc({ 0: TS }),
    ])
      expect(() => decodeBackendCursor(bad)).toThrow(CursorError);
  });

  test("an empty or exhausted buffered set: no events, no cursor", () => {
    expect(readBufferedEventPage([], { order: "asc", limit: 10 })).toEqual({ events: [], nextCursor: null, erasedDropped: 0 });
    const rows = FIXTURE.map(toRow);
    const last = [...FIXTURE].sort(comparePositions).at(-1)!;
    expect(readBufferedEventPage(rows, { order: "asc", limit: 10, cursor: last }).events).toEqual([]);
  });
});
