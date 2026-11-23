/**
 * The drain's v3 seat sidecar (DrainOptions.acceptSeatSidecar): off by default, v2 unchanged, seats appended after
 * fitLine exactly as the publication path does, never a second seat for one revision, and every malformed sidecar
 * refused whole. Synthetic ids and times only (the repo is public).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage } from "../staging.ts";
import type { Group } from "../writer.ts";
import { ACCEPTED_SUFFIX, DRAINED_SUFFIX, acceptedCompleteKey, drainBufferedBatches, parseAcceptedMarkerV3, seatLine, type DrainOptions } from "../buffered-drain.ts";
import { fitLine } from "../oversize.ts";
import { injectReceiptSeat, parseReceiptSeatSnapshot, acceptedReceiptSeat } from "../publication-seats.ts";
import { preparePublicationImage } from "../publication-image.ts";
import { scratchStore } from "./support.ts";
import { applyFenceChange, fenceEpoch, orgFenceAuthorityKey, s3LedgerKeys, s3LedgerObjects, sessionKeyHash } from "../ledger-s3.ts";

const ROOT = "96bulls";
const TENANT = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const ORIGIN = "origin-1";
const RECEIVED = "2030-01-02T00:00:00.000Z";
const SEAT = "9007199254740994";
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));
const enc = new TextEncoder();
const dec = new TextDecoder();
const KEYS = s3LedgerKeys(ROOT, TENANT);

type Rec = { id: string; rev: number; text?: string; raw?: string };
const lineOf = (r: Rec) => r.raw ?? JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: r.id, revision: String(r.rev), operation: "upsert", received_at: RECEIVED, payload: { text: r.text ?? `t-${r.id}-${r.rev}`, ts_ms: 1_893_542_400_000 } });
const digestOf = (r: Rec) => sha(`${r.id}:${r.rev}`);
function group(source: string, sessionId: string, recs: Rec[]): Group {
  return { source, sessionId, lines: recs.map(lineOf), meta: recs.map((r) => [`${r.id}:r${r.rev}`, digestOf(r), source, sessionId]) };
}

let cleanup: (() => void)[] = [];
afterEach(() => { cleanup.forEach((c) => c()); cleanup = []; });
const AUTHORITY = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1 };
async function scratch(): Promise<ObjectStore> {
  const s = scratchStore();
  cleanup.push(s.done);
  await s3LedgerObjects(s.store, ROOT).editFence(KEYS.fence, (f) => { applyFenceChange(f, {}); return { next: f, result: undefined }; });
  await s.store.put(objectKey(orgFenceAuthorityKey(ROOT, TENANT.orgId)), enc.encode(JSON.stringify({ ...AUTHORITY, orgId: TENANT.orgId })));
  await s.store.put(objectKey(acceptedCompleteKey(ROOT, TENANT.orgId)), enc.encode(JSON.stringify({ v: 1, kind: "buffered-accepted-complete", stages: 0, completedAtMs: 1 })));
  return s.store;
}

let batchNo = 0;
/** Stage the groups and return the key, the stage sha and the sessions (for the sidecar's fence snapshot). */
async function stage(store: ObjectStore, groups: Group[]) {
  let ord = 0;
  batchNo++;
  const prepared = prepareBufferedStage(ROOT, {
    claim_token: `00000000-0000-4000-8000-${String(batchNo).padStart(12, "0")}`,
    batch_digest: sha(`batch-${batchNo}`),
    received_at: RECEIVED,
    org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, origin_id: ORIGIN, user_id: TENANT.userId,
  }, groups, groups.map((g) => g.lines.map(() => ord++)));
  await putBufferedStage(store, prepared);
  return { key: prepared.spec.key, sha256: prepared.spec.sha256, groups };
}
type Staged = Awaited<ReturnType<typeof stage>>;
/** The export's sidecar body for a staged object: v2 (no seats) or v3 (seats per kept line). */
async function sidecar(store: ObjectStore, s: Staged, o: { v?: number; live?: boolean; kept?: number[][]; seats?: unknown } = {}) {
  const read = await s3LedgerObjects(store, ROOT).readFence(KEYS.fence);
  if (read.state !== "present") throw new Error("test: no fence");
  const fence = read.fence;
  const live = o.live ?? true;
  const kept = live ? (o.kept ?? s.groups.map((g) => g.lines.map((_, j) => j))) : [];
  const body: Record<string, unknown> = {
    v: o.v ?? 3, kind: "buffered-accepted", stage_sha256: s.sha256, live, kept,
    fence: {
      instance: fence.instance, resets: fence.resets, resetsCompleted: fence.resetsCompleted,
      epochs: Object.fromEntries(s.groups.map((g) => [sessionKeyHash(g.source, g.sessionId), fenceEpoch(fence, ORIGIN, sessionKeyHash(g.source, g.sessionId))])),
    },
  };
  if ((o.v ?? 3) !== 2) body.seats = o.seats ?? kept.map((ls, g) => ls.map((j, n) => ({ seat: String(BigInt(SEAT) + BigInt(g * 100 + n)), digest: s.groups[g]!.meta![j]![1] })));
  return JSON.stringify(body);
}
const putSidecar = (store: ObjectStore, s: Staged, text: string) => store.put(objectKey(s.key + ACCEPTED_SUFFIX), enc.encode(text));
const run = (store: ObjectStore, over: Partial<DrainOptions> = {}) =>
  drainBufferedBatches({ store, keyRoot: ROOT, orgs: [TENANT.orgId], dryRun: false, decompress, sleep: async () => {}, ...over });

async function allKeys(store: ObjectStore, prefix = ROOT): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}
/** Every key except the drain's own run lock (a refused run still takes and leaves it). */
const dataKeys = async (store: ObjectStore) => (await allKeys(store)).filter((k) => k !== `${ROOT}/.drain/lock.json`);
/** Raw canonical lines (exact text) per session id. */
async function rawLines(store: ObjectStore): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>();
  for (const key of await allKeys(store, `${ROOT}/${TENANT.orgId}/v1`)) {
    if (!key.includes("/raw/")) continue;
    const session = /\/s=([^/]+)\/raw\//.exec(key)![1]!;
    const lines = dec.decode(decompress(await readAll(await store.get(objectKey(key))))).split("\n").filter(Boolean);
    out.set(session, [...(out.get(session) ?? []), ...lines]);
  }
  return out;
}
async function storedEntry(store: ObjectStore, recordId: string) {
  const doc = (await s3LedgerObjects(store, ROOT).readIndex(KEYS.revisions(ORIGIN, recordId)))?.doc;
  return doc && Object.hasOwn(doc.records, recordId) ? doc.records[recordId] : undefined;
}
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const seatOfLine = (l: string) => (JSON.parse(l) as { accepted_seq?: string }).accepted_seq;

const R1: Rec = { id: "r1", rev: 1 }, R2: Rec = { id: "r2", rev: 2 };

describe("drain v3 seat sidecar: opt-in off", () => {
  test("off (default): a valid v3 sidecar is refused exactly like an unknown version and nothing is written", async () => {
    const summaries = [];
    for (const v of [3, 4]) {
      const store = await scratch();
      const s = await stage(store, [group("claude", "sess-1", [R1])]);
      let text = await sidecar(store, s);
      if (v === 4) text = text.replace('"v":3', '"v":4');
      await putSidecar(store, s, text);
      const before = await dataKeys(store);
      const summary = await run(store);
      expect(summary.objects.accepted_mismatch).toBe(1);
      expect(summary.objects.drained).toBe(0);
      expect(await dataKeys(store)).toEqual(before);
      summaries.push(summary);
    }
    // Same summary for v3-off and an unknown version.
    expect(JSON.stringify(summaries[0])).toBe(JSON.stringify(summaries[1]));
    // acceptSeatSidecar: false is the same as absent.
    const store = await scratch();
    const s = await stage(store, [group("claude", "sess-1", [R1])]);
    await putSidecar(store, s, await sidecar(store, s));
    expect((await run(store, { acceptSeatSidecar: false })).objects.accepted_mismatch).toBe(1);
  });

  test("a v2 sidecar drains to the same lines, index and summary with the opt-in off and on (v2 stays unseated)", async () => {
    const results = [];
    for (const on of [false, true]) {
      batchNo = 100;
      const store = await scratch();
      const s = await stage(store, [group("claude", "sess-1", [R1, R2]), group("claude", "sess-2", [{ id: "r3", rev: 1 }])]);
      await putSidecar(store, s, await sidecar(store, s, { v: 2 }));
      const summary = await run(store, on ? { acceptSeatSidecar: true } : {});
      const lines = await rawLines(store);
      results.push({ summary: JSON.stringify(summary), lines: JSON.stringify([...lines]), index: JSON.stringify(await Promise.all(["r1", "r2", "r3"].map((id) => storedEntry(store, id)))) });
      expect(summary.objects.drained).toBe(1);
      for (const ls of lines.values()) for (const l of ls) expect(seatOfLine(l)).toBeUndefined();
      // The stored line is the v2 path's fitLine output, byte for byte.
      expect(lines.get("sess-1")).toEqual([fitLine(lineOf(R1)), fitLine(lineOf(R2))]);
    }
    expect(results[1]).toEqual(results[0]!);
  });

  test("a non-boolean opt-in is refused before any work", async () => {
    const store = await scratch();
    for (const bad of ["true", 1, null]) await expect(run(store, { acceptSeatSidecar: bad as never })).rejects.toThrow("drain_accept_seat_sidecar_invalid");
  });
});

describe("drain v3 seat sidecar: opt-in on", () => {
  test("each kept line gets its seat after fitLine; the index keeps the pre-seat digest", async () => {
    const store = await scratch();
    const s = await stage(store, [group("claude", "sess-1", [R1, R2])]);
    await putSidecar(store, s, await sidecar(store, s));
    const summary = await run(store, { acceptSeatSidecar: true });
    expect(summary.objects.drained).toBe(1);
    expect(summary.lines.written).toBe(2);
    const lines = (await rawLines(store)).get("sess-1")!;
    expect(lines.map(seatOfLine)).toEqual([SEAT, String(BigInt(SEAT) + 1n)]);
    // The seated bytes are fitLine's bytes with the seat appended (publication-image.ts order).
    expect(lines[0]).toBe(fitLine(lineOf(R1)).slice(0, -1) + `,"accepted_seq":"${SEAT}"}`);
    // Pre-seat digest in the revision index, as publication's record digest.
    expect((await storedEntry(store, "r1"))![2]).toBe(digestOf(R1));
    expect(await exists(store, s.key + DRAINED_SUFFIX)).toBe(true);
  });

  test("only kept lines are seated; a not-kept line is neither written nor seated", async () => {
    const store = await scratch();
    const s = await stage(store, [group("claude", "sess-1", [R1, R2])]);
    await putSidecar(store, s, await sidecar(store, s, { kept: [[1]] }));
    const summary = await run(store, { acceptSeatSidecar: true });
    expect(summary.lines.not_kept).toBe(1);
    expect((await rawLines(store)).get("sess-1")!.map(seatOfLine)).toEqual([SEAT]);
    expect(await storedEntry(store, "r1")).toBeUndefined();
  });

  test("a not-live v3 sidecar (seats []) is stage_not_live, as v2", async () => {
    const store = await scratch();
    const s = await stage(store, [group("claude", "sess-1", [R1])]);
    await putSidecar(store, s, await sidecar(store, s, { live: false }));
    const summary = await run(store, { acceptSeatSidecar: true });
    expect(summary.objects.stage_not_live).toBe(1);
    expect(summary.objects.drained).toBe(0);
  });

  test("byte identity: the drain stores the same bytes the publication image seals for the same line and seat", async () => {
    const line = lineOf({ id: "r1", rev: 1, text: "literal é and \"quotes\"" });
    const digest = digestOf(R1);
    // Publication: the existing image path (publication-seats.test.ts fixture shape).
    const HEX = "a".repeat(64), OTHER = "b".repeat(64);
    const record = { ord: 0, source: "claude", session_id: "sess-1", record_id: "r1", revision_id: "r1:r1", revision: 1, digest };
    const spec = { key: "unused", sha256: HEX, bytes: 1, groupOrdinals: [[0]] };
    const receiptSeats = parseReceiptSeatSnapshot({ controlEpoch: HEX, sourceIdentitySha256: OTHER, allocatedHighWater: "9007199254740993", lastAssignedSeat: "9007199254740999",
      records: [{ ord: 0, seat: SEAT, bodyAccepted: true, candidateDigest: digest, firstDigest: digest, resetEpoch: "0", fenceEpoch: "0" }] }, [record], spec);
    const generation = "12345678-1234-1234-1234-123456789abc";
    const head = { groupIndex: 0, ordinals: [0], generation, prefix: `96bulls/org-a/upload-generations/g=${generation}`, headVersion: "1", revision: "1",
      previousKey: null, previousRawSha256: null, basePointer: null, source: "claude", sessionId: "sess-1", physicalWorkspace: "ws-a", physicalSource: "claude",
      physicalSession: "sess-1", machineId: "srv_fixture", fenceEpoch: "0", resetEpoch: "0" };
    const ticket = { schemaGeneration: HEX, attemptToken: "abcdefab-1234-1234-1234-123456789abc", leaseToken: "11111111-1111-1111-1111-111111111111", leaseUntil: "2030-01-05T00:05:00.000000Z",
      stageToken: "22222222-2222-2222-2222-222222222222", stageVersion: "1", outboxVersion: "1", tenant: { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" },
      originId: ORIGIN, batchId: "batch-a", batchDigest: HEX, claimToken: "33333333-3333-3333-3333-333333333333", stage: spec, records: [record], receiptSeats,
      acceptedOrdinals: [0], heads: [head] };
    const decoded = { header: { v: 1 as const, publication_state: "buffered" as const, claim_token: ticket.claimToken, batch_digest: HEX, received_at: RECEIVED,
      org_id: "org-a", workspace_id: "ws-a", origin_id: ORIGIN, user_id: "user-1", groups: [["claude", "sess-1"] as [string, string]] },
      groups: [{ source: "claude", sessionId: "sess-1", lines: [line] }], spec };
    const pub = scratchStore();
    cleanup.push(pub.done);
    const image = await preparePublicationImage(ticket as never, head as never, decoded as never, { bucket: "fixture", region: "us-east-1", keyRoot: ROOT, generation: HEX, provider: "s3" }, pub.store, new AbortController().signal);
    const published = dec.decode(zstdDecompressSync(image.newChunk.body)).split("\n").filter(Boolean);

    // Drain: the same line, the same seat, through the real drain.
    const store = await scratch();
    const g: Group = { source: "claude", sessionId: "sess-1", lines: [line], meta: [["r1:r1", digest, "claude", "sess-1"]] };
    const s = await stage(store, [g]);
    await putSidecar(store, s, await sidecar(store, s));
    expect((await run(store, { acceptSeatSidecar: true })).objects.drained).toBe(1);
    const drained = (await rawLines(store)).get("sess-1")!;
    expect(drained).toEqual(published);
    expect(drained[0]).toBe(injectReceiptSeat(fitLine(line), acceptedReceiptSeat(receiptSeats, 0), ORIGIN));
    // Integrity digests mirror publication: the index digest is the pre-seat record digest, and the chunk entry's
    // digest covers the seated bytes in both paths.
    expect((await storedEntry(store, "r1"))![2]).toBe(record.digest);
    const manifestKey = (await allKeys(store, `${ROOT}/${TENANT.orgId}/v1`)).find((k) => k.endsWith("/manifest.json"))!;
    const manifest = JSON.parse(dec.decode(await readAll(await store.get(objectKey(manifestKey)))));
    const strip = (c: Record<string, unknown>) => Object.fromEntries(Object.entries(c).filter(([k]) => /sha|bytes|lines/.test(k)));
    expect(manifest.raw.chunks.map(strip)).toEqual(image.manifest.raw.chunks.map((c) => strip(c as unknown as Record<string, unknown>)));
    expect(manifest.raw.chunks.map(strip)[0]).not.toEqual({});
  });
});

describe("drain v3 seat sidecar: never two seats for one revision, no retro-seating", () => {
  for (const second of ["equal", "different"] as const) {
    test(`a stored seated line is kept when a later sidecar names an ${second} seat (already_stored)`, async () => {
      const store = await scratch();
      const a = await stage(store, [group("claude", "sess-1", [R1])]);
      await putSidecar(store, a, await sidecar(store, a));
      expect((await run(store, { acceptSeatSidecar: true })).lines.written).toBe(1);
      const before = await rawLines(store);
      const b = await stage(store, [group("claude", "sess-1", [R1])]);
      const seat = second === "equal" ? SEAT : "42";
      await putSidecar(store, b, await sidecar(store, b, { seats: [[{ seat, digest: digestOf(R1) }]] }));
      const summary = await run(store, { acceptSeatSidecar: true });
      expect(summary.objects.drained).toBe(1);
      expect(summary.lines).toMatchObject({ written: 0, already_stored: 1 });
      expect(await rawLines(store)).toEqual(before);
      expect(before.get("sess-1")!.map(seatOfLine)).toEqual([SEAT]);
    });
  }

  test("a line stored unseated at an equal revision stays unseated (no retro-seating); a higher revision is seated", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [R1])]);
    await putSidecar(store, a, await sidecar(store, a, { v: 2 }));
    await run(store, { acceptSeatSidecar: true });
    const b = await stage(store, [group("claude", "sess-1", [R1])]);
    await putSidecar(store, b, await sidecar(store, b));
    const summary = await run(store, { acceptSeatSidecar: true });
    expect(summary.lines).toMatchObject({ written: 0, already_stored: 1 });
    expect((await rawLines(store)).get("sess-1")!.map(seatOfLine)).toEqual([undefined]);
    const c = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 2 }])]);
    await putSidecar(store, c, await sidecar(store, c));
    expect((await run(store, { acceptSeatSidecar: true })).lines.written).toBe(1);
    expect((await rawLines(store)).get("sess-1")!.map(seatOfLine)).toEqual([undefined, SEAT]);
  });

  test("a self-disagreeing sidecar is refused: one revision kept twice, with different or equal seats", async () => {
    for (const seats of [[[{ seat: SEAT, digest: digestOf(R1) }, { seat: "77", digest: digestOf(R1) }]], [[{ seat: SEAT, digest: digestOf(R1) }, { seat: SEAT, digest: digestOf(R1) }]]]) {
      const store = await scratch();
      const s = await stage(store, [group("claude", "sess-1", [R1, R1])]);
      await putSidecar(store, s, await sidecar(store, s, { seats }));
      const before = await dataKeys(store);
      const summary = await run(store, { acceptSeatSidecar: true });
      expect(summary.objects.accepted_mismatch).toBe(1);
      expect(summary.objects.drained).toBe(0);
      expect(await dataKeys(store)).toEqual(before);
    }
    // The same revision in two groups (scope aliases) is refused the same way.
    const store = await scratch();
    const s = await stage(store, [group("claude", "sess-1", [R1]), group("claude", "sess-2", [R1])]);
    await putSidecar(store, s, await sidecar(store, s));
    expect((await run(store, { acceptSeatSidecar: true })).objects.accepted_mismatch).toBe(1);
  });
});

describe("drain v3 seat sidecar: malformed sidecars are refused whole", () => {
  const d1 = digestOf(R1), d2 = digestOf(R2);
  const cases: [string, unknown, number[][]?][] = [
    ["missing seat", [[{ seat: SEAT, digest: d1 }]]],
    ["extra seat", [[{ seat: SEAT, digest: d1 }, { seat: "5", digest: d2 }, { seat: "6", digest: d2 }]]],
    ["seat for a line that is not kept", [[{ seat: SEAT, digest: d1 }, { seat: "5", digest: d2 }]], [[0]]],
    ["missing group", []],
    ["seats not an array", { 0: [] }],
    ["number seat", [[{ seat: 5, digest: d1 }, { seat: "6", digest: d2 }]]],
    ["zero seat", [[{ seat: "0", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["negative seat", [[{ seat: "-5", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["leading zero", [[{ seat: "05", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["exponent", [[{ seat: "5e3", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["fraction", [[{ seat: "5.0", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["above 2^63-1", [[{ seat: "9223372036854775808", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["twenty digits", [[{ seat: "10000000000000000000", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["whitespace", [[{ seat: " 5", digest: d1 }, { seat: "6", digest: d2 }]]],
    ["duplicate seat", [[{ seat: "5", digest: d1 }, { seat: "5", digest: d2 }]]],
    ["extra key", [[{ seat: "5", digest: d1, ord: 0 }, { seat: "6", digest: d2 }]]],
    ["missing digest", [[{ seat: "5" }, { seat: "6", digest: d2 }]]],
    ["malformed digest", [[{ seat: "5", digest: d1.toUpperCase() }, { seat: "6", digest: d2 }]]],
    ["digest of another line", [[{ seat: "5", digest: d2 }, { seat: "6", digest: d1 }]]],
  ];
  for (const [name, seats, kept] of cases) {
    test(name, async () => {
      const store = await scratch();
      const s = await stage(store, [group("claude", "sess-1", [R1, R2])]);
      await putSidecar(store, s, await sidecar(store, s, { seats, ...(kept ? { kept } : {}) }));
      const before = await dataKeys(store);
      const summary = await run(store, { acceptSeatSidecar: true });
      expect(summary.objects.accepted_mismatch).toBe(1);
      expect(summary.objects.drained).toBe(0);
      expect(summary.lines.written).toBe(0);
      expect(await dataKeys(store)).toEqual(before);
    });
  }

  test("a staged line that already carries accepted_seq (or names another origin) is refused", async () => {
    for (const raw of [lineOf(R1).replace(/}$/, ',"accepted_seq":"3"}'), lineOf(R1).replace(`"${ORIGIN}"`, '"origin-2"')]) {
      const store = await scratch();
      const s = await stage(store, [group("claude", "sess-1", [{ ...R1, raw }])]);
      await putSidecar(store, s, await sidecar(store, s));
      const summary = await run(store, { acceptSeatSidecar: true });
      expect(summary.objects.drained).toBe(0);
      expect(summary.lines.written).toBe(0);
    }
  });

  test("the parser alone: a valid sidecar parses; the extra key at the top level does not", () => {
    const ok = { v: 3, kind: "buffered-accepted", stage_sha256: "c".repeat(64), live: true, kept: [[0]], fence: null, seats: [[{ seat: "1", digest: "d".repeat(64) }]] };
    expect(parseAcceptedMarkerV3(JSON.stringify(ok))).not.toBeNull();
    expect(parseAcceptedMarkerV3(JSON.stringify({ ...ok, extra: 1 }))).toBeNull();
    expect(parseAcceptedMarkerV3(JSON.stringify({ ...ok, v: 2 }))).toBeNull();
    expect(parseAcceptedMarkerV3(JSON.stringify({ ...ok, seats: [[{ seat: "9223372036854775807", digest: "d".repeat(64) }]] }))).not.toBeNull();
  });

  test("seatLine refuses a digest that is not the line's own pre-seat digest", () => {
    expect(() => seatLine(lineOf(R1), ["r1:r1", digestOf(R1), "claude", "sess-1"], { seat: "5", digest: digestOf(R2) }, ORIGIN)).toThrow("publication_seats_losing_body");
  });
});
