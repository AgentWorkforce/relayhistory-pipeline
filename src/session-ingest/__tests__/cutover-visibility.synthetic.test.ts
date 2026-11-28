/**
 * W2 synthetic proof (docs/design/org-bridge-gate.md, sections 1-3; design only, no production wiring). The drain runs
 * unchanged. A stand-in `CutoverVisibility` (the doc's interface) models the SQL authority:
 *   - publish takes the org key EXCLUSIVE (one at a time here) and records its position (a logical snapshot);
 *   - an erase is one history row at the next position, under the key SHARED;
 *   - an effect is visible iff its row exists and no history row covering its scope came after its publish.
 * The publish is driven at the drain's marker PUT: after the bytes are verified, before the marker. A refused
 * publish fails that PUT, so the stage is never marked. The test reader serves a chunk only when the effect it maps to
 * (manifest `relayhistory_live` batch digest -> seq, the session key) is visible, checked before and after IO. A
 * not-visible chunk makes the scope incomplete; it is never dropped (no subset fold).
 *
 * Scenarios: a mixed stage [s1, s2] with a SQL-only erase of s2 while its manifest PUT is parked (erase first), the
 * reverse order (publish first), a read in progress across an erase commit, a crash before the publish, a lost
 * publish ACK, and a late drain after the bridge's token bump. Each one is read right after resumption, before any repair.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { chunkKeys, objectKey, parseManifest, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { ACCEPTED_SUFFIX, DRAINED_SUFFIX, acceptedCompleteKey, drainBufferedBatches, readLiveStaged } from "../buffered-drain.ts";
import { fenceEpoch, orgFenceAuthorityKey, s3LedgerKeys, s3LedgerObjects, sessionKeyHash } from "../ledger-s3.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage } from "../staging.ts";
import { LIVE_EXTENSION, liveMachineId, type Group } from "../writer.ts";
import { layoutComponent } from "../../shared/layout-component.ts";
import { scratchStore } from "./support.ts";

const ROOT = "96bulls";
const T = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const ORIGIN = "origin-1";
const RECEIVED = "2030-01-02T00:00:00.000Z";
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const enc = new TextEncoder();
const dec = new TextDecoder();
const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));
const KEYS = s3LedgerKeys(ROOT, T);

// ---- the stand-in SQL authority (the doc's CutoverVisibility) ----

type Effect = { kind: "drain" | "direct"; digest: string; sessionKey: string };
type Scope = { workspaceId?: string; userId?: string; sessionKey?: string };
const effectId = (e: Effect) => JSON.stringify([e.kind, e.digest, e.sessionKey]);

class StandInVisibility {
  #pos = 0;
  #token = 1;
  #exportPos: number;
  readonly rows = new Map<string, { at: number; scope: Required<Scope> }>();
  readonly history: { at: number; scope: Scope }[] = [];
  #key: Promise<void> = Promise.resolve();
  constructor() { this.#exportPos = this.#pos; }
  /** The bound export snapshot: history before it is already in the export's verdict. */
  bindExport() { this.#exportPos = ++this.#pos; }
  #covers(scope: Scope, of: Required<Scope>) {
    return (scope.workspaceId === undefined || scope.workspaceId === of.workspaceId) && (scope.userId === undefined || scope.userId === of.userId) &&
      (scope.sessionKey === undefined || scope.sessionKey === of.sessionKey);
  }
  async #exclusive<R>(fn: () => R): Promise<R> {
    const prev = this.#key;
    let release!: () => void;
    this.#key = new Promise<void>((r) => (release = r));
    await prev;
    try { return fn(); } finally { release(); }
  }
  publish(e: Effect & { org: string; generation: [number, number]; token?: number }, scope: Required<Scope>) {
    return this.#exclusive(() => {
      if (e.token !== undefined && e.token !== this.#token) return "refused_token" as const;
      if (this.history.some((h) => h.at > this.#exportPos && this.#covers(h.scope, scope))) return "refused_erased" as const;
      const id = effectId(e);
      if (!this.rows.has(id)) this.rows.set(id, { at: ++this.#pos, scope });
      return "visible" as const;
    });
  }
  /** One history row (any of the 15 sources through the capture trigger), committed after every in-flight publish. */
  erase(scope: Scope) { return this.#exclusive(() => { this.history.push({ at: ++this.#pos, scope }); }); }
  bumpToken() { return this.#exclusive(() => ++this.#token); }
  get token() { return this.#token; }
  async verdict(effects: Effect[]) {
    const out = new Map<string, "visible" | "revoked" | "absent">();
    for (const e of effects) {
      const row = this.rows.get(effectId(e));
      out.set(effectId(e), !row ? "absent" : this.history.some((h) => h.at > row.at && this.#covers(h.scope, row.scope)) ? "revoked" : "visible");
    }
    return out;
  }
}

// ---- fixture ----

let cleanup: (() => void)[] = [];
afterEach(() => { cleanup.forEach((c) => c()); cleanup = []; });

async function scratch() {
  const s = scratchStore();
  cleanup.push(s.done);
  const io = s3LedgerObjects(s.store, ROOT);
  await io.editFence(KEYS.fence, (f) => ({ next: f, result: undefined }));
  await s.store.put(objectKey(orgFenceAuthorityKey(ROOT, T.orgId)), enc.encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete",
    ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1, orgId: T.orgId })));
  await s.store.put(objectKey(acceptedCompleteKey(ROOT, T.orgId)), enc.encode(JSON.stringify({ v: 1, kind: "buffered-accepted-complete", stages: 1, completedAtMs: 1 })));
  return s.store;
}
const line = (id: string) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: id, revision: "1", operation: "upsert", received_at: RECEIVED, payload: { text: `t-${id}`, ts_ms: 1_893_542_400_000 } });
const group = (session: string, ids: string[]): Group => ({ source: "claude", sessionId: session, lines: ids.map(line), meta: ids.map((id) => [`${id}:r1`, sha(`${id}:1`), "claude", session]) });

/** A staged, accepted (live, every line kept) stage, with the FULL's slot entries for its records. */
async function stage(store: ObjectStore, groups: Group[]) {
  let ord = 0;
  const digest = sha(randomUUID());
  const prepared = prepareBufferedStage(ROOT, { claim_token: randomUUID(), batch_digest: digest, received_at: RECEIVED, org_id: T.orgId, workspace_id: T.workspaceId, origin_id: ORIGIN, user_id: T.userId },
    groups, groups.map((g) => g.lines.map(() => ord++)));
  await putBufferedStage(store, prepared);
  const io = s3LedgerObjects(store, ROOT);
  const read = await io.readFence(KEYS.fence);
  const fence = read.state === "present" ? read.fence : null;
  for (const g of groups) for (const [i] of g.lines.entries()) {
    const id = g.meta![i]![0].split(":")[0]!;
    await io.mergeIndex(KEYS.revisions(ORIGIN, id), fence!, ORIGIN, [{ recordId: id, revision: 1, revisionId: null, digest: sha(`${id}:1`), sessionKey: sessionKeyHash("claude", g.sessionId), epoch: 0, resets: 0 }]);
  }
  await store.put(objectKey(prepared.spec.key + ACCEPTED_SUFFIX), enc.encode(JSON.stringify({
    v: 2, kind: "buffered-accepted", stage_sha256: prepared.spec.sha256, live: true, kept: groups.map((g) => g.lines.map((_, j) => j)),
    fence: fence && { instance: fence.instance, resets: fence.resets, resetsCompleted: fence.resetsCompleted,
      epochs: Object.fromEntries(groups.map((g) => [sessionKeyHash("claude", g.sessionId), fenceEpoch(fence, ORIGIN, sessionKeyHash("claude", g.sessionId))])) },
  })));
  return { key: prepared.spec.key, digest };
}

/**
 * The drain's store, with the stand-in publish at the marker PUT (after the bytes are verified, before the marker):
 * every session of the stage is published, and any refusal fails the PUT, so the stage stays unmarked and unserved.
 * `park` holds one matching PUT until `resume()`.
 */
function drainStore(store: ObjectStore, vis: StandInVisibility, o: { park?: (key: string) => boolean; token?: () => number; loseAck?: boolean; crashBeforePublish?: boolean } = {}) {
  let resume!: () => void;
  const gate = new Promise<void>((r) => (resume = r));
  let reached!: () => void;
  const atGate = new Promise<void>((r) => (reached = r));
  let parkArmed = !!o.park, ackArmed = !!o.loseAck;
  const s: ObjectStore = new Proxy(store, {
    get(target, prop) {
      const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
      if (prop !== "put") return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      return async (k: unknown, ...rest: unknown[]) => {
        const key = String(k);
        if (parkArmed && o.park!(key)) { parkArmed = false; reached(); await gate; }
        if (key.endsWith(DRAINED_SUFFIX)) {
          if (o.crashBeforePublish) throw new Error("crash");
          const staged = await readLiveStaged(target, key.slice(0, -DRAINED_SUFFIX.length), decompress);
          for (const g of staged!.groups) {
            const sessionKey = sessionKeyHash(g.source, g.sessionId);
            const r = await vis.publish({ org: T.orgId, kind: "drain", digest: staged!.header.batch_digest, sessionKey, generation: [0, 0], ...(o.token ? { token: o.token() } : {}) },
              { workspaceId: T.workspaceId, userId: T.userId, sessionKey });
            if (r !== "visible") throw new Error(`publish_${r}`);
          }
          if (ackArmed) { ackArmed = false; throw new Error("publish_ack_lost"); }
        }
        return (v as (...a: unknown[]) => unknown).apply(target, [k, ...rest]);
      };
    },
  });
  return { store: s, atGate, resume: () => resume() };
}
const drain = (store: ObjectStore) => drainBufferedBatches({ store, keyRoot: ROOT, orgs: [T.orgId], dryRun: false, sleep: async () => {}, decompress, concurrency: 1 });

/**
 * The W2 test reader for one session: the pre-IO verdict, the chunk reads, the post-IO verdict. Every chunk maps to
 * its effect by the manifest's batch-digest index. Served only when every chunk's effect is visible both times;
 * otherwise `incomplete` (never a subset). `duringIo` runs between the chunk reads and the post-IO verdict.
 */
async function read(store: ObjectStore, vis: StandInVisibility, session: string, duringIo?: () => Promise<void>): Promise<{ state: "served"; records: string[] } | { state: "none" | "incomplete" }> {
  const machine = liveMachineId(T.workspaceId, ORIGIN, T.userId);
  const pointer = `${ROOT}/${T.orgId}/live-sessions/v1/ws=${layoutComponent(T.workspaceId)}/m=${machine}/h=claude/s=${layoutComponent(session)}.json`;
  let manifestKey: string;
  try {
    manifestKey = JSON.parse(dec.decode(await readAll(await store.get(objectKey(pointer))))).manifest_key;
  } catch {
    return { state: "none" };
  }
  const text = dec.decode(await readAll(await store.get(objectKey(manifestKey))));
  const manifest = await parseManifest(text, manifestKey);
  const bySeq = new Map<number, string>();
  for (const [d, seq] of ((manifest.extensions?.[LIVE_EXTENSION] as { batches?: [string, number][] })?.batches ?? [])) bySeq.set(seq, d);
  const sessionKey = sessionKeyHash("claude", session);
  const chunks = chunkKeys(manifest, { prefix: `${ROOT}/${T.orgId}` });
  const effects = chunks.map(({ entry }) => ({ kind: "drain" as const, digest: bySeq.get(entry.seq) ?? "unmapped", sessionKey }));
  const ok = async () => [...(await vis.verdict(effects)).values()].every((v) => v === "visible");
  if (!(await ok())) return { state: "incomplete" };
  const records: string[] = [];
  for (const { key } of chunks) {
    const raw = decompress(await readAll(await store.get(key)));
    for (const l of dec.decode(raw).split("\n").filter(Boolean)) records.push((JSON.parse(l) as { record_id: string }).record_id);
  }
  await duringIo?.();
  if (!(await ok())) return { state: "incomplete" };
  return { state: "served", records };
}
const s2Key = sessionKeyHash("claude", "s2");
const parkS2Manifest = (k: string) => k.includes("/s=s2/") && k.endsWith("/manifest.json");

describe("W2 synthetic proof: SQL visibility per (stage, session) effect", () => {
  test("erase first: a SQL-only erase of s2 commits while s2's manifest PUT is parked; s2 is never served, s1 is", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    const x = await stage(store, [group("s1", ["a1"]), group("s2", ["b1"])]);
    vis.bindExport();
    const d = drainStore(store, vis, { park: parkS2Manifest });
    const running = drain(d.store);
    await d.atGate;
    await vis.erase({ workspaceId: T.workspaceId, userId: T.userId, sessionKey: s2Key }); // nothing on S3 moves
    d.resume();
    const summary = await running;
    // The publish refused (s2 erased after the export): the marker PUT failed, so the stage is unmarked.
    expect(summary.objects.drained).toBe(0);
    expect(await store.head(objectKey(x.key + DRAINED_SUFFIX))).toBeUndefined();
    // Immediately after resumption, before any repair: s2's bytes exist but are never served. s1's effect is its own
    // (granularity per (stage, session)): published before s2's refusal and not covered by the erase, it is served.
    expect(await read(store, vis, "s2")).toEqual({ state: "incomplete" });
    expect(await read(store, vis, "s1")).toEqual({ state: "served", records: ["a1"] });
    expect(await vis.publish({ org: T.orgId, kind: "drain", digest: x.digest, sessionKey: s2Key, generation: [0, 0] },
      { workspaceId: T.workspaceId, userId: T.userId, sessionKey: s2Key })).toBe("refused_erased");
    expect(await read(store, vis, "s2")).toEqual({ state: "incomplete" });
  });

  test("publish first: the erase commit revokes s2 at once (no replay); s1 stays served", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    await stage(store, [group("s1", ["a1"]), group("s2", ["b1"])]);
    vis.bindExport();
    expect((await drain(drainStore(store, vis).store)).objects.drained).toBe(1);
    expect(await read(store, vis, "s2")).toEqual({ state: "served", records: ["b1"] });
    await vis.erase({ workspaceId: T.workspaceId, userId: T.userId, sessionKey: s2Key });
    // The bytes are still in the bucket (byte deletion comes later); the verdict already excludes them.
    expect(await read(store, vis, "s2")).toEqual({ state: "incomplete" });
    expect(await read(store, vis, "s1")).toEqual({ state: "served", records: ["a1"] });
  });

  test("a read in progress across an erase commit fails its post-IO verdict", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    await stage(store, [group("s2", ["b1"])]);
    vis.bindExport();
    await drain(drainStore(store, vis).store);
    const r = await read(store, vis, "s2", () => vis.erase({ workspaceId: T.workspaceId, userId: T.userId, sessionKey: s2Key }));
    expect(r).toEqual({ state: "incomplete" });
  });

  test("a user-scope erase (no session) revokes every session of that user", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    await stage(store, [group("s1", ["a1"]), group("s2", ["b1"])]);
    vis.bindExport();
    await drain(drainStore(store, vis).store);
    await vis.erase({ workspaceId: T.workspaceId, userId: T.userId });
    expect(await read(store, vis, "s1")).toEqual({ state: "incomplete" });
    expect(await read(store, vis, "s2")).toEqual({ state: "incomplete" });
  });

  test("crash before the publish: nothing is served; the rerun publishes, unless an erase came between", async () => {
    for (const eraseBetween of [false, true]) {
      const store = await scratch();
      const vis = new StandInVisibility();
      const x = await stage(store, [group("s2", ["b1"])]);
      vis.bindExport();
      const crashed = await drain(drainStore(store, vis, { crashBeforePublish: true }).store);
      expect(crashed.objects.drained).toBe(0);
      expect(await read(store, vis, "s2")).toEqual({ state: "incomplete" }); // bytes written, never served
      if (eraseBetween) await vis.erase({ workspaceId: T.workspaceId, userId: T.userId, sessionKey: s2Key });
      const rerun = await drain(drainStore(store, vis).store);
      expect(rerun.objects.drained).toBe(eraseBetween ? 0 : 1);
      expect((await read(store, vis, "s2")).state).toBe(eraseBetween ? "incomplete" : "served");
      expect(!!(await store.head(objectKey(x.key + DRAINED_SUFFIX)))).toBe(!eraseBetween);
    }
  });

  test("a lost publish ACK: the rerun's publish is idempotent (same effect) and the stage drains once", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    await stage(store, [group("s2", ["b1"])]);
    vis.bindExport();
    expect((await drain(drainStore(store, vis, { loseAck: true }).store)).objects.drained).toBe(0);
    expect(vis.rows.size).toBe(1);
    expect((await drain(drainStore(store, vis).store)).objects.drained).toBe(1);
    expect(vis.rows.size).toBe(1);
    expect(await read(store, vis, "s2")).toEqual({ state: "served", records: ["b1"] });
  });

  test("a late drain after the bridge's token bump is refused at the decision; its bytes are never served", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    await stage(store, [group("s2", ["b1"])]);
    vis.bindExport();
    const held = vis.token;
    const d = drainStore(store, vis, { park: parkS2Manifest, token: () => held });
    const running = drain(d.store);
    await d.atGate;
    await vis.bumpToken(); // the bridge step: the old drain lost its authority
    d.resume();
    expect((await running).objects.drained).toBe(0);
    expect(await read(store, vis, "s2")).toEqual({ state: "incomplete" });
  });

  test("an erase before the bound export is in the export's verdict, not a refusal (history is positioned)", async () => {
    const store = await scratch();
    const vis = new StandInVisibility();
    await vis.erase({ workspaceId: T.workspaceId, userId: T.userId, sessionKey: sessionKeyHash("claude", "gone") });
    await stage(store, [group("s2", ["b1"])]);
    vis.bindExport();
    expect((await drain(drainStore(store, vis).store)).objects.drained).toBe(1);
    expect(await read(store, vis, "s2")).toEqual({ state: "served", records: ["b1"] });
  });
});



