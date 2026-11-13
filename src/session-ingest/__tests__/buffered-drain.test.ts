import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage } from "../staging.ts";
import type { Group } from "../writer.ts";
import {
  ACCEPTED_SUFFIX,
  SUPERSEDED_SUFFIX,
  DRAINED_SUFFIX,
  acceptedCompleteKey,
  acquireDrainLock,
  drainBufferedBatches,
  drainHoldKey,
  drainLockKey,
  readOnlyStore,
  s3DrainLedger,
  type DrainLedger,
  type DrainOptions,
} from "../buffered-drain.ts";
import { parseDrainArgs, runDrain, validResumeKey } from "../../../scripts/buffered-drain.ts";
import { mkdtempSync, readFileSync as readFileSyncFs, rmSync } from "node:fs";
import { join } from "node:path";
import { scratchStore } from "./support.ts";
import { journalKey as journalKeyFor } from "../async-admission.ts";
import { applyFenceChange, fenceAuthorityKey, fenceEpoch, orgFenceAuthorityKey, s3ControlMirror, s3LedgerKeys, s3LedgerObjects, sessionKeyHash, type FenceChange, type IndexEntryInput } from "../ledger-s3.ts";
import type { OrderedControlInput } from "../ledger.ts";

const ROOT = "96bulls";
const TENANT = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const ORIGIN = "origin-1";
// Synthetic times only (the repo is public).
const RECEIVED = "2030-01-02T00:00:00.000Z";
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));
const enc = new TextEncoder();
const dec = new TextDecoder();

type Rec = { id: string; rev: number; digest?: string };
function group(source: string, sessionId: string, recs: Rec[]): Group {
  return {
    source, sessionId,
    lines: recs.map((r) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: r.id, revision: String(r.rev), operation: "upsert", received_at: RECEIVED, payload: { text: `t-${r.id}-${r.rev}`, ts_ms: 1_893_542_400_000 } })),
    meta: recs.map((r) => [`${r.id}:r${r.rev}`, r.digest ?? sha(`${r.id}:${r.rev}`), source, sessionId]),
  };
}

/**
 * A stand-in for the accepted-stage export (v2 verdicts): the stages it would record, refreshed into sidecars at the
 * start of every run() (as an export just before the drain), with a snapshot of the user's CURRENT S3 fence.
 * `live` / `keep` stand for SQL's verdict; a test that needs the snapshot to go stale passes { refresh: false }.
 */
type Verdict = { sha: string; lineCounts: number[]; sessions: [string, string][]; live: boolean; keep?: number[][] };
const VERDICTS = new Map<string, Verdict>();
const accept = (key: string, v: Verdict) => { VERDICTS.set(key, v); };
async function refreshVerdicts(store: ObjectStore) {
  // An unreadable fence gives no fence view (the drain refuses it before the verdict matters).
  const read = await s3LedgerObjects(store, ROOT).readFence(s3LedgerKeys(ROOT, TENANT).fence).catch(() => ({ state: "absent" as const }));
  for (const [key, v] of VERDICTS) {
    const fence = read.state === "present" ? read.fence : null;
    const marker = {
      v: 2, kind: "buffered-accepted", stage_sha256: v.sha, live: v.live,
      kept: v.live ? (v.keep ?? v.lineCounts.map((n) => [...Array(n).keys()])) : [],
      fence: fence && {
        instance: fence.instance, resets: fence.resets, resetsCompleted: fence.resetsCompleted,
        epochs: Object.fromEntries(v.sessions.map(([so, id]) => [sessionKeyHash(so, id), fenceEpoch(fence, ORIGIN, sessionKeyHash(so, id))])),
      },
    };
    await store.put(objectKey(key + ACCEPTED_SUFFIX), enc.encode(JSON.stringify(marker)));
  }
}
/** A raw object at a staged key, accepted (so the drain reads and classifies it). */
async function putStaged(store: ObjectStore, key: string, bytes: Uint8Array) {
  await store.put(objectKey(key), bytes);
  accept(key, { sha: sha(bytes), lineCounts: [], sessions: [], live: true });
}

async function stage(store: ObjectStore, groups: Group[], over: { batch?: string; claim?: string; received?: string; accepted?: boolean; live?: boolean; keep?: number[][] } = {}): Promise<string> {
  let ord = 0;
  const prepared = prepareBufferedStage(ROOT, {
    claim_token: over.claim ?? randomUUID(),
    batch_digest: sha(over.batch ?? randomUUID()),
    received_at: over.received ?? RECEIVED,
    org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, origin_id: ORIGIN, user_id: TENANT.userId,
  }, groups, groups.map((g) => g.lines.map(() => ord++)));
  await putBufferedStage(store, prepared);
  if (over.accepted !== false) accept(prepared.spec.key, {
    sha: prepared.spec.sha256, lineCounts: groups.map((g) => g.lines.length), sessions: groups.map((g) => [g.source, g.sessionId] as [string, string]),
    live: over.live ?? true, ...(over.keep ? { keep: over.keep } : {}),
  });
  return prepared.spec.key;
}

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
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const readJson = async (store: ObjectStore, key: string) => JSON.parse(dec.decode(await readAll(await store.get(objectKey(key)))));

/** Canonical chunk lines per session id (from the sealed chunks under `<root>/<org>/v1/`). */
async function canonical(store: ObjectStore): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>();
  for (const key of await allKeys(store, `${ROOT}/${TENANT.orgId}/v1`)) {
    if (!key.includes("/raw/")) continue;
    const session = /\/s=([^/]+)\/raw\//.exec(key)![1]!;
    const lines = dec.decode(decompress(await readAll(await store.get(objectKey(key))))).split("\n").filter(Boolean);
    out.set(session, [...(out.get(session) ?? []), ...lines.map((l) => { const r = JSON.parse(l); return `${r.record_id}@${r.revision}`; })]);
  }
  return out;
}

// #580's keys, formats and rules (ledger-s3.ts) only: no local copy of any of them.
const KEYS = s3LedgerKeys(ROOT, TENANT);
const uploadFenceKey = () => KEYS.fence;
const rearmKey = (source: string, sessionId: string) => KEYS.rearm(ORIGIN, source, sessionId);
const ownerKey = (source: string, sessionId: string) => KEYS.owner(ORIGIN, source, sessionId);
const SESS1 = sessionKeyHash("claude", "sess-1");
/** Create or change the user's fence exactly as ledger-s3 does (newFence on absent, applyFenceChange). */
async function putFence(store: ObjectStore, change: FenceChange = {}) {
  await s3LedgerObjects(store, ROOT).editFence(KEYS.fence, (f) => { applyFenceChange(f, change); return { next: f, result: undefined }; });
}
async function currentFence(store: ObjectStore) {
  const read = await s3LedgerObjects(store, ROOT).readFence(KEYS.fence);
  if (read.state !== "present") throw new Error("test: no fence");
  return read.fence;
}
/** Seed the per-origin revision shards through ledger-s3's own mergeIndex. */
async function seedIndex(store: ObjectStore, entries: IndexEntryInput[]) {
  const io = s3LedgerObjects(store, ROOT);
  const fence = await currentFence(store);
  for (const e of entries) await io.mergeIndex(KEYS.revisions(ORIGIN, e.recordId), fence, ORIGIN, [e]);
}
/** The stored index entry for a record, or undefined. */
async function storedEntry(store: ObjectStore, recordId: string) {
  const doc = (await s3LedgerObjects(store, ROOT).readIndex(KEYS.revisions(ORIGIN, recordId)))?.doc;
  return doc && Object.hasOwn(doc.records, recordId) ? doc.records[recordId] : undefined;
}
async function run(store: ObjectStore, over: Partial<DrainOptions> = {}, o: { refresh?: boolean } = {}) {
  if (o.refresh !== false) await refreshVerdicts(store);
  return drainBufferedBatches({ store, keyRoot: ROOT, orgs: [TENANT.orgId], dryRun: false, decompress, sleep: async () => {}, ...over });
}

let cleanup: (() => void)[] = [];
afterEach(() => { cleanup.forEach((c) => c()); cleanup = []; VERDICTS.clear(); });
/** A scratch store whose user has a backfilled, empty fence (0005 step 0), unless `fence: false`. */
/** A valid fence-authority marker (main's OrgFenceAuthorityDoc / FenceAuthorityDoc): every precondition attested. */
const AUTHORITY = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", writtenAtMs: 1 };
const putOrgAuthority = (store: ObjectStore, doc: unknown = { ...AUTHORITY, orgId: TENANT.orgId }) =>
  store.put(objectKey(orgFenceAuthorityKey(ROOT, TENANT.orgId)), enc.encode(JSON.stringify(doc)));
async function scratch(opts: { fence?: boolean; exportComplete?: boolean; authority?: boolean } = {}) {
  const s = scratchStore();
  cleanup.push(s.done);
  if (opts.fence !== false) await putFence(s.store);
  if (opts.authority !== false) await putOrgAuthority(s.store);
  if (opts.exportComplete !== false) await markExportComplete(s.store);
  return s.store;
}
const markExportComplete = (store: ObjectStore) =>
  store.put(objectKey(acceptedCompleteKey(ROOT, TENANT.orgId)), enc.encode(JSON.stringify({ v: 1, kind: "buffered-accepted-complete", stages: 0, completedAtMs: 1 })));
const putJournal = (store: ObjectStore, journal: unknown) => store.put(objectKey(journalKeyFor(ROOT, TENANT)), enc.encode(JSON.stringify(journal)));

describe("buffered backlog drain (0005)", () => {
  test("drains a staged batch into the canonical layout, the revision index and a drained marker", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }, { id: "r2", rev: 2 }]), group("claude", "sess-2", [{ id: "r3", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.sessions_written).toBe(2);
    expect(summary.lines).toEqual({ candidate: 3, written: 3, fenced: 0, stale: 0, already_stored: 0, revision_conflict: 0, not_kept: 0 });
    const lines = await canonical(store);
    expect(lines.get("sess-1")).toEqual(["r1@1", "r2@2"]);
    expect(lines.get("sess-2")).toEqual(["r3@1"]);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(true);
    expect(await storedEntry(store, "r1")).toEqual([1, "r1:r1", sha("r1:1"), SESS1, 0, 0]);
    expect(await storedEntry(store, "r2")).toEqual([2, "r2:r2", sha("r2:2"), SESS1, 0, 0]);
    // Ownership stubs (ledger-s3) for every written session, at the read's generation.
    expect(await readJson(store, ownerKey("claude", "sess-1"))).toEqual({ v: 1, originId: ORIGIN, source: "claude", sessionId: "sess-1", epoch: 0, resets: 0, machineId: expect.stringMatching(/^srv_[0-9a-f]{32}$/) });
    expect(await exists(store, ownerKey("claude", "sess-2"))).toBe(true);
    // The summary is counts only: no key, tenant id or content.
    const text = JSON.stringify(summary);
    for (const needle of ["org-a", "ws-a", "user-1", ORIGIN, "sess-1", "96bulls", "hello", "t-r1"]) expect(text).not.toContain(needle);
  });

  test("a rerun is a no-op; a rerun after a crash before the marker appends nothing", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    await run(store);
    const before = await allKeys(store);
    const again = await run(store);
    expect(again.objects.already_drained).toBe(1);
    expect(again.objects.drained).toBe(0);
    expect(await allKeys(store)).toEqual(before);

    // Crash between the canonical write and the marker: the rerun re-reads, writes nothing new, re-marks.
    await store.delete(objectKey(key + DRAINED_SUFFIX));
    const manifestKey = before.find((k) => k.endsWith("/manifest.json"))!;
    const revision = (await readJson(store, manifestKey)).revision;
    const resumed = await run(store);
    expect(resumed.objects.drained).toBe(1);
    expect(resumed.lines.already_stored).toBe(1);
    expect(resumed.lines.written).toBe(0);
    expect((await readJson(store, manifestKey)).revision).toBe(revision);
    expect((await canonical(store)).get("sess-1")).toEqual(["r1@1"]);
  });

  test("a crash before the index merge: the manifest replay dedupe keeps the rewrite idempotent", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    await run(store);
    await store.delete(objectKey(key + DRAINED_SUFFIX));
    await store.delete(objectKey(KEYS.revisions(ORIGIN, "r1")));
    const resumed = await run(store);
    expect(resumed.objects.drained).toBe(1);
    expect(resumed.lines.written).toBe(1);
    expect((await canonical(store)).get("sess-1")).toEqual(["r1@1"]);
  });

  test("a fenced (deleted) session is dropped; the rest of the batch lands", async () => {
    const store = await scratch();
    await putFence(store, { deletions: [{ originId: ORIGIN, sessions: [["claude", "sess-2"]] }, { originId: "other-origin", sessions: [["claude", "sess-1"]] }] });
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }, { id: "r3", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.lines.fenced).toBe(2);
    expect(summary.lines.written).toBe(1);
    const lines = await canonical(store);
    expect(lines.get("sess-1")).toEqual(["r1@1"]);
    expect(lines.has("sess-2")).toBe(false);
    expect(await storedEntry(store, "r2")).toBeUndefined();
    expect(await storedEntry(store, "r3")).toBeUndefined();
    // As admission's commit, the re-armed session gets its stub (with the writer machine) for the sweeper.
    expect((await readJson(store, ownerKey("claude", "sess-2"))).machineId).toMatch(/^srv_[0-9a-f]{32}$/);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(true);
    // An earlier partial run may have written the deleted session: it is re-armed for the eraser.
    expect(summary.rearmed).toBe(1);
    expect(await exists(store, rearmKey("claude", "sess-2"))).toBe(true);
    expect(await exists(store, rearmKey("claude", "sess-1"))).toBe(false);
  });

  test("equal revision with another digest is never written; stale and already-stored are skipped", async () => {
    const store = await scratch();
    const gen = { sessionKey: SESS1, epoch: 0, resets: 0 };
    await seedIndex(store, [
      { recordId: "conflict", revision: 3, revisionId: "x", digest: "f".repeat(64), ...gen },
      { recordId: "stale", revision: 5, revisionId: "y", digest: sha("stale:4"), ...gen },
      { recordId: "same", revision: 2, revisionId: "same:r2", digest: sha("same:2"), ...gen },
    ]);
    await stage(store, [group("claude", "sess-1", [{ id: "conflict", rev: 3 }, { id: "stale", rev: 4 }, { id: "same", rev: 2 }, { id: "fresh", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.lines).toEqual({ candidate: 4, written: 1, fenced: 0, stale: 1, already_stored: 1, revision_conflict: 1, not_kept: 0 });
    expect((await canonical(store)).get("sess-1")).toEqual(["fresh@1"]);
    expect(await storedEntry(store, "conflict")).toEqual([3, "x", "f".repeat(64), SESS1, 0, 0]); // the stored digest is kept
    expect(await storedEntry(store, "fresh")).toEqual([1, "fresh:r1", sha("fresh:1"), SESS1, 0, 0]);
  });

  test("revision-only stored entries (#587: seeded records, lazy slots) make an equal or lower revision stale", async () => {
    const store = await scratch();
    await seedIndex(store, [
      { recordId: "seeded", revision: 4, revisionId: null, digest: null, sessionKey: SESS1, epoch: 0, resets: 0 },
      { recordId: "slot", revision: 2, revisionId: null, digest: sha("slot:2"), sessionKey: SESS1, epoch: 0, resets: 0 },
    ]);
    await stage(store, [group("claude", "sess-1", [{ id: "seeded", rev: 4 }, { id: "slot", rev: 1 }, { id: "fresh", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.lines).toMatchObject({ candidate: 3, written: 1, stale: 2, already_stored: 0, revision_conflict: 0 });
    expect((await canonical(store)).get("sess-1")).toEqual(["fresh@1"]);
    expect(await storedEntry(store, "seeded")).toEqual([4, null, null, SESS1, 0, 0]); // never overwritten by the drain
  });

  test("across backlog batches: a later batch's equal revision with another digest is refused by the merged index", async () => {
    const store = await scratch();
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1, digest: "a".repeat(64) }])], { batch: "b1" });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1, digest: "b".repeat(64) }])], { batch: "b2" });
    const summary = await run(store, { concurrency: 2 });
    expect(summary.objects.drained).toBe(2);
    expect(summary.lines.written).toBe(1);
    expect(summary.lines.revision_conflict).toBe(1);
    expect((await canonical(store)).get("sess-1")).toEqual(["r1@1"]);
  });

  test("dry run lists and classifies, and writes nothing", async () => {
    const store = await scratch();
    await putFence(store, { deletions: [{ originId: ORIGIN, sessions: [["claude", "sess-2"]] }] });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    await putStaged(store, `${ROOT}/${TENANT.orgId}/buffered-batches/v1/ws=ws-a/m=srv_${"0".repeat(32)}/b=${"1".repeat(32)}-${"2".repeat(16)}-${"3".repeat(32)}.staged.ndjson.zst`, enc.encode("not zstd"));
    await refreshVerdicts(store); // the export ran; the dry run itself writes nothing
    const before = await allKeys(store);
    const summary = await run(store, { dryRun: true }, { refresh: false });
    expect(summary.mode).toBe("dry_run");
    expect(summary.objects.would_drain).toBe(1);
    expect(summary.objects.corrupt).toBe(1);
    expect(summary.objects.drained).toBe(0);
    expect(summary.lines.written).toBe(1);
    expect(summary.lines.fenced).toBe(1);
    expect(summary.sessions_written).toBe(1);
    expect(await allKeys(store)).toEqual(before);
    await expect(readOnlyStore(store).put(objectKey("96bulls/x/y.json"), enc.encode("{}"))).rejects.toThrow("drain_dry_run_write_refused");
  });

  test("corrupt, misfiled and redacted objects are reported, not fatal", async () => {
    const store = await scratch();
    const good = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const dir = good.slice(0, good.lastIndexOf("/"));
    // Garbage at a well-formed key.
    await putStaged(store, `${dir}/b=${"1".repeat(32)}-${"2".repeat(16)}-${"3".repeat(32)}.staged.ndjson.zst`, enc.encode("garbage"));
    // A valid object copied to a key that does not name its bytes.
    const bytes = await readAll(await store.get(objectKey(good)));
    await putStaged(store, `${dir}/b=${"4".repeat(32)}-${"5".repeat(16)}-${"6".repeat(32)}.staged.ndjson.zst`, bytes);
    // A buffered-GC redaction barrier.
    await putStaged(store, `${dir}/b=${"7".repeat(32)}-${"8".repeat(16)}-${"9".repeat(32)}.staged.ndjson.zst`, enc.encode(JSON.stringify({ v: 1, kind: "buffered-redaction" })));
    // A stray key in the namespace.
    await store.put(objectKey(`${dir}/stray.json`), enc.encode("{}"));
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.objects.corrupt).toBe(1);
    expect(summary.objects.key_mismatch).toBe(1);
    expect(summary.objects.redacted).toBe(1);
    expect(summary.foreign_keys).toBe(1);
    expect((await canonical(store)).get("sess-1")).toEqual(["r1@1"]);
  });

  test("an unreadable fence skips the batch (fail closed) and leaves it undrained", async () => {
    const store = await scratch();
    await store.put(objectKey(uploadFenceKey()), enc.encode("{\"v\":1,\"resets\":\"x\"}"));
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.fence_unreadable).toBe(1);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    expect((await canonical(store)).size).toBe(0);
  });

  test("resets: undeterminable without a reset time (today's #580 fence); pre-reset batches dropped with one", async () => {
    const store = await scratch();
    const early = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { received: "2030-01-02T00:00:00.000Z" });
    const late = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { received: "2030-01-04T00:00:00.000Z" });
    // A reset recorded with no time (lastResetAtMs stays 0): undeterminable.
    await putFence(store, { reset: { requestId: "q1", atMs: 0, result: 0 } });
    const blind = await run(store);
    expect(blind.objects.reset_undeterminable).toBe(2);
    expect((await canonical(store)).size).toBe(0);

    await putFence(store, { reset: { requestId: "q2", atMs: Date.parse("2030-01-03T00:00:00.000Z"), result: 0 } });
    const summary = await run(store);
    expect(summary.objects.pre_reset).toBe(1);
    expect(summary.objects.drained).toBe(1);
    const lines = await canonical(store);
    expect(lines.has("sess-1")).toBe(false);
    expect(lines.get("sess-2")).toEqual(["r2@1"]);
    expect((await readJson(store, early + DRAINED_SUFFIX)).outcome).toBe("pre_reset");
    expect((await readJson(store, late + DRAINED_SUFFIX)).outcome).toBe("drained");
  });

  test("pending ordered controls, as admission: a pending disable's session is deleted, a pending reset refuses the user", async () => {
    const store = await scratch();
    const mirror = s3ControlMirror(store, ROOT);
    const control = (version: string, kind: "selection" | "reset") => ({ epoch: "e".repeat(64), version, digest: `${kind === "reset" ? "a" : "d"}${version.padStart(63, "0")}`, kind });
    await mirror.beginOrdered({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-2"]], enabled: false, control: control("1", "selection") } as OrderedControlInput);
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    const first = await run(store);
    expect(first.objects.drained).toBe(1);
    expect(first.lines.fenced).toBe(1);
    expect([...(await canonical(store)).keys()]).toEqual(["sess-1"]);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(true);
    // A pending reset (SQL has not decided): no batch of the user is written.
    await mirror.beginOrdered({ tenant: TENANT, requestId: "req-1", control: control("2", "reset") } as OrderedControlInput);
    const key2 = await stage(store, [group("claude", "sess-3", [{ id: "r3", rev: 1 }])]);
    for (const dryRun of [true, false]) expect((await run(store, { dryRun })).objects.control_pending).toBe(1);
    expect(await exists(store, key2 + DRAINED_SUFFIX)).toBe(false);
    expect((await currentFence(store)).v).toBe(2); // main's v:2 while a control is pending: read as-is
  });

  test("fence authority, as admission: --execute writes nothing of an org without it; the dry run still classifies", async () => {
    const store = await scratch({ authority: false });
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const dry = await run(store, { dryRun: true });
    expect(dry.objects.would_drain).toBe(1);
    expect(dry.orgs_without_authority).toBe(1);
    const missing = await run(store);
    expect(missing.objects).toMatchObject({ fence_authority_missing: 1, drained: 0 });
    expect(missing.orgs_without_authority).toBe(1);
    // A present but invalid per-org marker refuses outright, even with a valid deployment-wide one.
    await store.put(objectKey(fenceAuthorityKey(ROOT)), enc.encode(JSON.stringify(AUTHORITY)));
    await putOrgAuthority(store, { ...AUTHORITY, orgId: "another-org" });
    expect((await run(store)).objects.fence_authority_invalid).toBe(1);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    // The deployment-wide marker alone (no per-org one) is enough, as for admission.
    await store.delete(objectKey(orgFenceAuthorityKey(ROOT, TENANT.orgId)));
    expect((await run(store)).objects.drained).toBe(1);
  });

  test("a reset begun but not finished (resetsCompleted < resets) skips the batch until it completes", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { received: "2030-01-04T00:00:00.000Z" });
    // ledger-s3: a reset recorded while it runs (result null) does not advance resetsCompleted.
    await putFence(store, { reset: { requestId: "q-running", atMs: Date.parse("2030-01-03T00:00:00.000Z"), result: null } });
    const f = await currentFence(store);
    expect(f.resetsCompleted).toBeLessThan(f.resets);
    for (const dryRun of [true, false]) expect((await run(store, { dryRun })).objects.reset_in_flight).toBe(1);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    expect((await canonical(store)).size).toBe(0);
  });

  test("reset margin: batches within the margin of the last reset, either side, are skipped unmarked", async () => {
    const store = await scratch();
    const R = Date.parse("2030-01-03T00:00:00.000Z");
    const at = (ms: number) => new Date(R + ms).toISOString();
    const keys = {
      before: await stage(store, [group("claude", "s-before", [{ id: "a", rev: 1 }])], { received: at(-6 * 60_000) }),
      nearBefore: await stage(store, [group("claude", "s-near-before", [{ id: "b", rev: 1 }])], { received: at(-2 * 60_000) }),
      nearAfter: await stage(store, [group("claude", "s-near-after", [{ id: "c", rev: 1 }])], { received: at(2 * 60_000) }),
      edge: await stage(store, [group("claude", "s-edge", [{ id: "e", rev: 1 }])], { received: at(5 * 60_000) }),
      after: await stage(store, [group("claude", "s-after", [{ id: "d", rev: 1 }])], { received: at(6 * 60_000) }),
    };
    await putFence(store, { reset: { requestId: "q1", atMs: R, result: 0 } });
    const summary = await run(store);
    expect(summary.objects.pre_reset).toBe(1);
    expect(summary.objects.reset_margin).toBe(2);
    expect(summary.objects.drained).toBe(2); // received exactly at reset + margin drains
    expect([...(await canonical(store)).keys()].sort()).toEqual(["s-after", "s-edge"]);
    expect(await exists(store, keys.nearBefore + DRAINED_SUFFIX)).toBe(false);
    expect(await exists(store, keys.nearAfter + DRAINED_SUFFIX)).toBe(false);
    expect((await readJson(store, keys.before + DRAINED_SUFFIX)).outcome).toBe("pre_reset");

    // A wider margin skips more; below the 1-minute floor is refused, here and at the CLI.
    const wide = await run(store, { resetMarginMs: 10 * 60_000 });
    expect(wide.objects.reset_margin).toBe(2);
    expect(wide.objects.already_drained).toBe(3);
    await expect(run(store, { resetMarginMs: 59_999 })).rejects.toThrow("drain_reset_margin_invalid");
    expect(() => parseDrainArgs(["--dry-run", "--bucket", "my-bucket", "--reset-margin-seconds", "59"])).toThrow("--reset-margin-seconds");
    expect(parseDrainArgs(["--dry-run", "--bucket", "my-bucket", "--reset-margin-seconds", "600"]).resetMarginMs).toBe(600_000);
  });

  test("0005 step 0b: only acknowledged stages are written; the rest are reported, unread", async () => {
    const store = await scratch();
    const ok = await stage(store, [group("claude", "sess-ok", [{ id: "a", rev: 1 }])]);
    const refused = await stage(store, [group("claude", "sess-refused", [{ id: "b", rev: 1 }])], { accepted: false });
    const wrong = await stage(store, [group("claude", "sess-wrong", [{ id: "c", rev: 1 }])], { accepted: false });
    accept(wrong, { sha: "0".repeat(64), lineCounts: [1], sessions: [["claude", "sess-wrong"]], live: true }); // a sidecar naming other bytes
    const reads: string[] = [];
    const watched = new Proxy(store, {
      get(target, prop) {
        if (prop === "getObject") return (k: Parameters<ObjectStore["getObject"]>[0]) => { reads.push(String(k)); return target.getObject(k); };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    const summary = await run(watched);
    expect(summary.objects).toMatchObject({ drained: 1, stage_unacknowledged: 1, accepted_mismatch: 1 });
    expect([...(await canonical(store)).keys()]).toEqual(["sess-ok"]);
    expect(reads).not.toContain(refused); // an unacknowledged stage is never even read
    for (const k of [refused, wrong]) expect(await exists(store, k + DRAINED_SUFFIX)).toBe(false);
    expect(await exists(store, ok + DRAINED_SUFFIX)).toBe(true);
  });

  test("0005 step 0b: an org whose accepted-stage export never completed writes nothing", async () => {
    const store = await scratch({ exportComplete: false });
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    for (const dryRun of [true, false]) expect((await run(store, { dryRun })).objects.stage_export_missing).toBe(1);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    await store.put(objectKey(acceptedCompleteKey(ROOT, TENANT.orgId)), enc.encode("{\"v\":1}")); // invalid: still missing
    expect((await run(store)).objects.stage_export_missing).toBe(1);
    await markExportComplete(store);
    expect((await run(store)).objects.drained).toBe(1);
  });

  test("a GC replacement SQL acknowledged drains; its unacknowledged original is not a peer", async () => {
    const store = await scratch();
    const claim = randomUUID();
    const original = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }, { id: "r2", rev: 1 }])], { batch: "same", claim, accepted: false });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "same", claim });
    const summary = await run(store);
    expect(summary.objects).toMatchObject({ drained: 1, stage_unacknowledged: 1, ambiguous_replacement: 0 });
    expect((await canonical(store)).get("sess-1")).toEqual(["r1@1"]);
    expect(await exists(store, original + DRAINED_SUFFIX)).toBe(false);
  });

  test("an interrupted GC replacement (two live objects for one claim) is never guessed", async () => {
    const store = await scratch();
    const claim = randomUUID();
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }, { id: "r2", rev: 1 }])], { batch: "same", claim });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "same", claim });
    // A retry of the same batch under another claim token is independent (and replays idempotently).
    await stage(store, [group("claude", "sess-9", [{ id: "r9", rev: 1 }])], { batch: "same" });
    const summary = await run(store);
    expect(summary.objects.ambiguous_replacement).toBe(2);
    expect(summary.objects.drained).toBe(1);
    expect([...(await canonical(store)).keys()]).toEqual(["sess-9"]);
  });

  test("a deletion that lands during the write re-arms erasure for the written session", async () => {
    const store = await scratch();
    const base = s3DrainLedger(store, ROOT);
    const before = await base.readFence(KEYS.fence);
    if (before.state !== "present") throw new Error("test: no fence");
    const deletedFence = structuredClone(before.fence);
    applyFenceChange(deletedFence, { deletions: [{ originId: ORIGIN, sessions: [["claude", "sess-1"]] }] });
    deletedFence.version += 1;
    let reads = 0;
    // Reads: the claim-time read, the post-ownership re-read, then the commit-time read (sess-1 deleted by then).
    const ledger: DrainLedger = { ...base, async readFence() { return reads++ < 2 ? before : { ...before, fence: deletedFence }; } };
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    const summary = await run(store, { ledger });
    expect(summary.objects.drained).toBe(1);
    expect(summary.rearmed).toBe(1);
    const rearm = await readJson(store, rearmKey("claude", "sess-1"));
    expect(rearm).toMatchObject({ v: 1, originId: ORIGIN, source: "claude", sessionId: "sess-1" });
    expect(await exists(store, rearmKey("claude", "sess-2"))).toBe(false);
    // The deleted session's revisions are not merged; the other session's are.
    expect(await storedEntry(store, "r1")).toBeUndefined();
    expect(await storedEntry(store, "r2")).toBeDefined();
  });

  test("ownership stubs land before any canonical write; a fence that moves meanwhile leaves the batch for a rerun", async () => {
    const store = await scratch();
    const base = s3DrainLedger(store, ROOT);
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    let canonicalAtOwner = -1;
    const ordered: DrainLedger = { ...base, async registerOwner(k, doc) { canonicalAtOwner = (await canonical(store)).size; return base.registerOwner(k, doc); } };
    await run(store, { ledger: ordered });
    expect(canonicalAtOwner).toBe(0);
    expect((await canonical(store)).size).toBe(1);

    const store2 = await scratch();
    const base2 = s3DrainLedger(store2, ROOT);
    const key2 = await stage(store2, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const first = await base2.readFence(KEYS.fence);
    if (first.state !== "present") throw new Error("test: no fence");
    let n = 0;
    const moving: DrainLedger = { ...base2, async readFence() { return n++ === 0 ? first : { ...first, fence: { ...first.fence, version: first.fence.version + 1 } }; } };
    const summary = await run(store2, { ledger: moving });
    expect(summary.objects.fence_moved).toBe(1);
    expect((await canonical(store2)).size).toBe(0);
    expect(await exists(store2, key2 + DRAINED_SUFFIX)).toBe(false);
  });

  test("a user without a backfilled fence is refused: nothing written, nothing marked", async () => {
    const store = await scratch({ fence: false });
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    for (const dryRun of [true, false]) {
      const summary = await run(store, { dryRun });
      expect(summary.objects.fence_absent).toBe(1);
      expect(summary.objects.drained + summary.objects.would_drain).toBe(0);
    }
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    expect((await canonical(store)).size).toBe(0);
  });

  test("privacy journal: an open control or an unreadable journal skips the batch", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    await putJournal(store, { v: 3, seq: 2, pending: [{ id: "o:e:3", startMs: 1 }], ambiguous: [] });
    expect((await run(store)).objects.privacy_control_open).toBe(1);
    await putJournal(store, { v: 3, seq: 2, pending: [], ambiguous: ["o:e:3"] });
    expect((await run(store)).objects.privacy_control_open).toBe(1);
    await putJournal(store, { v: 2, seq: 2 });
    expect((await run(store)).objects.journal_unreadable).toBe(1);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    expect((await canonical(store)).size).toBe(0);
    // A settled journal (no open control) lets it through.
    await putJournal(store, { v: 3, seq: 2, pending: [], ambiguous: [] });
    expect((await run(store)).objects.drained).toBe(1);
  });

  test("a privacy control that applies during the write re-arms every written session", async () => {
    const store = await scratch();
    const base = s3DrainLedger(store, ROOT);
    let reads = 0;
    const ledger: DrainLedger = { ...base, async readJournal() { return { seq: reads++ === 0 ? 4 : 5, pending: 0, ambiguous: 0 }; } };
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    const summary = await run(store, { ledger });
    expect(summary.objects.drained).toBe(1);
    expect(summary.rearmed).toBe(2);
    expect(await exists(store, rearmKey("claude", "sess-1"))).toBe(true);
    expect(await exists(store, rearmKey("claude", "sess-2"))).toBe(true);
  });

  test("max-objects stops early with a checkpoint; resuming from it drains the rest", async () => {
    const store = await scratch();
    for (let i = 0; i < 4; i++) await stage(store, [group("claude", `sess-${i}`, [{ id: `r${i}`, rev: 1 }])]);
    let checkpoint: string | undefined;
    const first = await run(store, { maxObjects: 2, concurrency: 1, onCheckpoint: (k) => { checkpoint = k; } });
    expect(first.stopped).toBe("max_objects");
    expect(first.objects.drained).toBe(2);
    const second = await run(store, { startAfter: checkpoint!, concurrency: 3 });
    expect(second.stopped).toBe("complete");
    expect(second.objects.drained).toBe(2);
    expect(second.objects.already_drained).toBe(0);
    expect((await canonical(store)).size).toBe(4);
  });

  test("an index entry the fence has forgotten (a reset since) counts as absent, as the claim; the merge restamps it", async () => {
    const store = await scratch();
    // Stored at resets 0; then a reset (well before the batch, past the margin) forgets it.
    await seedIndex(store, [{ recordId: "r1", revision: 5, revisionId: "old", digest: "a".repeat(64), sessionKey: SESS1, epoch: 0, resets: 0 }]);
    await putFence(store, { reset: { requestId: "q1", atMs: Date.parse("2030-01-01T00:00:00.000Z"), result: 0 } });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.lines.stale).toBe(0);
    expect(summary.lines.written).toBe(1);
    expect(await storedEntry(store, "r1")).toEqual([1, "r1:r1", sha("r1:1"), SESS1, 0, 1]);
  });

  test("--assume-empty-fence classifies an unbackfilled user in a dry run, and is refused with execute", async () => {
    const store = await scratch({ fence: false });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    await refreshVerdicts(store);
    const before = await allKeys(store);
    const summary = await run(store, { dryRun: true, assumeEmptyFence: true }, { refresh: false });
    expect(summary.objects.would_drain).toBe(1);
    expect(summary.objects.fence_absent).toBe(0);
    expect(summary.fence_assumed_empty).toBe(1);
    expect(summary.lines.written).toBe(1);
    expect(await allKeys(store)).toEqual(before);
    await expect(run(store, { dryRun: false, assumeEmptyFence: true })).rejects.toThrow("drain_assume_empty_fence_requires_dry_run");
    expect(await allKeys(store)).toEqual(before); // refused before the lock or any write
    expect(() => parseDrainArgs(["--execute", "--bucket", "my-bucket", "--kms-key-arn", `arn:aws:kms:us-east-1:000000000000:key/${"0".repeat(8)}-0000-0000-0000-${"0".repeat(12)}`, "--assume-empty-fence"])).toThrow("only for --dry-run");
    expect(parseDrainArgs(["--dry-run", "--bucket", "my-bucket", "--assume-empty-fence"]).assumeEmptyFence).toBe(true);
  });

  test("one drain at a time: a live lease refuses both modes; an expired one is taken and released", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const T = Date.parse("2030-01-05T00:00:00.000Z");
    const now = () => new Date(T);
    await store.put(objectKey(drainLockKey(ROOT)), enc.encode(JSON.stringify({ v: 1, kind: "buffered-drain-lock", holder: "drain-other", expiresAtMs: T + 60_000 })));
    await expect(run(store, { now })).rejects.toThrow("drain_locked");
    await expect(run(store, { now, dryRun: true })).rejects.toThrow("drain_locked");
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);

    await store.put(objectKey(drainLockKey(ROOT)), enc.encode(JSON.stringify({ v: 1, kind: "buffered-drain-lock", holder: "drain-other", expiresAtMs: T - 1 })));
    const summary = await run(store, { now, lockHolder: "drain-test" });
    expect(summary.objects.drained).toBe(1);
    expect(await readJson(store, drainLockKey(ROOT))).toEqual({ v: 1, kind: "buffered-drain-lock", holder: "drain-test", expiresAtMs: 0, token: 1 });

    // A lock this code cannot read is treated as held.
    await store.put(objectKey(drainLockKey(ROOT)), enc.encode("{}"));
    await expect(run(store, { now })).rejects.toThrow("drain_lock_invalid");
  });

  test("a lease taken over by another holder fails its renewal (the run then stops)", async () => {
    const store = await scratch();
    let t = 1_000_000;
    const lease = await acquireDrainLock(store, ROOT, { holder: "drain-a", ttlMs: 60_000, now: () => t });
    await lease.renew();
    await expect(acquireDrainLock(store, ROOT, { holder: "drain-b", ttlMs: 60_000, now: () => t })).rejects.toThrow("drain_locked");
    t += 120_000; // expired: b takes it over
    await acquireDrainLock(store, ROOT, { holder: "drain-b", ttlMs: 60_000, now: () => t });
    await expect(lease.renew()).rejects.toThrow("drain_lock_lost");
    await lease.release(); // never clobbers b's lease
    expect((await readJson(store, drainLockKey(ROOT))).holder).toBe("drain-b");
  });

  test("finding 4: a partial write holds the user; a conflicting equal-revision batch never reaches canonical", async () => {
    const store = await scratch();
    // Batch A: sess-1 and sess-2. Batch B: the SAME record of sess-1 at the SAME revision, other content.
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 5, digest: "a".repeat(64) }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "A" });
    const b = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 5, digest: "b".repeat(64) }])], { batch: "B" });
    let failing = true;
    const flaky = new Proxy(store, {
      get(target, prop) {
        if (prop === "put") return async (...args: Parameters<ObjectStore["put"]>) => {
          // A non-retryable failure on sess-2's manifest: sess-1's chunk and manifest land, A's index entries do not.
          if (failing && String(args[0]).includes("/s=sess-2/") && String(args[0]).endsWith("manifest.json")) throw new ObjectStoreError("unauthorized", "fs", "injected");
          return target.put(...args);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
      },
    });
    const first = await run(flaky, { concurrency: 1 });
    expect(first.objects.write_failed).toBe(1);
    // Whatever B's position, it is not written in that run: user_stopped (after A) or drained-before-A is impossible
    // only if B sorts first; assert the invariant directly instead: canonical never holds B's digest for r1.
    const contents = async () => {
      const out: string[] = [];
      for (const k of await allKeys(store, `${ROOT}/${TENANT.orgId}/v1`)) {
        if (!k.includes("/raw/") || !k.includes("/s=sess-1/")) continue;
        for (const l of dec.decode(decompress(await readAll(await store.get(objectKey(k))))).split("\n").filter(Boolean)) out.push(JSON.parse(l).payload.text);
      }
      return out;
    };
    const hold = await readJson(store, drainHoldKey(ROOT, TENANT));
    expect(hold).toEqual({ v: 1, kind: "buffered-drain-hold", keys: [a] });
    failing = false;
    // Reruns: only A may write for the user until it has re-derived its index entries; then B is a conflict.
    for (let i = 0; i < 3; i++) await run(store, { concurrency: 1 });
    expect(await exists(store, a + DRAINED_SUFFIX)).toBe(true);
    expect(await exists(store, b + DRAINED_SUFFIX)).toBe(true); // marked: its only record was refused as a conflict
    expect(await contents()).toEqual(["t-r1-5"]); // one copy of r1 (A's), never B's
    expect(await storedEntry(store, "r1")).toEqual([5, "r1:r5", "a".repeat(64), SESS1, 0, 0]);
    expect((await readJson(store, drainHoldKey(ROOT, TENANT))).keys).toEqual([]);
  });

  test("finding 4: the lease carries a fencing token, and a stale lease stops in-flight batches before any write", async () => {
    const store = await scratch();
    let t = Date.parse("2030-01-05T00:00:00.000Z");
    const first = await acquireDrainLock(store, ROOT, { holder: "drain-a", ttlMs: 60_000, now: () => t });
    expect(first.token).toBe(1);
    await first.release();
    const second = await acquireDrainLock(store, ROOT, { holder: "drain-b", ttlMs: 60_000, now: () => t });
    expect(second.token).toBe(2);
    await second.release();
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    // The clock jumps past the lease TTL while the batch is between its ownership step and its write.
    const base = s3DrainLedger(store, ROOT);
    const ledger: DrainLedger = { ...base, async registerOwner(k, doc) { await base.registerOwner(k, doc); t += 10 * 60_000; } };
    const summary = await run(store, { ledger, lockTtlMs: 60_000, now: () => new Date(t) });
    expect(summary.objects.lock_lost).toBe(1);
    expect((await canonical(store)).size).toBe(0);
  });

  test("finding 7: the fence authority is re-read during the run (at most every 60 s)", async () => {
    const store = await scratch();
    for (let i = 0; i < 2; i++) await stage(store, [group("claude", `sess-${i}`, [{ id: `r${i}`, rev: 1 }])]);
    let t = Date.parse("2030-01-05T00:00:00.000Z");
    let groups = 0, pulled = false;
    // The operator pulls the org's authority after the first batch (seen by the store at once); 61 s later the run sees it.
    const watched = new Proxy(store, {
      get(target, prop) {
        if (prop === "getObject") return (k: Parameters<ObjectStore["getObject"]>[0]) =>
          pulled && String(k) === orgFenceAuthorityKey(ROOT, TENANT.orgId) ? Promise.reject(new ObjectStoreError("not_found", "fs", "pulled")) : target.getObject(k);
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
      },
    });
    const summary = await run(watched, {
      concurrency: 1, now: () => new Date(t),
      onProgress: () => {
        if (groups++ === 0) {
          pulled = true;
          t += 61_000;
        }
      },
    });
    expect(summary.objects.drained).toBe(1);
    expect(summary.objects.fence_authority_missing).toBe(1);
  });

  test("owner stubs carry the writer machine, so the re-arm sweeper can bind the drain's markers", async () => {
    const store = await scratch();
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    await run(store);
    const stub = await readJson(store, ownerKey("claude", "sess-1"));
    expect(stub.machineId).toMatch(/^srv_[0-9a-f]{32}$/);
  });

  test("finding 1: SQL's verdict not live (e.g. disabled then re-enabled before the export) is never written", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { live: false });
    const summary = await run(store);
    expect(summary.objects.stage_not_live).toBe(1);
    expect((await canonical(store)).size).toBe(0);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
  });

  test("finding 1: a disable then re-enable AFTER the export moves the session's epoch: the stage waits for a re-export", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    await refreshVerdicts(store); // the export ran
    const mirror = s3ControlMirror(store, ROOT);
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: false });
    await mirror.setSessionUploads({ tenant: TENANT, originId: ORIGIN, sessions: [["claude", "sess-1"]], enabled: true });
    expect((await currentFence(store)).deleted).toEqual({}); // re-enabled: nothing tombstoned, the epoch moved
    const summary = await run(store, {}, { refresh: false });
    expect(summary.objects.fence_moved_since_export).toBe(1);
    expect((await canonical(store)).size).toBe(0);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    // A reset begun after the export moves resets: the same.
    await mirror.resetUploadedSessions(TENANT, "req-after");
    expect((await run(store, {}, { refresh: false })).objects.fence_moved_since_export).toBe(1);
  });

  test("finding 2: only the lines SQL keeps are written (GC-kept groups, lazy body_accepted)", async () => {
    const store = await scratch();
    await stage(store, [group("claude", "sess-1", [{ id: "keep", rev: 1 }, { id: "drop", rev: 1 }]), group("claude", "sess-2", [{ id: "gone", rev: 1 }])],
      { keep: [[0], []] });
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.lines).toMatchObject({ candidate: 1, written: 1, not_kept: 2 });
    expect([...(await canonical(store)).entries()]).toEqual([["sess-1", ["keep@1"]]]);
    expect(await storedEntry(store, "drop")).toBeUndefined();
  });

  test("finding 6: the CLI resumes from a checkpoint that ends on an accepted-stage sidecar", async () => {
    const store = await scratch();
    for (let i = 0; i < 3; i++) await stage(store, [group("claude", `sess-${i}`, [{ id: `r${i}`, rev: 1 }])]);
    await refreshVerdicts(store);
    const dir = mkdtempSync(join(process.env.TMPDIR ?? ".", "drain-cli-"));
    cleanup.push(() => rmSync(dir, { recursive: true, force: true }));
    const checkpoint = join(dir, "checkpoint");
    const kms = `arn:aws:kms:us-east-1:000000000000:key/${"0".repeat(8)}-0000-0000-0000-${"0".repeat(12)}`;
    const args = (more: string[]) => parseDrainArgs(["--execute", "--bucket", "my-bucket", "--kms-key-arn", kms, "--org", TENANT.orgId, "--concurrency", "1", "--checkpoint-file", checkpoint, ...more]);
    const deps = { store, orgs: async () => [TENANT.orgId], stderr: () => {} };
    const first = await runDrain(args(["--max-objects", "1"]), deps);
    expect(first.objects.drained).toBe(1);
    const saved = readFileSyncFs(checkpoint, "utf8").trim();
    expect(saved.endsWith(ACCEPTED_SUFFIX)).toBe(true); // a group's last listed key is its sidecar
    expect(validResumeKey(saved)).toBe(true);
    // A group can also end on the export's create-only superseded record (finding 7).
    expect(validResumeKey(saved.slice(0, -ACCEPTED_SUFFIX.length) + ".superseded.json")).toBe(true);
    const rest = await runDrain(args([]), deps);
    expect(rest.objects).toMatchObject({ drained: 2, already_drained: 0 });
    expect((await canonical(store)).size).toBe(3);
  });

  test("CLI: the mode is never implied and --execute needs the KMS key", () => {
    expect(() => parseDrainArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--dry-run", "--execute"])).toThrow("exactly one of");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--dry-run", "--start-after", "96bulls/../x"])).toThrow("--start-after");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--dry-run", "--concurrency", "99"])).toThrow("--concurrency");
    expect(parseDrainArgs(["--dry-run", "--bucket", "my-bucket", "--org", "o1", "--org", "o2"])).toEqual({ mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: ["o1", "o2"], concurrency: 4 });
  });

  // ---- second review: finding 4 (hold intent, lock-time recheck) and the fencing margin ----

  /** A store proxy whose `put` runs `hook` first (which may throw, or await). */
  const hooked = (store: ObjectStore, hook: (key: string) => void | Promise<void>) => new Proxy(store, {
    get(target, prop) {
      if (prop === "put") return async (...args: Parameters<ObjectStore["put"]>) => { await hook(String(args[0])); return target.put(...args); };
      const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
      return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
    },
  });
  /** `record:payload text` of every canonical sess-1 line. */
  const sess1Texts = async (store: ObjectStore) => {
    const out: string[] = [];
    for (const k of await allKeys(store, `${ROOT}/${TENANT.orgId}/v1`)) {
      if (!k.includes("/raw/") || !k.includes("/s=sess-1/")) continue;
      for (const l of dec.decode(decompress(await readAll(await store.get(objectKey(k))))).split("\n").filter(Boolean)) {
        const r = JSON.parse(l);
        out.push(`${r.record_id}:${r.payload.text}`);
      }
    }
    return out;
  };

  test("finding 4 (same run): a batch queued behind a failing batch's shard lock re-checks the user and stops", async () => {
    const store = await scratch();
    // Both batches carry r1 at the same revision with other content (one shard), plus a sess-2 record.
    const one = (tag: string, d: string) => [group("claude", "sess-1", [{ id: "r1", rev: 5, digest: d.repeat(64) }]), group("claude", "sess-2", [{ id: `r-${tag}`, rev: 1 }])];
    await stage(store, one("x", "a"), { batch: "X" });
    await stage(store, one("y", "b"), { batch: "Y" });
    // The first sess-2 manifest put fails (non-retryable), but only once BOTH batches are past their start checks,
    // so the second is queued on the first's shard lock when the first fails.
    let journalReads = 0, failed = false;
    const base = s3DrainLedger(store, ROOT);
    const ledger: DrainLedger = { ...base, async readJournal(t) { journalReads++; return base.readJournal(t); } };
    const flaky = hooked(store, async (k) => {
      if (!failed && k.includes("/s=sess-2/") && k.endsWith("manifest.json")) {
        while (journalReads < 2) await new Promise((r) => setTimeout(r, 1));
        await new Promise((r) => setTimeout(r, 20));
        failed = true;
        throw new ObjectStoreError("unauthorized", "fs", "injected");
      }
    });
    const summary = await run(flaky, { ledger, concurrency: 2 });
    expect(summary.objects.write_failed).toBe(1);
    expect(summary.objects.user_stopped).toBe(1);
    expect(summary.objects.drained).toBe(0);
    // Only the failing batch's copy of r1 reached canonical.
    expect((await sess1Texts(store)).length).toBe(1);
  });

  test("finding 4 (crash): the hold is an intent written before the first canonical byte; the next run restores first", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 5, digest: "a".repeat(64) }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "A" });
    // SIGKILL right after sess-1's manifest lands: from then on nothing reaches storage (no partial() path, no hold write).
    let dead = false;
    const t0 = Date.parse("2030-01-05T00:00:00.000Z");
    const crashing = hooked(store, (k) => {
      if (dead) throw new ObjectStoreError("unauthorized", "fs", "process gone");
      if (k.includes("/s=sess-1/") && k.endsWith("manifest.json")) dead = true;
    });
    await run(crashing, { concurrency: 1, now: () => new Date(t0) }).catch(() => {});
    expect(dead).toBe(true);
    expect(await readJson(store, drainHoldKey(ROOT, TENANT))).toEqual({ v: 1, kind: "buffered-drain-hold", keys: [a] });
    // A conflicting batch arrives, sorting BEFORE A (the key's batch segment is sha256(batch digest)); the next run
    // (the dead lease has expired) reaches it first, holds it, and restores A.
    const segment = (label: string) => sha(sha(label));
    let label = "B";
    for (let i = 0; segment(label) >= segment("A"); i++) label = `B${i}`;
    const b = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 5, digest: "b".repeat(64) }])], { batch: label });
    expect(b < a).toBe(true);
    const later = () => new Date(t0 + 60 * 60_000);
    const second = await run(store, { concurrency: 1, now: later });
    expect(second.objects.user_hold).toBe(1);
    expect(second.objects.drained).toBe(1);
    expect(await exists(store, a + DRAINED_SUFFIX)).toBe(true);
    expect(await storedEntry(store, "r1")).toEqual([5, "r1:r5", "a".repeat(64), SESS1, 0, 0]);
    expect((await readJson(store, drainHoldKey(ROOT, TENANT))).keys).toEqual([]);
    await run(store, { concurrency: 1, now: later });
    expect(await exists(store, b + DRAINED_SUFFIX)).toBe(true); // marked: refused as a conflict
    expect(await sess1Texts(store)).toEqual(["r1:t-r1-5"]);
  });

  test("finding 4: a hold intent that cannot be written refuses the batch before any canonical byte", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const holdKey = drainHoldKey(ROOT, TENANT);
    const summary = await run(hooked(store, (k) => { if (k === holdKey) throw new ObjectStoreError("unauthorized", "fs", "injected"); }));
    expect(summary.objects.hold_failed).toBe(1);
    expect(summary.objects.drained).toBe(0);
    expect((await canonical(store)).size).toBe(0);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    expect(await storedEntry(store, "r1")).toBeUndefined();
    // Once the hold can be written, the batch drains and its intent is cleared.
    expect((await run(store)).objects.drained).toBe(1);
    expect((await readJson(store, holdKey)).keys).toEqual([]);
  });

  test("fencing: the lease counts as dead a third of its TTL before it expires", async () => {
    const store = await scratch();
    let t = Date.parse("2030-01-05T00:00:00.000Z");
    const lease = await acquireDrainLock(store, ROOT, { holder: "drain-a", ttlMs: 60_000, now: () => t });
    t += 39_999;
    expect(lease.alive()).toBe(true);
    t += 1;
    expect(lease.alive()).toBe(false);
    await lease.renew();
    expect(lease.alive()).toBe(true);
  });

  test("fencing: the lease is re-checked between groups inside one batch's write, and the write stops at once", async () => {
    const store = await scratch();
    let t = Date.parse("2030-01-05T00:00:00.000Z");
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }]), group("claude", "sess-3", [{ id: "r3", rev: 1 }])]);
    // The first session's manifest lands, then the clock passes the lease's safety margin (not its expiry).
    const slow = hooked(store, (k) => { if (k.endsWith("manifest.json") && k.includes("/s=sess-1/")) t += 45_000; });
    const summary = await run(slow, { lockTtlMs: 60_000, now: () => new Date(t) });
    expect(summary.objects.lock_lost).toBe(1);
    expect([...(await canonical(store)).keys()]).toEqual(["sess-1"]);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    // The intent survives, so the next run restores this batch before any other of the user.
    expect((await readJson(store, drainHoldKey(ROOT, TENANT))).keys).toEqual([key]);
  });

  // ---- lander review of d24acaaf: P1-1 (holds released on every terminal outcome), P1-2 (checkpoint) ----

  /** An earlier run's hold naming `keys` (as partial() leaves it after a failed write). */
  const putHold = (store: ObjectStore, keys: string[]) =>
    store.put(objectKey(drainHoldKey(ROOT, TENANT)), enc.encode(JSON.stringify({ v: 1, kind: "buffered-drain-hold", keys })));
  const holdKeys = async (store: ObjectStore) => (await readJson(store, drainHoldKey(ROOT, TENANT))).keys as string[];

  test("P1-1: a held batch that turns pre_reset releases its hold, then is marked; the user's later batch drains", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "A", received: "2030-01-02T00:00:00.000Z" });
    await putHold(store, [a]);
    await putFence(store, { reset: { requestId: "q1", atMs: Date.parse("2030-01-03T00:00:00.000Z"), result: 0 } });
    const c = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "C", received: "2030-01-04T00:00:00.000Z" });
    const first = await run(store, { concurrency: 1 });
    expect(first.objects.pre_reset).toBe(1);
    expect((await readJson(store, a + DRAINED_SUFFIX)).outcome).toBe("pre_reset");
    expect(await holdKeys(store)).toEqual([]);
    // C drained in the same run if it sorted after A, else on the next: never user_hold for good.
    if (!(await exists(store, c + DRAINED_SUFFIX))) expect((await run(store, { concurrency: 1 })).objects.drained).toBe(1);
    expect(await exists(store, c + DRAINED_SUFFIX)).toBe(true);
    expect((await canonical(store)).get("sess-2")).toEqual(["r2@1"]);
  });

  for (const [name, spoil] of [
    ["vanished", (store: ObjectStore, k: string) => store.delete(objectKey(k))],
    ["redacted", (store: ObjectStore, k: string) => store.put(objectKey(k), enc.encode(JSON.stringify({ v: 1, kind: "buffered-redaction" })))],
    ["corrupt", (store: ObjectStore, k: string) => store.put(objectKey(k), enc.encode("garbage"))],
    ["already_drained", (store: ObjectStore, k: string) => store.put(objectKey(k + DRAINED_SUFFIX), enc.encode(JSON.stringify({ v: 1, kind: "buffered-drained", outcome: "pre_reset" })))],
  ] as const) {
    test(`P1-1: a hold entry whose batch is ${name} holds nothing: the user's next batch prunes it and drains`, async () => {
      const store = await scratch();
      const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "A" });
      await putHold(store, [a]);
      await spoil(store, a);
      const c = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "C" });
      // The dry run sees through it without writing.
      const dry = await run(store, { dryRun: true });
      expect(dry.objects.would_drain).toBe(1);
      expect(dry.objects.user_hold).toBe(0);
      expect(await holdKeys(store)).toEqual([a]);
      const summary = await run(store, { concurrency: 1 });
      expect(summary.objects.drained).toBe(1);
      expect(summary.objects.user_hold).toBe(0);
      expect(summary.holds_pruned).toBe(1);
      expect(await exists(store, c + DRAINED_SUFFIX)).toBe(true);
      expect(await holdKeys(store)).toEqual([]);
    });
  }

  test("P1-1: a live held batch still holds the user (pruning never drops an entry that can drain)", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "A" });
    await putHold(store, [a]);
    const c = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "C" });
    const summary = await run(store, { concurrency: 1 });
    expect(summary.holds_pruned).toBe(0);
    // A drains (re-deriving its entries) and releases; C drains after it, in this run or the next.
    if (!(await exists(store, c + DRAINED_SUFFIX))) await run(store, { concurrency: 1 });
    expect(await exists(store, a + DRAINED_SUFFIX)).toBe(true);
    expect(await exists(store, c + DRAINED_SUFFIX)).toBe(true);
    expect(await holdKeys(store)).toEqual([]);
  });

  test("P1-1: an ambiguous replacement that was held releases its hold (it is never drained)", async () => {
    const store = await scratch();
    const claim = randomUUID();
    const a1 = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }, { id: "r2", rev: 1 }])], { batch: "same", claim });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "same", claim });
    await putHold(store, [a1]);
    const c = await stage(store, [group("claude", "sess-3", [{ id: "r3", rev: 1 }])], { batch: "C" });
    const first = await run(store, { concurrency: 1 });
    expect(first.objects.ambiguous_replacement).toBe(2);
    expect(await holdKeys(store)).toEqual([]);
    if (!(await exists(store, c + DRAINED_SUFFIX))) await run(store, { concurrency: 1 });
    expect(await exists(store, c + DRAINED_SUFFIX)).toBe(true);
  });

  test("P1-1: a hold release that fails leaves the pre_reset batch unmarked (hold_failed), for a rerun", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { received: "2030-01-02T00:00:00.000Z" });
    await putHold(store, [a]);
    await putFence(store, { reset: { requestId: "q1", atMs: Date.parse("2030-01-03T00:00:00.000Z"), result: 0 } });
    const holdKey = drainHoldKey(ROOT, TENANT);
    const failing = await run(hooked(store, (k) => { if (k === holdKey) throw new ObjectStoreError("unauthorized", "fs", "injected"); }));
    expect(failing.objects.hold_failed).toBe(1);
    expect(await exists(store, a + DRAINED_SUFFIX)).toBe(false);
    expect(await holdKeys(store)).toEqual([a]);
    expect((await run(store)).objects.pre_reset).toBe(1);
    expect(await holdKeys(store)).toEqual([]);
  });

  test("P1-2: retryable outcomes never advance the checkpoint; a rerun from it drains them", async () => {
    const store = await scratch();
    for (let i = 0; i < 2; i++) await stage(store, [group("claude", `sess-${i}`, [{ id: `r${i}`, rev: 1 }])]);
    await putJournal(store, { v: 3, seq: 2, pending: [{ id: "o:e:3", startMs: 1 }], ambiguous: [] });
    let checkpoint: string | undefined;
    const first = await run(store, { concurrency: 2, onCheckpoint: (k) => { checkpoint = k; } });
    expect(first.objects.privacy_control_open).toBe(2);
    expect(checkpoint).toBeUndefined();
    await putJournal(store, { v: 3, seq: 2, pending: [], ambiguous: [] });
    const second = await run(store, { ...(checkpoint ? { startAfter: checkpoint } : {}) });
    expect(second.objects.drained).toBe(2);
  });

  test("P1-2: the watermark stops before the first group left for a rerun, and passes terminal ones", async () => {
    const store = await scratch();
    const keys: string[] = [];
    for (let i = 0; i < 4; i++) keys.push(await stage(store, [group("claude", `sess-${i}`, [{ id: `r${i}`, rev: 1 }])]));
    const sorted = [...keys].sort();
    // The third object in key order cannot be read just now (a storage error: internal_error, retryable).
    const flaky = new Proxy(store, {
      get(target, prop) {
        if (prop === "getObject") return async (...args: Parameters<ObjectStore["getObject"]>) => {
          if (String(args[0]) === sorted[2]) throw new ObjectStoreError("unauthorized", "fs", "injected");
          return target.getObject(...args);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
      },
    });
    const marks: string[] = [];
    const first = await run(flaky, { concurrency: 1, onCheckpoint: (k) => { marks.push(k); } });
    expect(first.objects.drained).toBe(3);
    expect(first.objects.internal_error).toBe(1);
    // Never at or past the unreadable object, though the fourth drained.
    expect(marks.at(-1)! < sorted[2]!).toBe(true);
    const second = await run(store, { startAfter: marks.at(-1)! });
    expect(second.objects.drained).toBe(1);
    expect(second.objects.already_drained).toBe(1);
    expect((await canonical(store)).size).toBe(4);
  });

  test("P2: a dry run refuses a checkpoint (the CLI too), and max-objects never splits or overruns by a group", async () => {
    const store = await scratch();
    await expect(run(store, { dryRun: true, onCheckpoint: () => {} })).rejects.toThrow("drain_checkpoint_requires_execute");
    expect(() => parseDrainArgs(["--dry-run", "--bucket", "my-bucket", "--checkpoint-file", "/tmp/x"])).toThrow("--checkpoint-file");
    // Two objects in one batch group (a retry under another claim), then a single.
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "G" });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "G" });
    await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "H" });
    const summary = await run(store, { maxObjects: 2, concurrency: 1 });
    expect(summary.listed).toBeLessThanOrEqual(2);
    expect(summary.stopped).toBe("max_objects");
  });

  // ---- the same two P1s across 0005 step 0b's outcomes (#585) ----

  test("P1-1 (#585): a held stage that a later export superseded holds nothing: pruned, the user's next batch drains", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "A" });
    await putHold(store, [a]);
    await store.put(objectKey(a + SUPERSEDED_SUFFIX), enc.encode(JSON.stringify({ v: 1, kind: "buffered-superseded", stage_sha256: "0".repeat(64) })));
    const c = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "C" });
    const summary = await run(store, { concurrency: 1 });
    expect(summary.objects.stage_superseded).toBe(1);
    expect(summary.holds_pruned).toBe(1);
    expect(await exists(store, c + DRAINED_SUFFIX)).toBe(true);
    expect(await holdKeys(store)).toEqual([]);
  });

  test("P1-1 (#585): a held stage SQL's verdict refuses (stage_not_live) releases its hold", async () => {
    const store = await scratch();
    const a = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { batch: "A", live: false });
    await putHold(store, [a]);
    const c = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { batch: "C" });
    const first = await run(store, { concurrency: 1 });
    expect(first.objects.stage_not_live).toBe(1);
    expect(await holdKeys(store)).toEqual([]);
    if (!(await exists(store, c + DRAINED_SUFFIX))) await run(store, { concurrency: 1 });
    expect(await exists(store, c + DRAINED_SUFFIX)).toBe(true);
  });

  test("P1-2 (#585): an org whose export is missing never advances the checkpoint; after the export a rerun drains", async () => {
    const store = await scratch({ exportComplete: false });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    let checkpoint: string | undefined;
    const first = await run(store, { onCheckpoint: (k) => { checkpoint = k; } });
    expect(first.objects.stage_export_missing).toBe(1);
    expect(checkpoint).toBeUndefined();
    await markExportComplete(store);
    expect((await run(store)).objects.drained).toBe(1);
  });
});
