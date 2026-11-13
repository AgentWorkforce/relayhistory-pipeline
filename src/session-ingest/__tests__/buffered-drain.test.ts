import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage } from "../staging.ts";
import type { Group } from "../writer.ts";
import {
  DRAINED_SUFFIX,
  drainBufferedBatches,
  mergeRevisionRecords,
  readOnlyStore,
  rearmKey,
  revisionIndexKey,
  s3DrainLedger,
  uploadFenceKey,
  type DrainLedger,
  type DrainOptions,
  type UploadFence,
} from "../buffered-drain.ts";
import { parseDrainArgs } from "../../../scripts/buffered-drain.ts";
import { scratchStore } from "./support.ts";
import { journalKey as journalKeyFor } from "../async-admission.ts";

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

async function stage(store: ObjectStore, groups: Group[], over: { batch?: string; claim?: string; received?: string } = {}): Promise<string> {
  let ord = 0;
  const prepared = prepareBufferedStage(ROOT, {
    claim_token: over.claim ?? randomUUID(),
    batch_digest: sha(over.batch ?? randomUUID()),
    received_at: over.received ?? RECEIVED,
    org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, origin_id: ORIGIN, user_id: TENANT.userId,
  }, groups, groups.map((g) => g.lines.map(() => ord++)));
  await putBufferedStage(store, prepared);
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

const putFence = (store: ObjectStore, fence: UploadFence) => store.put(objectKey(uploadFenceKey(ROOT, TENANT)), enc.encode(JSON.stringify(fence)));
const run = (store: ObjectStore, over: Partial<DrainOptions> = {}) =>
  drainBufferedBatches({ store, keyRoot: ROOT, orgs: [TENANT.orgId], dryRun: false, decompress, sleep: async () => {}, ...over });

let cleanup: (() => void)[] = [];
afterEach(() => { cleanup.forEach((c) => c()); cleanup = []; });
/** A scratch store whose user has a backfilled, empty fence (0005 step 0), unless `fence: false`. */
async function scratch(opts: { fence?: boolean } = {}) {
  const s = scratchStore();
  cleanup.push(s.done);
  if (opts.fence !== false) await putFence(s.store, { v: 1, resets: 0, deleted: {} });
  return s.store;
}
const putJournal = (store: ObjectStore, journal: unknown) => store.put(objectKey(journalKeyFor(ROOT, TENANT)), enc.encode(JSON.stringify(journal)));

describe("buffered backlog drain (0005)", () => {
  test("drains a staged batch into the canonical layout, the revision index and a drained marker", async () => {
    const store = await scratch();
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }, { id: "r2", rev: 2 }]), group("claude", "sess-2", [{ id: "r3", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.sessions_written).toBe(2);
    expect(summary.lines).toEqual({ candidate: 3, written: 3, fenced: 0, stale: 0, already_stored: 0, revision_conflict: 0 });
    const lines = await canonical(store);
    expect(lines.get("sess-1")).toEqual(["r1@1", "r2@2"]);
    expect(lines.get("sess-2")).toEqual(["r3@1"]);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(true);
    const index = await readJson(store, revisionIndexKey(ROOT, TENANT, ORIGIN, "claude", "sess-1"));
    expect(index).toEqual({ v: 1, records: { r1: [1, "r1:r1", sha("r1:1")], r2: [2, "r2:r2", sha("r2:2")] } });
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
    await store.delete(objectKey(revisionIndexKey(ROOT, TENANT, ORIGIN, "claude", "sess-1")));
    const resumed = await run(store);
    expect(resumed.objects.drained).toBe(1);
    expect(resumed.lines.written).toBe(1);
    expect((await canonical(store)).get("sess-1")).toEqual(["r1@1"]);
  });

  test("a fenced (deleted) session is dropped; the rest of the batch lands", async () => {
    const store = await scratch();
    await putFence(store, { v: 1, resets: 0, deleted: { [ORIGIN]: [["claude", "sess-2"]], "other-origin": [["claude", "sess-1"]] } });
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }, { id: "r3", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.lines.fenced).toBe(2);
    expect(summary.lines.written).toBe(1);
    const lines = await canonical(store);
    expect(lines.get("sess-1")).toEqual(["r1@1"]);
    expect(lines.has("sess-2")).toBe(false);
    expect(await exists(store, revisionIndexKey(ROOT, TENANT, ORIGIN, "claude", "sess-2"))).toBe(false);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(true);
    // An earlier partial run may have written the deleted session: it is re-armed for the eraser.
    expect(summary.rearmed).toBe(1);
    expect(await exists(store, rearmKey(ROOT, TENANT, "claude", "sess-2"))).toBe(true);
    expect(await exists(store, rearmKey(ROOT, TENANT, "claude", "sess-1"))).toBe(false);
  });

  test("equal revision with another digest is never written; stale and already-stored are skipped", async () => {
    const store = await scratch();
    await store.put(objectKey(revisionIndexKey(ROOT, TENANT, ORIGIN, "claude", "sess-1")), enc.encode(JSON.stringify({
      v: 1, records: { conflict: [3, "x", "f".repeat(64)], stale: [5, "y", sha("stale:4")], same: [2, "same:r2", sha("same:2")] },
    })));
    await stage(store, [group("claude", "sess-1", [{ id: "conflict", rev: 3 }, { id: "stale", rev: 4 }, { id: "same", rev: 2 }, { id: "fresh", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.drained).toBe(1);
    expect(summary.lines).toEqual({ candidate: 4, written: 1, fenced: 0, stale: 1, already_stored: 1, revision_conflict: 1 });
    expect((await canonical(store)).get("sess-1")).toEqual(["fresh@1"]);
    const index = await readJson(store, revisionIndexKey(ROOT, TENANT, ORIGIN, "claude", "sess-1"));
    expect(index.records.conflict).toEqual([3, "x", "f".repeat(64)]); // the stored digest is kept
    expect(index.records.fresh).toEqual([1, "fresh:r1", sha("fresh:1")]);
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
    await putFence(store, { v: 1, resets: 0, deleted: { [ORIGIN]: [["claude", "sess-2"]] } });
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    await store.put(objectKey(`${ROOT}/${TENANT.orgId}/buffered-batches/v1/ws=ws-a/m=srv_${"0".repeat(32)}/b=${"1".repeat(32)}-${"2".repeat(16)}-${"3".repeat(32)}.staged.ndjson.zst`), enc.encode("not zstd"));
    const before = await allKeys(store);
    const summary = await run(store, { dryRun: true });
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
    await store.put(objectKey(`${dir}/b=${"1".repeat(32)}-${"2".repeat(16)}-${"3".repeat(32)}.staged.ndjson.zst`), enc.encode("garbage"));
    // A valid object copied to a key that does not name its bytes.
    const bytes = await readAll(await store.get(objectKey(good)));
    await store.put(objectKey(`${dir}/b=${"4".repeat(32)}-${"5".repeat(16)}-${"6".repeat(32)}.staged.ndjson.zst`), bytes);
    // A buffered-GC redaction barrier.
    await store.put(objectKey(`${dir}/b=${"7".repeat(32)}-${"8".repeat(16)}-${"9".repeat(32)}.staged.ndjson.zst`), enc.encode(JSON.stringify({ v: 1, kind: "buffered-redaction" })));
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
    await store.put(objectKey(uploadFenceKey(ROOT, TENANT)), enc.encode("{\"v\":1,\"resets\":\"x\"}"));
    const key = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])]);
    const summary = await run(store);
    expect(summary.objects.fence_unreadable).toBe(1);
    expect(await exists(store, key + DRAINED_SUFFIX)).toBe(false);
    expect((await canonical(store)).size).toBe(0);
  });

  test("resets: undeterminable without lastResetAtMs; pre-reset batches dropped with it", async () => {
    const store = await scratch();
    const early = await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }])], { received: "2030-01-02T00:00:00.000Z" });
    const late = await stage(store, [group("claude", "sess-2", [{ id: "r2", rev: 1 }])], { received: "2030-01-04T00:00:00.000Z" });
    await putFence(store, { v: 1, resets: 1, deleted: {} });
    const blind = await run(store);
    expect(blind.objects.reset_undeterminable).toBe(2);
    expect((await canonical(store)).size).toBe(0);

    await putFence(store, { v: 1, resets: 1, deleted: {}, lastResetAtMs: Date.parse("2030-01-03T00:00:00.000Z") });
    const summary = await run(store);
    expect(summary.objects.pre_reset).toBe(1);
    expect(summary.objects.drained).toBe(1);
    const lines = await canonical(store);
    expect(lines.has("sess-1")).toBe(false);
    expect(lines.get("sess-2")).toEqual(["r2@1"]);
    expect((await readJson(store, early + DRAINED_SUFFIX)).outcome).toBe("pre_reset");
    expect((await readJson(store, late + DRAINED_SUFFIX)).outcome).toBe("drained");
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
    let reads = 0;
    const ledger: DrainLedger = {
      ...base,
      async readFence(t) {
        return reads++ === 0 ? { v: 1, resets: 0, deleted: {} } : { v: 1, resets: 0, deleted: { [ORIGIN]: [["claude", "sess-1"]] } };
      },
    };
    await stage(store, [group("claude", "sess-1", [{ id: "r1", rev: 1 }]), group("claude", "sess-2", [{ id: "r2", rev: 1 }])]);
    const summary = await run(store, { ledger });
    expect(summary.objects.drained).toBe(1);
    expect(summary.rearmed).toBe(1);
    const rearm = await readJson(store, rearmKey(ROOT, TENANT, "claude", "sess-1"));
    expect(rearm).toMatchObject({ v: 1, originId: ORIGIN, source: "claude", sessionId: "sess-1" });
    expect(await exists(store, rearmKey(ROOT, TENANT, "claude", "sess-2"))).toBe(false);
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
    expect(await exists(store, rearmKey(ROOT, TENANT, "claude", "sess-1"))).toBe(true);
    expect(await exists(store, rearmKey(ROOT, TENANT, "claude", "sess-2"))).toBe(true);
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

  test("the revision merge is commutative: max wins, an equal revision keeps the stored entry", () => {
    const a = { r: [2, "a", "1".repeat(64)] as [number, string, string] };
    const b = { r: [2, "b", "2".repeat(64)] as [number, string, string] };
    expect(mergeRevisionRecords(a, b)).toEqual({ records: a, changed: false });
    expect(mergeRevisionRecords(a, { r: [3, "c", "3".repeat(64)] }).records.r[0]).toBe(3);
  });

  test("CLI: the mode is never implied and --execute needs the KMS key", () => {
    expect(() => parseDrainArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--dry-run", "--execute"])).toThrow("exactly one of");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--dry-run", "--start-after", "96bulls/../x"])).toThrow("--start-after");
    expect(() => parseDrainArgs(["--bucket", "my-bucket", "--dry-run", "--concurrency", "99"])).toThrow("--concurrency");
    expect(parseDrainArgs(["--dry-run", "--bucket", "my-bucket", "--org", "o1", "--org", "o2"])).toEqual({ mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: ["o1", "o2"], concurrency: 4 });
  });
});
