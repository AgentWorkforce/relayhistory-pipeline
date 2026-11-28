/**
 * The consumer in FIFO ordered mode (consumer.ts `ordered`, revision-dispatch.ts) on the real pointer stage:
 * PGlite with every migration, the fs object store and the Lambda's scope-lock shape (pointer-lock-harness.ts).
 *
 * Mocked boundaries (named in the review receipt):
 *   - SQS: records are built as the Lambda ESM delivers them (eventSourceARN *.fifo, MessageGroupId,
 *     SentTimestamp, ApproximateReceiveCount); ChangeMessageVisibility is a recorder.
 *   - The search stage: `searchScope` (the consumer's test seam) is a model index. Each pass replaces
 *     the scope's searchable markers with the record ids of the scope's LIVE pointer rows (session_index,
 *     deleted_at IS NULL) read from their current manifests, which is the contract indexSearchScope keeps.
 *     It can be made busy, and it counts passes.
 *   - The incarnation stamp: the fs store has no versionIds, so its ETag (inode, mtime, size) names
 *     the write here. In production the bucket is versioned and the S3 versionId comes first
 *     (revision-events.ts revisionDedupId; consumer.ts checkRevisions).
 * Synthetic, secret-free data only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, parseSessionKey, rawChunkKey, readAll, type ManifestV1 } from "../../shared/vendor.ts";
import type { SearchIndexResult } from "../search-index.ts";
import type { SessionScope } from "../pointer.ts";
import { dispatchSqsBatch, type RevisionWiring, type SqsRecord } from "../revision-dispatch.ts";
import { revisionMessage, type RevisionEvent } from "../revision-events.ts";
import { orderedRetrySeconds, type ConsumerDeps } from "../consumer.ts";
import { loc, pointerRig } from "./pointer-lock-harness.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });

const T0 = Date.parse("2026-11-02T00:00:00.000Z");
const ARN = "arn:aws:sqs:us-east-1:000000000000:synthetic-revisions.fifo";

/** Capture the JSON log lines one act writes. */
async function logged<T>(act: () => Promise<T>): Promise<{ result: T; lines: Record<string, unknown>[] }> {
  const lines: Record<string, unknown>[] = [];
  const out = process.stdout.write, err = process.stderr.write;
  const tap = (w: typeof process.stdout.write, s: NodeJS.WriteStream) => ((chunk: unknown, ...rest: unknown[]) => {
    for (const l of String(chunk).split("\n")) if (l.startsWith("{")) try { lines.push(JSON.parse(l)); } catch {}
    return (w as (...a: unknown[]) => boolean).call(s, chunk, ...rest);
  }) as typeof process.stdout.write;
  process.stdout.write = tap(out, process.stdout);
  process.stderr.write = tap(err, process.stderr);
  try { return { result: await act(), lines }; } finally { process.stdout.write = out; process.stderr.write = err; }
}

async function rig() {
  const r = await pointerRig();
  cleanup.push(r.close);
  const index = new Map<string, string[]>();
  /** The raw.content_sha256 of each manifest the last pass indexed, per scope. */
  const digests = new Map<string, string[]>();
  const passes: string[] = [];
  let busy = 0;
  /** Set by a test: runs inside the next search pass, before it reads, once (park a pass). */
  const hold: { inPass?: () => Promise<void>; crash?: boolean } = {};
  const sid = (s: SessionScope) => [s.orgId, s.workspaceId, s.harness, s.sessionId].join("/");
  const searchScope = async (scope: SessionScope): Promise<SearchIndexResult> => {
    passes.push(sid(scope));
    if (hold.inPass) { const h = hold.inPass; hold.inPass = undefined; await h(); }
    if (hold.crash) { hold.crash = false; throw Object.assign(new Error("synthetic crash"), { code: "synthetic_crash" }); }
    if (busy > 0) { busy -= 1; return { manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, busy: true, reasons: ["scope_busy"] }; }
    const live = await r.pg.query<{ manifest_key: string }>(
      "SELECT manifest_key FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND deleted_at IS NULL",
      [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId]);
    const markers: string[] = [];
    const seen: string[] = [];
    for (const { manifest_key } of live.rows) {
      const l = parseSessionKey(manifest_key)!;
      let manifest: ManifestV1;
      try { manifest = JSON.parse(new TextDecoder().decode(await readAll(await r.store.get(objectKey(manifest_key))))) as ManifestV1; } catch { continue; }
      seen.push(manifest.raw.content_sha256);
      for (const c of manifest.raw.chunks) {
        const raw = zstdDecompressSync(await readAll(await r.store.get(rawChunkKey(l, c.seq, 0, c.sha256, "ndjson"))));
        for (const line of new TextDecoder().decode(raw).split("\n")) if (line) markers.push((JSON.parse(line) as { record_id: string }).record_id);
      }
    }
    index.set(sid(scope), markers.sort());
    digests.set(sid(scope), seen.sort());
    return { manifests: live.rows.length, rows: markers.length, inserted: markers.length, deleted: 0, complete: true, reasons: [] };
  };
  const visibility: { handles: string[]; seconds: number }[] = [];
  const now = () => T0 + 1_500;
  const wiring: RevisionWiring = {
    config: { queueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/synthetic-revisions.fifo", mode: "direct", busyRetrySeconds: 5 },
    sender: async () => ({ failed: [] }),
    changeVisibility: async (handles, seconds) => { visibility.push({ handles: [...handles], seconds }); },
    now,
  };
  const deps = (): ConsumerDeps => ({ ...r.deps(), search: {} as never, searchScope: searchScope as never });
  const etagOf = async (key: string) => (await r.store.head(objectKey(key)))!.version.eTag!.replace(/^"|"$/g, "");
  let n = 0;
  /** One FIFO record for a writer-side revision event, as the ESM delivers it. */
  const record = (event: RevisionEvent, sentAt = T0, receives = 1): SqsRecord => {
    const m = revisionMessage(event)!;
    n += 1;
    return { messageId: `m${n}`, receiptHandle: `rh${n}`, body: m.MessageBody, eventSourceARN: ARN,
      attributes: { MessageGroupId: m.MessageGroupId, MessageDeduplicationId: m.MessageDeduplicationId, SentTimestamp: String(sentAt), ApproximateReceiveCount: String(receives) } };
  };
  const writer = async (revision: number, marker?: string, l = loc, committedAt = new Date(T0).toISOString()): Promise<RevisionEvent> => {
    const key = await r.writeManifest(revision, l, marker);
    return { key, revision, etag: await etagOf(key), committedAt, initial: revision === 1, source: "writer" };
  };
  const run = (records: SqsRecord[]) => logged(() => dispatchSqsBatch(records, deps(), wiring));
  const liveDigest = async (key: string) =>
    (JSON.parse(new TextDecoder().decode(await readAll(await r.store.get(objectKey(key))))) as ManifestV1).raw.content_sha256;
  return { r, index, digests, liveDigest, passes, visibility, record, writer, run, hold, makeBusy: (k: number) => { busy = k; }, sid: sid({ orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" }) };
}

const named = (lines: Record<string, unknown>[], msg: string) => lines.filter((l) => l.msg === msg || l.message === msg);

describe("burst coalescing", () => {
  test("five revisions of one session in one FIFO batch: one pointer pass, one search pass, the newest content; four superseded, one searchable", async () => {
    const t = await rig();
    const events: RevisionEvent[] = [];
    for (let rev = 1; rev <= 5; rev++) events.push(await t.writer(rev, `r${rev}`, loc, new Date(T0 + rev * 100).toISOString()));
    const { result, lines } = await t.run(events.map((e, i) => t.record(e, T0 + i * 100)));
    expect(result).toEqual({ batchItemFailures: [] });
    expect(t.passes).toEqual([t.sid]);
    expect(t.index.get(t.sid)).toEqual(["r5"]);
    expect(named(lines, "pointer indexed").length).toBe(1);
    const sup = named(lines, "revision superseded");
    expect(sup.map((l) => l.revision)).toEqual([1, 2, 3, 4]);
    expect(sup.every((l) => l.superseded_by_revision === 5 && typeof l.group_lag_ms === "number")).toBe(true);
    const ok = named(lines, "revision searchable");
    expect(ok[0]!.incarnation_sha256).toBe(revisionMessage(events[4]!)!.MessageDeduplicationId);
    expect(sup[0]!.incarnation_sha256).toBe(revisionMessage(events[0]!)!.MessageDeduplicationId);
    expect(ok).toEqual([expect.objectContaining({ revision: 5, upload_kind: "append", event_source: "writer", commit_to_searchable_ms: 1_000, queue_wait_ms: 1_100, receive_count: 1 })]);
    const pass = named(lines, "search index reconciled")[0]!;
    expect(pass).toMatchObject({ coalesced: 5, enqueued_at_ms: T0, queue_wait_ms: 1_500, receive_count: 1 });
  });

  test("first upload and an append are measured separately", async () => {
    const t = await rig();
    const first = await t.writer(1, "r1", loc, new Date(T0).toISOString());
    const a = await t.run([t.record(first, T0)]);
    expect(named(a.lines, "revision searchable")).toEqual([expect.objectContaining({ upload_kind: "initial", revision: 1, commit_to_searchable_ms: 1_500 })]);
    const second = await t.writer(2, "r2", loc, new Date(T0 + 500).toISOString());
    const b = await t.run([t.record(second, T0 + 500)]);
    expect(named(b.lines, "revision searchable")).toEqual([expect.objectContaining({ upload_kind: "append", revision: 2, commit_to_searchable_ms: 1_000 })]);
  });

  test("two machines of one session (one group) coalesce into one search pass; another session runs its own", async () => {
    const t = await rig();
    const m2 = { ...loc, machineId: "srv_cc" };
    const other = { ...loc, sessionId: "sess_2" };
    const a = await t.writer(1, "a1");
    const b = await t.writer(1, "b1", m2);
    const c = await t.writer(1, "c1", other);
    const { result } = await t.run([t.record(a), t.record(b), t.record(c)]);
    expect(result).toEqual({ batchItemFailures: [] });
    expect(t.passes.sort()).toEqual(["org_1/ws_1/claude/sess_1", "org_1/ws_1/claude/sess_2"]);
  });
});

describe("concurrent revisions of one session: no requeue, no reorder", () => {
  test("busy: the group's messages fail with a short visibility, nothing is requeued, the other group is done; the redelivery coalesces with a later arrival", async () => {
    const t = await rig();
    const e1 = await t.writer(1, "r1");
    const other = await t.writer(1, "o1", { ...loc, sessionId: "sess_2" });
    t.makeBusy(1);
    const recs = [t.record(e1), t.record(other)];
    const first = await t.run(recs);
    expect(first.result.batchItemFailures).toEqual([{ itemIdentifier: recs[0]!.messageId }]);
    expect(t.r.requeued).toEqual([]);
    expect(t.visibility).toEqual([{ handles: [recs[0]!.receiptHandle!], seconds: 5 }]);
    expect(named(first.lines, "revision retry scheduled")).toEqual([expect.objectContaining({ reason: "scope busy", delay_seconds: 5 })]);
    // While it waited, revision 2 committed. SQS redelivers m1 first, then m2, in one batch.
    const e2 = await t.writer(2, "r2");
    const again = await t.run([{ ...recs[0]!, attributes: { ...recs[0]!.attributes!, ApproximateReceiveCount: "2" } }, t.record(e2)]);
    expect(again.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["r2"]);
    expect(named(again.lines, "revision superseded").map((l) => l.revision)).toEqual([1]);
    expect(named(again.lines, "search index reconciled")[0]).toMatchObject({ receive_count: 2, coalesced: 2 });
  });

  test("busy backs off per receive (5, 10, 20 ... capped at 300 s) so a long-held lock does not walk the group into the DLQ", async () => {
    const t = await rig();
    const e1 = await t.writer(1, "r1");
    t.makeBusy(3);
    for (const receives of [1, 2, 3]) await t.run([t.record(e1, T0, receives)]);
    expect(t.visibility.map((v) => v.seconds)).toEqual([5, 10, 20]);
    expect(orderedRetrySeconds(5, 9)).toBe(300);
    expect(orderedRetrySeconds(5, 1_000)).toBe(300);
  });

  test("FIFO partial-batch rule: a failure fails every LATER message of its group, never an earlier one or another group", async () => {
    const t = await rig();
    const e1 = await t.writer(1, "r1");
    const okOther = t.record(await t.writer(1, "o1", { ...loc, sessionId: "sess_2" }));
    const first = t.record(e1);
    const group = first.attributes!.MessageGroupId!;
    const poison: SqsRecord = { messageId: "poison", receiptHandle: "rhp", body: "not json", eventSourceARN: ARN, attributes: { MessageGroupId: group, SentTimestamp: String(T0) } };
    const after = t.record(await t.writer(2, "r2"));
    const { result } = await t.run([first, poison, after, okOther]);
    expect(result.batchItemFailures.map((f) => f.itemIdentifier).sort()).toEqual(["poison", after.messageId].sort());
    // The tripped follower gets the short visibility; the poison message does not (it is for the DLQ).
    expect(t.visibility).toEqual([{ handles: [after.receiptHandle!], seconds: 5 }]);
  });

  test("a delayed retry of an older revision never exposes it", async () => {
    const t = await rig();
    const e1 = await t.writer(1, "r1");
    const e2 = await t.writer(2, "r2");
    await t.run([t.record(e2)]);
    expect(t.index.get(t.sid)).toEqual(["r2"]);
    const passesBefore = t.passes.length;
    const late = await t.run([t.record(e1, T0, 4)]);
    expect(late.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["r2"]);
    // A batch of superseded revisions only: no search pass (it would repeat revision 2's), whatever skip-current says.
    expect(t.passes.length).toBe(passesBefore);
    expect(named(late.lines, "search index skipped")).toEqual([expect.objectContaining({ reason: "superseded_in_group", coalesced: 1 })]);
    expect(named(late.lines, "revision superseded")).toEqual([expect.objectContaining({ revision: 1, superseded_by_revision: 2 })]);
    expect(named(late.lines, "revision searchable")).toEqual([]);
    const row = await t.r.row();
    expect(row).toEqual([expect.objectContaining({ revision: "2", deleted_at: null })]);
  });
});

describe("trailing superseded batches (ceil(n/10) passes) skip the search pass", () => {
  test("a backlog over 10: batches of only superseded stamped revisions run no search pass; the live write's own batch does; unstamped events always run", async () => {
    const t = await rig();
    const events: RevisionEvent[] = [];
    for (let rev = 1; rev <= 12; rev++) events.push(await t.writer(rev, `r${rev}`));
    // 12 queued for the group: the ESM delivers 10, then 2. The live write (revision 12) is in the second batch.
    const first = await t.run(events.slice(0, 10).map((e) => t.record(e)));
    expect(first.result).toEqual({ batchItemFailures: [] });
    // Every message is superseded and revision 12's own message is still to come: no pass yet.
    expect(t.passes.length).toBe(0);
    expect(named(first.lines, "search index skipped")).toEqual([expect.objectContaining({ reason: "superseded_in_group", coalesced: 10 })]);
    // The second batch carries revision 11 (superseded) and 12 (live): one pass, the live content.
    const second = await t.run(events.slice(10).map((e) => t.record(e)));
    expect(t.passes.length).toBe(1);
    expect(t.index.get(t.sid)).toEqual(["r12"]);
    expect(named(second.lines, "revision searchable")).toEqual([expect.objectContaining({ revision: 12 })]);
    // A late redelivery of superseded-only stamped events: skipped.
    await t.run([t.record(events[2]!), t.record(events[5]!)]);
    expect(t.passes.length).toBe(1);
    // An unstamped event (a reconcile body without stamps) always runs.
    await t.run([t.record({ key: events[0]!.key, source: "reconcile" })]);
    expect(t.passes.length).toBe(2);
  });
});

describe("in-flight pass: park it, commit a newer revision, then crash or lose the ack", () => {
  test("crash: the parked pass fails, SQS redelivers in order with the newer arrival, the newest live state becomes searchable", async () => {
    const t = await rig();
    const e1 = await t.writer(1, "r1");
    let e2!: RevisionEvent;
    const m1 = t.record(e1);
    // Park pass 1 inside the search stage; while parked, revision 2 commits. Then pass 1 crashes.
    t.hold.inPass = async () => { e2 = await t.writer(2, "r2"); };
    t.hold.crash = true;
    const first = await t.run([m1]);
    expect(first.result.batchItemFailures).toEqual([{ itemIdentifier: m1.messageId }]);
    expect(t.index.get(t.sid)).toBeUndefined();
    // Redelivery: m1 again (receive 2), then revision 2's own message, in group order.
    const again = await t.run([{ ...m1, attributes: { ...m1.attributes!, ApproximateReceiveCount: "2" } }, t.record(e2)]);
    expect(again.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["r2"]);
    expect(named(again.lines, "revision superseded")).toEqual([expect.objectContaining({ revision: 1, superseded_by_revision: 2 })]);
    expect(named(again.lines, "revision searchable")).toEqual([expect.objectContaining({ revision: 2, upload_kind: "append" })]);
  });

  test("lost ack: the parked pass succeeds on what was live when it read, its delete is lost, the redelivery with the newer arrival converges on the newest", async () => {
    const t = await rig();
    const e1 = await t.writer(1, "r1");
    let e2!: RevisionEvent;
    const m1 = t.record(e1);
    // Pass 1 reads and indexes revision 1; revision 2 commits after that read.
    const first = await t.run([m1]);
    expect(first.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["r1"]);
    e2 = await t.writer(2, "r2");
    // The ack (DeleteMessage) was lost: SQS redelivers m1 after its visibility timeout, now behind nothing, with revision 2's message.
    const again = await t.run([{ ...m1, attributes: { ...m1.attributes!, ApproximateReceiveCount: "2" } }, t.record(e2)]);
    expect(again.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["r2"]);
    // Redelivered alone after that (a second lost ack): superseded-only, no pass, nothing older exposed.
    const passes = t.passes.length;
    await t.run([{ ...m1, attributes: { ...m1.attributes!, ApproximateReceiveCount: "3" } }]);
    expect(t.passes.length).toBe(passes);
    expect(t.index.get(t.sid)).toEqual(["r2"]);
  });
});

describe("old work paused AFTER its live read, manifest replaced, then resumed", () => {
  /**
   * What this design guarantees here is CONVERGENCE, not a commit-time refusal. The pointer upsert commits
   * what the old pass read (it is revision-fenced, not stamp-fenced). The replacement's own event is the
   * next message of the same FIFO group, so its pass re-reads and publishes the replacement. Any stale
   * window lasts at most that one pass. A commit-time stamp refusal would belong in the search
   * transaction (SEARCH_INDEX_PREREAD_ENABLED revalidates the source stamp under its locks); it is not
   * part of this diff.
   */
  test("the replacement's own event, next in the group, makes the searchable result match the replacement's content digest; the paused event is superseded", async () => {
    const t = await rig();
    const a = await t.writer(1, "a1");
    let b!: RevisionEvent;
    // Pause A's pass inside the pointer lock after its manifest read, just before its upsert commits; replace the manifest (revision 2).
    t.r.hooks.beforeUpsert = async () => { b = await t.writer(2, "b2"); };
    const old = await t.run([t.record(a)]);
    expect(old.result).toEqual({ batchItemFailures: [] });
    // The pointer row committed what A's read saw: revision 1 (not refused).
    expect(await t.r.row()).toEqual([expect.objectContaining({ revision: "1" })]);
    // B's event is next in the group: the result converges on B.
    const next = await t.run([t.record(b)]);
    expect(next.result).toEqual({ batchItemFailures: [] });
    expect(await t.r.row()).toEqual([expect.objectContaining({ revision: "2", deleted_at: null })]);
    expect(t.index.get(t.sid)).toEqual(["b2"]);
    expect(t.digests.get(t.sid)).toEqual([await t.liveDigest(b.key)]);
    expect(named(next.lines, "revision searchable")).toEqual([expect.objectContaining({ revision: 2 })]);
    // A delayed redelivery of A afterwards: superseded, no pass, digest unchanged.
    const passes = t.passes.length;
    const late = await t.run([t.record(a, T0, 2)]);
    expect(named(late.lines, "revision superseded")).toEqual([expect.objectContaining({ revision: 1, superseded_by_revision: 2 })]);
    expect(t.passes.length).toBe(passes);
    expect(t.digests.get(t.sid)).toEqual([await t.liveDigest(b.key)]);
  });
});

describe("T126 erase, then re-create with the same session and revision number", () => {
  /**
   * Finding (pre-existing on main, not this change): sessions.upsert_session_index applies a row only when
   * `excluded.revision > s.revision`, and a plain delete tombstones the row at its revision. A re-create that
   * reuses revision 1 is therefore "unchanged": the row stays tombstoned and the new content is not
   * searchable until the session's next revision (2). The same happens on the standard-queue path. The
   * first assertions pin that, so a pointer-fence fix flips them deliberately. This change neither causes
   * nor fixes it: its dedup ID admits the new event (asserted below), and the consumer re-reads what is live.
   */
  test("both incarnations are admitted (distinct dedup IDs, one group); delayed old work paused before its commit never republishes old content; the new incarnation is searchable from its next revision", async () => {
    const t = await rig();
    // Incarnation A: revision 1, marker "old".
    const a = await t.writer(1, "old");
    expect((await t.run([t.record(a)])).result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["old"]);
    // Erase: the manifest goes; its delete event enters the same group.
    await t.r.store.delete(objectKey(a.key));
    const erase = await t.run([t.record({ key: a.key, deleted: true, sequencer: "0B", committedAt: new Date(T0 + 10).toISOString(), source: "forward" })]);
    expect(erase.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual([]);
    // Incarnation B: the same key and revision number 1, different content, inside the dedup window.
    const b = await t.writer(1, "new");
    expect(b.etag).not.toBe(a.etag);
    const ma = revisionMessage(a)!, mb = revisionMessage(b)!;
    expect(ma.MessageGroupId).toBe(mb.MessageGroupId);
    expect(ma.MessageDeduplicationId).not.toBe(mb.MessageDeduplicationId);
    // A delayed retry of A's event is in flight, paused inside the pointer lock before its read and commit.
    let release!: () => void;
    const gate = new Promise<void>((ok) => (release = ok));
    let paused!: () => void;
    const reached = new Promise<void>((ok) => (paused = ok));
    t.r.hooks.atLock = async () => { paused(); await gate; };
    const delayed = t.run([t.record(a, T0, 3)]);
    await reached;
    release();
    const old = await delayed;
    expect(old.result).toEqual({ batchItemFailures: [] });
    expect(named(old.lines, "revision superseded")).toEqual([expect.objectContaining({ revision: 1, event_source: "writer" })]);
    expect(t.index.get(t.sid)).not.toContain("old");
    // B's own event is delivered and processed (not deduplicated away).
    const nb = await t.run([t.record(b)]);
    expect(nb.result).toEqual({ batchItemFailures: [] });
    expect(named(nb.lines, "pointer indexed")).toEqual([expect.objectContaining({ outcome: "unchanged", revision: 1 })]);
    // The pre-existing fence (see above): still tombstoned, nothing searchable, and never the old marker.
    expect(t.index.get(t.sid)).toEqual([]);
    expect(await t.r.row()).toEqual([expect.objectContaining({ revision: "1", deleted_at: expect.anything() })]);
    // B's next revision makes the new incarnation searchable; A's event once more is superseded and changes nothing.
    const b2 = await t.writer(2, "new");
    const nb2 = await t.run([t.record(b2), t.record(a, T0, 5)]);
    expect(nb2.result).toEqual({ batchItemFailures: [] });
    expect(t.index.get(t.sid)).toEqual(["new"]);
    expect(named(nb2.lines, "revision superseded")).toEqual([expect.objectContaining({ revision: 1, superseded_by_revision: 2 })]);
    expect(named(nb2.lines, "revision searchable")).toEqual([expect.objectContaining({ revision: 2, upload_kind: "append" })]);
    // The searchable result matches the re-created manifest's content digest, not incarnation A's.
    expect(t.digests.get(t.sid)).toEqual([await t.liveDigest(b2.key)]);
    expect(await t.r.row()).toEqual([expect.objectContaining({ revision: "2", deleted_at: null })]);
  });
});

describe("dispatch", () => {
  test("without the wiring every batch takes the previous path; a mixed FIFO/standard batch is refused whole", async () => {
    const t = await rig();
    const e = await t.writer(1, "r1");
    const fifoRec = t.record(e);
    const std: SqsRecord = { messageId: "s1", body: JSON.stringify({ reconcile: { key: e.key } }), eventSourceARN: "arn:aws:sqs:us-east-1:000000000000:synthetic-events" };
    const mixed = await logged(() => dispatchSqsBatch([fifoRec, std], { ...t.r.deps() }, { config: { queueUrl: "x.fifo", mode: "direct", busyRetrySeconds: 5 }, sender: async () => ({ failed: [] }), changeVisibility: async () => {} }));
    expect(mixed.result.batchItemFailures.length).toBe(2);
    const plain = await logged(() => dispatchSqsBatch([std], t.r.deps()));
    expect(plain.result).toEqual({ batchItemFailures: [] });
  });
});
