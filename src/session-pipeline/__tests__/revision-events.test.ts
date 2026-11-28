/**
 * Per-session FIFO revision events (revision-events.ts): the producer-side group and dedup derivation, the
 * forwarder (the durable outbox for a lost writer enqueue), the writer hook, the reconcile sender and the env
 * grammar. The SQS boundary is a recorded fake sender that applies FIFO content-dedup over one window, so
 * "deduplicated" here means what SQS would do with these exact MessageDeduplicationIds. Synthetic data only.
 */
import { describe, expect, test } from "bun:test";
import { forwardManifestEvents, revisionDedupId, revisionEventsFromEnv, revisionGroupId, revisionMessage, sendRevisions, sqsRevisionSender, writerRevisionHook,
  type RevisionMessage, type RevisionSender } from "../revision-events.ts";
import { parseQueueBody } from "../s3-events.ts";
import { sendReconcileRevision } from "../lambda.ts";

const KEY = "96bulls/org_1/v1/ws=ws_1/m=srv_bb/h=claude/d=2026/11/01/s=sess_1/manifest.json";
const OTHER_MACHINE = "96bulls/org_1/v1/ws=ws_1/m=srv_cc/h=claude/d=2026/11/01/s=sess_1/manifest.json";
const OTHER_SESSION = "96bulls/org_1/v1/ws=ws_1/m=srv_bb/h=claude/d=2026/11/01/s=sess_2/manifest.json";
const OTHER_ORG = "96bulls/org_2/v1/ws=ws_1/m=srv_bb/h=claude/d=2026/11/01/s=sess_1/manifest.json";
const CHUNK = "96bulls/org_1/v1/ws=ws_1/m=srv_bb/h=claude/d=2026/11/01/s=sess_1/raw/0000000000-0-abcdef12.ndjson.zst";

/** An SQS FIFO queue over one dedup window: accepted messages in order; a repeated MessageDeduplicationId is dropped. */
function fifo(opts: { failIds?: (m: RevisionMessage) => boolean; throwOnce?: boolean } = {}) {
  const accepted: RevisionMessage[] = [];
  const seen = new Set<string>();
  let calls = 0;
  let threw = false;
  const sender: RevisionSender = async (entries) => {
    calls += 1;
    if (opts.throwOnce && !threw) { threw = true; throw Object.assign(new Error("synthetic"), { code: "sqs_unavailable" }); }
    const failed: string[] = [];
    for (const e of entries) {
      if (opts.failIds?.(e)) { failed.push(e.Id); continue; }
      if (seen.has(e.MessageDeduplicationId)) continue;
      seen.add(e.MessageDeduplicationId);
      accepted.push(e);
    }
    return { failed };
  };
  return { sender, accepted, calls: () => calls };
}

const s3Body = (key: string, opts: { name?: string; eTag?: string; versionId?: string; sequencer?: string } = {}) =>
  JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: opts.name ?? "ObjectCreated:Put", eventTime: "2026-11-01T10:00:00.000Z",
    s3: { bucket: { name: "bucket" }, object: { key, ...(opts.eTag ? { eTag: opts.eTag } : {}), ...(opts.versionId ? { versionId: opts.versionId } : {}), sequencer: opts.sequencer ?? "0A" } } }] });

describe("group: the logical session scope", () => {
  test("every machine and date of one session shares a group; another session or org does not", () => {
    const g = revisionGroupId(KEY)!;
    expect(g).toMatch(/^[0-9a-f]{64}$/);
    expect(revisionGroupId(OTHER_MACHINE)).toBe(g);
    expect(revisionGroupId(KEY.replace("d=2026/11/01", "d=2026/10/31"))).toBe(g);
    expect(revisionGroupId(OTHER_SESSION)).not.toBe(g);
    expect(revisionGroupId(OTHER_ORG)).not.toBe(g);
    expect(revisionGroupId("not/a/session")).toBeUndefined();
  });
  test("only manifest keys become messages; ids never carry the key in clear", () => {
    expect(revisionMessage({ key: CHUNK, source: "writer" })).toBeUndefined();
    const m = revisionMessage({ key: KEY, version: "v1", etag: "e1", revision: 3, source: "writer" })!;
    expect(m.MessageGroupId).not.toContain("sess_1");
    expect(m.MessageDeduplicationId).not.toContain("sess_1");
    expect(m.MessageDeduplicationId).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe("T126 erase/re-create inside one dedup window", () => {
  test("same session and revision number, new incarnation: both admitted in order; an identical retry dedupes", async () => {
    const q = fifo();
    // Incarnation A: revision 1, versionId vA. Erased (delete marker). Incarnation B: revision 1 again, new content, versionId vB.
    const a = { key: KEY, revision: 1, version: "vA", etag: "eA", committedAt: "2026-11-01T10:00:00.000Z", initial: true, source: "writer" as const };
    const erase = { key: KEY, deleted: true, sequencer: "0B", source: "forward" as const };
    const b = { key: KEY, revision: 1, version: "vB", etag: "eB", committedAt: "2026-11-01T10:01:00.000Z", initial: true, source: "writer" as const };
    await sendRevisions(q.sender, [a, erase, b]);
    // Retries of each, from the writer and from the forwarder, inside the window.
    await sendRevisions(q.sender, [{ ...b }, { key: KEY, version: "vB", etag: "eB", source: "forward" }, { ...a }]);
    expect(q.accepted.map((m) => JSON.parse(m.MessageBody).revision)).toEqual([
      expect.objectContaining({ version: "vA", revision: 1 }), expect.objectContaining({ deleted: true }), expect.objectContaining({ version: "vB", revision: 1 }),
    ]);
    expect(new Set(q.accepted.map((m) => m.MessageGroupId)).size).toBe(1);
    expect(revisionDedupId(a)).not.toBe(revisionDedupId(b));
  });
  test("a second erase of the re-created incarnation is not suppressed by the first erase", () => {
    const first = revisionDedupId({ key: KEY, deleted: true, sequencer: "0B" });
    expect(revisionDedupId({ key: KEY, deleted: true, sequencer: "0D" })).not.toBe(first);
    expect(revisionDedupId({ key: KEY, deleted: true, sequencer: "0B" })).toBe(first);
    // A delete never collides with a PUT of the same key.
    expect(first).not.toBe(revisionDedupId({ key: KEY }));
  });
  test("without a versionId the ETag names the write; with neither, the commit time does", () => {
    expect(revisionDedupId({ key: KEY, etag: "e1" })).not.toBe(revisionDedupId({ key: KEY, etag: "e2" }));
    expect(revisionDedupId({ key: KEY, committedAt: "2026-11-01T10:00:00.000Z" })).not.toBe(revisionDedupId({ key: KEY, committedAt: "2026-11-01T10:00:01.000Z" }));
  });
});

describe("the writer hook", () => {
  test("emits revision, versionId, ETag, commit time and initial/append", async () => {
    const q = fifo();
    await writerRevisionHook(q.sender)([{ key: KEY, revision: 1, version: "v1", etag: "e1", committedAt: "2026-11-01T10:00:00.000Z" },
      { key: KEY, revision: 2, version: "v2", etag: "e2", committedAt: "2026-11-01T10:00:02.000Z" }]);
    const bodies = q.accepted.map((m) => parseQueueBody(m.MessageBody));
    expect(bodies.map((b) => b.kind === "events" && b.events[0]!.revision)).toEqual([
      { source: "writer", revision: 1, committedAt: "2026-11-01T10:00:00.000Z", initial: true },
      { source: "writer", revision: 2, committedAt: "2026-11-01T10:00:02.000Z", initial: false },
    ]);
  });
  test("an SQS failure never throws into the write", async () => {
    const q = fifo({ throwOnce: true });
    await expect(writerRevisionHook(q.sender)([{ key: KEY, revision: 1, version: "v1", committedAt: "2026-11-01T10:00:00.000Z" }])).resolves.toBeUndefined();
    expect(q.accepted).toEqual([]);
  });
});

describe("enqueue-failure recovery: the forwarder is the outbox", () => {
  test("a commit whose writer enqueue failed reaches the FIFO queue from its S3 notification, deduped against a writer copy that did land", async () => {
    const q = fifo({ throwOnce: true });
    // The writer's enqueue for v1 fails (thrown); for v2 it lands.
    await writerRevisionHook(q.sender)([{ key: KEY, revision: 1, version: "v1", etag: "e1", committedAt: "2026-11-01T10:00:00.000Z" }]);
    await writerRevisionHook(q.sender)([{ key: KEY, revision: 2, version: "v2", etag: "e2", committedAt: "2026-11-01T10:00:02.000Z" }]);
    expect(q.accepted.length).toBe(1);
    // Both S3 notifications arrive on the standard queue and are forwarded.
    const out = await forwardManifestEvents([
      { messageId: "n1", body: s3Body(KEY, { eTag: "\"e1\"", versionId: "v1", sequencer: "01" }) },
      { messageId: "n2", body: s3Body(KEY, { eTag: "\"e2\"", versionId: "v2", sequencer: "02" }) },
    ], q.sender);
    expect(out).toEqual({ forwarded: 2, failed: [], passThrough: [] });
    // v2 deduplicated against the writer copy; v1 recovered.
    expect(q.accepted.map((m) => (JSON.parse(m.MessageBody).revision as { version: string }).version)).toEqual(["v2", "v1"]);
  });
  test("a forward that SQS rejects fails exactly its notification, which stays on the standard queue", async () => {
    const q = fifo({ failIds: (m) => (JSON.parse(m.MessageBody).revision as { version?: string }).version === "v2" });
    const out = await forwardManifestEvents([
      { messageId: "n1", body: s3Body(KEY, { eTag: "e1", versionId: "v1" }) },
      { messageId: "n2", body: s3Body(OTHER_SESSION, { eTag: "e2", versionId: "v2" }) },
    ], q.sender);
    expect(out.failed).toEqual(["n2"]);
    expect(out.forwarded).toBe(1);
  });
  test("a thrown SendMessageBatch fails every notification behind it", async () => {
    const q = fifo({ throwOnce: true });
    const out = await forwardManifestEvents([{ messageId: "n1", body: s3Body(KEY, { eTag: "e1" }) }], q.sender);
    expect(out.failed).toEqual(["n1"]);
  });
  test("deletes are forwarded into the session's group; non-manifest bodies pass through whole; test events vanish", async () => {
    const q = fifo();
    const out = await forwardManifestEvents([
      { messageId: "d1", body: s3Body(KEY, { name: "ObjectRemoved:DeleteMarkerCreated", sequencer: "03" }) },
      { messageId: "c1", body: s3Body(CHUNK, { eTag: "e" }) },
      { messageId: "bad", body: "not json" },
      { messageId: "t", body: JSON.stringify({ Event: "s3:TestEvent" }) },
    ], q.sender);
    expect(out.passThrough.map((r) => r.messageId)).toEqual(["c1", "bad"]);
    expect(q.accepted.length).toBe(1);
    expect(JSON.parse(q.accepted[0]!.MessageBody).revision).toMatchObject({ key: KEY, deleted: true, source: "forward" });
    expect(q.accepted[0]!.MessageGroupId).toBe(revisionGroupId(KEY)!);
  });
  test("over 10 events go out in batches of 10", async () => {
    const q = fifo();
    const events = Array.from({ length: 23 }, (_, i) => ({ key: KEY, version: `v${i}`, source: "forward" as const }));
    expect(await sendRevisions(q.sender, events)).toEqual({ sent: 23, failed: [], skipped: 0 });
    expect(q.calls()).toBe(3);
  });
  test("reconcile re-sends a committed manifest into the group (the net under a lost notification) and throws when SQS refuses", async () => {
    const q = fifo();
    await sendReconcileRevision(q.sender, JSON.stringify({ reconcile: { key: KEY, etag: "e9", version: "v9" } }));
    expect(JSON.parse(q.accepted[0]!.MessageBody).revision).toEqual({ key: KEY, source: "reconcile", etag: "e9", version: "v9" });
    // The same write already sent by the writer dedupes.
    await writerRevisionHook(q.sender)([{ key: KEY, revision: 9, version: "v9", etag: "e9", committedAt: "2026-11-01T10:00:00.000Z" }]);
    expect(q.accepted.length).toBe(1);
    const refusing = fifo({ failIds: () => true });
    await expect(sendReconcileRevision(refusing.sender, JSON.stringify({ reconcile: { key: KEY } }))).rejects.toThrow("reconcile revision enqueue failed");
  });
});

describe("parse", () => {
  test("a revision body is an event with its metadata; a delete drops stamps; malformed bodies are invalid", () => {
    expect(parseQueueBody(JSON.stringify({ revision: { key: KEY, source: "writer", version: "v1", etag: "e1", revision: 2, committed_at: "2026-11-01T10:00:00.000Z", initial: false } }))).toEqual({
      kind: "events", events: [{ provider: "s3", key: KEY, etag: "e1", version: "v1", eventTime: "2026-11-01T10:00:00.000Z",
        revision: { source: "writer", revision: 2, committedAt: "2026-11-01T10:00:00.000Z", initial: false } }],
    });
    const del = parseQueueBody(JSON.stringify({ revision: { key: KEY, source: "forward", deleted: true, version: "x" } }));
    expect(del.kind === "events" && del.events[0]).toEqual({ provider: "s3", key: KEY, deleted: true, revision: { source: "forward" } });
    expect(parseQueueBody(JSON.stringify({ revision: { key: KEY } })).kind).toBe("invalid");
    expect(parseQueueBody(JSON.stringify({ revision: { key: "", source: "writer" } })).kind).toBe("invalid");
    expect(parseQueueBody(JSON.stringify({ revision: { key: KEY, source: "writer", revision: -1, committed_at: "nope" } }))).toMatchObject({ events: [{ revision: { source: "writer" } }] });
  });
});

describe("env", () => {
  const URL = "https://sqs.us-east-1.amazonaws.com/000000000000/synthetic-revisions.fifo";
  test("unset is off; set is direct unless forward; bounds enforced", () => {
    expect(revisionEventsFromEnv({})).toBeUndefined();
    expect(revisionEventsFromEnv({ SESSION_REVISION_QUEUE_URL: URL })).toEqual({ queueUrl: URL, mode: "direct", busyRetrySeconds: 5 });
    expect(revisionEventsFromEnv({ SESSION_REVISION_QUEUE_URL: URL, SESSION_EVENTS_MODE: "forward", SESSION_REVISION_BUSY_RETRY_SECONDS: "2" }))
      .toEqual({ queueUrl: URL, mode: "forward", busyRetrySeconds: 2 });
    expect(() => revisionEventsFromEnv({ SESSION_EVENTS_MODE: "forward" })).toThrow("needs SESSION_REVISION_QUEUE_URL");
    expect(() => revisionEventsFromEnv({ SESSION_REVISION_QUEUE_URL: URL.replace(".fifo", "") })).toThrow("FIFO");
    expect(() => revisionEventsFromEnv({ SESSION_REVISION_QUEUE_URL: URL, SESSION_EVENTS_MODE: "Forward" })).toThrow("direct or forward");
    expect(() => revisionEventsFromEnv({ SESSION_REVISION_QUEUE_URL: URL, SESSION_REVISION_BUSY_RETRY_SECONDS: "0" })).toThrow("1-60");
    expect(() => sqsRevisionSender("https://sqs.us-east-1.amazonaws.com/000000000000/std", async () => ({}))).toThrow("FIFO");
  });
});
