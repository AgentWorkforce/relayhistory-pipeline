/**
 * Per-session revision events on an SQS FIFO queue (SESSION_REVISION_QUEUE_URL; default off).
 * The design and rollout are in docs/handoffs/session-revision-fifo.md.
 *
 * Today every manifest PUT notifies a STANDARD queue, so revisions of one session land in different
 * concurrent batches and contend for the session's search lock (scope_busy requeues). On the FIFO
 * queue, the message group is the logical session scope. SQS hands one group to one consumer
 * batch at a time and in order, so a session is serialized without a lock wait. Arrivals during an
 * in-flight pass collect in the group and come together in the next batch, which collapses them
 * into one pass over the live manifest (consumer.ts). Nothing waits on a fixed delay: the queue
 * runs with DelaySeconds 0 and no batching window.
 *
 * Identity. The event is a hint: the consumer always re-reads the live manifest, so no event's own
 * content is ever published. It names exactly one physical write:
 *
 *   group = sha256("96bulls:revision-group" ␟ prefix ␟ ws ␟ harness ␟ session)  (prefix carries the org)
 *   dedup = sha256("96bulls:revision-dedup" ␟ manifest key ␟ S3 versionId ␟ ETag)
 *
 * The incarnation comes from the S3 versionId. The transcript bucket is versioned, so every PUT,
 * including a re-create after erase that reuses a revision number, gets a new versionId. The
 * writer reads it from the PUT ack, and the forwarder and reconcile read it from the S3 record or
 * listing, so the copies of one write share one ID and SQS keeps a single copy for its 5-minute
 * window. A different write never shares an ID. The revision number is not part of the ID
 * because it can repeat (erase, then re-upload).
 *
 * Producers, in order of latency:
 *   1. the writer (writer.ts `onManifestCommitted`), right after its manifest CAS commits;
 *   2. the S3-notification forwarder (forwardManifestEvents): the standard queue keeps receiving
 *      every manifest PUT/DELETE notification and, in SESSION_EVENTS_MODE=forward, only re-keys
 *      them onto the FIFO queue (no database, no manifest read). This is the durable outbox. A
 *      write whose writer-side enqueue failed still has its S3 notification, which the standard
 *      queue retries until it is forwarded. Erase deletes take the same route, so a delete
 *      enters the session's group in order;
 *   3. the scheduled reconcile (lambda.ts reconcileRecent), which re-sends any manifest whose
 *      pointer row is missing or stale: the net under a lost S3 notification.
 */
import { createHash } from "node:crypto";
import { parseSessionKey } from "../shared/vendor.ts";
import { errorFields, log } from "../shared/log.ts";
import { parseQueueBody, REVISION_BODY_KEY, type RevisionMeta } from "./s3-events.ts";

export { REVISION_BODY_KEY, type RevisionMeta };

/** One revision event as it travels on the FIFO queue. */
export interface RevisionEvent {
  key: string;
  etag?: string;
  version?: string;
  /** The manifest's revision number, when the producer knows it (the writer). Logged, never part of the identity. */
  revision?: number;
  /** When the manifest committed (writer: its clock at the CAS ack; forwarder: the S3 eventTime). */
  committedAt?: string;
  /** Revision 1 of a new manifest (the first upload); false for an append; absent when unknown. */
  initial?: boolean;
  deleted?: boolean;
  /** S3's per-key sequencer, when the producer saw the notification (forwarder). Part of a delete's identity only. */
  sequencer?: string;
  source: RevisionMeta["source"];
}

export interface RevisionMessage {
  Id: string;
  MessageBody: string;
  MessageGroupId: string;
  MessageDeduplicationId: string;
}

/**
 * sha256 over the parts encoded as a JSON array of strings: unambiguous for any key bytes (a key can contain
 * any character, including a separator), and stable across producers. The store namespace is implicit: one
 * transcript bucket per deployment, and the key carries the deployment root and org.
 */
const sha = (parts: readonly string[]) => createHash("sha256").update(JSON.stringify(parts)).digest("hex");

/** The logical session's group, from the manifest key alone, so every producer derives the same one. Undefined: not a session key. */
export function revisionGroupId(key: string): string | undefined {
  const loc = parseSessionKey(key);
  if (!loc) return undefined;
  return sha(["96bulls:revision-group", loc.prefix ?? "", loc.workspaceId, loc.harness, loc.sessionId]);
}

/**
 * One physical write's ID. A PUT is named by its versionId and ETag. A delete notification
 * carries neither, so it is named by its S3 sequencer (unique per event per key), falling back
 * to its event time. Erase, re-create, erase again inside one dedup window therefore yields two
 * distinct delete IDs, and a delete is never mistaken for the PUT it removes.
 */
export function revisionDedupId(event: Pick<RevisionEvent, "key" | "version" | "etag" | "deleted" | "sequencer" | "committedAt">): string {
  if (event.deleted) return sha(["96bulls:revision-dedup", event.key, "delete", event.sequencer ?? "", event.sequencer ? "" : event.committedAt ?? ""]);
  // Neither stamp (a store that returns none): the commit time keeps two writes apart.
  const unstamped = !event.version && !event.etag ? event.committedAt ?? "" : "";
  return sha(["96bulls:revision-dedup", event.key, "put", event.version ?? "", event.etag ?? "", unstamped]);
}

/** The SQS SendMessageBatch entry for one event. Undefined: the key is not a session manifest (nothing to serialize). */
export function revisionMessage(event: RevisionEvent, id = "0"): RevisionMessage | undefined {
  const loc = parseSessionKey(event.key);
  if (!loc || loc.rest !== "manifest.json") return undefined;
  const group = revisionGroupId(event.key)!;
  const body: Record<string, unknown> = {
    key: event.key,
    source: event.source,
    ...(event.etag ? { etag: event.etag } : {}),
    ...(event.version ? { version: event.version } : {}),
    ...(event.revision !== undefined ? { revision: event.revision } : {}),
    ...(event.committedAt ? { committed_at: event.committedAt } : {}),
    ...(event.initial !== undefined ? { initial: event.initial } : {}),
    ...(event.deleted ? { deleted: true } : {}),
  };
  return { Id: id, MessageBody: JSON.stringify({ [REVISION_BODY_KEY]: body }), MessageGroupId: group, MessageDeduplicationId: revisionDedupId(event) };
}

/** Sends up to 10 entries (one SQS SendMessageBatch); returns the Ids that failed. Throws only when the call itself failed. */
export type RevisionSender = (entries: readonly RevisionMessage[]) => Promise<{ failed: string[] }>;

export const SQS_BATCH_MAX = 10;

/**
 * The SQS sender over any signed JSON call (lambda: awsJsonCall with the execution role). A
 * per-entry failure is reported, not thrown, so the caller can fail exactly the inputs behind it.
 */
export function sqsRevisionSender(queueUrl: string, call: (target: string, body: Record<string, unknown>) => Promise<unknown>): RevisionSender {
  if (!/\.fifo$/.test(queueUrl)) throw new Error("revision queue must be a FIFO queue");
  return async (entries) => {
    if (entries.length === 0) return { failed: [] };
    if (entries.length > SQS_BATCH_MAX) throw new Error("revision batch over 10 entries");
    const out = (await call("AmazonSQS.SendMessageBatch", { QueueUrl: queueUrl, Entries: entries })) as { Failed?: { Id?: string }[] } | undefined;
    return { failed: (out?.Failed ?? []).map((f) => String(f.Id)) };
  };
}

/** Send events in batches of 10. Returns the input indexes that were not accepted. Never throws. */
export async function sendRevisions(sender: RevisionSender, events: readonly RevisionEvent[]): Promise<{ sent: number; failed: number[]; skipped: number }> {
  const failed: number[] = [];
  let sent = 0;
  let skipped = 0;
  const entries: { index: number; message: RevisionMessage }[] = [];
  events.forEach((event, index) => {
    const message = revisionMessage(event, String(index));
    if (message) entries.push({ index, message });
    else skipped += 1;
  });
  for (let i = 0; i < entries.length; i += SQS_BATCH_MAX) {
    const chunk = entries.slice(i, i + SQS_BATCH_MAX);
    try {
      const { failed: ids } = await sender(chunk.map((e) => e.message));
      const bad = new Set(ids);
      for (const e of chunk) {
        if (bad.has(e.message.Id)) failed.push(e.index);
        else sent += 1;
      }
    } catch (error) {
      log.warn("revision enqueue failed", { stage: "revision-events", count: chunk.length, ...errorFields(error) });
      for (const e of chunk) failed.push(e.index);
    }
  }
  return { sent, failed, skipped };
}

/** What the writer knows at its manifest commit (writer.ts WriteTarget.onManifestCommitted). */
export interface ManifestCommit {
  key: string;
  revision: number;
  etag?: string;
  version?: string;
  committedAt: string;
}

/**
 * The writer-side producer: fire after the manifest CAS acked. Never throws and never fails the
 * write. A lost enqueue is recovered by the forwarder from the write's own S3 notification (module
 * header, producer 2), so this is a latency fast path rather than the durable record.
 */
export function writerRevisionHook(sender: RevisionSender): (commits: readonly ManifestCommit[]) => Promise<void> {
  return async (commits) => {
    const { failed } = await sendRevisions(
      sender,
      commits.map((c) => ({ key: c.key, ...(c.etag ? { etag: c.etag } : {}), ...(c.version ? { version: c.version } : {}),
        revision: c.revision, committedAt: c.committedAt, initial: c.revision === 1, source: "writer" as const })),
    );
    // Metadata only: a count; the forwarder carries these writes.
    if (failed.length) log.warn("revision enqueue failed", { stage: "revision-events", count: failed.length, reason: "forwarder_recovers" });
  };
}

export interface ForwardRecord { messageId: string; body: string }

/**
 * SESSION_EVENTS_MODE=forward on the standard queue: S3 manifest notifications become FIFO revision
 * events. Returns which records were consumed, so their messages are acked, and which failed, which
 * stay on the standard queue for its retry and DLQ. Records that carry no manifest event (buffered
 * stage keys, raw chunks, invalid bodies) are returned in `passThrough` for the normal batch path,
 * which keeps its existing behaviour for them.
 */
export async function forwardManifestEvents(records: readonly ForwardRecord[], sender: RevisionSender): Promise<{ forwarded: number; failed: string[]; passThrough: ForwardRecord[] }> {
  const passThrough: ForwardRecord[] = [];
  const events: RevisionEvent[] = [];
  const owner: string[] = [];
  for (const record of records) {
    const parsed = parseQueueBody(record.body);
    if (parsed.kind !== "events" || parsed.events.length === 0) {
      if (parsed.kind === "test") continue;
      passThrough.push(record);
      continue;
    }
    const manifests = parsed.events.filter((e) => parseSessionKey(e.key)?.rest === "manifest.json");
    // A body with any non-manifest event goes through the normal path whole: it is never split.
    if (manifests.length !== parsed.events.length) {
      passThrough.push(record);
      continue;
    }
    for (const e of manifests) {
      events.push({ key: e.key, ...(e.etag ? { etag: e.etag } : {}), ...(e.version ? { version: e.version } : {}),
        ...(e.eventTime ? { committedAt: e.eventTime } : {}), ...(e.revision?.committedAt && !e.eventTime ? { committedAt: e.revision.committedAt } : {}),
        ...(e.deleted ? { deleted: true } : {}), ...(e.sequencer ? { sequencer: e.sequencer } : {}), source: e.revision ? e.revision.source : "forward" });
      owner.push(record.messageId);
    }
  }
  const { sent, failed } = await sendRevisions(sender, events);
  const failedIds = [...new Set(failed.map((i) => owner[i]!))];
  log.info("revision events forwarded", { stage: "revision-events", count: sent, failed: failed.length, skipped: passThrough.length });
  return { forwarded: sent, failed: failedIds, passThrough };
}

export interface RevisionEventsConfig {
  queueUrl: string;
  /** direct (default): the standard queue is consumed as today. forward: its manifest events are re-keyed onto the FIFO queue. */
  mode: "direct" | "forward";
  busyRetrySeconds: number;
}

const FIFO_URL = /^https:\/\/sqs\.[a-z0-9-]+\.amazonaws\.com\/\d{12}\/[A-Za-z0-9_-]{1,75}\.fifo$/;

/**
 * SESSION_REVISION_QUEUE_URL (unset: off, exactly the previous behaviour), SESSION_EVENTS_MODE
 * (direct | forward, default direct; forward needs the URL), SESSION_REVISION_BUSY_RETRY_SECONDS
 * (1-60, default 5). A malformed value refuses init: a typo must not silently stop forwarding.
 */
export function revisionEventsFromEnv(env: Readonly<Record<string, string | undefined>>): RevisionEventsConfig | undefined {
  const url = env.SESSION_REVISION_QUEUE_URL;
  const mode = env.SESSION_EVENTS_MODE ?? "direct";
  if (mode !== "direct" && mode !== "forward") throw new Error("session pipeline: SESSION_EVENTS_MODE must be direct or forward");
  if (url === undefined || url === "") {
    if (mode === "forward") throw new Error("session pipeline: SESSION_EVENTS_MODE=forward needs SESSION_REVISION_QUEUE_URL");
    return undefined;
  }
  if (!FIFO_URL.test(url)) throw new Error("session pipeline: SESSION_REVISION_QUEUE_URL must be an SQS FIFO queue URL");
  const rawRetry = env.SESSION_REVISION_BUSY_RETRY_SECONDS;
  const busyRetrySeconds = rawRetry === undefined ? 5 : Number(rawRetry);
  if (!Number.isInteger(busyRetrySeconds) || busyRetrySeconds < 1 || busyRetrySeconds > 60)
    throw new Error("session pipeline: SESSION_REVISION_BUSY_RETRY_SECONDS must be an integer 1-60");
  return { queueUrl: url, mode, busyRetrySeconds };
}
