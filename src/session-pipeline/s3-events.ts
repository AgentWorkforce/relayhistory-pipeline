/**
 * The S3 half of the pipeline's StorageEventSource (adapter.ts plugs in R2;
 * this is "S3 via SQS"): one SQS message body from an S3 event notification
 * becomes zero or more normalized `ObjectLanded`s.
 *
 * S3 sends `{"Records":[...]}` (usually one record) and, once when a
 * notification is configured, `{"Event":"s3:TestEvent",...}`, which is
 * ignored. Keys arrive URL-encoded with `+` for a space. Puts are any
 * `ObjectCreated:*`; a delete is `ObjectRemoved:Delete` (a version deleted by
 * id, which is what the erasure job does) or `ObjectRemoved:DeleteMarkerCreated`
 * (a plain delete on a versioned bucket). Lifecycle expiry of noncurrent
 * versions is not notified and needs nothing: the current version is unchanged.
 *
 * The scheduled reconcile puts `{"reconcile":{"key","etag","version"}}` bodies
 * on the same queue, so its work takes the same path, retries and DLQ. A search
 * pass requeued by the consumer (busy, deferred, past the batch deadline) puts
 * `{"reconcile":{"key","requeued":true,"oldest_event_time"?}}` (`requeueBody`; the
 * backport lineage's ecb5d981). Both extra fields are telemetry only and never part
 * of the event a stage reads: `requeued` marks the consumer's own requeue
 * (`queue_origin`), and `oldest_event_time` carries the oldest upload event time
 * the requeued pass stands for (`s3_event_age_ms`). An older build reads only `key`,
 * `etag` and `version` and ignores both.
 *
 * `provider` is the Locker the consumer reads the manifest from (its
 * ManifestReader's provider), stamped on every event and from there on the
 * session_index row's storage_provider. It defaults to "s3", the AWS Lambda
 * behaviour; the self-host consumer passes the Locker's provider (gcs on a GCS
 * install), so the read API, which refuses a row whose provider is not its own
 * Locker's, can serve what the consumer indexed.
 */
import type { ObjectLanded, StorageProvider } from "@rhc/lib/pipeline/adapter.ts";

export const RECONCILE_BODY_KEY = "reconcile";
/** Optional field of a requeued reconcile body: the oldest upload event time (ISO 8601) it stands for. */
export const OLDEST_EVENT_TIME_FIELD = "oldest_event_time";
/** Field of a requeued reconcile body (`true`): the message is the consumer's own requeue. */
export const REQUEUED_FIELD = "requeued";
/**
 * Optional field of a requeued reconcile body (SEARCH_INDEX_DEADLINE_ISOLATION_ENABLED holds only): the large-read job
 * incarnation the hold waits for (source epoch, opaque fingerprint), the hold's sequence, and its id (a digest of the
 * scope, incarnation and sequence; search-deadline-isolation.ts holdId). Internal; never logged.
 */
export const LARGE_READ_HOLD_FIELD = "large_read_hold";
export interface LargeReadHoldBody { epoch: string; fingerprint: string; seq: number; id: string }
/** A carried hold, validated: an unrecognised shape is dropped (the message is then a plain requeue). */
export function parseLargeReadHold(value: unknown): LargeReadHoldBody | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const v = value as Record<string, unknown>;
  if (typeof v.epoch !== "string" || !/^[1-9][0-9]{0,18}$/.test(v.epoch)) return undefined;
  if (typeof v.fingerprint !== "string" || !/^[0-9a-f]{64}$/.test(v.fingerprint)) return undefined;
  if (typeof v.seq !== "number" || !Number.isSafeInteger(v.seq) || v.seq < 1 || v.seq > 100_000) return undefined;
  if (typeof v.id !== "string" || !/^[0-9a-f]{64}$/.test(v.id)) return undefined;
  return { epoch: v.epoch, fingerprint: v.fingerprint, seq: v.seq, id: v.id };
}

/** Where an SQS message came from, for `queue_origin`: an S3 notification, this consumer's requeue, or a sweep body. */
export type QueueOrigin = "s3" | "requeue" | "sweep";
/** FIFO revision events (revision-events.ts): `{"revision":{"key","etag","version","revision","committed_at","initial","deleted","source"}}`. */
export const REVISION_BODY_KEY = "revision";

/** What a FIFO revision event adds to the landed event: metadata for coalescing and latency, never content. */
export interface RevisionMeta {
  source: "writer" | "forward" | "reconcile";
  revision?: number;
  committedAt?: string;
  initial?: boolean;
}

interface S3Record {
  eventSource?: unknown;
  eventName?: unknown;
  eventTime?: unknown;
  s3?: {
    bucket?: { name?: unknown };
    object?: { key?: unknown; size?: unknown; eTag?: unknown; versionId?: unknown; sequencer?: unknown };
  };
}

export interface LandedEvent extends ObjectLanded {
  /** S3's per-key ordering token (hex, compare by length then value). */
  sequencer?: string;
  /** A requeued reconcile body's carried upload event time (ISO 8601): telemetry only, stripped before any stage. */
  oldestEventTime?: string;
  /** A deadline-isolation hold's carried expectation (LARGE_READ_HOLD_FIELD), stripped before any stage. */
  hold?: LargeReadHoldBody;
  /** Present only on a FIFO revision event. */
  revision?: RevisionMeta;
}

export type ParsedBody =
  | { kind: "events"; events: LandedEvent[]; origin: QueueOrigin }
  | { kind: "test" }
  | { kind: "invalid" };

/** S3 encodes keys like form values: `+` is a space, everything else percent-encoded. */
export function decodeS3Key(raw: string): string | null {
  try {
    return decodeURIComponent(raw.replace(/\+/g, " "));
  } catch {
    return null;
  }
}

function fromRecord(record: S3Record, provider: StorageProvider): LandedEvent | null {
  if (record.eventSource !== "aws:s3" || typeof record.eventName !== "string") return null;
  const name = record.eventName;
  const created = name.startsWith("ObjectCreated:");
  const deleted = name === "ObjectRemoved:Delete" || name === "ObjectRemoved:DeleteMarkerCreated";
  if (!created && !deleted) return null;
  const object = record.s3?.object;
  if (typeof object?.key !== "string" || !object.key) return null;
  const key = decodeS3Key(object.key);
  if (!key) return null;
  const bucket = record.s3?.bucket?.name;
  return {
    provider,
    key,
    ...(typeof bucket === "string" ? { bucket } : {}),
    ...(created && typeof object.eTag === "string" && object.eTag ? { etag: object.eTag.replace(/^"|"$/g, "") } : {}),
    ...(created && typeof object.versionId === "string" && object.versionId ? { version: object.versionId } : {}),
    ...(created && typeof object.size === "number" ? { size: object.size } : {}),
    ...(typeof record.eventTime === "string" ? { eventTime: record.eventTime } : {}),
    ...(typeof object.sequencer === "string" && object.sequencer ? { sequencer: object.sequencer } : {}),
    ...(deleted ? { deleted: true } : {}),
  };
}

/** Parse one SQS message body. Never throws. */
export function parseQueueBody(body: string, provider: StorageProvider = "s3"): ParsedBody {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return { kind: "invalid" };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return { kind: "invalid" };
  const v = value as Record<string, unknown>;
  if (v.Event === "s3:TestEvent") return { kind: "test" };
  const rev = v[REVISION_BODY_KEY] as Record<string, unknown> | undefined;
  if (rev && typeof rev === "object" && !Array.isArray(rev)) {
    if (typeof rev.key !== "string" || !rev.key) return { kind: "invalid" };
    const source = rev.source === "writer" || rev.source === "forward" || rev.source === "reconcile" ? rev.source : undefined;
    if (!source) return { kind: "invalid" };
    const meta: RevisionMeta = {
      source,
      ...(typeof rev.revision === "number" && Number.isSafeInteger(rev.revision) && rev.revision > 0 ? { revision: rev.revision } : {}),
      ...(typeof rev.committed_at === "string" && Number.isFinite(Date.parse(rev.committed_at)) ? { committedAt: rev.committed_at } : {}),
      ...(typeof rev.initial === "boolean" ? { initial: rev.initial } : {}),
    };
    const deleted = rev.deleted === true;
    return {
      kind: "events",
      // queue_origin (T124 telemetry): a reconcile hint is a sweep; a writer or forwarded event stands for its S3 write.
      origin: source === "reconcile" ? "sweep" : "s3",
      events: [
        {
          provider,
          key: rev.key,
          ...(!deleted && typeof rev.etag === "string" && rev.etag ? { etag: rev.etag } : {}),
          ...(!deleted && typeof rev.version === "string" && rev.version ? { version: rev.version } : {}),
          ...(meta.committedAt ? { eventTime: meta.committedAt } : {}),
          ...(deleted ? { deleted: true } : {}),
          revision: meta,
        },
      ],
    };
  }
  const reconcile = v[RECONCILE_BODY_KEY] as
    | { key?: unknown; etag?: unknown; version?: unknown; [OLDEST_EVENT_TIME_FIELD]?: unknown; [REQUEUED_FIELD]?: unknown; [LARGE_READ_HOLD_FIELD]?: unknown }
    | undefined;
  if (reconcile && typeof reconcile === "object") {
    if (typeof reconcile.key !== "string" || !reconcile.key) return { kind: "invalid" };
    const carried = reconcile[OLDEST_EVENT_TIME_FIELD];
    const oldestEventTime = typeof carried === "string" && carried ? carried : undefined;
    const hold = parseLargeReadHold(reconcile[LARGE_READ_HOLD_FIELD]);
    return {
      kind: "events",
      events: [
        {
          provider,
          key: reconcile.key,
          ...(typeof reconcile.etag === "string" && reconcile.etag ? { etag: reconcile.etag } : {}),
          ...(typeof reconcile.version === "string" && reconcile.version ? { version: reconcile.version } : {}),
          ...(oldestEventTime ? { oldestEventTime } : {}),
          ...(hold ? { hold } : {}),
        },
      ],
      // A requeue written before the marker existed is still a requeue when it carries a time.
      origin: reconcile[REQUEUED_FIELD] === true || oldestEventTime ? "requeue" : "sweep",
    };
  }
  if (!Array.isArray(v.Records)) return { kind: "invalid" };
  const events: LandedEvent[] = [];
  for (const record of v.Records as S3Record[]) {
    const event = record && typeof record === "object" ? fromRecord(record, provider) : null;
    if (event) events.push(event);
  }
  return { kind: "events", events, origin: "s3" };
}

/** The body of a requeued search pass: the manifest key, the `requeued` marker, and the carried time when known. */
export function requeueBody(manifestKey: string, oldestEventTime?: string, hold?: LargeReadHoldBody): string {
  return JSON.stringify({
    [RECONCILE_BODY_KEY]: {
      key: manifestKey, [REQUEUED_FIELD]: true,
      ...(oldestEventTime ? { [OLDEST_EVENT_TIME_FIELD]: oldestEventTime } : {}),
      ...(hold ? { [LARGE_READ_HOLD_FIELD]: { epoch: hold.epoch, fingerprint: hold.fingerprint, seq: hold.seq, id: hold.id } } : {}),
    },
  });
}

/** S3 sequencers order events for one key: longer is later, then lexical. */
export function sequencerAfter(a: string | undefined, b: string | undefined): boolean {
  if (a === undefined) return false;
  if (b === undefined) return true;
  const len = Math.max(a.length, b.length);
  return a.padEnd(len, "0") > b.padEnd(len, "0");
}
