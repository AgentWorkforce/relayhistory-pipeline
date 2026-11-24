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
 * `{"reconcile":{"key","requeued":true,"oldest_event_time"?}}` (`requeueBody`).
 * Both extra fields are telemetry only and never part of the event a stage reads:
 * `requeued` marks the message as the consumer's own requeue (`queue_origin`), and
 * the optional `oldest_event_time` carries the oldest upload event time the
 * requeued pass stands for (`s3_event_age_ms`). An older build reads only `key`,
 * `etag` and `version` and ignores both. A reconcile body without either field (the
 * sweep, or a requeue written by an older build) parses as before, origin `sweep`.
 *
 * A pass requeued for the opt-in batch deadline (consumer.ts, SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS)
 * also carries `batch_deadline_deferrals`: how often that scope has been put back for the
 * deadline. It is read only by the consumer's deadline rule, never by a stage, and only bodies
 * written by that rule carry it. A build that predates it reads `key`, `etag` and `version` only
 * (the parse below picks named fields and ignores any other), so it ignores the counter.
 */
import type { ObjectLanded } from "@rhc/lib/pipeline/adapter.ts";

export const RECONCILE_BODY_KEY = "reconcile";
/** Optional field of a requeued reconcile body: the oldest upload event time (ISO 8601) it stands for. */
export const OLDEST_EVENT_TIME_FIELD = "oldest_event_time";
/** Field of a requeued reconcile body (`true`): the message is the consumer's own requeue. */
export const REQUEUED_FIELD = "requeued";
/**
 * Optional field of a reconcile body requeued for the batch deadline: the scope's deferral count
 * (a non-negative integer). Absent, or anything but a non-negative safe integer: read as 0.
 */
export const BATCH_DEADLINE_DEFERRALS_FIELD = "batch_deadline_deferrals";

/**
 * Where an SQS message came from, for the `queue_origin` log field: an S3 event
 * notification, a search pass this consumer requeued, or a reconcile body from
 * the sweep (or any producer that marks neither).
 */
export type QueueOrigin = "s3" | "requeue" | "sweep";

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
  /**
   * A requeued reconcile body's carried upload event time (ISO 8601): telemetry only.
   * The consumer strips it before any stage sees the event.
   */
  oldestEventTime?: string;
  /**
   * A reconcile body's `batch_deadline_deferrals` when it is a positive safe integer; absent
   * otherwise (read as 0). The consumer strips it before any stage sees the event.
   */
  batchDeadlineDeferrals?: number;
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

function fromRecord(record: S3Record): LandedEvent | null {
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
    provider: "s3",
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
export function parseQueueBody(body: string): ParsedBody {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return { kind: "invalid" };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return { kind: "invalid" };
  const v = value as Record<string, unknown>;
  if (v.Event === "s3:TestEvent") return { kind: "test" };
  const reconcile = v[RECONCILE_BODY_KEY] as
    | { key?: unknown; etag?: unknown; version?: unknown; [OLDEST_EVENT_TIME_FIELD]?: unknown; [REQUEUED_FIELD]?: unknown; [BATCH_DEADLINE_DEFERRALS_FIELD]?: unknown }
    | undefined;
  if (reconcile && typeof reconcile === "object") {
    if (typeof reconcile.key !== "string" || !reconcile.key) return { kind: "invalid" };
    const carried = reconcile[OLDEST_EVENT_TIME_FIELD];
    const oldestEventTime = typeof carried === "string" && carried ? carried : undefined;
    const batchDeadlineDeferrals = deferralCount(reconcile[BATCH_DEADLINE_DEFERRALS_FIELD]);
    return {
      kind: "events",
      events: [
        {
          provider: "s3",
          key: reconcile.key,
          ...(typeof reconcile.etag === "string" && reconcile.etag ? { etag: reconcile.etag } : {}),
          ...(typeof reconcile.version === "string" && reconcile.version ? { version: reconcile.version } : {}),
          ...(oldestEventTime ? { oldestEventTime } : {}),
          ...(batchDeadlineDeferrals > 0 ? { batchDeadlineDeferrals } : {}),
        },
      ],
      // A requeue written before the marker existed is still a requeue when it carries a time.
      origin: reconcile[REQUEUED_FIELD] === true || oldestEventTime ? "requeue" : "sweep",
    };
  }
  if (!Array.isArray(v.Records)) return { kind: "invalid" };
  const events: LandedEvent[] = [];
  for (const record of v.Records as S3Record[]) {
    const event = record && typeof record === "object" ? fromRecord(record) : null;
    if (event) events.push(event);
  }
  return { kind: "events", events, origin: "s3" };
}

/** A carried deferral count: a non-negative safe integer as is; anything else (malformed, negative, fractional, a string) is 0. Never throws. */
export function deferralCount(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

/**
 * The body of a requeued search pass: the manifest key (all a stage reads), the
 * `requeued` marker, `oldest_event_time` when the carried time is known, and
 * `batch_deadline_deferrals` only when the batch-deadline rule passes one (every
 * other requeue's body is exactly as before).
 */
export function requeueBody(manifestKey: string, oldestEventTime?: string, batchDeadlineDeferrals?: number): string {
  return JSON.stringify({
    [RECONCILE_BODY_KEY]: {
      key: manifestKey,
      [REQUEUED_FIELD]: true,
      ...(oldestEventTime ? { [OLDEST_EVENT_TIME_FIELD]: oldestEventTime } : {}),
      ...(batchDeadlineDeferrals !== undefined ? { [BATCH_DEADLINE_DEFERRALS_FIELD]: batchDeadlineDeferrals } : {}),
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
