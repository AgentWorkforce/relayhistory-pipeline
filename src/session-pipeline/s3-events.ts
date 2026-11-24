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
 * on the same queue, so its work takes the same path, retries and DLQ.
 *
 * `provider` is the Locker the consumer reads the manifest from (its
 * ManifestReader's provider), stamped on every event and from there on the
 * session_index row's storage_provider. It defaults to "s3", the AWS Lambda
 * behaviour; the self-host consumer passes the Locker's provider (gcs on a GCS
 * install), so the read API, which refuses a row whose provider is not its own
 * Locker's, can serve what the consumer indexed.
 *
 * A pass requeued for the opt-in batch deadline (consumer.ts, SEARCH_INDEX_BATCH_DEADLINE_MARGIN_MS)
 * also carries `batch_deadline_deferrals`: how often that scope has been put back for the
 * deadline. Only the consumer's deadline rule reads it, never a stage, and only bodies written
 * by that rule carry it; every other body is exactly `{"reconcile":{"key"}}` as before. The parse
 * below picks named fields and ignores any other, so a build without the rule ignores the counter,
 * and a body carrying fields this build does not read (such as `oldest_event_time` or `requeued`)
 * parses as its key, etag and version.
 */
import type { ObjectLanded, StorageProvider } from "@rhc/lib/pipeline/adapter.ts";

export const RECONCILE_BODY_KEY = "reconcile";
/**
 * Optional field of a reconcile body requeued for the batch deadline: the scope's deferral count
 * (a non-negative integer). Absent, or anything but a non-negative safe integer: read as 0.
 */
export const BATCH_DEADLINE_DEFERRALS_FIELD = "batch_deadline_deferrals";

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
   * A reconcile body's `batch_deadline_deferrals` when it is a positive safe integer; absent
   * otherwise (read as 0). The consumer strips it before any stage sees the event.
   */
  batchDeadlineDeferrals?: number;
}

export type ParsedBody =
  | { kind: "events"; events: LandedEvent[] }
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
  const reconcile = v[RECONCILE_BODY_KEY] as
    | { key?: unknown; etag?: unknown; version?: unknown; [BATCH_DEADLINE_DEFERRALS_FIELD]?: unknown }
    | undefined;
  if (reconcile && typeof reconcile === "object") {
    if (typeof reconcile.key !== "string" || !reconcile.key) return { kind: "invalid" };
    const batchDeadlineDeferrals = deferralCount(reconcile[BATCH_DEADLINE_DEFERRALS_FIELD]);
    return {
      kind: "events",
      events: [
        {
          provider,
          key: reconcile.key,
          ...(typeof reconcile.etag === "string" && reconcile.etag ? { etag: reconcile.etag } : {}),
          ...(typeof reconcile.version === "string" && reconcile.version ? { version: reconcile.version } : {}),
          ...(batchDeadlineDeferrals > 0 ? { batchDeadlineDeferrals } : {}),
        },
      ],
    };
  }
  if (!Array.isArray(v.Records)) return { kind: "invalid" };
  const events: LandedEvent[] = [];
  for (const record of v.Records as S3Record[]) {
    const event = record && typeof record === "object" ? fromRecord(record, provider) : null;
    if (event) events.push(event);
  }
  return { kind: "events", events };
}

/** A carried deferral count: a non-negative safe integer as is; anything else (malformed, negative, fractional, a string) is 0. Never throws. */
export function deferralCount(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

/**
 * The body of a requeued manifest: `{"reconcile":{"key"}}`, plus `batch_deadline_deferrals` only
 * when the batch-deadline rule passes one. Every other requeue's body is byte-identical to before.
 */
export function requeueBody(manifestKey: string, batchDeadlineDeferrals?: number): string {
  return JSON.stringify({
    [RECONCILE_BODY_KEY]: {
      key: manifestKey,
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
