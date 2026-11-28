/**
 * Which consumer path an SQS batch takes (lambda.ts), with SESSION_REVISION_QUEUE_URL set:
 *
 *   a FIFO batch (eventSourceARN *.fifo)  -> processBatch in ordered mode (per-session group, coalesced)
 *   a standard batch, SESSION_EVENTS_MODE=forward -> manifest events re-keyed onto the FIFO queue;
 *                                            anything else in the batch -> processBatch as today
 *   a standard batch, direct (default)     -> processBatch exactly as today
 *
 * Unset: every batch goes to processBatch exactly as before.
 */
import { processBatch, type BatchResponse, type ConsumerDeps, type QueueRecord } from "./consumer.ts";
import { forwardManifestEvents, type RevisionEventsConfig, type RevisionSender } from "./revision-events.ts";

export interface SqsRecord {
  messageId: string;
  body: string;
  receiptHandle?: string;
  eventSourceARN?: string;
  attributes?: Record<string, string>;
}

export interface RevisionWiring {
  config: RevisionEventsConfig;
  sender: RevisionSender;
  /** ChangeMessageVisibilityBatch on the FIFO queue for these receipt handles. */
  changeVisibility(receiptHandles: readonly string[], seconds: number): Promise<void>;
  now?: () => number;
}

export const isFifoRecord = (r: SqsRecord) => typeof r.eventSourceARN === "string" ? r.eventSourceARN.endsWith(".fifo") : typeof r.attributes?.MessageGroupId === "string";

const num = (v: string | undefined) => {
  const n = v === undefined ? NaN : Number(v);
  return Number.isFinite(n) ? n : undefined;
};

export function queueRecords(records: readonly SqsRecord[]): QueueRecord[] {
  return records.map((r) => {
    const sentAt = num(r.attributes?.SentTimestamp);
    const receiveCount = num(r.attributes?.ApproximateReceiveCount);
    return {
      messageId: r.messageId,
      body: r.body,
      ...(r.attributes?.MessageGroupId ? { groupId: r.attributes.MessageGroupId } : {}),
      ...(sentAt !== undefined ? { sentAt } : {}),
      ...(receiveCount !== undefined ? { receiveCount } : {}),
    };
  });
}

export async function dispatchSqsBatch(records: readonly SqsRecord[], deps: ConsumerDeps, wiring?: RevisionWiring): Promise<BatchResponse> {
  if (!wiring) return processBatch(records.map((r) => ({ messageId: r.messageId, body: r.body })), deps);
  if (records.length > 0 && records.every(isFifoRecord)) {
    const handles = new Map(records.map((r) => [r.messageId, r.receiptHandle]));
    return processBatch(queueRecords(records), {
      ...deps,
      ordered: {
        busyRetrySeconds: wiring.config.busyRetrySeconds,
        ...(wiring.now ? { now: wiring.now } : {}),
        retryLater: async (ids, seconds) => {
          const list = ids.map((id) => handles.get(id)).filter((h): h is string => typeof h === "string");
          if (list.length) await wiring.changeVisibility(list, seconds);
        },
      },
    });
  }
  if (records.some(isFifoRecord)) {
    // A mixed batch cannot come from one event source mapping; refuse it whole rather than guess.
    return { batchItemFailures: records.map((r) => ({ itemIdentifier: r.messageId })) };
  }
  if (wiring.config.mode !== "forward") return processBatch(queueRecords(records), deps);
  const forwarded = await forwardManifestEvents(records, wiring.sender);
  const rest = forwarded.passThrough.length ? await processBatch(queueRecords(forwarded.passThrough), deps) : { batchItemFailures: [] };
  const failed = new Set([...forwarded.failed, ...rest.batchItemFailures.map((f) => f.itemIdentifier)]);
  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
}
