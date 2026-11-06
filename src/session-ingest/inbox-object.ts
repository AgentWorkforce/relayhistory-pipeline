/**
 * Dormant S3 inbox storage primitive. No HTTP route imports this module.
 * The caller must authorize tenant/consent and scrub the payload BEFORE calling.
 * This is not the complete database-independent acceptance protocol: auth,
 * erasure barriers, consumer/reconciliation and routing remain integration gates.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, type ObjectStore } from "../shared/vendor.ts";

const hash = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
const encoder = new TextEncoder();
const MAX_BYTES = 3 * 1024 * 1024;
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

export interface InboxObjectInput {
  tenant: { orgId: string; workspaceId: string; userId: string };
  originId: string;
  batchId: string;
  batchDigest: string;
  /** Exact scrubbed, encoded batch bytes; this primitive does not scrub. */
  payload: Uint8Array;
  /** Exact durable receipt bytes to recover following a lost HTTP response. */
  receipt: Uint8Array;
}

function identity(input: InboxObjectInput) {
  const parts = [input.tenant.orgId, input.tenant.workspaceId, input.tenant.userId, input.originId, input.batchId];
  if (parts.some(p => typeof p !== "string" || !p || encoder.encode(p).length > 512 || LONE_SURROGATE.test(p) || /[\u0000-\u001f\u007f]/.test(p))) throw Error("inbox_identity_invalid");
  if (!/^[a-f0-9]{64}$/.test(input.batchDigest)) throw Error("inbox_digest_invalid");
  return parts;
}

/** Opaque segments prevent tenant data from introducing traversal or prefix ambiguity. */
export function inboxObjectKey(input: InboxObjectInput): string {
  const p = identity(input);
  return `96bulls/upload-inbox/v1/t=${hash(JSON.stringify(p.slice(0, 3)))}/o=${hash(p[3]!)}/b=${hash(p[4]!)}.inbox`;
}

function objectBytes(input: InboxObjectInput): Uint8Array {
  if (!(input.payload instanceof Uint8Array) || !input.payload.length || !(input.receipt instanceof Uint8Array) || !input.receipt.length) throw Error("inbox_bytes_invalid");
  if (input.payload.length + input.receipt.length > MAX_BYTES) throw Error("inbox_object_too_large");
  const header = encoder.encode(JSON.stringify({v:1, tenant:{orgId:input.tenant.orgId,workspaceId:input.tenant.workspaceId,userId:input.tenant.userId}, originId:input.originId, batchId:input.batchId,
    batchDigest:input.batchDigest, payloadBytes:input.payload.length, payloadSha256:hash(input.payload),
    receiptBytes:input.receipt.length, receiptSha256:hash(input.receipt)}) + "\n");
  const length = header.length + input.receipt.length + input.payload.length;
  if (length > MAX_BYTES) throw Error("inbox_object_too_large");
  const bytes = new Uint8Array(length);
  bytes.set(header); bytes.set(input.receipt, header.length); bytes.set(input.payload, header.length + input.receipt.length);
  return bytes;
}

/** Bounded streaming comparison, never trust object metadata alone for a replay. */
async function matches(store: ObjectStore, key: string, expected: Uint8Array): Promise<boolean> {
  const object = await store.getObject(objectKey(key));
  if (object.meta.size !== expected.length) { await object.body.cancel(); return false; }
  const reader = object.body.getReader();
  let offset = 0;
  try {
    for (;;) {
      const item = await reader.read();
      if (item.done) return offset === expected.length;
      if (offset + item.value.length > expected.length) return false;
      for (let i = 0; i < item.value.length; i++) if (item.value[i] !== expected[offset + i]) return false;
      offset += item.value.length;
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/**
 * Payload and receipt are one immutable object, so there is no receipt-after-PUT
 * database commit. An uncertain PUT never fabricates success; retry verifies the
 * existing exact object. Concurrent identical retries converge on the same key.
 */
export async function writeInboxObject(store: ObjectStore, input: InboxObjectInput): Promise<{key: string; receipt: Uint8Array; replay: boolean}> {
  const key = inboxObjectKey(input), bytes = objectBytes(input), receipt = input.receipt.slice();
  try {
    const ack = await store.put(objectKey(key), bytes, {ifNoneMatch:true, contentSha256:hash(bytes), contentType:"application/octet-stream"});
    if (ack.created) return {key, receipt, replay:false};
  } catch (error) {
    if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
  }
  if (!await matches(store, key, bytes)) throw Error("inbox_batch_conflict_or_corruption");
  return {key, receipt, replay:true};
}
