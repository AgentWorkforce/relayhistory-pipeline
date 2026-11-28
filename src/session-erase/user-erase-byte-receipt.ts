/**
 * The operator user erase's BYTE-PHASE RECEIPT: physical evidence of WHERE the SQL erase's byte phase ran, written into
 * that same store by the job that erased it, once the byte phase verified (bytes_verified). The hold bridge's
 * generation proof (session-ingest/user-erase-generation.ts) requires it to be present in the bridge's OWN store with a
 * matching destination, before any effect: a bridge pointed at another physical store (a misconfigured store B labelled
 * as bucket A) does not find it there, and refuses. A matching declaration alone never counts.
 *
 * CODE ONLY, NOT WIRED: user-erase-main.ts does not write it yet. Writing it needs PutObject for the eraser principal
 * on `<keyRoot>/.ledger/operator-user-erase/` (a bucket-policy / IAM change: Khaliq's explicit OK), so until then every
 * user erase stays refused by the bridge (generation_byte_receipt_missing). Hashes, counts and the destination only:
 * no org, workspace, user or request id.
 *
 * The grant, stated: the eraser principal (and no other) gets PutObject AND GetObject on exactly that prefix. GET is
 * needed because a retry that finds its receipt already written reads it back to confirm it is the same (create-only
 * adoption); a strict PutObject-only grant would fail that retry closed.
 *
 * Threat model, stated: this binds against MISCONFIGURATION (wrong bucket, region, endpoint, key root, routing). A
 * principal that can write this prefix in store B could forge it; the prefix's write grant is the trust boundary.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";

/** One bucket of the destination: what the store was built for (endpoint null for the provider's default). */
export interface StoreBucket { bucket: string; region: string; endpoint: string | null }
/** The full destination: the Locker bucket, the separate ledger bucket when routed (null otherwise), the key root. */
export interface StoreDestination { keyRoot: string; main: StoreBucket; ledger: StoreBucket | null }

const sha = (...parts: string[]) => createHash("sha256").update(JSON.stringify(["operator-user-erase-bytes/v1", ...parts])).digest("hex");
export const byteRequestHash = (requestId: string) => "h_" + sha("request", requestId).slice(0, 32);
export const byteTenantHash = (orgId: string, workspaceId: string, userId: string) => "h_" + sha("tenant", orgId, workspaceId, userId).slice(0, 32);
export const prefixesDigest = (prefixes: readonly string[]) => sha("prefixes", ...[...prefixes].sort());

/** `<keyRoot>/.ledger/operator-user-erase/<request hash>.json`, in the Locker (main) side of a routed store. */
export const userEraseByteReceiptKey = (keyRoot: string, requestId: string) => `${keyRoot}/.ledger/operator-user-erase/${byteRequestHash(requestId)}.json`;

export interface UserEraseByteReceipt {
  v: 1;
  kind: "operator-user-erase-bytes";
  request: string;
  tenant: string;
  /** Where the byte phase ran: the eraser's bucket, region, endpoint and key root. */
  destination: StoreBucket & { keyRoot: string };
  /** prefixesDigest(userErasePrefixes(...)): the exact prefixes the byte phase verified. */
  prefixes: string;
  verifiedAtMs: number;
}

const str = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v.length <= 512;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
export function validStoreBucket(v: unknown): v is StoreBucket {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const b = v as Record<string, unknown>;
  return Object.keys(b).length === 3 && typeof b.bucket === "string" && BUCKET.test(b.bucket) && str(b.region) && (b.endpoint === null || str(b.endpoint));
}
export function validStoreDestination(v: unknown): v is StoreDestination {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const d = v as Record<string, unknown>;
  return Object.keys(d).length === 3 && str(d.keyRoot) && validStoreBucket(d.main) && (d.ledger === null || validStoreBucket(d.ledger));
}
export const sameBucket = (a: StoreBucket, b: StoreBucket) => a.bucket === b.bucket && a.region === b.region && a.endpoint === b.endpoint;
export const sameDestination = (a: StoreDestination, b: StoreDestination) => a.keyRoot === b.keyRoot && sameBucket(a.main, b.main)
  && (a.ledger === null ? b.ledger === null : b.ledger !== null && sameBucket(a.ledger, b.ledger));
/** A canonical encoding (fixed field order), for hashing. */
export const destinationText = (d: StoreDestination) => JSON.stringify([d.keyRoot, [d.main.bucket, d.main.region, d.main.endpoint],
  d.ledger && [d.ledger.bucket, d.ledger.region, d.ledger.endpoint]]);

export function validUserEraseByteReceipt(v: unknown): v is UserEraseByteReceipt {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const r = v as Record<string, any>;
  const d = r.destination;
  return Object.keys(r).length === 7 && r.v === 1 && r.kind === "operator-user-erase-bytes" && /^h_[0-9a-f]{32}$/u.test(r.request)
    && /^h_[0-9a-f]{32}$/u.test(r.tenant) && d && typeof d === "object" && Object.keys(d).length === 4 && str(d.keyRoot)
    && validStoreBucket({ bucket: d.bucket, region: d.region, endpoint: d.endpoint }) && /^[0-9a-f]{64}$/u.test(r.prefixes)
    && Number.isSafeInteger(r.verifiedAtMs) && r.verifiedAtMs > 0;
}

/** The receipt for a verified byte phase. */
export function userEraseByteReceipt(req: { requestId: string; orgId: string; workspaceId: string; userId: string }, destination: StoreBucket & { keyRoot: string },
  prefixes: readonly string[], verifiedAtMs: number): UserEraseByteReceipt {
  return { v: 1, kind: "operator-user-erase-bytes", request: byteRequestHash(req.requestId), tenant: byteTenantHash(req.orgId, req.workspaceId, req.userId),
    destination: { bucket: destination.bucket, region: destination.region, endpoint: destination.endpoint, keyRoot: destination.keyRoot },
    prefixes: prefixesDigest(prefixes), verifiedAtMs };
}

/**
 * Write it create-only into the store the byte phase erased. A receipt already there must be the same (but for its
 * time), else it throws: a request's byte phase has one destination.
 */
export async function writeUserEraseByteReceipt(store: ObjectStore, receipt: UserEraseByteReceipt, requestId: string): Promise<void> {
  const key = userEraseByteReceiptKey(receipt.destination.keyRoot, requestId);
  try {
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(receipt)), { contentType: "application/json", ifNoneMatch: true });
  } catch (e) {
    if (!(isObjectStoreError(e, "precondition_failed") || isObjectStoreError(e, "already_exists"))) throw e;
    const got = await readUserEraseByteReceipt(store, receipt.destination.keyRoot, requestId);
    const strip = (r: UserEraseByteReceipt) => JSON.stringify({ ...r, verifiedAtMs: 0 });
    if (!got || strip(got) !== strip(receipt)) throw new Error("user_erase_byte_receipt_conflict");
  }
}

/** Read it from `store`: null when absent; throws on an unreadable or invalid one. */
export async function readUserEraseByteReceipt(store: ObjectStore, keyRoot: string, requestId: string): Promise<UserEraseByteReceipt | null> {
  let raw: Uint8Array;
  try { raw = await readAll((await store.getObject(objectKey(userEraseByteReceiptKey(keyRoot, requestId)))).body); }
  catch (e) { if (isObjectStoreError(e, "not_found")) return null; throw e; }
  const v = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
  if (!validUserEraseByteReceipt(v)) throw new Error("user_erase_byte_receipt_invalid");
  return v;
}
