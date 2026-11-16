/**
 * The log materializer (docs/design/s3-ledger-log-commit.md, slice 3): applies a log-mode stream's entries to the
 * read-side state (the per-session manifests and chunks, the revision index, the outcome-carrying batch docs) so
 * readers, search and the read API work unchanged.
 *
 * How: each logged batch is replayed, in log order, through the UNCHANGED legacy admission core (deliverPrepared:
 * claim -> canonical writes -> commit) on a ledger WITHOUT logModeGuard. The legacy core is the only writer of
 * those objects, so log mode yields exactly the state legacy admission yields for the same inputs (the
 * equivalence suite pins it byte for byte), and every legacy property carries over: idempotency (the batch docs:
 * a re-applied batch is a replay), resume after a crash at any step (a pending claim resumes), concurrency (two
 * materializers on one stream race on the same claim tokens; the loser retries and finds a replay), and the
 * privacy rules applied at write time.
 *
 * Privacy, re-checked at materialization (never resurrect erased data):
 *   - the user's fence: the legacy claim drops records of sessions deleted since the entry, and retries while a
 *     reset is pending;
 *   - the privacy journal: a reset / re-enable completed after the upload began refuses the batch (the payload
 *     carries its journal seq); refused batches are skipped and their payloads deleted;
 *   - the admission snapshot (LogBatch.admitted, slice 4): the fence's resets and each written session's epoch at
 *     the append. A reset pending or running is a retry; one that completed since the append refuses the batch
 *     (skipped:delivery_privacy_superseded); a session whose epoch moved since (deleted, maybe re-enabled) has its
 *     records dropped. So an upload admitted before a control is never written as one made after it. The legacy
 *     claim then registers the owner stubs under the current fence, as it always does;
 *   - upload consent (Remaining 1 of the design): the payload carries the inbox's operator sharing cap, and the
 *     consent gate runs again at apply time (admit phase, after the legacy claim and before any canonical write),
 *     with that cap and this process's INGEST_UPLOAD_CONSENT config. A refusal skips the batch for good
 *     (skipped:upload_consent_required) and its payload is deleted, as the journal re-check does: the materializer
 *     never holds a stream behind one batch, so it cannot wait for a consent that may never come;
 *   - payloads under ledger/log/o=<origin>/p/ live only until their entry is applied: after its marker is written
 *     they are deleted. eraseLogPayloads deletes a user's unapplied payloads outright (a batch whose payload is gone
 *     is skipped as erased, never written).
 *
 * Progress: `m/<seq>.json`, written once (create-if-absent) after entry `seq` and every entry before it are
 * applied. The highest marker is the cursor. A crash before the marker re-applies the entry (idempotent).
 *
 * Streams: the registry `<keyRoot>/.ledger/log-streams/<h>.json` names every log-mode stream (written by the
 * consumer when it takes a stream and by the operator tool), so the sweeper finds streams this process never saw.
 * A registry doc (v2) is keyed by a hash and holds only a pointer to the stream's root (`{v: 2, root}`: the org and
 * workspace ids as path segments, the user and origin only as hashes); the ids themselves live in the stream's
 * identity doc (`<root>/stream.json`) inside the user's directory, so erasing that directory leaves no raw user or
 * origin id behind (Remaining 8). A pointer whose identity doc is gone (erased) is skipped. Registry docs written
 * before that (v1: raw ids, another key) are still read, and migrateLogStreamRegistry rewrites them.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { assertNoLaterPrivacyControl, INBOX_NAMESPACE, inboxConsentCap, outcomeKey, receiptDir, writeOutcome } from "./async-admission.ts";
import { deliverPrepared, type DeliverDeps } from "./deliver.ts";
import { LedgerUnavailableError, type LedgerTenant } from "./ledger.ts";
import { logKeys, readEntries, type LogEntry, type LogKeys } from "./ledger-log.ts";
import { admissionFenceView, fenceEpoch, hasPendingReset, s3LedgerObjects, sessionKeyHash } from "./ledger-s3.ts";
import { checkUploadConsent, UploadConsentRequired, type OperatorSharingCap, type UploadConsentConfig } from "./upload-consent.ts";
import { DeliveryError } from "./vendor/delivery/delivery-contracts.ts";
import type { PreparedDelivery } from "./vendor/delivery/prepare.ts";

const sha256 = (v: string) => createHash("sha256").update(v).digest("hex");
const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));

export const logStreamsRoot = (keyRoot: string) => `${keyRoot}/.ledger/log-streams/`;
/** A stream's registry doc (v2): keyed by a hash of its root, so the key names no id either. */
export const logStreamRegistryKey = (keyRoot: string, tenant: LedgerTenant, originId: string) =>
  `${logStreamsRoot(keyRoot)}${sha256(JSON.stringify(["log-stream-v2", logKeys(keyRoot, tenant, originId).root])).slice(0, 40)}.json`;
/** Where a v1 registry doc (raw ids in its body) was written: read until migrateLogStreamRegistry rewrites it. */
export const legacyLogStreamRegistryKey = (keyRoot: string, tenant: LedgerTenant, originId: string) =>
  `${logStreamsRoot(keyRoot)}${sha256(JSON.stringify([tenant.orgId, tenant.workspaceId, tenant.userId, originId])).slice(0, 40)}.json`;

export interface LogStreamRef { tenant: LedgerTenant; originId: string }

const isStrs = (...v: unknown[]) => v.every((x) => typeof x === "string");
async function putOnce(store: ObjectStore, key: string, doc: unknown) {
  try { await store.put(objectKey(key), enc(doc), { ifNoneMatch: true, contentType: "application/json" }); }
  catch (error) { if (!(isObjectStoreError(error) && error.code === "already_exists")) throw error; }
}

/**
 * Name a stream in the registry. Idempotent (create-if-absent of identical bytes). The identity doc (inside the user's
 * directory) is written FIRST, so a registry pointer never names a stream whose identity was not yet written; a
 * pointer left behind by an erasure only reads as gone.
 */
export async function registerLogStream(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string): Promise<void> {
  const keys = logKeys(keyRoot, tenant, originId);
  await putOnce(store, keys.identity, { v: 1, orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId, originId });
  await putOnce(store, logStreamRegistryKey(keyRoot, tenant, originId), { v: 2, root: keys.root });
}

/** A stream root a v2 pointer may name: under this key root's admission inbox, ending in one stream's directory. */
const streamRootShape = (keyRoot: string, root: string) =>
  root.startsWith(`${keyRoot}/${INBOX_NAMESPACE}/`) && !root.split("/").includes("..") && /\/ledger\/log\/o=[0-9a-f]{32}$/.test(root);

/** One registry doc, resolved: the stream it names, or undefined (corrupt, malformed, erased, or a pointer that lies). */
async function resolveRegistryDoc(store: ObjectStore, keyRoot: string, key: string): Promise<(LogStreamRef & { v: 1 | 2 }) | undefined> {
  // One corrupt registry doc never stops the sweep: it is skipped (a store fault still throws).
  let doc: unknown;
  try { doc = await readJson(store, key); } catch (error) { if (error instanceof SyntaxError) return undefined; throw error; }
  const d = doc as { v?: unknown; orgId?: unknown; workspaceId?: unknown; userId?: unknown; originId?: unknown; root?: unknown } | undefined;
  if (d?.v === 1 && isStrs(d.orgId, d.workspaceId, d.userId, d.originId))
    return { v: 1, tenant: { orgId: d.orgId as string, workspaceId: d.workspaceId as string, userId: d.userId as string }, originId: d.originId as string };
  if (d?.v !== 2 || typeof d.root !== "string" || !streamRootShape(keyRoot, d.root)) return undefined;
  let id: unknown;
  try { id = await readJson(store, `${d.root}/stream.json`); } catch (error) { if (error instanceof SyntaxError) return undefined; throw error; }
  const x = id as { v?: unknown; orgId?: unknown; workspaceId?: unknown; userId?: unknown; originId?: unknown } | undefined;
  // Gone (the user's directory was erased) or malformed: skipped.
  if (x?.v !== 1 || !isStrs(x.orgId, x.workspaceId, x.userId, x.originId)) return undefined;
  const ref = { tenant: { orgId: x.orgId as string, workspaceId: x.workspaceId as string, userId: x.userId as string }, originId: x.originId as string };
  // The pointer, its key and the identity must all name the same stream: a doc copied elsewhere names nothing.
  let bound: string;
  try { bound = logKeys(keyRoot, ref.tenant, ref.originId).root; } catch { return undefined; }
  if (bound !== d.root || logStreamRegistryKey(keyRoot, ref.tenant, ref.originId) !== key) return undefined;
  return { v: 2, ...ref };
}

async function* registryKeys(store: ObjectStore, keyRoot: string): AsyncGenerator<string> {
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(logStreamsRoot(keyRoot).slice(0, -1)), cursor);
    for (const item of page.items) {
      const key = String(item.key);
      if (key.startsWith(logStreamsRoot(keyRoot)) && key.endsWith(".json")) yield key;
    }
    cursor = page.nextCursor;
  } while (cursor);
}

/** Every registered stream, once each (a stream with both a v1 and a v2 doc mid-migration is listed once). */
export async function* listLogStreams(store: ObjectStore, keyRoot: string): AsyncGenerator<LogStreamRef> {
  const seen = new Set<string>();
  for await (const key of registryKeys(store, keyRoot)) {
    const ref = await resolveRegistryDoc(store, keyRoot, key);
    if (!ref) continue;
    let root: string;
    try { root = logKeys(keyRoot, ref.tenant, ref.originId).root; } catch { continue; }
    if (seen.has(root)) continue;
    seen.add(root);
    yield { tenant: ref.tenant, originId: ref.originId };
  }
}

export interface RegistryMigration { legacy: number; migrated: number }

/**
 * Rewrite every v1 registry doc (raw ids, outside the user's directory) as the v2 layout: the identity doc and the
 * hash-keyed pointer (registerLogStream), then the v1 doc deleted. Dry run unless `apply`. Idempotent and resumable: a
 * crash between the steps leaves a v1 doc whose stream is also registered (listed once), and the next run finishes it.
 */
export async function migrateLogStreamRegistry(store: ObjectStore, keyRoot: string, opts: { apply: boolean }): Promise<RegistryMigration> {
  const legacy: { key: string; ref: LogStreamRef }[] = [];
  for await (const key of registryKeys(store, keyRoot)) {
    const ref = await resolveRegistryDoc(store, keyRoot, key);
    if (ref?.v === 1) legacy.push({ key, ref });
  }
  if (!opts.apply) return { legacy: legacy.length, migrated: 0 };
  let migrated = 0;
  for (const { key, ref } of legacy) {
    await registerLogStream(store, keyRoot, ref.tenant, ref.originId);
    await store.delete(objectKey(key)).catch((error) => { if (!(isObjectStoreError(error) && error.code === "not_found")) throw error; });
    migrated++;
  }
  return { legacy: legacy.length, migrated };
}

/** Record a batch the materializer skipped for good (idempotent: the same code every time for a final refusal). */
export async function writeLogSkip(store: ObjectStore, keys: LogKeys, batchId: string, code: string, atMs: number): Promise<void> {
  await store.put(objectKey(keys.skipped(batchId)), enc({ v: 1, code, atMs }), { contentType: "application/json" });
}

/** The refusal code a skipped batch was recorded with, or undefined. A malformed record reads as skipped (fail closed). */
export async function readLogSkip(store: ObjectStore, keys: LogKeys, batchId: string): Promise<string | undefined> {
  let doc: unknown;
  try { doc = await readJson(store, keys.skipped(batchId)); }
  catch (error) { if (error instanceof SyntaxError) return "log_skip_unreadable"; throw error; }
  if (doc === undefined) return undefined;
  const code = (doc as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "log_skip_unreadable";
}

async function readJson(store: ObjectStore, key: string): Promise<unknown | undefined> {
  try { return JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body))); }
  catch (error) { if (isObjectStoreError(error) && error.code === "not_found") return undefined; throw error; }
}

async function listKeys(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
    for (const item of page.items) { const k = String(item.key); if (k.startsWith(prefix)) out.push(k); }
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}

/** The highest applied entry seq of a stream, or -1. */
export async function materializedThrough(store: ObjectStore, keys: LogKeys): Promise<number> {
  let max = -1;
  for (const k of await listKeys(store, keys.markers)) {
    const m = /\/m\/(\d{20})\.json$/.exec(k);
    if (m) max = Math.max(max, Number(m[1]));
  }
  return max;
}

/**
 * Delete every payload of a user's log streams (erasure). A batch whose payload is gone is skipped as erased by the
 * materializer, never written. Returns how many were deleted.
 */
export async function eraseLogPayloads(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<number> {
  let n = 0;
  for await (const s of listLogStreams(store, keyRoot)) {
    if (s.tenant.orgId !== tenant.orgId || s.tenant.workspaceId !== tenant.workspaceId || s.tenant.userId !== tenant.userId) continue;
    for (const k of await listKeys(store, logKeys(keyRoot, s.tenant, s.originId).payloads)) { await store.delete(objectKey(k)); n++; }
  }
  return n;
}

/**
 * A logged batch's payload (ledger-log-admit.ts): the kept records, the receipt, the upload's journal seq and a per-
 * attempt nonce. Its key is its content hash, so one attempt's own retried PUTs are idempotent, while every admission
 * attempt writes a FRESH object (a fresh lastModified): an identical re-put never leaves a stale payload that orphan
 * collection could take between a retry's PUT and its append. No admission time inside: received_at is the entry's
 * own atMs (the append). `receivedAtMs` is still read from a slice-3-era payload.
 */
interface StoredPayload { v: 1; journalSeq?: number; receivedAtMs?: number; consentCap?: unknown; receipt: PreparedDelivery["receipt"]; records: PreparedDelivery["records"] }
/** The payload's consent cap: absent (undefined), valid, or malformed (null: the payload is refused as invalid). */
const payloadConsentCap = (p: StoredPayload): OperatorSharingCap | undefined | null => {
  if (p.consentCap === undefined) return undefined;
  try { return inboxConsentCap(p.consentCap) ?? null; } catch { return null; }
};
const isStrField = (v: unknown) => typeof v === "string";
const validRecord = (r: unknown) => {
  const x = r as Record<string, unknown> | null;
  return !!x && typeof x === "object" && isStrField(x.source) && isStrField(x.record_id) && isStrField(x.revision_id) && isStrField(x.digest)
    && Number.isSafeInteger(x.revision) && (x.session_id === null || x.session_id === undefined || isStrField(x.session_id));
};
const validPayload = (v: unknown): v is StoredPayload => {
  const p = v as StoredPayload | undefined;
  return !!p && p.v === 1 && !!p.receipt && typeof p.receipt === "object" && typeof (p.receipt as { receiptId?: unknown }).receiptId === "string"
    && Array.isArray(p.records) && p.records.every(validRecord)
    && (p.journalSeq === undefined || Number.isSafeInteger(p.journalSeq))
    && (p.receivedAtMs === undefined || Number.isSafeInteger(p.receivedAtMs));
};
/** Log fields that name a stream or a batch without its ids: a hash prefix. */
const hid = (v: string) => sha256(v).slice(0, 16);

/** Streams the sweep materializes at once (each one serial): bounds a replica's load when many streams are dirty. */
export const LOG_SWEEP_CONCURRENCY = 4;

/**
 * An unreferenced payload older than this is collected (an admission puts its payload, then appends within seconds).
 * Longer than ledger-log's LOG_PENDING_MAX_AGE_MS: an entry left unresolved past that bound is never resent (only read
 * back), so no entry can land naming a payload this collection removed.
 */
export const ORPHAN_GRACE_MS = 60 * 60_000;

export type MaterializedOutcome = "accepted" | "accepted_all_deleted" | "replay" | "replay_fast" | "accepted_staged" | `skipped:${string}`;

export interface LogMaterializerDeps {
  store: ObjectStore;
  keyRoot: string;
  /** The legacy admission deps, on a ledger WITHOUT logModeGuard (the materializer is the one legacy writer allowed). */
  deliver: DeliverDeps;
  /** INGEST_UPLOAD_CONSENT, as the admission consumer has it: consent is re-checked at apply time (with the payload's cap). */
  uploadConsent?: UploadConsentConfig;
  nowMs?: () => number;
  log?: (fields: Record<string, unknown>) => void;
}

export interface MaterializeResult { applied: number; through: number; outcomes: MaterializedOutcome[] }

export class LogMaterializer {
  readonly #running = new Map<string, Promise<void>>();
  readonly #again = new Set<string>();
  readonly #io: ReturnType<typeof s3LedgerObjects>;
  /** Per stream, the last entry this process applied: the sweep skips a stream whose next entry does not exist. */
  readonly #through = new Map<string, number>();
  #sweeping: Promise<void> | undefined;
  constructor(readonly deps: LogMaterializerDeps) { this.#io = s3LedgerObjects(deps.store, deps.keyRoot, deps.nowMs ? { nowMs: deps.nowMs } : {}); }

  #id(tenant: LedgerTenant, originId: string) { return logKeys(this.deps.keyRoot, tenant, originId).root; }

  /** Apply every unapplied entry of one stream, in order. Throws on a retryable failure (the cursor stays put). */
  async materialize(tenant: LedgerTenant, originId: string): Promise<MaterializeResult> {
    const { store, keyRoot } = this.deps;
    const now = this.deps.nowMs ?? Date.now;
    const keys = logKeys(keyRoot, tenant, originId);
    let through = await materializedThrough(store, keys);
    const outcomes: MaterializedOutcome[] = [];
    let applied = 0;
    // Every payload any entry names (applied or not): the orphan collection never removes one of these.
    const referenced = new Set<string>();
    for await (const entry of readEntries(store, keys, Math.max(through, 0))) {
      if (entry.kind === "batches") for (const b of entry.batches) for (const ref of b.payloads) referenced.add(ref.key);
      if (entry.seq <= through) continue;
      if (entry.kind === "batches") for (const b of entry.batches) outcomes.push(await this.#applyBatch(tenant, originId, entry, b, now));
      await this.#mark(keys, entry);
      await this.#dropPayloads(entry);
      through = entry.seq;
      applied++;
    }
    this.#through.set(this.#id(tenant, originId), through);
    await this.#collectOrphans(keys, referenced);
    return { applied, through, outcomes };
  }

  /**
   * Payloads no entry will ever reference: a put whose append failed for good, or one the window answered. Only
   * after the stream is fully applied and only past ORPHAN_GRACE_MS by the store's own clock (lastModified against the
   * wall clock), so an in-flight admission's payload (put, not yet appended) is never collected.
   */
  async #collectOrphans(keys: LogKeys, referenced: ReadonlySet<string>) {
    const { store } = this.deps;
    let cursor: string | undefined;
    do {
      const page = await store.list(objectKey(keys.payloads.slice(0, -1)), cursor);
      for (const item of page.items) {
        if (!String(item.key).startsWith(keys.payloads) || referenced.has(String(item.key))) continue;
        if (Date.now() - item.lastModified.getTime() > ORPHAN_GRACE_MS) await store.delete(objectKey(String(item.key))).catch(() => {});
      }
      cursor = page.nextCursor;
    } while (cursor);
  }

  /**
   * Apply one batch. A final skip (a refusal that is the same answer forever) also records a refused outcome in the
   * batch's receipt directory and retires its admitted one, so the upload's status reads refused, never admitted for
   * data that was not written.
   */
  async #applyBatch(tenant: LedgerTenant, originId: string, entry: Extract<LogEntry, { kind: "batches" }>, b: Extract<LogEntry, { kind: "batches" }>["batches"][number], now: () => number): Promise<MaterializedOutcome> {
    const outcome = await this.#applyBatchOnce(tenant, originId, entry, b, now);
    if (outcome.startsWith("skipped:")) {
      const code = outcome.slice("skipped:".length);
      this.deps.log?.({ route: "ingest.log_materialize", outcome: "skipped", error_code: code, stream: hid(entry.stream), seq: entry.seq, batch: hid(b.batchId) });
      // The skip record FIRST (the replay answer reads it: admission-consumer.ts re-checks it after writing admitted),
      // then the refused outcome, then the admitted one retired. Any interleaving with a replay ends refused.
      await writeLogSkip(this.deps.store, logKeys(this.deps.keyRoot, tenant, originId), b.batchId, code, now());
      const dir = receiptDir(this.deps.keyRoot, tenant, originId, b.receiptId);
      await writeOutcome(this.deps.store, dir, { v: 1, state: "refused", code, atMs: now() });
      await this.deps.store.delete(objectKey(outcomeKey(dir, "admitted"))).catch((error) => { if (!(isObjectStoreError(error) && error.code === "not_found")) throw error; });
    }
    return outcome;
  }

  async #applyBatchOnce(tenant: LedgerTenant, originId: string, entry: Extract<LogEntry, { kind: "batches" }>, b: Extract<LogEntry, { kind: "batches" }>["batches"][number], now: () => number): Promise<MaterializedOutcome> {
    const { store, keyRoot } = this.deps;
    const records: PreparedDelivery["records"] = [];
    let receipt: PreparedDelivery["receipt"] | undefined;
    let journalSeq = 0;
    let receivedAtMs: number | undefined;
    let consentCap: OperatorSharingCap | undefined;
    for (const ref of b.payloads) {
      let p: unknown;
      try { p = await readJson(store, ref.key); } catch (error) { if (error instanceof SyntaxError) return "skipped:payload_invalid"; throw error; }
      // Gone before its entry was applied: erased (eraseLogPayloads). Never written.
      if (p === undefined) return "skipped:payload_erased";
      if (!validPayload(p)) return "skipped:payload_invalid";
      const cap = payloadConsentCap(p);
      if (cap === null) return "skipped:payload_invalid";
      // One batch, one inbox message, one cap: payloads of one batch that disagree are not something admission writes.
      if (cap && consentCap && JSON.stringify(cap) !== JSON.stringify(consentCap)) return "skipped:payload_invalid";
      consentCap ??= cap;
      receipt ??= p.receipt;
      receivedAtMs ??= p.receivedAtMs;
      // The snapshot's journal seq first; a payload without one (a slice-2-era payload) judges as seq 0, the
      // strictest answer (any completed control refuses it).
      journalSeq = Math.max(journalSeq, b.admitted?.journalSeq ?? p.journalSeq ?? 0);
      records.push(...p.records);
    }
    if (!receipt) return "skipped:payload_erased";
    receivedAtMs ??= entry.atMs;
    let kept = records;
    if (b.admitted) {
      // The admission snapshot, re-judged against the fence now (the legacy claim/commit pair's snapshot recheck,
      // stretched across the append -> apply gap).
      const fence = admissionFenceView(await this.#io.readFence(this.#io.keysFor(tenant).fence));
      if (hasPendingReset(fence)) throw new LedgerUnavailableError("ledger_control_pending");
      if (fence.resets > fence.resetsCompleted) throw new LedgerUnavailableError("ledger_reset_in_flight");
      if (fence.resets > b.admitted.resets) return "skipped:delivery_privacy_superseded";
      const moved = new Set(Object.entries(b.admitted.epochs).filter(([k, e]) => fenceEpoch(fence, originId, k) !== e).map(([k]) => k));
      if (moved.size > 0) kept = records.filter((r) => typeof r.session_id !== "string" || r.session_id === "" || !moved.has(sessionKeyHash(r.source, r.session_id)));
    }
    const prepared: PreparedDelivery = { originId, batchId: b.batchId, batchDigest: b.digest, receipt, records: kept };
    // Consent at apply time, with the admission's cap and this process's config (the consumer's consentGate, same
    // inputs). Admit phase only: it runs after the legacy claim and BEFORE any canonical write, so a refusal leaves
    // nothing written. The commit-phase re-check would run after the canonical writes, where a skip would strand them.
    const uploadConsent = this.deps.uploadConsent;
    const consentGate = (uploadConsent && uploadConsent.enforcement !== "off") || consentCap
      ? async (dropped: ReadonlySet<string>, phase: "admit" | "commit") => {
          if (phase !== "admit") return;
          await checkUploadConsent(uploadConsent, tenant, originId, kept, { dropped, phase, route: "log.materialize", ...(consentCap ? { operator: consentCap } : {}) }, this.deps.log);
        }
      : undefined;
    try {
      const r = await deliverPrepared({
        ...this.deps.deliver,
        // received_at is the admission time (the append), not the apply time.
        ...(receivedAtMs !== undefined ? { now: () => new Date(receivedAtMs), target: { ...this.deps.deliver.target, now: () => new Date(receivedAtMs) } } : {}),
        // The journal re-check at write time: a reset completed after the upload began refuses the batch.
        admissionGate: () => assertNoLaterPrivacyControl(store, keyRoot, tenant, journalSeq, now()),
        ...(consentGate ? { consentGate } : {}),
      }, tenant, prepared);
      return r.outcome as MaterializedOutcome;
    } catch (error) {
      // A definitive refusal (403/409/413/422/400: batch-id or revision conflict, privacy superseded): the same answer
      // forever, so the batch is skipped and the stream moves on. Anything else is a retry: the cursor stays.
      // Consent refused at apply (a 503 on the request path, where the inbox keeps the bytes for a redrive): here the
      // batch is already admitted and its inbox retired, and holding the stream would block every later entry, so it
      // is a final skip (its payload deleted). A consent STORE fault is not UploadConsentRequired: a retry.
      if (error instanceof UploadConsentRequired) return "skipped:upload_consent_required";
      if (error instanceof DeliveryError && error.status !== 503 && error.status !== 429) return `skipped:${error.code}`;
      throw error;
    }
  }

  async #mark(keys: LogKeys, entry: LogEntry) {
    try { await this.deps.store.put(objectKey(keys.marker(entry.seq)), enc({ v: 1, seq: entry.seq }), { ifNoneMatch: true, contentType: "application/json" }); }
    // Another materializer applied it first: the same state (the legacy core is idempotent).
    catch (error) { if (!(isObjectStoreError(error) && error.code === "already_exists")) throw error; }
  }

  /** After the marker: the entry's payloads are no longer needed (the canonical state holds the records). */
  async #dropPayloads(entry: LogEntry) {
    if (entry.kind !== "batches") return;
    for (const b of entry.batches) for (const ref of b.payloads) await this.deps.store.delete(objectKey(ref.key)).catch(() => {});
  }

  /** Schedule one stream (coalesced: a stream already running runs again once). Never throws. */
  kick(tenant: LedgerTenant, originId: string): void {
    const id = this.#id(tenant, originId);
    if (this.#running.has(id)) { this.#again.add(id); return; }
    const run = (async () => {
      do {
        this.#again.delete(id);
        try { await this.materialize(tenant, originId); }
        catch (error) {
          const code = (error as { code?: unknown })?.code;
          this.deps.log?.({ route: "ingest.log_materialize", outcome: "retry", error_code: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "materialize_failed" });
          return;
        }
      } while (this.#again.has(id));
    })().finally(() => this.#running.delete(id));
    this.#running.set(id, run);
  }

  /**
   * The sweeper: streams whose kick was lost, or that another process admitted. Returns once the registry is listed;
   * the materializing runs in the background, LOG_SWEEP_CONCURRENCY streams at a time, and a sweep still running is
   * not started again. A stream this process applied before is skipped unless its next entry exists (one HEAD), so an
   * idle stream costs no replay.
   */
  async sweep(): Promise<void> {
    if (this.#sweeping) return;
    // Everything runs in the background (the caller, the admission scan loop, never waits on it): the registry
    // listing, the idle checks (one HEAD per known stream, LOG_SWEEP_CONCURRENCY at a time) and the materializing.
    this.#sweeping = this.#sweepOnce().then(() => {}, () => {}).finally(() => { this.#sweeping = undefined; });
  }

  async #sweepOnce(): Promise<void> {
    const listed: LogStreamRef[] = [];
    for await (const s of listLogStreams(this.deps.store, this.deps.keyRoot)) listed.push(s);
    let next = 0;
    const worker = async () => {
      while (next < listed.length) {
        const s = listed[next++]!;
        const known = this.#through.get(this.#id(s.tenant, s.originId));
        if (known !== undefined) {
          let idle = false;
          try { idle = (await this.deps.store.head(objectKey(logKeys(this.deps.keyRoot, s.tenant, s.originId).entry(known + 1)))) === undefined; }
          catch { idle = false; }
          if (idle) continue;
        }
        this.kick(s.tenant, s.originId);
        await this.#running.get(this.#id(s.tenant, s.originId));
      }
    };
    await Promise.all(Array.from({ length: Math.min(LOG_SWEEP_CONCURRENCY, listed.length) }, worker));
  }

  async settle(): Promise<void> {
    while (this.#running.size > 0 || this.#sweeping) await Promise.allSettled([...this.#running.values(), ...(this.#sweeping ? [this.#sweeping] : [])]);
  }
}
