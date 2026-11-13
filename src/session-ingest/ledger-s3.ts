/**
 * The upload ledger in S3 (decision 0005, docs/decisions/0005-s3-native-upload-ledger.md): the same
 * `Ledger` decisions `ledger-sql.ts` makes in Postgres, for INLINE acceptance only, with zero SQL. It is
 * behind INGEST_LEDGER=s3 (default sql), and only the upload-admission consumer runs it.
 *
 * Every object sits under the per-user admission directory (async-admission.ts `admissionUserDir`, the
 * one that holds `privacy-journal.json`), which is never desktop-writable:
 *
 *   ledger/batches/o=<h(origin)>/b=<h(batchId)>.json    the batch record: create-only claim,
 *       {v, originId, batchId, digest, receiptId, state, claimToken, fenceVersion, resets, epochs}
 *       pending -> committed with If-Match. Same digest committed: replay. Same digest pending: resume
 *       (a new claim token and fence snapshot, If-Match). Other digest: batch_conflict.
 *   ledger/revisions/o=<h(origin)>/s=<h(source \x1f sessionId) | _origin>.json   the revision index:
 *       {v, originId, source, sessionId, records: {<recordId>: [revision, revisionId, digest]}}
 *       read at claim (an equal or lower stored revision is `stale`, relayhistory-cloud #250), merged at
 *       commit with If-Match (max wins, an equal revision keeps the stored digest), retried on a lost CAS.
 *   upload-fences.json   the fence: {v, version, resets, deleted: {<origin>: [[source, sessionId]...]},
 *       epochs: {<origin>: {<h(pair)>: n}}, resetRequests: {<requestId>: n | null}, ordered?}
 *       edited with If-Match by every selection, reset and ordered control. Unreadable: a retry, never empty.
 *   ledger/rearm/o=<h(origin)>/<h(source \x1f sessionId)>.json   the re-arm marker for the eraser:
 *       {v, originId, source, sessionId, machineId, atMs}, create-only.
 *
 * h(x) = sha256(x) hex [0:32].
 *
 * Mirrors sessions.ingest_claim_batch_mode (canonical) and sessions.ingest_commit_batch
 * (supabase/migrations/20261105050010_operator_session_erase_fence_check.sql):
 *
 *   claim   batch record (replay / batch_conflict) -> fence (deleted) -> revision indexes of the live
 *           records (stale, in-batch revision reuse = revision_conflict) -> the batch record, committed
 *           at once when nothing needs writing (replay_fast).
 *   commit  fence (what changed since the claim's snapshot) -> re-arm markers for the fenced sessions ->
 *           merge the written revisions of the others -> fence again (a selection or reset that landed
 *           during the merge) -> clear the fenced sessions' indexes -> batch record committed. A fenced
 *           session the fence no longer deletes (a reset or a re-enable since the claim) is
 *           `ledger_attempt_superseded`, as SQL's `invalidated`: the retry claims again, and the
 *           consumer's privacy-journal gate decides.
 *
 * Re-arm markers are written before any index merge, so a crash after the merge (whose retry is a
 * replay_fast that skips this commit) never loses one. The index can lag the manifest (a crash between
 * the canonical write and the merge resumes) but never leads it: the merge runs after the canonical write.
 *
 * Nothing here deletes: an index is "cleared" by a CAS write of an empty record set, so the consumer needs
 * no DeleteObject on its promotion grant. Errors are LedgerUnavailableError with a code only, never a key.
 *
 * Refused (LedgerUnavailableError, never a fallback): buffered acceptance, the staged fan-out, Babysitter
 * retention, frozen-supabase custody and every source/privacy-authority call, and acquisition grants.
 */
import { createHash, randomUUID } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { admissionUserDir } from "./async-admission.ts";
import type { IngestAcceptanceMode } from "./buffered-protocol.ts";
import { orderedControl, type OrderedControl, type OrderedControlResult } from "./control-protocol.ts";
import {
  LedgerUnavailableError,
  type ClaimInput,
  type ClaimResult,
  type CommitInput,
  type CommitResult,
  type Ledger,
  type LedgerRecord,
  type LedgerTenant,
  type OrderedControlInput,
  type RevisionConflict,
  type SelectionInput,
} from "./ledger.ts";
import { MAX_DELIVERY_CONFLICTS } from "./vendor/delivery/delivery-contracts.ts";
import { assertTenantSegment, backoffMs } from "./writer.ts";

// ---- INGEST_LEDGER ----

export const INGEST_LEDGER_VAR = "INGEST_LEDGER";
export type IngestLedgerMode = "sql" | "s3";

/** Unset, "" or "sql": sql (today's ledger, byte for byte). Exactly "s3" otherwise; anything else throws. */
export function ingestLedgerMode(raw: string | undefined): IngestLedgerMode {
  if (raw === undefined || raw === "" || raw === "sql") return "sql";
  if (raw === "s3") return "s3";
  throw new Error(`session ingest: ${INGEST_LEDGER_VAR} must be sql or s3`);
}

// ---- keys ----

const sha256Hex = (v: string) => createHash("sha256").update(v).digest("hex");
const h = (v: string) => sha256Hex(v).slice(0, 32);
const pairHash = (source: string, sessionId: string) => h(`${source}\u001f${sessionId}`);

export function s3LedgerKeys(keyRoot: string, tenant: LedgerTenant) {
  const dir = admissionUserDir(keyRoot, tenant);
  return {
    dir,
    fence: `${dir}/upload-fences.json`,
    batch: (originId: string, batchId: string) => `${dir}/ledger/batches/o=${h(originId)}/b=${h(batchId)}.json`,
    revisionsRoot: `${dir}/ledger/revisions`,
    revisions: (originId: string, source: string | null, sessionId: string | null) =>
      `${dir}/ledger/revisions/o=${h(originId)}/s=${sessionId === null || source === null ? "_origin" : pairHash(source, sessionId)}.json`,
    rearm: (originId: string, source: string, sessionId: string) => `${dir}/ledger/rearm/o=${h(originId)}/${pairHash(source, sessionId)}.json`,
  };
}

// ---- object shapes ----

type Decision = "apply" | { outcome: "replay" | "stale"; result: number };
interface OrderedMark { epoch: string; version: string; digest: string; kind: "selection" | "reset"; result: number | null }
export interface FenceDoc {
  v: 1;
  /** Bumped by every edit: a commit whose claim saw this version has nothing to recheck. */
  version: number;
  resets: number;
  deleted: Record<string, [string, string][]>;
  /** Per origin, per h(pair): bumped by every disable (and by a reset of the pair), as ingest_fence_versions. */
  epochs: Record<string, Record<string, number>>;
  /** A reset's request id: null while it runs, then the count it tombstoned (a replay answers it). */
  resetRequests: Record<string, number | null>;
  ordered?: OrderedMark;
}
export interface BatchDoc {
  v: 1;
  originId: string;
  batchId: string;
  digest: string;
  receiptId: string;
  state: "pending" | "committed";
  claimToken: string | null;
  fenceVersion: number;
  resets: number;
  /** The claim's fence snapshot: the epoch of every session of the batch. */
  epochs: Record<string, number>;
}
export interface IndexDoc { v: 1; originId: string; source: string | null; sessionId: string | null; records: Record<string, [number, string, string]> }

const emptyFence = (): FenceDoc => ({ v: 1, version: 0, resets: 0, deleted: {}, epochs: {}, resetRequests: {} });

const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const count = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const str = (v: unknown): v is string => typeof v === "string" && v.length > 0;
const HEX64 = /^[0-9a-f]{64}$/;
const TOKEN = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const pairOk = (p: unknown): p is [string, string] => Array.isArray(p) && p.length === 2 && str(p[0]) && str(p[1]);
/** A client-supplied id used as a fence map key (origin, reset request): never the prototype setter's name. */
const mapKey = (v: unknown): v is string => str(v) && v !== "__proto__";
/** Own properties only: an origin named `constructor` or `toString` must never read Object.prototype. */
const own = <T>(o: Record<string, T>, k: string): T | undefined => (Object.hasOwn(o, k) ? o[k] : undefined);

export function validFence(v: unknown): v is FenceDoc {
  if (!isObject(v) || v.v !== 1 || !count(v.version) || !count(v.resets) || !isObject(v.deleted) || !isObject(v.epochs) || !isObject(v.resetRequests)) return false;
  if (!Object.values(v.deleted).every((list) => Array.isArray(list) && list.every(pairOk))) return false;
  if (!Object.values(v.epochs).every((m) => isObject(m) && Object.values(m).every(count))) return false;
  if (!Object.values(v.resetRequests).every((n) => n === null || count(n))) return false;
  const o = v.ordered;
  return o === undefined || (isObject(o) && str(o.epoch) && typeof o.version === "string" && /^[1-9][0-9]{0,18}$/.test(o.version) && str(o.digest)
    && (o.kind === "selection" || o.kind === "reset") && (o.result === null || count(o.result)));
}
export function validBatch(v: unknown): v is BatchDoc {
  return isObject(v) && v.v === 1 && str(v.originId) && str(v.batchId) && typeof v.digest === "string" && HEX64.test(v.digest)
    && typeof v.receiptId === "string" && /^rhr_[0-9a-f]{64}$/.test(v.receiptId) && (v.state === "pending" || v.state === "committed")
    && (v.claimToken === null || (typeof v.claimToken === "string" && TOKEN.test(v.claimToken))) && count(v.fenceVersion) && count(v.resets)
    && isObject(v.epochs) && Object.values(v.epochs).every(count);
}
export function validIndex(v: unknown): v is IndexDoc {
  return isObject(v) && v.v === 1 && str(v.originId) && (v.source === null || str(v.source)) && (v.sessionId === null || str(v.sessionId)) && isObject(v.records)
    && Object.values(v.records).every((r) => Array.isArray(r) && r.length === 3 && Number.isSafeInteger(r[0]) && (r[0] as number) >= 1 && str(r[1]) && typeof r[2] === "string" && HEX64.test(r[2]));
}

// ---- store IO ----

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

/** A store failure as a ledger code: never the message (it can quote a key). */
function storeFailure(error: unknown): LedgerUnavailableError {
  if (error instanceof LedgerUnavailableError) return error;
  if (isObjectStoreError(error)) return new LedgerUnavailableError(`ledger_store_${error.code}`);
  return new LedgerUnavailableError("ledger_store_error");
}

async function readDoc<T>(store: ObjectStore, key: string, valid: (v: unknown) => v is T, invalidCode: string): Promise<{ doc: T; version: ObjectVersion } | null> {
  let raw: Uint8Array, version: ObjectVersion;
  try {
    const got = await store.getObject(objectKey(key));
    version = got.meta.version;
    raw = await readAll(got.body);
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw storeFailure(error);
  }
  let value: unknown;
  try { value = JSON.parse(decoder.decode(raw)); } catch { throw new LedgerUnavailableError(invalidCode); }
  // Fail closed: an unreadable ledger object is a retry, never an empty one.
  if (!valid(value)) throw new LedgerUnavailableError(invalidCode);
  return { doc: value, version };
}

/** Create-only (no version) or compare-and-swap. false: lost the race (re-read and retry). */
async function casPut(store: ObjectStore, key: string, doc: unknown, version: ObjectVersion | undefined): Promise<boolean> {
  try {
    await store.put(objectKey(key), encoder.encode(JSON.stringify(doc)), version ? { ifMatch: version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
    return true;
  } catch (error) {
    if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists")) return false;
    throw storeFailure(error);
  }
}

/** UTF-8 byte order: Postgres `COLLATE "C"`, as the SQL ledger sorts. */
const byteCompare = (a: string, b: string) => Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
const pairCompare = (a: [string, string], b: [string, string]) => byteCompare(a[0], b[0]) || byteCompare(a[1], b[1]);
const pairId = (source: string, sessionId: string) => JSON.stringify([source, sessionId]);

/** Bounded parallel map (one invocation's sockets). */
async function mapLimit<T, R>(items: readonly T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) { const i = next++; out[i] = await fn(items[i]!); }
  }));
  return out;
}

// ---- the ledger ----

export interface S3LedgerOptions {
  /** Only inline acceptance: "buffered" refuses at construction. */
  acceptanceMode?: IngestAcceptanceMode;
  /** CAS attempts per object before `ledger_*_contended` (default 8, as the manifest and the journal). */
  casAttempts?: number;
  nowMs?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  newToken?: () => string;
}

const PARALLEL = 6;

/**
 * The ledger's objects with their exact semantics, shared so other S3-native jobs (the backlog drain, the fence
 * backfill, the control routes' fence-first write) read and merge the SAME formats rather than their own:
 *
 *   keysFor(tenant)                     the per-user keys (refuses a non-segment org)
 *   readFence(key)                      the fence, or an empty one when absent; unreadable throws (fail closed)
 *   editFence(key, edit)                CAS edit; `edit` returns {next: null} for no change; version + 1 per write
 *   readIndex(key)                      a revision index, or null when absent; unreadable throws
 *   mergeIndex(key, shape, records)     max revision wins, an equal revision keeps the stored digest; CAS-retried
 *   clearIndex(key)                     forget a session's revisions (an empty record set, never a delete)
 *   writeRearm(key, marker)             create-only re-arm marker for the eraser
 *   readBatch(key)                      a batch record, or null when absent
 */
export function s3LedgerObjects(store: ObjectStore, keyRoot: string, opts: Omit<S3LedgerOptions, "acceptanceMode" | "newToken"> = {}) {
  const attempts = opts.casAttempts ?? 8;
  if (!Number.isSafeInteger(attempts) || attempts < 1 || attempts > 64) throw new LedgerUnavailableError("ledger_s3_options_refused");
  const nowMs = opts.nowMs ?? Date.now;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const random = opts.random ?? Math.random;
  const backoff = (attempt: number) => sleep(backoffMs(attempt, random));

  const keysFor = (tenant: LedgerTenant) => {
    try { assertTenantSegment(keyRoot, tenant.orgId); } catch { throw new LedgerUnavailableError("ledger_tenant_not_a_segment"); }
    if (!str(tenant.userId) || typeof tenant.workspaceId !== "string") throw new LedgerUnavailableError("ledger_input_refused");
    return s3LedgerKeys(keyRoot, tenant);
  };
  const readFence = async (key: string) => (await readDoc(store, key, validFence, "ledger_fence_invalid")) ?? { doc: emptyFence(), version: undefined };

  /** CAS edit of the fence, as async-admission.ts editJournal. `edit` returns null for "no change". */
  async function editFence<T>(key: string, edit: (f: FenceDoc) => { next: FenceDoc | null; result: T }): Promise<T> {
    for (let attempt = 0; attempt < attempts; attempt++) {
      const { doc, version } = await readFence(key);
      const { next, result } = edit(structuredClone(doc));
      if (!next) return result;
      next.version = doc.version + 1;
      if (await casPut(store, key, next, version)) return result;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_fence_contended");
  }

  const readIndex = (key: string) => readDoc(store, key, validIndex, "ledger_revision_index_invalid");

  /** Merge written records into one session's index: per record the highest (revision desc, revisionId, ord), replacing only a LOWER stored revision. */
  async function mergeIndex(key: string, shape: Omit<IndexDoc, "v" | "records">, records: LedgerRecord[]): Promise<void> {
    const best = new Map<string, LedgerRecord>();
    for (const r of records) {
      const cur = best.get(r.recordId);
      if (!cur || r.revision > cur.revision || (r.revision === cur.revision && byteCompare(r.revisionId, cur.revisionId) < 0)) best.set(r.recordId, r);
    }
    for (let attempt = 0; attempt < attempts; attempt++) {
      const got = await readIndex(key);
      const map = new Map(Object.entries(got?.doc.records ?? {}));
      let changed = false;
      for (const [id, r] of best) {
        const stored = map.get(id);
        if (stored && stored[0] >= r.revision) continue; // #250: an equal revision keeps the stored digest
        map.set(id, [r.revision, r.revisionId, r.digest]);
        changed = true;
      }
      if (!changed) return;
      const doc: IndexDoc = { v: 1, ...shape, records: Object.fromEntries(map) };
      if (await casPut(store, key, doc, got?.version)) return;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_revision_contended");
  }

  /** Forget a session's stored revisions (the disable / reset half of ingest_apply_selection): an empty record set, never a delete. Returns how many it forgot. */
  async function clearIndex(key: string): Promise<number> {
    for (let attempt = 0; attempt < attempts; attempt++) {
      const got = await readIndex(key);
      const n = got ? Object.keys(got.doc.records).length : 0;
      if (!got || n === 0) return 0;
      if (await casPut(store, key, { ...got.doc, records: {} }, got.version)) return n;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_revision_contended");
  }

  async function writeRearm(key: string, marker: { originId: string; source: string; sessionId: string; machineId: string }): Promise<void> {
    // Create-only: an unswept marker for the session is already enough for the eraser.
    await casPut(store, key, { v: 1, ...marker, atMs: nowMs() }, undefined);
  }

  const readBatch = (key: string) => readDoc(store, key, validBatch, "ledger_batch_record_invalid");

  return { attempts, backoff, nowMs, keysFor, readFence, editFence, readIndex, mergeIndex, clearIndex, writeRearm, readBatch };
}

export function s3Ledger(store: ObjectStore, keyRoot: string, opts: S3LedgerOptions = {}): Ledger {
  if (opts.acceptanceMode === "buffered") throw new LedgerUnavailableError("ledger_s3_mode_refused");
  const io = s3LedgerObjects(store, keyRoot, opts);
  const { attempts, backoff, keysFor, readFence, editFence, readIndex, mergeIndex, clearIndex, writeRearm } = io;
  const newToken = opts.newToken ?? randomUUID;

  const unsupported = (name: string) => async (): Promise<never> => { throw new LedgerUnavailableError(`ledger_s3_unsupported_${name}`); };

  function checkClaimInput(input: ClaimInput): void {
    if (input.custody) throw new LedgerUnavailableError("ledger_s3_custody_unsupported");
    if (input.acquisition !== undefined) throw new LedgerUnavailableError("ledger_s3_acquisition_unsupported");
    if (!mapKey(input.originId) || !str(input.batchId) || typeof input.batchDigest !== "string" || !HEX64.test(input.batchDigest)
      || typeof input.receiptId !== "string" || !/^rhr_[0-9a-f]{64}$/.test(input.receiptId) || !Array.isArray(input.sessions) || !input.sessions.every(pairOk))
      throw new LedgerUnavailableError("ledger_input_refused");
    checkRecords(input.records);
  }
  function checkRecords(records: LedgerRecord[]): void {
    if (!Array.isArray(records) || !records.every((r) => isObject(r) && str(r.source) && (r.sessionId === null || str(r.sessionId)) && str(r.recordId) && str(r.revisionId)
      && Number.isSafeInteger(r.revision) && r.revision >= 1 && typeof r.digest === "string" && HEX64.test(r.digest)))
      throw new LedgerUnavailableError("ledger_input_refused");
  }

  /** Every distinct (source, session) the batch names: its session list and its records' sessions (SQL's UNION). */
  function batchPairs(sessions: [string, string][], records: LedgerRecord[]): [string, string][] {
    const all = new Map<string, [string, string]>();
    for (const p of sessions) all.set(pairId(p[0], p[1]), [p[0], p[1]]);
    for (const r of records) if (r.sessionId !== null) all.set(pairId(r.source, r.sessionId), [r.source, r.sessionId]);
    return [...all.values()].sort(pairCompare);
  }

  /** Claim steps (b) and (c): the deletion filter, then #250's revision rules over the stored indexes. */
  async function judge(input: ClaimInput, keys: ReturnType<typeof s3LedgerKeys>, fence: FenceDoc) {
    const fenced = new Set((own(fence.deleted, input.originId) ?? []).map(([s, id]) => pairId(s, id)));
    const deleted = batchPairs(input.sessions, input.records).filter(([s, id]) => fenced.has(pairId(s, id)));
    const deletedSet = new Set(deleted.map(([s, id]) => pairId(s, id)));
    const live = input.records.map((r, ord) => ({ ...r, ord })).filter((r) => r.sessionId === null || !deletedSet.has(pairId(r.source, r.sessionId)));
    const indexKeys = [...new Set(live.map((r) => keys.revisions(input.originId, r.source, r.sessionId)))];
    const loaded = await mapLimit(indexKeys, PARALLEL, async (k) => [k, (await readIndex(k))?.doc.records ?? {}] as const);
    const indexes = new Map(loaded.map(([k, recs]) => [k, new Map(Object.entries(recs))]));
    const storedOf = (r: (typeof live)[number]) => indexes.get(keys.revisions(input.originId, r.source, r.sessionId))!.get(r.recordId)?.[0];
    // Identity order (record_id, revision, revision_id, ord); a NEW revision is judged against its first peer.
    const sorted = [...live].sort((a, b) => byteCompare(a.recordId, b.recordId) || a.revision - b.revision || byteCompare(a.revisionId, b.revisionId) || a.ord - b.ord);
    const peers = new Map<string, (typeof live)[number]>();
    for (const r of sorted) { const k = JSON.stringify([r.recordId, r.revision]); if (!peers.has(k)) peers.set(k, r); }
    const conflicts: RevisionConflict[] = [];
    const stale: number[] = [];
    let twins = 0;
    for (const r of sorted) {
      const stored = storedOf(r);
      if (stored !== undefined && stored >= r.revision) { stale.push(r.ord); continue; }
      const peer = peers.get(JSON.stringify([r.recordId, r.revision]))!;
      if (peer.revisionId !== r.revisionId && peer.digest !== r.digest) {
        twins++;
        if (conflicts.length < MAX_DELIVERY_CONFLICTS)
          conflicts.push({ recordId: r.recordId, submittedRevisionId: r.revisionId, submittedRevision: r.revision, submittedDigest: r.digest,
            currentRevisionId: peer.revisionId, currentRevision: r.revision, currentDigest: peer.digest });
      }
    }
    stale.sort((a, b) => a - b);
    return { deleted, stale, conflicts, twins, fast: stale.length === live.length };
  }

  /** A selection's fence + index effect (ingest_apply_selection). `mark` decides an ordered control inside the same fence edit. */
  async function applySelection(input: SelectionInput, control?: OrderedControl): Promise<{ outcome: "applied" | "replay" | "stale"; updated: number }> {
    const mark = control ? orderedMark(control) : undefined;
    if (!mapKey(input.originId) || !Array.isArray(input.sessions) || !input.sessions.every(pairOk) || typeof input.enabled !== "boolean") throw new LedgerUnavailableError("ledger_input_refused");
    const keys = keysFor(input.tenant);
    const pairs = batchPairs(input.sessions, []);
    const indexOf = ([s, id]: [string, string]) => keys.revisions(input.originId, s, id);
    if (input.enabled) {
      // Forget first, then lift the tombstone: an upload in between still sees the session deleted, and
      // one after it never meets revisions the disable left behind.
      const { doc } = await readFence(keys.fence);
      const fenced = new Set((own(doc.deleted, input.originId) ?? []).map(([s, id]) => pairId(s, id)));
      await mapLimit(pairs.filter(([s, id]) => fenced.has(pairId(s, id))), PARALLEL, (p) => clearIndex(indexOf(p)));
      return editFence<{ outcome: "applied" | "replay" | "stale"; updated: number }>(keys.fence, (f) => {
        const decided = mark?.(f);
        if (decided && decided !== "apply") return { next: null, result: { outcome: decided.outcome, updated: decided.result } };
        const want = new Set(pairs.map(([s, id]) => pairId(s, id)));
        const before = own(f.deleted, input.originId) ?? [];
        const after = before.filter(([s, id]) => !want.has(pairId(s, id)));
        const updated = before.length - after.length;
        if (after.length) f.deleted[input.originId] = after; else delete f.deleted[input.originId];
        if (control) finishOrdered(f, control, updated);
        return { next: updated > 0 || control ? f : null, result: { outcome: "applied" as const, updated } };
      });
    }
    const decided = await editFence<Decision>(keys.fence, (f) => {
      const d = mark?.(f);
      if (d && d !== "apply") return { next: null, result: d };
      const have = new Map((own(f.deleted, input.originId) ?? []).map((p) => [pairId(p[0], p[1]), p]));
      const epochs = (own(f.epochs, input.originId) ?? (f.epochs[input.originId] = {}));
      for (const [s, id] of pairs) {
        have.set(pairId(s, id), [s, id]);
        const k = pairHash(s, id);
        epochs[k] = (epochs[k] ?? 0) + 1;
      }
      f.deleted[input.originId] = [...have.values()].sort(pairCompare);
      return { next: f, result: "apply" as const };
    });
    if (decided !== "apply") return { outcome: decided.outcome, updated: decided.result };
    // Then forget: a commit that merged before this edit is cleared here; one after it sees the fence.
    const forgotten = await mapLimit(pairs, PARALLEL, (p) => clearIndex(indexOf(p)));
    const updated = forgotten.reduce((a, b) => a + b, 0);
    if (control) await editFence(keys.fence, (f) => ({ next: finishOrdered(f, control, updated) ? f : null, result: undefined }));
    return { outcome: "applied", updated };
  }

  /** A reset (ingest_apply_reset): begin (resets + 1), list the owned sessions, tombstone them, forget their revisions. */
  async function applyReset(tenant: LedgerTenant, requestId: string, control?: OrderedControl): Promise<{ outcome: "applied" | "replay" | "stale"; deleted: number }> {
    const mark = control ? orderedMark(control) : undefined;
    if (!mapKey(requestId)) throw new LedgerUnavailableError("ledger_input_refused");
    const keys = keysFor(tenant);
    const begun = await editFence<Decision>(keys.fence, (f) => {
      const d = mark?.(f);
      if (d && d !== "apply") return { next: null, result: d };
      const prior = own(f.resetRequests, requestId);
      if (typeof prior === "number") return { next: null, result: { outcome: "replay" as const, result: prior } };
      // Begun by an earlier attempt that stopped part way: finish it (the ordered mark, if any, is kept).
      if (prior === null) return { next: control ? f : null, result: "apply" as const };
      f.resetRequests[requestId] = null;
      f.resets += 1;
      return { next: f, result: "apply" as const };
    });
    if (begun !== "apply") return { outcome: begun.outcome, deleted: begun.result };
    // Owned: every session index with stored revisions (a pending claim is caught at its commit by the reset count).
    const owned: { key: string; originId: string; pair: [string, string] }[] = [];
    let cursor: string | undefined;
    const prefix = keys.revisionsRoot + "/";
    try {
      do {
        const page = await store.list(objectKey(keys.revisionsRoot), cursor);
        for (const item of page.items) {
          const key = String(item.key);
          if (!key.startsWith(prefix) || key.endsWith("/s=_origin.json")) continue;
          const got = await readIndex(key);
          if (got && got.doc.source !== null && got.doc.sessionId !== null && Object.keys(got.doc.records).length > 0)
            owned.push({ key, originId: got.doc.originId, pair: [got.doc.source, got.doc.sessionId] });
        }
        cursor = page.nextCursor;
      } while (cursor);
    } catch (error) {
      throw storeFailure(error);
    }
    const total = new Set(owned.map((o) => pairId(...o.pair))).size;
    await editFence(keys.fence, (f) => {
      if (typeof own(f.resetRequests, requestId) === "number") return { next: null, result: undefined };
      for (const o of owned) {
        const have = new Map((own(f.deleted, o.originId) ?? []).map((p) => [pairId(p[0], p[1]), p]));
        have.set(pairId(...o.pair), o.pair);
        f.deleted[o.originId] = [...have.values()].sort(pairCompare);
        const epochs = (own(f.epochs, o.originId) ?? (f.epochs[o.originId] = {}));
        const k = pairHash(...o.pair);
        epochs[k] = (epochs[k] ?? 0) + 1;
      }
      f.resetRequests[requestId] = total;
      if (control) finishOrdered(f, control, total);
      return { next: f, result: undefined };
    });
    await mapLimit(owned, PARALLEL, (o) => clearIndex(o.key));
    return { outcome: "applied", deleted: total };
  }

  /**
   * The ordered-control decision kept in the fence. SQL stays the ordering authority while controls
   * dual-write (decision 0005 "Controls"), so this only dedupes: a lower version of the same epoch is stale,
   * the same version replays (another digest refuses), anything higher applies.
   */
  const orderedMark = (control: OrderedControl) => (f: FenceDoc): "apply" | { outcome: "replay" | "stale"; result: number } => {
    const o = f.ordered;
    if (o && o.epoch === control.epoch) {
      const cur = BigInt(o.version), v = BigInt(control.version);
      if (v < cur) return { outcome: "stale", result: 0 };
      if (v === cur) {
        if (o.digest !== control.digest || o.kind !== control.kind) throw new LedgerUnavailableError("ledger_control_conflict");
        if (o.result !== null) return { outcome: "replay", result: o.result };
        return "apply"; // begun, not finished: finish it (every step is idempotent)
      }
    }
    f.ordered = { epoch: control.epoch, version: control.version, digest: control.digest, kind: control.kind, result: null };
    return "apply";
  };

  /** Record an applied ordered control's count (its replay answer). True when the fence changed. */
  const finishOrdered = (f: FenceDoc, control: OrderedControl, n: number): boolean => {
    const o = f.ordered;
    if (!o || o.epoch !== control.epoch || o.version !== control.version || o.result !== null) return false;
    o.result = n;
    return true;
  };

  return {
    frozenWitness: unsupported("frozen_witness"),
    frozenReceipt: unsupported("frozen_receipt"),
    sourceControlBegin: unsupported("source_control"),
    sourceControlComplete: unsupported("source_control"),
    privacyAuthorityBegin: unsupported("privacy_authority"),
    privacyAuthorityComplete: unsupported("privacy_authority"),
    prepareBufferStage: unsupported("buffered"),
    commitBuffered: unsupported("buffered"),
    fanoutFence: unsupported("staged_fanout"),
    commitRevisions: unsupported("staged_fanout"),
    registerBabysitterRetention: unsupported("babysitter_retention"),
    babysitterRetentionWritten: unsupported("babysitter_retention"),

    async claim(input: ClaimInput): Promise<ClaimResult> {
      checkClaimInput(input);
      const keys = keysFor(input.tenant);
      const batchKey = keys.batch(input.originId, input.batchId);
      for (let attempt = 0; attempt < attempts; attempt++) {
        // (a) A known batch id: its receipt, a resume, or a batch-id 409.
        const stored = await readDoc(store, batchKey, validBatch, "ledger_batch_record_invalid");
        if (stored) {
          if (stored.doc.originId !== input.originId || stored.doc.batchId !== input.batchId) throw new LedgerUnavailableError("ledger_batch_record_invalid");
          if (stored.doc.digest !== input.batchDigest) return { kind: "batch_conflict", currentDigest: stored.doc.digest };
          if (stored.doc.state === "committed") return { kind: "replay", receiptId: stored.doc.receiptId };
        }
        // (b) + (c): the fence, then the stored revisions of what is left.
        const { doc: fence } = await readFence(keys.fence);
        const judged = await judge(input, keys, fence);
        if (judged.twins > 0) return { kind: "revision_conflict", conflicts: judged.conflicts, count: judged.twins };
        // (d) The claim; committed at once when nothing needs writing (replay_fast).
        const token = judged.fast ? null : newToken();
        const epochs: Record<string, number> = {};
        for (const [s, id] of batchPairs(input.sessions, input.records)) epochs[pairHash(s, id)] = own(fence.epochs, input.originId)?.[pairHash(s, id)] ?? 0;
        const doc: BatchDoc = { v: 1, originId: input.originId, batchId: input.batchId, digest: input.batchDigest, receiptId: input.receiptId,
          state: judged.fast ? "committed" : "pending", claimToken: token, fenceVersion: fence.version, resets: fence.resets, epochs };
        // A lost create or CAS is a concurrent claim of this batch id: answer as a retry would.
        if (!(await casPut(store, batchKey, doc, stored?.version))) { await backoff(attempt); continue; }
        const base = { deleted: judged.deleted, stale: judged.stale, resets: fence.resets };
        if (judged.fast) return { kind: "replay_fast", ...base };
        return { kind: stored ? "resume" : "claimed", claimToken: token!, ...base };
      }
      throw new LedgerUnavailableError("ledger_batch_contended");
    },

    async commit(input: CommitInput): Promise<CommitResult> {
      if (!mapKey(input.originId) || !str(input.batchId) || typeof input.batchDigest !== "string" || !HEX64.test(input.batchDigest)
        || typeof input.claimToken !== "string" || !TOKEN.test(input.claimToken) || typeof input.machineId !== "string" || !/^srv_[0-9a-f]{32}$/.test(input.machineId)
        || !Array.isArray(input.sessions) || !input.sessions.every(pairOk))
        throw new LedgerUnavailableError("ledger_input_refused");
      checkRecords(input.records);
      const keys = keysFor(input.tenant);
      const batchKey = keys.batch(input.originId, input.batchId);
      const stored = await readDoc(store, batchKey, validBatch, "ledger_batch_record_invalid");
      if (!stored || stored.doc.digest !== input.batchDigest || stored.doc.originId !== input.originId || stored.doc.batchId !== input.batchId)
        throw new LedgerUnavailableError("ledger_no_claim");
      if (stored.doc.claimToken !== input.claimToken) throw new LedgerUnavailableError("ledger_attempt_superseded");
      // Already committed by this attempt (a commit retried after its answer was lost): idempotent.
      if (stored.doc.state === "committed") return { rearmed: 0 };
      const snapshot = stored.doc;
      const pairs = batchPairs(input.sessions, input.records);
      if (pairs.some(([s, id]) => !Object.hasOwn(snapshot.epochs, pairHash(s, id)))) throw new LedgerUnavailableError("ledger_attempt_mismatch");

      // ingest_attempt_fenced: a session deleted now, or whose epoch or the reset count moved since the claim.
      const fencedBy = (fence: FenceDoc): { fenced: [string, string][]; deleted: Set<string> } => {
        const deleted = new Set((own(fence.deleted, input.originId) ?? []).map(([s, id]) => pairId(s, id)));
        if (fence.version === snapshot.fenceVersion) return { fenced: [], deleted };
        const fenced = pairs.filter(([s, id]) => deleted.has(pairId(s, id)) || fence.resets !== snapshot.resets
          || (own(fence.epochs, input.originId)?.[pairHash(s, id)] ?? 0) !== snapshot.epochs[pairHash(s, id)]);
        return { fenced, deleted };
      };
      const rearm = (list: [string, string][]) => mapLimit(list, PARALLEL, ([s, id]) =>
        writeRearm(keys.rearm(input.originId, s, id), { originId: input.originId, source: s, sessionId: id, machineId: input.machineId }));

      // 1. The fence, and re-arm markers for what it reached, BEFORE any index merge.
      const first = fencedBy((await readFence(keys.fence)).doc);
      await rearm(first.fenced);
      // 2. The written revisions become stored (max wins, an equal revision keeps the stored digest).
      const firstSet = new Set(first.fenced.map(([s, id]) => pairId(s, id)));
      const groups = new Map<string, { shape: Omit<IndexDoc, "v" | "records">; records: LedgerRecord[] }>();
      for (const r of input.records) {
        if (r.sessionId !== null && firstSet.has(pairId(r.source, r.sessionId))) continue;
        const key = keys.revisions(input.originId, r.source, r.sessionId);
        const g = groups.get(key) ?? { shape: { originId: input.originId, source: r.sessionId === null ? null : r.source, sessionId: r.sessionId }, records: [] };
        g.records.push(r);
        groups.set(key, g);
      }
      await mapLimit([...groups], PARALLEL, ([key, g]) => mergeIndex(key, g.shape, g.records));
      // 3. The fence again: a selection or reset that landed during the merge has its index cleared here
      //    (one that lands after this read clears it itself, after its own fence edit).
      const second = fencedBy((await readFence(keys.fence)).doc);
      const late = second.fenced.filter(([s, id]) => !firstSet.has(pairId(s, id)));
      await rearm(late);
      const fenced = [...first.fenced, ...late];
      await mapLimit(fenced, PARALLEL, ([s, id]) => clearIndex(keys.revisions(input.originId, s, id)));
      // A fenced session the fence no longer deletes (reset or re-enabled since the claim): SQL's invalidated.
      if (fenced.some(([s, id]) => !second.deleted.has(pairId(s, id)))) throw new LedgerUnavailableError("ledger_attempt_superseded");
      // 4. pending -> committed.
      if (!(await casPut(store, batchKey, { ...snapshot, state: "committed" }, stored.version))) {
        const now = await readDoc(store, batchKey, validBatch, "ledger_batch_record_invalid");
        if (!(now && now.doc.state === "committed" && now.doc.claimToken === input.claimToken && now.doc.digest === input.batchDigest))
          throw new LedgerUnavailableError("ledger_attempt_superseded");
      }
      return { rearmed: fenced.length };
    },

    // Decision 0005: no tenant slots. The Lambda's reserved concurrency and SQS batch size bound it.
    async acquireSlot() { return { ok: true }; },
    async releaseSlot() {},

    async setSessionUploads(input: SelectionInput): Promise<void> {
      await applySelection(input);
    },
    async resetUploadedSessions(tenant: LedgerTenant, requestId: string): Promise<number> {
      return (await applyReset(tenant, requestId)).deleted;
    },
    async applyOrderedControl(input: OrderedControlInput): Promise<OrderedControlResult> {
      if (input.custody) throw new LedgerUnavailableError("ledger_s3_custody_unsupported");
      let control: OrderedControl;
      try { control = orderedControl(input.control); } catch { throw new LedgerUnavailableError("ledger_input_refused"); }
      if (control.kind === "selection") {
        if (!("originId" in input)) throw new LedgerUnavailableError("ledger_input_refused");
        const r = await applySelection(input, control);
        return { control: { ...control, outcome: r.outcome }, updated: r.updated };
      }
      if (!("requestId" in input)) throw new LedgerUnavailableError("ledger_input_refused");
      const r = await applyReset(input.tenant, input.requestId, control);
      return { control: { ...control, outcome: r.outcome }, deleted: r.deleted };
    },
  };
}
