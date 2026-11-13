/**
 * The one-off buffered backlog drain (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered
 * backlog"). Since the buffered cutover every upload was staged under
 * `<keyRoot>/<org>/buffered-batches/v1/…staged.ndjson.zst`, and the publisher that would move it into
 * the canonical layout never ran. This works through those objects from S3 alone: no SQL, no outbox.
 * It runs AFTER 0005's one-time fence backfill (step 0) and refuses any user not yet backfilled.
 *
 * Per staged object, in key order, with bounded concurrency:
 *
 *   1. Skip it when its create-only `drained` marker (`<staged key>.drained.json`) exists.
 *   2. Read it (bounded), refuse a buffered-GC redaction barrier, check that the key names exactly this
 *      header and these bytes (org, workspace, machine, batch digest, claim token, sha256 of the stored
 *      frame), and validate every line and its ledger meta.
 *   3. Read the user's fence (`upload-fences.json`) and privacy journal (`privacy-journal.json`).
 *      - Fence absent: `fence_absent`, skipped (not backfilled; it could republish a deleted session).
 *        Fence unreadable: `fence_unreadable`, skipped. Deleted (origin, source, session) lines drop.
 *      - Journal unreadable: skipped. A control pending or unresolved (`ambiguous`) in the journal:
 *        `privacy_control_open`, skipped, as the async consumer waits rather than guess.
 *      - Superseded by a later reset: the fence holds a reset COUNT (`resets`) and the journal a
 *        `seq` watermark with no times; a buffered staged header holds `received_at` but no journal
 *        witness (only async inbox objects carry one). So supersession is determinable only when the
 *        fence also carries `lastResetAtMs` (an optional field this module proposes for the backfill).
 *        `resets > 0` without it: `reset_undeterminable`, skipped. With it, `received_at <=
 *        lastResetAtMs`: `pre_reset`, dropped and marked.
 *   4. Read the per-session revision index (0005 `ledger/revisions/…`) for every surviving session:
 *      a lower revision is `stale`, an equal one with the stored digest is `already_stored`, an equal
 *      one with another digest is a `revision_conflict` and is never written (the reader fold refuses
 *      it, relayhistory-transform.ts chooseRelayhistoryRevision).
 *   5. Write the surviving groups through the writer (`writeGroups` with the staged batch digest and
 *      received_at, lines refitted with `fitLine`), as the staged fan-out would. The fan-out itself
 *      refuses buffered objects on purpose (`buffered_publication_refused`); this is the drain's own
 *      entry and does not touch that check. Idempotent under manifest CAS: a replay of the same batch
 *      digest appends nothing.
 *   6. Merge the written revisions into the index (CAS, max wins, equal keeps the stored digest).
 *   7. Re-read the fence and journal. A deletion of a written session, a reset, or any journal `seq`
 *      move that landed meanwhile writes a create-only re-arm marker for the eraser (0005
 *      `ledger/rearm/…`). Every session dropped as deleted in step 3 is re-armed too: an earlier
 *      partial run of this batch may have written it before the deletion.
 *   8. Write the create-only drained marker. Only now is the object done; any failure before this
 *      leaves it for a rerun, which is safe because every step above is idempotent.
 *
 * Fence, journal and index access sits behind `DrainLedger` so it can be swapped for the exports of
 * src/session-ingest/ledger-s3.ts once that lands; `s3DrainLedger` is the interim implementation of
 * the 0005 object formats.
 *
 * Within one process, batches touching the same (tenant, origin, session) index are serialised, so the
 * index check and the write cannot race each other. Run ONE drain process at a time.
 *
 * Nothing here logs, and the summary holds counts only: never a key, tenant id or content.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { journalKey } from "./async-admission.ts";
import type { LedgerTenant } from "./ledger.ts";
import { fitLine } from "./oversize.ts";
import { BUFFER_STAGE_MAX_BYTES, decodeStaged, type StagedHeader } from "./staging.ts";
import {
  IngestWriteError,
  NO_SESSION_HARNESS,
  NO_SESSION_ID,
  backoffMs,
  liveMachineId,
  writeGroups,
  type Group,
  type LineMeta,
  type WriteTarget,
} from "./writer.ts";

export const DRAINED_SUFFIX = ".drained.json";
export const STAGED_SUFFIX = ".staged.ndjson.zst";
export const MAX_DRAIN_CONCURRENCY = 32;
const INDEX_CAS_ATTEMPTS = 8;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const sha256Hex = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
const HEX64 = /^[0-9a-f]{64}$/;
const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const ident = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v.length <= 512 && !/[\x00-\x1f\x7f]/.test(v);

/** `<root>/<org>/buffered-batches/v1/ws=<ws>/m=srv_<32>/b=<batch32>-<content16>-<claim32>.staged.ndjson.zst` */
const STAGED_KEY =
  /^([a-z0-9][a-z0-9-]{0,62})\/((?!\.)[A-Za-z0-9._-]{1,128})\/buffered-batches\/v1\/ws=((?!\.)[A-Za-z0-9._-]{1,128})\/m=(srv_[0-9a-f]{32})\/b=([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{32})\.staged\.ndjson\.zst$/;

export interface StagedKeyParts {
  keyRoot: string;
  orgId: string;
  workspace: string;
  machine: string;
  batch32: string;
  content16: string;
  claim32: string;
}

export function parseStagedKey(key: string): StagedKeyParts | null {
  const m = STAGED_KEY.exec(key);
  if (!m || key.split("/").some((s) => s === "." || s === "..")) return null;
  return { keyRoot: m[1]!, orgId: m[2]!, workspace: m[3]!, machine: m[4]!, batch32: m[5]!, content16: m[6]!, claim32: m[7]! };
}

// ---- 0005 objects (interim; see DrainLedger) ----

/** 0005 `upload-fences.json`, plus the optional `lastResetAtMs` the drain needs for pre-reset batches. */
export interface UploadFence {
  v: 1;
  resets: number;
  deleted: Record<string, [source: string, sessionId: string][]>;
  /** NOT in 0005 yet: when the last reset was applied. Without it pre-reset batches are not determinable. */
  lastResetAtMs?: number;
}

/** 0005 revision index entry: [revision, revisionId, digest]. */
export type RevisionEntry = [revision: number, revisionId: string, digest: string];
export interface RevisionIndex {
  v: 1;
  records: Record<string, RevisionEntry>;
}

/** What the drain reads from the v3 privacy journal (async-admission.ts). */
export interface JournalState {
  seq: number;
  /** Controls begun and not yet settled. */
  pending: number;
  /** Controls a consumer completed conservatively; unresolved until their next attempt. */
  ambiguous: number;
}

export interface RearmEntry {
  originId: string;
  source: string;
  sessionId: string;
  machineId: string;
  atMs: number;
}

/**
 * What the drain needs from the S3 ledger. Swap for ledger-s3.ts's exports once that module lands;
 * every method must fail closed (throw) on an unreadable object, never treat it as absent.
 */
export interface DrainLedger {
  /** null when the user has no fence object. */
  readFence(tenant: LedgerTenant): Promise<UploadFence | null>;
  /** null when the user has no journal (no ordered control ever ran). */
  readJournal(tenant: LedgerTenant): Promise<JournalState | null>;
  /** null when the session has no index yet. `sessionId` null is the origin's `_origin` index. */
  readRevisions(tenant: LedgerTenant, originId: string, source: string, sessionId: string | null): Promise<RevisionIndex | null>;
  /** CAS merge: max revision wins, an equal revision keeps the stored entry. */
  mergeRevisions(tenant: LedgerTenant, originId: string, source: string, sessionId: string | null, records: Record<string, RevisionEntry>): Promise<void>;
  /** Create-only re-arm marker for the eraser. */
  rearm(tenant: LedgerTenant, entry: RearmEntry): Promise<void>;
}

export class DrainLedgerError extends Error {
  constructor(readonly code: "fence_invalid" | "journal_invalid" | "revision_index_invalid" | "revision_index_contended") {
    super(`buffered drain ledger: ${code}`);
    this.name = "DrainLedgerError";
  }
}

/** The per-user admission directory that holds `privacy-journal.json` (async-admission.ts). */
export function userAdmissionDir(keyRoot: string, tenant: LedgerTenant): string {
  const key = journalKey(keyRoot, tenant);
  return key.slice(0, key.length - "/privacy-journal.json".length);
}
export const uploadFenceKey = (keyRoot: string, tenant: LedgerTenant) => `${userAdmissionDir(keyRoot, tenant)}/upload-fences.json`;
export function revisionScope(source: string, sessionId: string | null): string {
  return sessionId === null ? "_origin" : sha256Hex(`${source}\u001f${sessionId}`).slice(0, 32);
}
export const revisionIndexKey = (keyRoot: string, tenant: LedgerTenant, originId: string, source: string, sessionId: string | null) =>
  `${userAdmissionDir(keyRoot, tenant)}/ledger/revisions/o=${sha256Hex(originId).slice(0, 32)}/s=${revisionScope(source, sessionId)}.json`;
export const rearmKey = (keyRoot: string, tenant: LedgerTenant, source: string, sessionId: string) =>
  `${userAdmissionDir(keyRoot, tenant)}/ledger/rearm/${sha256Hex(`${source}\u001f${sessionId}`).slice(0, 32)}.json`;

export function parseFence(text: string): UploadFence {
  let v: unknown;
  try {
    v = JSON.parse(text);
  } catch {
    throw new DrainLedgerError("fence_invalid");
  }
  if (!isObject(v) || v.v !== 1 || !Number.isSafeInteger(v.resets) || (v.resets as number) < 0 || !isObject(v.deleted)) throw new DrainLedgerError("fence_invalid");
  if (v.lastResetAtMs !== undefined && (!Number.isSafeInteger(v.lastResetAtMs) || (v.lastResetAtMs as number) < 0)) throw new DrainLedgerError("fence_invalid");
  for (const pairs of Object.values(v.deleted)) {
    if (!Array.isArray(pairs)) throw new DrainLedgerError("fence_invalid");
    for (const p of pairs) if (!Array.isArray(p) || p.length !== 2 || typeof p[0] !== "string" || typeof p[1] !== "string") throw new DrainLedgerError("fence_invalid");
  }
  return v as unknown as UploadFence;
}

/** The v3 journal shape (async-admission.ts validJournal); anything else fails closed. */
export function parseJournal(text: string): JournalState {
  let v: unknown;
  try {
    v = JSON.parse(text);
  } catch {
    throw new DrainLedgerError("journal_invalid");
  }
  if (!isObject(v) || v.v !== 3 || !Number.isSafeInteger(v.seq) || (v.seq as number) < 0 || !Array.isArray(v.pending) || !Array.isArray(v.ambiguous) ||
      !(v.pending as unknown[]).every((p) => isObject(p) && typeof p.id === "string" && Number.isSafeInteger(p.startMs)) ||
      !(v.ambiguous as unknown[]).every((a) => typeof a === "string")) throw new DrainLedgerError("journal_invalid");
  return { seq: v.seq as number, pending: v.pending.length, ambiguous: v.ambiguous.length };
}

export function parseRevisionIndex(text: string): RevisionIndex {
  let v: unknown;
  try {
    v = JSON.parse(text);
  } catch {
    throw new DrainLedgerError("revision_index_invalid");
  }
  if (!isObject(v) || v.v !== 1 || !isObject(v.records)) throw new DrainLedgerError("revision_index_invalid");
  for (const e of Object.values(v.records))
    if (!Array.isArray(e) || e.length !== 3 || !Number.isSafeInteger(e[0]) || (e[0] as number) < 0 || typeof e[1] !== "string" || typeof e[2] !== "string")
      throw new DrainLedgerError("revision_index_invalid");
  return v as unknown as RevisionIndex;
}

/** Commutative merge (0005 "Risks"): max wins; an equal revision keeps the stored entry. */
export function mergeRevisionRecords(stored: Record<string, RevisionEntry>, incoming: Record<string, RevisionEntry>): { records: Record<string, RevisionEntry>; changed: boolean } {
  const records = { ...stored };
  let changed = false;
  for (const [id, entry] of Object.entries(incoming)) {
    const prev = records[id];
    if (!prev || entry[0] > prev[0]) {
      records[id] = entry;
      changed = true;
    }
  }
  return { records, changed };
}

async function readText(store: ObjectStore, key: string, max = 8 * 1024 * 1024): Promise<{ text: string; version: ObjectVersion } | null> {
  let object;
  try {
    object = await store.getObject(objectKey(key));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
  const bytes = await boundedRead(object.body, max);
  return { text: decoder.decode(bytes), version: object.meta.version };
}

async function boundedRead(body: ReadableStream<Uint8Array>, max: number): Promise<Uint8Array> {
  const reader = body.getReader();
  const parts: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const r = await reader.read();
      if (r.done) break;
      size += r.value.byteLength;
      if (size > max) throw new Error("object_too_large");
      parts.push(r.value);
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
  const out = new Uint8Array(size);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.byteLength;
  }
  return out;
}

/** The 0005 object formats over an ObjectStore. Interim: replace with ledger-s3.ts. */
export function s3DrainLedger(store: ObjectStore, keyRoot: string, opts: { sleep?: (ms: number) => Promise<void>; random?: () => number } = {}): DrainLedger {
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  return {
    async readFence(tenant) {
      const got = await readText(store, uploadFenceKey(keyRoot, tenant));
      return got ? parseFence(got.text) : null;
    },
    async readJournal(tenant) {
      const got = await readText(store, journalKey(keyRoot, tenant));
      return got ? parseJournal(got.text) : null;
    },
    async readRevisions(tenant, originId, source, sessionId) {
      const got = await readText(store, revisionIndexKey(keyRoot, tenant, originId, source, sessionId));
      return got ? parseRevisionIndex(got.text) : null;
    },
    async mergeRevisions(tenant, originId, source, sessionId, incoming) {
      const key = revisionIndexKey(keyRoot, tenant, originId, source, sessionId);
      for (let attempt = 0; attempt < INDEX_CAS_ATTEMPTS; attempt++) {
        const got = await readText(store, key);
        const stored = got ? parseRevisionIndex(got.text) : null;
        const { records, changed } = mergeRevisionRecords(stored?.records ?? {}, incoming);
        if (stored && !changed) return;
        const body = encoder.encode(JSON.stringify({ v: 1, records } satisfies RevisionIndex));
        try {
          await store.put(objectKey(key), body, got ? { ifMatch: got.version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
          return;
        } catch (error) {
          if (!isObjectStoreError(error, "precondition_failed") && !isObjectStoreError(error, "already_exists")) throw error;
          await sleep(backoffMs(attempt, opts.random));
        }
      }
      throw new DrainLedgerError("revision_index_contended");
    },
    async rearm(tenant, entry) {
      const body = encoder.encode(JSON.stringify({ v: 1, ...entry }));
      try {
        await store.put(objectKey(rearmKey(keyRoot, tenant, entry.source, entry.sessionId)), body, { ifNoneMatch: true, contentType: "application/json" });
      } catch (error) {
        // An earlier re-arm for this session is still pending: the eraser sweeps the session either way.
        if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
      }
    },
  };
}

/** A store that refuses every mutation: the dry run's defence in depth. */
export function readOnlyStore(store: ObjectStore): ObjectStore {
  return new Proxy(store, {
    get(target, prop) {
      if (prop === "put" || prop === "delete") return async () => { throw new Error("drain_dry_run_write_refused"); };
      const value = (target as unknown as Record<PropertyKey, unknown>)[prop];
      return typeof value === "function" ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  });
}

// ---- classification ----

export type ObjectOutcome =
  | "drained"
  | "would_drain"
  | "already_drained"
  | "pre_reset"
  | "redacted"
  | "vanished"
  | "corrupt"
  | "key_mismatch"
  | "ambiguous_replacement"
  | "fence_absent"
  | "fence_unreadable"
  | "journal_unreadable"
  | "privacy_control_open"
  | "reset_undeterminable"
  | "index_unreadable"
  | "write_failed"
  | "index_merge_failed"
  | "fence_recheck_failed"
  | "marker_failed"
  | "internal_error";

const OUTCOMES: ObjectOutcome[] = [
  "drained", "would_drain", "already_drained", "pre_reset", "redacted", "vanished", "corrupt", "key_mismatch",
  "ambiguous_replacement", "fence_absent", "fence_unreadable", "journal_unreadable", "privacy_control_open", "reset_undeterminable", "index_unreadable", "write_failed",
  "index_merge_failed", "fence_recheck_failed", "marker_failed", "internal_error",
];

export interface DrainSummary {
  mode: "dry_run" | "execute";
  orgs: number;
  /** Staged objects listed (after start-after). */
  listed: number;
  /** Listed keys that are neither a staged object nor a drained marker. */
  foreign_keys: number;
  objects: Record<ObjectOutcome, number>;
  lines: {
    /** Lines in drainable batches (after the reset check). */
    candidate: number;
    /** Written (execute) or would be written (dry run). */
    written: number;
    fenced: number;
    stale: number;
    already_stored: number;
    revision_conflict: number;
  };
  /** Session groups written (execute) or that would be (dry run). */
  sessions_written: number;
  rearmed: number;
  /** Write failures by closed code (IngestWriteError code, or `storage`). */
  write_failures: Record<string, number>;
  stopped: "complete" | "max_objects";
}

function emptySummary(mode: DrainSummary["mode"]): DrainSummary {
  return {
    mode, orgs: 0, listed: 0, foreign_keys: 0,
    objects: Object.fromEntries(OUTCOMES.map((o) => [o, 0])) as Record<ObjectOutcome, number>,
    lines: { candidate: 0, written: 0, fenced: 0, stale: 0, already_stored: 0, revision_conflict: 0 },
    sessions_written: 0, rearmed: 0, write_failures: {}, stopped: "complete",
  };
}

type Decoded = { header: StagedHeader; groups: Group[]; sha256: string };
type Read = { kind: "live"; decoded: Decoded } | { kind: "redacted" | "vanished" | "corrupt" | "key_mismatch" | "internal_error" };

/** Validate a decoded staged object against its key and the buffered line contract. */
export function validateStaged(key: StagedKeyParts, packedSha256: string, header: StagedHeader, groups: Group[]): "ok" | "corrupt" | "key_mismatch" {
  const h = header as unknown as Record<string, unknown>;
  if (h.publication_state !== "buffered" || !ident(h.org_id) || !ident(h.workspace_id) || !ident(h.origin_id) || !ident(h.user_id) ||
      typeof h.batch_digest !== "string" || !HEX64.test(h.batch_digest) || typeof h.received_at !== "string" || !Number.isFinite(Date.parse(h.received_at)) ||
      !Array.isArray(h.groups) || h.groups.length === 0 || h.groups.length > 500) return "corrupt";
  if (header.org_id !== key.orgId || layoutComponent(header.workspace_id) !== key.workspace ||
      liveMachineId(header.workspace_id, header.origin_id, header.user_id) !== key.machine ||
      sha256Hex(header.batch_digest).slice(0, 32) !== key.batch32 || sha256Hex(header.claim_token).slice(0, 32) !== key.claim32 ||
      packedSha256.slice(0, 16) !== key.content16) return "key_mismatch";
  for (const g of groups) {
    if (!ident(g.source) || !ident(g.sessionId) || g.lines.length === 0 || g.meta?.length !== g.lines.length) return "corrupt";
    const origin = g.source === NO_SESSION_HARNESS && g.sessionId === NO_SESSION_ID;
    for (let i = 0; i < g.lines.length; i++) {
      const [revisionId, digest, source, sessionId] = g.meta[i]! as LineMeta;
      if (!ident(revisionId) || typeof digest !== "string" || !HEX64.test(digest) || !ident(source)) return "corrupt";
      if (sessionId === null ? !origin : !(ident(sessionId) && source === g.source && sessionId === g.sessionId)) return "corrupt";
      let line: unknown;
      try {
        line = JSON.parse(g.lines[i]!);
      } catch {
        return "corrupt";
      }
      if (!isObject(line) || !ident(line.record_id) || line.origin_id !== header.origin_id || typeof line.revision !== "string" ||
          !/^[1-9][0-9]{0,15}$/.test(line.revision) || !Number.isSafeInteger(Number(line.revision)) ||
          (line.operation !== "upsert" && line.operation !== "delete")) return "corrupt";
    }
  }
  return "ok";
}

async function readStagedObject(store: ObjectStore, key: string, parts: StagedKeyParts, decompress: (b: Uint8Array) => Uint8Array): Promise<Read> {
  let object;
  try {
    object = await store.getObject(objectKey(key));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return { kind: "vanished" };
    throw error;
  }
  let packed: Uint8Array;
  try {
    packed = await boundedRead(object.body, BUFFER_STAGE_MAX_BYTES);
  } catch (error) {
    if ((error as Error)?.message === "object_too_large") return { kind: "corrupt" };
    throw error;
  }
  // buffered-gc.ts ensureBufferedBarrier: the original key overwritten by a small JSON redaction barrier.
  if (packed[0] === 0x7b) {
    try {
      const v = JSON.parse(decoder.decode(packed)) as { kind?: unknown };
      return { kind: isObject(v) && v.kind === "buffered-redaction" ? "redacted" : "corrupt" };
    } catch {
      return { kind: "corrupt" };
    }
  }
  try {
    const raw = decompress(packed);
    if (raw.byteLength > BUFFER_STAGE_MAX_BYTES) return { kind: "corrupt" };
    const { header, groups } = decodeStaged(raw);
    const sha256 = sha256Hex(packed);
    const verdict = validateStaged(parts, sha256, header, groups);
    return verdict === "ok" ? { kind: "live", decoded: { header, groups, sha256 } } : { kind: verdict };
  } catch {
    return { kind: "corrupt" };
  }
}

// ---- listing ----

interface BatchGroup {
  /** Staged keys sharing one `…/b=<batch32>-` prefix, in key order. */
  staged: string[];
  markers: Set<string>;
  lastKey: string;
}

/**
 * Listed keys grouped by `…/m=…/b=<batch32>`. Every object of one batch digest (retries with other
 * claim tokens, a GC replacement, their markers) sorts contiguously, so the group is complete.
 */
async function* batchGroups(store: ObjectStore, prefix: string, startAfter: string | undefined, summary: DrainSummary): AsyncGenerator<BatchGroup> {
  let current: (BatchGroup & { id: string }) | null = null;
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    cursor = page.nextCursor;
    for (const item of page.items) {
      const key = String(item.key);
      if (startAfter !== undefined && key <= startAfter) continue;
      const staged = key.endsWith(DRAINED_SUFFIX) ? key.slice(0, -DRAINED_SUFFIX.length) : key;
      const parts = parseStagedKey(staged);
      if (!parts) {
        summary.foreign_keys++;
        continue;
      }
      const id = staged.slice(0, staged.lastIndexOf("/b=") + 3 + 32);
      if (current && current.id !== id) {
        yield current;
        current = null;
      }
      current ??= { id, staged: [], markers: new Set(), lastKey: key };
      current.lastKey = key;
      if (staged === key) current.staged.push(key);
      else current.markers.add(staged);
    }
  } while (cursor);
  if (current) yield current;
}

// ---- locks ----

/** Per-key FIFO mutex; multi-key acquisition in sorted order, so two batches never deadlock. */
class KeyedLocks {
  readonly #tails = new Map<string, Promise<void>>();
  async acquire(keys: string[]): Promise<() => void> {
    const releases: (() => void)[] = [];
    for (const key of [...new Set(keys)].sort()) releases.push(await this.#one(key));
    return () => releases.reverse().forEach((r) => r());
  }
  async #one(key: string): Promise<() => void> {
    const prev = this.#tails.get(key) ?? Promise.resolve();
    let release!: () => void;
    const mine = new Promise<void>((r) => (release = r));
    const tail = prev.then(() => mine);
    this.#tails.set(key, tail);
    await prev;
    return () => {
      release();
      if (this.#tails.get(key) === tail) this.#tails.delete(key);
    };
  }
}

// ---- the drain ----

export interface DrainOptions {
  store: ObjectStore;
  keyRoot: string;
  /** Orgs to drain. Required: discovery (a delimiter listing) is the caller's (scripts/buffered-drain.ts). */
  orgs: string[];
  dryRun: boolean;
  /** Ledger override (tests; later ledger-s3.ts). Default: s3DrainLedger over `store`. */
  ledger?: DrainLedger;
  decompress: (bytes: Uint8Array) => Uint8Array;
  /** Resume: skip every key at or before this one (S3 key order). */
  startAfter?: string;
  concurrency?: number;
  /** Stop pulling new batches after this many staged objects. */
  maxObjects?: number;
  /** Called with the low watermark: every key at or before it is processed. Persist it to resume. */
  onCheckpoint?: (key: string) => void | Promise<void>;
  /** Called after each batch group with the running summary (counts only). */
  onProgress?: (summary: DrainSummary) => void;
  rawChunkSealer?: WriteTarget["rawChunkSealer"];
  now?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

/** S3 key order for org prefixes: compare `<org>/`, not `<org>` (`a-b/` sorts before `a/`). */
const orgOrder = (orgs: string[]) => [...new Set(orgs)].sort((a, b) => (`${a}/` < `${b}/` ? -1 : `${a}/` > `${b}/` ? 1 : 0));

export async function drainBufferedBatches(opts: DrainOptions): Promise<DrainSummary> {
  const summary = emptySummary(opts.dryRun ? "dry_run" : "execute");
  const concurrency = opts.concurrency ?? 4;
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > MAX_DRAIN_CONCURRENCY) throw new Error("drain_concurrency_invalid");
  if (opts.maxObjects !== undefined && (!Number.isSafeInteger(opts.maxObjects) || opts.maxObjects < 1)) throw new Error("drain_max_objects_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new Error("drain_key_root_invalid");
  for (const org of opts.orgs) if (!/^(?!\.)[A-Za-z0-9._-]{1,128}$/.test(org)) throw new Error("drain_org_invalid");
  const store = opts.dryRun ? readOnlyStore(opts.store) : opts.store;
  const ledger = opts.ledger ?? s3DrainLedger(store, opts.keyRoot, { ...(opts.sleep ? { sleep: opts.sleep } : {}), ...(opts.random ? { random: opts.random } : {}) });
  const target: WriteTarget = {
    store, keyRoot: opts.keyRoot,
    ...(opts.rawChunkSealer ? { rawChunkSealer: opts.rawChunkSealer } : {}),
    ...(opts.now ? { now: opts.now } : {}),
    ...(opts.sleep ? { sleep: opts.sleep } : {}),
    ...(opts.random ? { random: opts.random } : {}),
  };
  const locks = new KeyedLocks();
  const orgs = orgOrder(opts.orgs);
  summary.orgs = orgs.length;

  async function* allGroups(): AsyncGenerator<BatchGroup> {
    for (const org of orgs) {
      const prefix = `${opts.keyRoot}/${org}/buffered-batches/v1`;
      // Every key under a prefix that sorts before start-after, and does not contain it, is before it.
      if (opts.startAfter !== undefined && `${prefix}/` < opts.startAfter && !opts.startAfter.startsWith(`${prefix}/`)) continue;
      yield* batchGroups(opts.store, prefix, opts.startAfter, summary);
    }
  }

  // Low-watermark checkpointing over concurrent groups.
  let pulled = 0, nextToCommit = 0;
  const done = new Map<number, string>();
  let checkpointChain: Promise<void> = Promise.resolve();
  let checkpointError: unknown;
  const complete = (seq: number, lastKey: string) => {
    done.set(seq, lastKey);
    let mark: string | undefined;
    while (done.has(nextToCommit)) {
      mark = done.get(nextToCommit);
      done.delete(nextToCommit++);
    }
    if (mark !== undefined && opts.onCheckpoint) {
      const key = mark;
      checkpointChain = checkpointChain.then(() => (checkpointError === undefined ? opts.onCheckpoint!(key) : undefined)).catch((e) => { checkpointError ??= e; });
    }
  };

  let objects = 0;
  let fatal: { error: unknown } | undefined;
  const iterator = allGroups();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      for (;;) {
        if (fatal || checkpointError !== undefined) return;
        if (opts.maxObjects !== undefined && objects >= opts.maxObjects) {
          summary.stopped = "max_objects";
          return;
        }
        let next: IteratorResult<BatchGroup>;
        try {
          next = await iterator.next();
        } catch (error) {
          // A listing failure is fatal, but in-flight batches finish first (each is idempotent anyway).
          fatal ??= { error };
          return;
        }
        if (next.done) return;
        const group = next.value;
        const seq = pulled++;
        objects += group.staged.length;
        summary.listed += group.staged.length;
        await drainGroup(group);
        complete(seq, group.lastKey);
        opts.onProgress?.(summary);
      }
    }),
  );
  await checkpointChain;
  if (fatal) throw fatal.error;
  if (checkpointError !== undefined) throw checkpointError;
  return summary;

  async function drainGroup(group: BatchGroup) {
    // Read everything not yet drained, then decide per claim token (GC replacement detection).
    const reads = new Map<string, Read>();
    for (const key of group.staged) {
      if (group.markers.has(key)) continue;
      try {
        reads.set(key, await readStagedObject(store, key, parseStagedKey(key)!, opts.decompress));
      } catch {
        // A storage failure (not a content verdict): left for a rerun.
        reads.set(key, { kind: "internal_error" });
      }
    }
    for (const key of group.staged) {
      if (group.markers.has(key)) {
        summary.objects.already_drained++;
        continue;
      }
      const read = reads.get(key)!;
      if (read.kind !== "live") {
        summary.objects[read.kind]++;
        continue;
      }
      // A buffered-GC replacement keeps the batch digest and claim token under a new content hash. More
      // than one live (or drained, or unreadable just now) object for one claim means a GC swap was
      // interrupted: never guess.
      const claim32 = parseStagedKey(key)!.claim32;
      const peers = group.staged.filter((k) => k !== key && parseStagedKey(k)!.claim32 === claim32 && (group.markers.has(k) || reads.get(k)?.kind === "live" || reads.get(k)?.kind === "internal_error"));
      if (peers.length) {
        summary.objects.ambiguous_replacement++;
        continue;
      }
      try {
        summary.objects[await drainOne(key, read.decoded)]++;
      } catch {
        summary.objects.internal_error++;
      }
    }
  }

  async function drainOne(key: string, { header, groups, sha256 }: Decoded): Promise<ObjectOutcome> {
    const tenant: LedgerTenant = { orgId: header.org_id, workspaceId: header.workspace_id, userId: header.user_id };
    const receivedMs = Date.parse(header.received_at);
    let fence: UploadFence | null;
    try {
      fence = await ledger.readFence(tenant);
    } catch {
      return "fence_unreadable";
    }
    // 0005 step 0: a user without a backfilled fence may have deletions recorded only in SQL.
    if (!fence) return "fence_absent";
    let journal: JournalState | null;
    try {
      journal = await ledger.readJournal(tenant);
    } catch {
      return "journal_unreadable";
    }
    if (journal && (journal.pending > 0 || journal.ambiguous > 0)) return "privacy_control_open";
    const counts = { candidate: 0, written: 0, fenced: 0, stale: 0, already_stored: 0, revision_conflict: 0 };
    if (fence.resets > 0) {
      if (fence.lastResetAtMs === undefined) return "reset_undeterminable";
      if (receivedMs <= fence.lastResetAtMs) {
        if (!opts.dryRun && !(await putMarker(key, sha256, "pre_reset", counts, 0))) return "marker_failed";
        return "pre_reset";
      }
    }
    const deleted = new Set((fence.deleted[header.origin_id] ?? []).map((p) => JSON.stringify(p)));
    type Kept = { group: Group; lines: string[]; meta: LineMeta[] };
    const kept: Kept[] = [];
    /** Deleted sessions this batch carries: an earlier partial run may have written them (re-armed below). */
    const fencedSessions = new Map<string, [source: string, sessionId: string]>();
    for (const g of groups) {
      const k: Kept = { group: g, lines: [], meta: [] };
      g.lines.forEach((line, i) => {
        counts.candidate++;
        const m = g.meta![i]!;
        if (m[3] !== null && deleted.has(JSON.stringify([m[2], m[3]]))) {
          counts.fenced++;
          fencedSessions.set(JSON.stringify([m[2], m[3]]), [m[2], m[3]]);
        } else {
          k.lines.push(line);
          k.meta.push(m);
        }
      });
      if (k.lines.length) kept.push(k);
    }

    // Revision index scopes for the surviving lines.
    const scopeOf = (m: LineMeta) => `${m[3] === null ? "_origin" : revisionScope(m[2], m[3])}`;
    const scopes = new Map<string, { source: string; sessionId: string | null }>();
    for (const k of kept) for (const m of k.meta) scopes.set(scopeOf(m), { source: m[2], sessionId: m[3] });
    const lockPrefix = JSON.stringify([tenant.orgId, tenant.workspaceId, tenant.userId, header.origin_id]);
    const release = await locks.acquire([...scopes.keys()].map((s) => `${lockPrefix}\u001f${s}`));
    try {
      const indexes = new Map<string, Record<string, RevisionEntry>>();
      try {
        for (const [scope, { source, sessionId }] of scopes) indexes.set(scope, (await ledger.readRevisions(tenant, header.origin_id, source, sessionId))?.records ?? {});
      } catch {
        return "index_unreadable";
      }
      const toWrite: Group[] = [];
      const merges = new Map<string, Record<string, RevisionEntry>>();
      const inBatch = new Map<string, RevisionEntry>();
      for (const k of kept) {
        const lines: string[] = [];
        const meta: LineMeta[] = [];
        k.lines.forEach((line, i) => {
          const m = k.meta[i]!;
          const r = JSON.parse(line) as { record_id: string; revision: string };
          const revision = Number(r.revision);
          const scope = scopeOf(m);
          const stored = indexes.get(scope)![r.record_id];
          const seen = inBatch.get(`${scope}\u001f${r.record_id}`);
          if (stored && revision < stored[0]) counts.stale++;
          else if (stored && revision === stored[0]) {
            if (stored[2] === m[1]) counts.already_stored++;
            else counts.revision_conflict++;
          } else if (seen && revision === seen[0] && seen[2] !== m[1]) counts.revision_conflict++;
          else {
            const entry: RevisionEntry = [revision, m[0], m[1]];
            inBatch.set(`${scope}\u001f${r.record_id}`, entry);
            const into = merges.get(scope) ?? {};
            into[r.record_id] = mergeRevisionRecords(into, { [r.record_id]: entry }).records[r.record_id]!;
            merges.set(scope, into);
            lines.push(fitLine(line));
            meta.push(m);
          }
        });
        if (lines.length) toWrite.push({ source: k.group.source, sessionId: k.group.sessionId, lines, meta });
      }
      counts.written = toWrite.reduce((n, g) => n + g.lines.length, 0);

      if (opts.dryRun) {
        addCounts(counts, toWrite.length);
        return "would_drain";
      }
      let machineId = liveMachineId(header.workspace_id, header.origin_id, header.user_id);
      if (toWrite.length) {
        try {
          machineId = (await writeGroups(target, { orgId: header.org_id, workspaceId: header.workspace_id, originId: header.origin_id, userId: header.user_id, batchDigest: header.batch_digest }, toWrite, new Date(receivedMs))).machineId;
        } catch (error) {
          const code = error instanceof IngestWriteError ? error.code : "storage";
          summary.write_failures[code] = (summary.write_failures[code] ?? 0) + 1;
          return "write_failed";
        }
      }
      try {
        for (const [scope, records] of merges) {
          const { source, sessionId } = scopes.get(scope)!;
          await ledger.mergeRevisions(tenant, header.origin_id, source, sessionId, records);
        }
      } catch {
        return "index_merge_failed";
      }
      // 0005 commit recheck: a deletion or reset that landed while this batch was written re-arms erasure.
      try {
        const after = await ledger.readFence(tenant);
        const journalAfter = await ledger.readJournal(tenant);
        const deletedAfter = new Set((after?.deleted[header.origin_id] ?? []).map((p) => JSON.stringify(p)));
        // A vanished fence, a moved reset, or any privacy control that began or applied meanwhile: re-arm.
        const resetMoved = !after || after.resets !== fence.resets || after.lastResetAtMs !== fence.lastResetAtMs ||
          (journalAfter?.seq ?? 0) !== (journal?.seq ?? 0) || (journalAfter?.pending ?? 0) > 0 || (journalAfter?.ambiguous ?? 0) > 0;
        const atMs = (opts.now ?? (() => new Date()))().getTime();
        const rearm = new Map(fencedSessions);
        for (const g of toWrite) {
          const sid = g.meta![0]![3];
          if (sid === null) continue;
          const source = g.meta![0]![2];
          if (resetMoved || deletedAfter.has(JSON.stringify([source, sid]))) rearm.set(JSON.stringify([source, sid]), [source, sid]);
        }
        // Create-only and keyed per session, so a deleted session is re-armed at most once per user.
        for (const [source, sessionId] of rearm.values()) {
          await ledger.rearm(tenant, { originId: header.origin_id, source, sessionId, machineId, atMs });
          summary.rearmed++;
        }
      } catch {
        return "fence_recheck_failed";
      }
      if (!(await putMarker(key, sha256, "drained", counts, toWrite.length))) return "marker_failed";
      addCounts(counts, toWrite.length);
      return "drained";
    } finally {
      release();
    }
  }

  function addCounts(c: DrainSummary["lines"], sessions: number) {
    for (const k of Object.keys(c) as (keyof DrainSummary["lines"])[]) summary.lines[k] += c[k];
    summary.sessions_written += sessions;
  }

  async function putMarker(key: string, sha256: string, outcome: "drained" | "pre_reset", counts: DrainSummary["lines"], sessions: number): Promise<boolean> {
    const at = (opts.now ?? (() => new Date()))().toISOString();
    const body = encoder.encode(JSON.stringify({ v: 1, kind: "buffered-drained", outcome, at, staged_sha256: sha256, sessions_written: sessions, lines: counts }));
    try {
      await store.put(objectKey(key + DRAINED_SUFFIX), body, { ifNoneMatch: true, contentType: "application/json" });
      return true;
    } catch (error) {
      // A concurrent or earlier run marked it: the object is drained either way.
      return isObjectStoreError(error, "already_exists") || isObjectStoreError(error, "precondition_failed");
    }
  }
}
