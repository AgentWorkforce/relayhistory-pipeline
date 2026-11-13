/**
 * The one-off buffered backlog drain (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered
 * backlog"). Since the buffered cutover every upload was staged under
 * `<keyRoot>/<org>/buffered-batches/v1/…staged.ndjson.zst`, and the publisher that would move it into
 * the canonical layout never ran. This works through those objects from S3 alone: no SQL, no outbox.
 * It runs AFTER 0005's one-time fence backfill (step 0) and refuses any user not yet backfilled.
 *
 * Per staged object, in key order, with bounded concurrency:
 *
 *   0b. --execute writes nothing of an org without a valid fence-authority marker, by admission's own rule
 *      (ledger-s3 requireFenceAuthority: per-org marker, else the deployment-wide one): `fence_authority_missing`
 *      or `fence_authority_invalid`. The dry run classifies anyway and counts `orgs_without_authority`.
 *   1. Skip it when its create-only `drained` marker (`<staged key>.drained.json`) exists.
 *   2. Read it (bounded), refuse a buffered-GC redaction barrier, check that the key names exactly this
 *      header and these bytes (org, workspace, machine, batch digest, claim token, sha256 of the stored
 *      frame), and validate every line and its ledger meta.
 *   3. Read the user's fence (`upload-fences.json`) and privacy journal (`privacy-journal.json`).
 *      - As admission: a PENDING ordered reset (`hasPendingReset`) is `control_pending`, skipped; a pending ordered
 *        disable's sessions count as deleted (`isDeletedForAdmission`). Fences may be v1 or v2 (v2 while pending).
 *      - Fence absent: `fence_absent`, skipped (not backfilled; it could republish a deleted session).
 *        Fence unreadable: `fence_unreadable`, skipped. Deleted (origin, source, session) lines drop.
 *      - Journal unreadable: skipped. A control pending or unresolved (`ambiguous`) in the journal:
 *        `privacy_control_open`, skipped, as the async consumer waits rather than guess.
 *      - Superseded by a later reset: the fence holds a reset COUNT (`resets`) and the journal a
 *        `seq` watermark with no times; a buffered staged header holds `received_at` but no journal
 *        witness (only async inbox objects carry one). So supersession uses the fence's
 *        `lastResetAtMs` (ledger-s3; the backfill sets it from SQL's reset times). `resets > 0` with
 *        `lastResetAtMs` 0: `reset_undeterminable`, skipped. A reset begun but not finished
 *        (`resetsCompleted < resets`): `reset_in_flight`, skipped (a claim finishes it; rerun after). With it, and a clock-skew margin M
 *        (default 5 minutes, floor 1 minute; `received_at` is the Worker's clock, `lastResetAtMs`
 *        the database's): `received_at <= lastResetAtMs - M` is `pre_reset`, dropped and marked;
 *        within M on either side of the reset is `reset_margin`, skipped and left undrained (never
 *        written, never marked); only `received_at >= lastResetAtMs + M` drains.
 *   4. Read the per-origin revision index shards (ledger-s3, per (origin, record id)). As the claim
 *      does, an entry the fence has forgotten (`entryForgotten`: a disable or reset since) counts as
 *      absent. Then an equal or higher stored revision is skipped, counted as `stale` (lower),
 *      `already_stored` (equal, same digest) or `revision_conflict` (equal, another digest: never
 *      written, since the reader fold refuses it, relayhistory-transform.ts chooseRelayhistoryRevision).
 *   4b. Register the ownership stubs of the sessions to be written (ledger-s3 `registerOwner`) BEFORE any
 *      canonical write, as the claim does, then re-read the fence: if it moved, `fence_moved` (rerun).
 *   5. Write the surviving groups through the writer (`writeGroups` with the staged batch digest and
 *      received_at, lines refitted with `fitLine`), as the staged fan-out would. The fan-out itself
 *      refuses buffered objects on purpose (`buffered_publication_refused`); this is the drain's own
 *      entry and does not touch that check. Idempotent under manifest CAS: a replay of the same batch
 *      digest appends nothing.
 *   6. As ledger-s3's commit: re-read the fence and journal; a deletion, epoch bump or reset that reached
 *      a written session, or any journal `seq` move, writes a create-only re-arm marker for the eraser
 *      (`writeRearm`) BEFORE any index merge.
 *   7. Merge the other written revisions with `mergeIndex`, stamped with step 3's generation (max wins,
 *      equal keeps the stored digest, forgotten entries dropped). Every session dropped as deleted in step 3 is re-armed too: an earlier
 *      partial run of this batch may have written it before the deletion.
 *   8. Write the create-only drained marker. Only now is the object done; any failure before this
 *      leaves it for a rerun, which is safe because every step above is idempotent.
 *
 * Fence, index and re-arm access goes through ledger-s3.ts `s3LedgerObjects` (`s3DrainLedger` adapts it
 * behind `DrainLedger`, which tests override), so the drain reads and writes exactly the formats and
 * semantics of the upload consumer and the control mirror. Only the privacy journal is read here.
 *
 * Within one process, batches touching the same (tenant, origin, session) index are serialised, so the
 * index check and the write cannot race each other. Across processes, an execute run holds a
 * create-only lease at `<keyRoot>/.drain/lock.json` (holder + expiry, CAS-renewed) and every run refuses
 * to start while an unexpired lease exists: ONE drain process at a time.
 *
 * Nothing here logs, and the summary holds counts only: never a key, tenant id or content.
 */
import { createHash, randomUUID } from "node:crypto";
import { isObjectStoreError, objectKey, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { journalKey } from "./async-admission.ts";
import type { LedgerTenant } from "./ledger.ts";
import {
  entryForgotten, fenceAuthorityKey, fenceEpoch, hasPendingReset, isDeletedForAdmission, newFence, orgFenceAuthorityKey, recordGeneration, s3LedgerObjects,
  sessionKeyHash, validFenceAuthority, validOrgFenceAuthority, type FenceDoc, type FenceRead, type IndexEntry, type IndexEntryInput,
} from "./ledger-s3.ts";
import { fitLine } from "./oversize.ts";
import { routedRawChunkSealerDeps } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { BUFFER_STAGE_MAX_BYTES, decodeStaged, type StagedHeader } from "./staging.ts";
import {
  IngestWriteError,
  NO_SESSION_HARNESS,
  NO_SESSION_ID,
  liveMachineId,
  writeGroups,
  type Group,
  type LineMeta,
  type WriteTarget,
} from "./writer.ts";

export const DRAINED_SUFFIX = ".drained.json";
/** Clock-skew margin around a user's last reset (see step 3): default and floor. */
export const RESET_MARGIN_DEFAULT_MS = 5 * 60_000;
export const RESET_MARGIN_MIN_MS = 60_000;
export const STAGED_SUFFIX = ".staged.ndjson.zst";
export const MAX_DRAIN_CONCURRENCY = 32;
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

// ---- the S3 upload ledger's objects (ledger-s3.ts, #580) ----

/** What the drain reads from the v3 privacy journal (async-admission.ts). */
export interface JournalState {
  seq: number;
  /** Controls begun and not yet settled. */
  pending: number;
  /** Controls a consumer completed conservatively; unresolved until their next attempt. */
  ambiguous: number;
}

/**
 * The drain's ledger: ledger-s3.ts `s3LedgerObjects` itself (keys, fence read, revision shards, ownership stubs,
 * re-arm markers; the SAME formats and rules as admission), plus the privacy journal reader. Tests override
 * single methods. Every method fails closed (throws) on an unreadable object.
 */
export type DrainLedger = Pick<ReturnType<typeof s3LedgerObjects>, "keysFor" | "readFence" | "readIndex" | "mergeIndex" | "registerOwner" | "writeRearm"> & {
  /** null when the user has no journal (no ordered control ever ran). */
  readJournal(tenant: LedgerTenant): Promise<JournalState | null>;
};

export class DrainLedgerError extends Error {
  constructor(readonly code: "journal_invalid") {
    super(`buffered drain ledger: ${code}`);
    this.name = "DrainLedgerError";
  }
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

export function s3DrainLedger(store: ObjectStore, keyRoot: string, opts: { sleep?: (ms: number) => Promise<void>; random?: () => number; nowMs?: () => number } = {}): DrainLedger {
  const io = s3LedgerObjects(store, keyRoot, opts);
  return {
    keysFor: io.keysFor, readFence: io.readFence, readIndex: io.readIndex, mergeIndex: io.mergeIndex, registerOwner: io.registerOwner, writeRearm: io.writeRearm,
    async readJournal(tenant) {
      const got = await readText(store, journalKey(keyRoot, tenant));
      return got ? parseJournal(got.text) : null;
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
  | "reset_margin"
  | "reset_in_flight"
  | "control_pending"
  | "fence_authority_missing"
  | "fence_authority_invalid"
  | "index_unreadable"
  | "write_failed"
  | "index_merge_failed"
  | "fence_recheck_failed"
  | "fence_moved"
  | "marker_failed"
  | "internal_error";

const OUTCOMES: ObjectOutcome[] = [
  "drained", "would_drain", "already_drained", "pre_reset", "redacted", "vanished", "corrupt", "key_mismatch",
  "ambiguous_replacement", "fence_absent", "fence_unreadable", "journal_unreadable", "privacy_control_open", "reset_undeterminable", "reset_margin", "reset_in_flight", "control_pending", "fence_authority_missing", "fence_authority_invalid", "index_unreadable", "write_failed",
  "index_merge_failed", "fence_recheck_failed", "fence_moved", "marker_failed", "internal_error",
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
  /** Orgs without a valid fence-authority marker (per-org, else deployment-wide): --execute writes nothing of them. */
  orgs_without_authority: number;
  /** Dry run with assumeEmptyFence: batches whose user had no fence, classified as if it were empty. */
  fence_assumed_empty: number;
  /** Write failures by closed code (IngestWriteError code, or `storage`). */
  write_failures: Record<string, number>;
  stopped: "complete" | "max_objects";
}

function emptySummary(mode: DrainSummary["mode"]): DrainSummary {
  return {
    mode, orgs: 0, listed: 0, foreign_keys: 0,
    objects: Object.fromEntries(OUTCOMES.map((o) => [o, 0])) as Record<ObjectOutcome, number>,
    lines: { candidate: 0, written: 0, fenced: 0, stale: 0, already_stored: 0, revision_conflict: 0 },
    sessions_written: 0, rearmed: 0, orgs_without_authority: 0, fence_assumed_empty: 0, write_failures: {}, stopped: "complete",
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
  /** The org's fence authority, as admission requires it (ledger-s3 requireFenceAuthority). */
  authority: Authority;
  lastKey: string;
}

/**
 * Listed keys grouped by `…/m=…/b=<batch32>`. Every object of one batch digest (retries with other
 * claim tokens, a GC replacement, their markers) sorts contiguously, so the group is complete.
 */
type Authority = "ok" | "missing" | "invalid";

/**
 * Exactly admission's rule (ledger-s3 requireFenceAuthority): the org's own marker
 * (`.ledger/fence-authority/<org>.json`, with that orgId), else the deployment-wide one; a present but invalid
 * per-org marker refuses outright. The marker attests the drain's own preconditions (fence, ownership and revision
 * backfills complete, control mirror on, re-arm sweeper live, Worker HTTP admission off).
 */
async function orgAuthority(store: ObjectStore, keyRoot: string, orgId: string): Promise<Authority> {
  const read = async (key: string) => {
    const got = await readText(store, key, 64 * 1024);
    if (!got) return null;
    try {
      return { v: JSON.parse(got.text) as unknown };
    } catch {
      return { v: undefined };
    }
  };
  try {
    const org = await read(orgFenceAuthorityKey(keyRoot, orgId));
    if (org) return validOrgFenceAuthority(org.v) && (org.v as { orgId: string }).orgId === orgId ? "ok" : "invalid";
    const all = await read(fenceAuthorityKey(keyRoot));
    if (!all) return "missing";
    return validFenceAuthority(all.v) ? "ok" : "invalid";
  } catch {
    return "invalid";
  }
}

async function* batchGroups(store: ObjectStore, prefix: string, startAfter: string | undefined, summary: DrainSummary, authority: Authority): AsyncGenerator<BatchGroup> {
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
      current ??= { id, staged: [], markers: new Set(), authority, lastKey: key };
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

// ---- the run lock ----

/** One drain process at a time: a create-only lease at `<keyRoot>/.drain/lock.json`, renewed by CAS. */
export const DRAIN_LOCK_TTL_MS = 15 * 60_000;
export const drainLockKey = (keyRoot: string) => `${keyRoot}/.drain/lock.json`;
interface DrainLockBody { v: 1; kind: "buffered-drain-lock"; holder: string; expiresAtMs: number }

export class DrainLockError extends Error {
  constructor(readonly code: "drain_locked" | "drain_lock_invalid" | "drain_lock_lost") {
    super(code);
    this.name = "DrainLockError";
  }
}

async function readLock(store: ObjectStore, key: string): Promise<{ body: DrainLockBody; version: ObjectVersion } | null> {
  const got = await readText(store, key, 64 * 1024);
  if (!got) return null;
  let v: unknown;
  try {
    v = JSON.parse(got.text);
  } catch {
    throw new DrainLockError("drain_lock_invalid");
  }
  // Fail closed: a lock this code cannot read is held.
  if (!isObject(v) || v.v !== 1 || v.kind !== "buffered-drain-lock" || typeof v.holder !== "string" || !Number.isSafeInteger(v.expiresAtMs)) throw new DrainLockError("drain_lock_invalid");
  return { body: v as unknown as DrainLockBody, version: got.version };
}

/** The dry run never writes the lock, but still refuses to run beside a live drain. */
export async function assertDrainUnlocked(store: ObjectStore, keyRoot: string, nowMs: number): Promise<void> {
  const held = await readLock(store, drainLockKey(keyRoot));
  if (held && held.body.expiresAtMs > nowMs) throw new DrainLockError("drain_locked");
}

export interface DrainLease { renew(): Promise<void>; release(): Promise<void> }

/** Take the lease: create-only, or CAS over an expired one. A live lease (anyone's) refuses. */
export async function acquireDrainLock(store: ObjectStore, keyRoot: string, opts: { holder: string; ttlMs: number; now: () => number }): Promise<DrainLease> {
  const key = drainLockKey(keyRoot);
  const body = (expiresAtMs: number) => encoder.encode(JSON.stringify({ v: 1, kind: "buffered-drain-lock", holder: opts.holder, expiresAtMs } satisfies DrainLockBody));
  const held = await readLock(store, key);
  if (held && held.body.expiresAtMs > opts.now()) throw new DrainLockError("drain_locked");
  let version: ObjectVersion;
  try {
    version = (await store.put(objectKey(key), body(opts.now() + opts.ttlMs), held ? { ifMatch: held.version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" })).version;
  } catch (error) {
    // Another process took it between the read and the write.
    if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists")) throw new DrainLockError("drain_locked");
    throw error;
  }
  return {
    async renew() {
      try {
        version = (await store.put(objectKey(key), body(opts.now() + opts.ttlMs), { ifMatch: version, contentType: "application/json" })).version;
      } catch (error) {
        if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists")) throw new DrainLockError("drain_lock_lost");
        throw error;
      }
    },
    async release() {
      // Expire it in place (the drain never deletes). A lost lease is someone else's now: leave it.
      await store.put(objectKey(key), body(0), { ifMatch: version, contentType: "application/json" }).catch(() => {});
    },
  };
}

// ---- the drain ----

export interface DrainOptions {
  store: ObjectStore;
  keyRoot: string;
  /** Orgs to drain. Required: discovery (a delimiter listing) is the caller's (scripts/buffered-drain.ts). */
  orgs: string[];
  dryRun: boolean;
  /**
   * Dry run only: classify a user with no fence object as if the fence were empty (counted in
   * `fence_assumed_empty`), so a dry run before the backfill still measures the backlog. Refused with
   * `dryRun: false`: an execute run never writes for an unbackfilled user.
   */
  assumeEmptyFence?: boolean;
  /** Lease TTL for the run lock (renewed every third of it). Default DRAIN_LOCK_TTL_MS. */
  lockTtlMs?: number;
  /** Clock-skew margin around the last reset. Default RESET_MARGIN_DEFAULT_MS; below RESET_MARGIN_MIN_MS refuses. */
  resetMarginMs?: number;
  /** Lease holder id (no personal data). Default a random `drain-<uuid>`. */
  lockHolder?: string;
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
  if (opts.assumeEmptyFence && !opts.dryRun) throw new Error("drain_assume_empty_fence_requires_dry_run");
  const lockTtlMs = opts.lockTtlMs ?? DRAIN_LOCK_TTL_MS;
  if (!Number.isSafeInteger(lockTtlMs) || lockTtlMs < 1_000) throw new Error("drain_lock_ttl_invalid");
  const resetMarginMs = opts.resetMarginMs ?? RESET_MARGIN_DEFAULT_MS;
  if (!Number.isSafeInteger(resetMarginMs) || resetMarginMs < RESET_MARGIN_MIN_MS) throw new Error("drain_reset_margin_invalid");
  const store = opts.dryRun ? readOnlyStore(opts.store) : opts.store;
  const ledger = opts.ledger ?? s3DrainLedger(store, opts.keyRoot, { ...(opts.sleep ? { sleep: opts.sleep } : {}), ...(opts.random ? { random: opts.random } : {}) });
  const target: WriteTarget = {
    store, keyRoot: opts.keyRoot,
    // SCRUB_ENGINE routing, as every production write constructor (scrub-cutover-reachability.test.ts).
    ...routedRawChunkSealerDeps(),
    ...(opts.now ? { now: opts.now } : {}),
    ...(opts.sleep ? { sleep: opts.sleep } : {}),
    ...(opts.random ? { random: opts.random } : {}),
  };
  const nowMs = () => (opts.now ?? (() => new Date()))().getTime();
  let lease: DrainLease | undefined;
  let lockLost: unknown;
  let renewTimer: ReturnType<typeof setInterval> | undefined;
  if (opts.dryRun) await assertDrainUnlocked(opts.store, opts.keyRoot, nowMs());
  else {
    lease = await acquireDrainLock(store, opts.keyRoot, { holder: opts.lockHolder ?? `drain-${randomUUID()}`, ttlMs: lockTtlMs, now: nowMs });
    let renewing = false;
    renewTimer = setInterval(() => {
      if (renewing || lockLost !== undefined) return;
      renewing = true;
      lease!.renew().catch((e) => { lockLost ??= e; }).finally(() => { renewing = false; });
    }, Math.max(500, Math.floor(lockTtlMs / 3)));
  }
  const locks = new KeyedLocks();
  const orgs = orgOrder(opts.orgs);
  summary.orgs = orgs.length;

  async function* allGroups(): AsyncGenerator<BatchGroup> {
    for (const org of orgs) {
      const prefix = `${opts.keyRoot}/${org}/buffered-batches/v1`;
      // Every key under a prefix that sorts before start-after, and does not contain it, is before it.
      if (opts.startAfter !== undefined && `${prefix}/` < opts.startAfter && !opts.startAfter.startsWith(`${prefix}/`)) continue;
      const authority = await orgAuthority(opts.store, opts.keyRoot, org);
      if (authority !== "ok") summary.orgs_without_authority++;
      yield* batchGroups(opts.store, prefix, opts.startAfter, summary, authority);
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
  try {
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      for (;;) {
        if (fatal || checkpointError !== undefined || lockLost !== undefined) return;
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
  } finally {
    if (renewTimer) clearInterval(renewTimer);
    if (lockLost === undefined) await lease?.release();
  }
  if (fatal) throw fatal.error;
  if (checkpointError !== undefined) throw checkpointError;
  if (lockLost !== undefined) throw lockLost;
  return summary;

  async function drainGroup(group: BatchGroup) {
    // Read everything not yet drained, then decide per claim token (GC replacement detection).
    const reads = new Map<string, Read>();
    for (const key of group.staged) {
      if (group.markers.has(key) || (!opts.dryRun && group.authority !== "ok")) continue;
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
      // --execute writes only for an org admission itself would admit (the dry run still classifies, and reports
      // the org in orgs_without_authority).
      if (!opts.dryRun && group.authority !== "ok") {
        summary.objects[group.authority === "missing" ? "fence_authority_missing" : "fence_authority_invalid"]++;
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
    const origin = header.origin_id;
    const receivedMs = Date.parse(header.received_at);
    let keys: ReturnType<DrainLedger["keysFor"]>;
    let read: FenceRead;
    try {
      keys = ledger.keysFor(tenant);
      read = await ledger.readFence(keys.fence);
    } catch {
      return "fence_unreadable";
    }
    // 0005 step 0: a user without a backfilled fence may have deletions recorded only in SQL.
    let fence: FenceDoc;
    if (read.state === "absent") {
      if (!(opts.dryRun && opts.assumeEmptyFence)) return "fence_absent";
      summary.fence_assumed_empty++;
      fence = newFence(); // a classification view only: never written
    } else fence = read.fence;
    let journal: JournalState | null;
    try {
      journal = await ledger.readJournal(tenant);
    } catch {
      return "journal_unreadable";
    }
    if (journal && (journal.pending > 0 || journal.ambiguous > 0)) return "privacy_control_open";
    const counts = { candidate: 0, written: 0, fenced: 0, stale: 0, already_stored: 0, revision_conflict: 0 };
    // ledger-s3 resetsCompleted: a reset begun but not finished is still applying its tombstones (a claim
    // finishes it). Writing now could republish a session it is about to tombstone: skip, unmarked.
    // As admission's claim (ledger-s3 hasPendingReset): an ordered reset SQL has not decided yet refuses the user.
    if (hasPendingReset(fence)) return "control_pending";
    if (fence.resetsCompleted < fence.resets) return "reset_in_flight";
    if (fence.resets > 0) {
      // ledger-s3 `lastResetAtMs`: 0 means no reset time was recorded (it only ever rises).
      if (fence.lastResetAtMs === 0) return "reset_undeterminable";
      if (receivedMs <= fence.lastResetAtMs - resetMarginMs) {
        if (!opts.dryRun && !(await putMarker(key, sha256, "pre_reset", counts, 0))) return "marker_failed";
        return "pre_reset";
      }
      // Too close to the reset to order across two clocks: never written, never marked, reported.
      if (receivedMs < fence.lastResetAtMs + resetMarginMs) return "reset_margin";
    }
    type Kept = { group: Group; lines: string[]; meta: LineMeta[] };
    const kept: Kept[] = [];
    /** Deleted sessions this batch carries: an earlier partial run may have written them (re-armed below). */
    const fencedSessions = new Map<string, [source: string, sessionId: string]>();
    for (const g of groups) {
      const k: Kept = { group: g, lines: [], meta: [] };
      g.lines.forEach((line, i) => {
        counts.candidate++;
        const m = g.meta![i]!;
        // As admission (ledger-s3 isDeletedForAdmission): tombstoned, or named by a PENDING ordered disable.
        if (m[3] !== null && isDeletedForAdmission(fence, origin, m[2], m[3])) {
          counts.fenced++;
          fencedSessions.set(JSON.stringify([m[2], m[3]]), [m[2], m[3]]);
        } else {
          k.lines.push(line);
          k.meta.push(m);
        }
      });
      if (k.lines.length) kept.push(k);
    }

    // The revision index is per (origin, record id) in 64 shards (ledger-s3): lock the shards this batch touches.
    const recordOf = (line: string) => JSON.parse(line) as { record_id: string; revision: string };
    const shardKeys = new Set<string>();
    for (const k of kept) for (const line of k.lines) shardKeys.add(keys.revisions(origin, recordOf(line).record_id));
    const release = await locks.acquire([...shardKeys]);
    try {
      const shards = new Map<string, Record<string, IndexEntry>>();
      try {
        for (const sk of shardKeys) {
          const doc = (await ledger.readIndex(sk))?.doc;
          if (doc && doc.originId !== origin) return "index_unreadable";
          shards.set(sk, doc?.records ?? {});
        }
      } catch {
        return "index_unreadable";
      }
      const toWrite: Group[] = [];
      const entries = new Map<string, IndexEntryInput[]>();
      const inBatch = new Map<string, [number, string]>();
      const writtenSessions = new Map<string, [string, string]>();
      for (const k of kept) {
        const lines: string[] = [];
        const meta: LineMeta[] = [];
        k.lines.forEach((line, i) => {
          const m = k.meta[i]!;
          const r = recordOf(line);
          const revision = Number(r.revision);
          const sk = keys.revisions(origin, r.record_id);
          const shard = shards.get(sk)!;
          const e = Object.hasOwn(shard, r.record_id) ? shard[r.record_id] : undefined;
          // As the claim (ledger-s3 judge): an entry the fence has forgotten counts as absent; an equal or
          // higher stored revision is stale. Split only for the counts.
          const stored = e && !entryForgotten(fence, origin, e) ? e : undefined;
          const seen = inBatch.get(r.record_id);
          if (stored && revision < stored[0]) counts.stale++;
          // A revision-only stored entry (#587: SQL's seeded records and lazy slots) has no digest to compare: stale.
          else if (stored && revision === stored[0] && stored[2] === null) counts.stale++;
          else if (stored && revision === stored[0]) {
            if (stored[2] === m[1]) counts.already_stored++;
            else counts.revision_conflict++;
          } else if (seen && revision === seen[0] && seen[1] !== m[1]) counts.revision_conflict++;
          else {
            inBatch.set(r.record_id, [revision, m[1]]);
            // Stamped with this read's generation, as a commit stamps its claim's.
            const [sessionKey, epoch, resets] = recordGeneration(fence, origin, m[2], m[3]);
            entries.set(sk, [...(entries.get(sk) ?? []), { recordId: r.record_id, revision, revisionId: m[0], digest: m[1], sessionKey, epoch, resets }]);
            if (m[3] !== null) writtenSessions.set(JSON.stringify([m[2], m[3]]), [m[2], m[3]]);
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
      const snapshot = { instance: fence.instance, version: fence.version, resets: fence.resets, resetsCompleted: fence.resetsCompleted };
      const epochOf = (f: FenceDoc, s: string, id: string) => fenceEpoch(f, origin, sessionKeyHash(s, id));
      // Ownership (ingest_session_ownership) BEFORE any canonical write, as the claim: a reset that begins after
      // this lists these sessions. Then the fence again: if it moved, leave the batch for a rerun.
      if (writtenSessions.size) {
        try {
          for (const [s, id] of writtenSessions.values()) {
            const [, epoch, resets] = recordGeneration(fence, origin, s, id);
            await ledger.registerOwner(keys.owner(origin, s, id), { v: 1, originId: origin, source: s, sessionId: id, epoch, resets });
          }
          const again = await ledger.readFence(keys.fence);
          if (again.state !== "present" || again.fence.instance !== snapshot.instance || again.fence.version !== snapshot.version) return "fence_moved";
        } catch {
          return "fence_recheck_failed";
        }
      }
      let machineId = liveMachineId(header.workspace_id, origin, header.user_id);
      if (toWrite.length) {
        try {
          machineId = (await writeGroups(target, { orgId: header.org_id, workspaceId: header.workspace_id, originId: origin, userId: header.user_id, batchDigest: header.batch_digest }, toWrite, new Date(receivedMs))).machineId;
        } catch (error) {
          const code = error instanceof IngestWriteError ? error.code : "storage";
          summary.write_failures[code] = (summary.write_failures[code] ?? 0) + 1;
          return "write_failed";
        }
      }
      // The commit (ledger-s3 commit): read the fence, re-arm what it reached BEFORE any index merge, then merge
      // the rest stamped with the first read's generation; mergeIndex drops what the latest fence forgot.
      let after: FenceDoc;
      const fencedNow = new Set<string>();
      try {
        const now = await ledger.readFence(keys.fence);
        const journalAfter = await ledger.readJournal(tenant);
        after = now.state === "present" ? now.fence : newFence();
        const same = now.state === "present" && now.fence.instance === snapshot.instance && now.fence.version === snapshot.version;
        const journalMoved = (journalAfter?.seq ?? 0) !== (journal?.seq ?? 0) || (journalAfter?.pending ?? 0) > 0 || (journalAfter?.ambiguous ?? 0) > 0;
        const rearm = new Map(fencedSessions);
        for (const [pk, [s, id]] of writtenSessions) {
          const reached = journalMoved || hasPendingReset(after) || (!same && (now.state !== "present" || isDeletedForAdmission(after, origin, s, id) || after.resets !== snapshot.resets || after.resetsCompleted !== snapshot.resetsCompleted || epochOf(after, s, id) !== epochOf(fence, s, id)));
          if (reached) {
            rearm.set(pk, [s, id]);
            fencedNow.add(pk);
          }
        }
        for (const [s, id] of rearm.values()) {
          await ledger.writeRearm(keys.rearm(origin, s, id), { originId: origin, source: s, sessionId: id, machineId });
          summary.rearmed++;
        }
      } catch {
        return "fence_recheck_failed";
      }
      try {
        for (const [sk, list] of entries) {
          const keep = list.filter((e) => e.sessionKey === null || ![...fencedNow].some((pk) => { const [s, id] = JSON.parse(pk) as [string, string]; return sessionKeyHash(s, id) === e.sessionKey; }));
          if (keep.length) await ledger.mergeIndex(sk, after, origin, keep);
        }
      } catch {
        return "index_merge_failed";
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
