/**
 * The upload ledger in S3 (decision 0005, docs/decisions/0005-s3-native-upload-ledger.md): the same
 * `Ledger` decisions `ledger-sql.ts` makes in Postgres, for INLINE acceptance only, with zero SQL. It is
 * behind INGEST_LEDGER=s3 (default sql), and only the upload-admission consumer runs it.
 *
 * Every per-user object sits under the per-user admission directory (async-admission.ts `admissionUserDir`,
 * the one that holds `privacy-journal.json`), which is never desktop-writable:
 *
 *   ledger/batches/o=<h(origin)>/b=<h(batchId)>.json    the batch record: create-only claim,
 *       {v, originId, batchId, digest, receiptId, state, claimToken, fenceVersion, fenceInstance, resets, epochs}
 *       pending -> committed with If-Match. Same digest committed: replay. Same digest pending: resume
 *       (a new claim token and fence snapshot, If-Match). Other digest: batch_conflict.
 *   ledger/revisions/o=<h(origin)>/r=<shard>.json   the revision index, PER ORIGIN (as ingest_record_revisions'
 *       key (org, ws, origin, record_id) and the reader's per-origin fold), sharded by sha256(recordId):
 *       {v, originId, shard, records: {<recordId>: [revision, revisionId, digest, sessionKey | null, epoch, resets]}}
 *       Each entry carries the session it was written under and that session's generation, so a disable or a
 *       reset forgets it without rewriting it (IndexEntry). Read at claim (an equal or lower stored revision of
 *       the same record id, from ANY session of the origin, is `stale`, relayhistory-cloud #250); merged at
 *       commit with If-Match (max wins, an equal revision keeps the stored entry), retried on a lost CAS.
 *   ledger/owners/o=<h(origin)>/s=<sessionKey>.json   the session's ownership stub (ingest_session_ownership):
 *       {v, originId, source, sessionId, epoch, resets}, registered at CLAIM, before any canonical write, so a
 *       reset that lands mid-flight lists the session and tombstones it.
 *   upload-fences.json   the fence: {v, instance, version, resets, resetsCompleted, lastResetAtMs, deleted: {<origin>: [[source,
 *       sessionId]...]}, epochs: {<origin>: {<sessionKey>: n}}, resetRequests: {<requestId>: n | null}, ordered?}
 *       edited with If-Match by every selection, reset and ordered control. Unreadable: a retry, never empty.
 *       Created only by `newFence` (a random instance id), changed only by `applyFenceChange`'s rules.
 *   ledger/rearm/o=<h(origin)>/<sessionKey>.json   the re-arm marker for the eraser (RearmDoc):
 *       {v, originId, source, sessionId, machineId, atMs}. Created by the first re-arm; every later re-arm of the
 *       session RE-ARMS it (If-Match, atMs strictly later), so its content changes and the sweeper erases again.
 *   ledger/rearm/o=<h(origin)>/<sessionKey>.done/<h(marker bytes)>.json   the sweeper's completion record for one
 *       marker content (rearm-sweeper.ts RearmDoneDoc), create-only. Nothing here deletes a marker.
 *
 * And one deployment-wide object, written by an operator (never by this code):
 *
 *   <keyRoot>/.ledger/fence-authority.json   FenceAuthorityDoc. While it is absent or invalid, a ledger built
 *       with `fenceAuthority: "required"` answers every claim with a retry (`ledger_fence_authority_missing`),
 *       never an admit. See FenceAuthorityDoc for what writing it asserts.
 *
 * h(x) = sha256(x) hex [0:32]. sessionKey = sessionKeyHash(source, sessionId) = h(JSON.stringify([source, sessionId])).
 *
 * Mirrors sessions.ingest_claim_batch_mode (canonical) and sessions.ingest_commit_batch
 * (supabase/migrations/20261105050010_operator_session_erase_fence_check.sql):
 *
 *   claim   batch record (replay / batch_conflict) -> fence (deleted) -> revision indexes of the live
 *           records (stale, in-batch revision reuse = revision_conflict) -> ownership stubs of the live
 *           sessions, then the fence again (moved: claim again) -> the batch record, committed at once when
 *           nothing needs writing (replay_fast).
 *   commit  fence (what changed since the claim's snapshot) -> re-arm markers for the fenced sessions ->
 *           merge the written revisions of the others, stamped with the claim's generation (IndexEntry: a
 *           disable or reset since makes them stale at once) -> batch record committed. A fenced
 *           session the fence no longer deletes (a re-enable since the claim) is
 *           `ledger_attempt_superseded`, as SQL's `invalidated`: the retry claims again, and the
 *           consumer's privacy-journal gate decides.
 *
 * Re-arm markers are written before any index merge, so a crash after the merge (whose retry is a
 * replay_fast that skips this commit) never loses one. The index can lag the manifest (a crash between
 * the canonical write and the merge resumes) but never leads it: the merge runs after the canonical write.
 *
 * Nothing here deletes or rewrites an index to forget it: a disable bumps the session's fence epoch and a reset
 * the user's reset count, which makes every entry of the old generation stale (IndexEntry). A selection is one
 * fence CAS however many sessions it names; a reset also lists and reads the user's ownership stubs once.
 * Errors are LedgerUnavailableError with a code only, never a key.
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
/**
 * The session key: the fence's epoch key, the ownership stub's and re-arm marker's file name, and an index
 * entry's session. JSON-encoded, so no (source, sessionId) pair can collide with another (a separator byte could).
 */
export const sessionKeyHash = (source: string, sessionId: string) => h(JSON.stringify([source, sessionId]));

/** The revision index is sharded per origin by sha256(recordId)'s first byte, mod this. */
export const REVISION_SHARDS = 64;
export const revisionShard = (recordId: string) => (createHash("sha256").update(recordId).digest()[0]! % REVISION_SHARDS).toString(16).padStart(2, "0");
const SHARD = /^[0-9a-f]{2}$/;

export function s3LedgerKeys(keyRoot: string, tenant: LedgerTenant) {
  const dir = admissionUserDir(keyRoot, tenant);
  return {
    dir,
    fence: `${dir}/upload-fences.json`,
    batch: (originId: string, batchId: string) => `${dir}/ledger/batches/o=${h(originId)}/b=${h(batchId)}.json`,
    revisionsRoot: `${dir}/ledger/revisions`,
    /** The shard object holding `recordId`'s entry for this origin. */
    revisions: (originId: string, recordId: string) => `${dir}/ledger/revisions/o=${h(originId)}/r=${revisionShard(recordId)}.json`,
    revisionShard: (originId: string, shard: string) => `${dir}/ledger/revisions/o=${h(originId)}/r=${shard}.json`,
    ownersRoot: `${dir}/ledger/owners`,
    owner: (originId: string, source: string, sessionId: string) => `${dir}/ledger/owners/o=${h(originId)}/s=${sessionKeyHash(source, sessionId)}.json`,
    rearm: (originId: string, source: string, sessionId: string) => `${dir}/ledger/rearm/o=${h(originId)}/${sessionKeyHash(source, sessionId)}.json`,
  };
}

/** The deployment-wide fence-authority marker (FenceAuthorityDoc). */
export const fenceAuthorityKey = (keyRoot: string) => `${keyRoot}/.ledger/fence-authority.json`;

// ---- object shapes ----

type Decision = "apply" | { outcome: "replay" | "stale"; result: number };
/** An ordered control's dedupe mark. A reset's mark names its request id: only a finish that ran it may close it. */
interface OrderedMark { epoch: string; version: string; digest: string; kind: "selection" | "reset"; result: number | null; requestId?: string }
export interface FenceDoc {
  v: 1;
  /** Random per created fence (newFence): a commit compares it with its claim's, so a deleted and recreated fence is never "unchanged". */
  instance: string;
  /** Bumped by every edit: a commit whose claim saw this (instance, version) has nothing to recheck. */
  version: number;
  /** Resets BEGUN (a reset's first CAS bumps it). New entries and owner stubs are stamped with it. */
  resets: number;
  /**
   * The reset count whose tombstones have all landed (<= resets). Entries are forgotten by THIS count, so a
   * begun-but-unfinished reset leaves every entry judged as before; the next reset retry or claim finishes it.
   */
  resetsCompleted: number;
  /** ms since epoch of the latest reset; only ever increases (applyFenceChange). 0: never reset. */
  lastResetAtMs: number;
  deleted: Record<string, [string, string][]>;
  /** Per origin, per sessionKey: bumped by every new deletion (a disable, or a reset tombstoning it), as ingest_fence_versions. */
  epochs: Record<string, Record<string, number>>;
  /** A reset's request id: null while it runs, then the count it tombstoned (a replay answers it). */
  resetRequests: Record<string, number | null>;
  ordered?: OrderedMark;
}
/** A fence read: ABSENT (no object) is distinct from a present fence with nothing in it. */
export type FenceRead = { state: "absent" } | { state: "present"; fence: FenceDoc; version: ObjectVersion };

export interface BatchDoc {
  v: 1;
  originId: string;
  batchId: string;
  digest: string;
  receiptId: string;
  state: "pending" | "committed";
  claimToken: string | null;
  fenceVersion: number;
  /** The claim's fence instance; null when the fence was absent. */
  fenceInstance: string | null;
  resets: number;
  /** The claim's fence snapshot: the epoch of every session of the batch, by sessionKey. */
  epochs: Record<string, number>;
}
/**
 * One stored revision: [revision, revisionId, digest, sessionKey | null, epoch, resets]. The last three are the
 * session it was written under and that session's generation (its fence epoch and the user's reset count, of
 * the claim that wrote it). An entry is FORGOTTEN (entryForgotten: a claim treats it as absent, a merge drops
 * it) once its generation is below the fence's for that session: a disable bumps the epoch, a reset bumps the
 * count, so a control costs one fence CAS however many records it forgets. Records with no session
 * (sessionKey null) are never forgotten by a control, as SQL keeps session-less revision rows.
 *
 * `revisionId` and `digest` may be null: a REVISION-ONLY entry. SQL has stored revisions without them
 * (`ingest_seed_proven_records`, the operator bootstrap from proven canonical objects, writes no revision id or
 * digest; buffered mode with the lazy policy judges by `ingest_lazy_native_slots`, which has a digest but no
 * revision id), and its stale check compares the stored revision only, "whatever its digest" (claim step c,
 * 20261105050010). So does this one: a claim judges every entry by `revision` alone. The backfill (#579) copies
 * those rows in this form; the ingest itself always writes full entries.
 */
export type IndexEntry = [number, string | null, string | null, string | null, number, number];
export interface IndexDoc { v: 1; originId: string; shard: string; records: Record<string, IndexEntry> }
/**
 * A re-arm marker: "this session's canonical objects may hold data written after its erasure; erase it again".
 * `machineId` is the writer's machine (writer.ts liveMachineId of the user's workspace and origin), which with the
 * marker's own admission directory (org, workspace) names exactly the canonical prefix the commit wrote.
 */
export interface RearmDoc { v: 1; originId: string; source: string; sessionId: string; machineId: string; atMs: number }
/** The ownership stub (ingest_session_ownership): the OLDEST generation this session was claimed under at its current epoch. */
export interface OwnerDoc { v: 1; originId: string; source: string; sessionId: string; epoch: number; resets: number }

/**
 * `<keyRoot>/.ledger/fence-authority.json`. An operator writes it, once, and ONLY when all of these hold:
 *   fenceBackfill "complete"  the one-time fence backfill (0005 "The buffered backlog" step 0) has copied every
 *                             user's SQL deletions and resets (upload_deletions, upload_resets) into their fence;
 *   revisionBackfill "complete"  SQL ingest_record_revisions is copied into the index shards at the current generation
 *                             (s3LedgerObjects mergeIndex, recordGeneration), so a pre-cutover revision is still stale;
 *   ownershipBackfill "complete" SQL ingest_session_ownership (and every session with SQL revisions) is copied into
 *                             owner stubs (registerOwner), so an S3 reset owns sessions only SQL knew. Both copies run
 *                             AFTER every writer of SQL ownership has stopped (INGEST_LEDGER=s3 everywhere it admits):
 *                             the S3 reset reads only owner stubs;
 *   controlMirror "on"        every control route writes the S3 fence before SQL (INGEST_S3_FENCE_MIRROR=on on
 *                             the Worker, or this ledger's own controls on self-host), so no deletion lands in
 *                             SQL alone from then on;
 *   rearmSweeper "live"       the re-arm sweeper (rearm-sweeper.ts) runs against this bucket and key root, armed and
 *                             not in dry run, so every `ledger/rearm/` marker is erased again;
 *   workerHttpAdmission "off" the Worker's HTTP upload path (its ledger is SQL: INGEST_LEDGER=s3 refuses there) no
 *                             longer admits for any user this deployment's S3 consumers serve: stopped, or redirected
 *                             to direct upload, BEFORE the revision and ownership backfills run. An admission it made
 *                             after them would be absent from the S3 index and owner stubs, so a direct upload could
 *                             later re-admit an older or equal revision (#250) and the session would escape S3 resets.
 *                             0005's "compare against the HTTP path" step therefore runs BEFORE the marker is written.
 * Without it an absent fence cannot be told from a user whose SQL deletions were never copied, so admission
 * refuses (retries) rather than treat absent as empty. Deleting it is the kill switch for NEW claims only: each
 * ledger trusts a valid read for up to 60 s (fenceAuthorityTtlMs), and a batch already claimed still commits
 * (commit never reads the marker), so its canonical objects can land after the marker is gone.
 */
export interface FenceAuthorityDoc { v: 1; fenceBackfill: "complete"; revisionBackfill: "complete"; ownershipBackfill: "complete"; controlMirror: "on"; rearmSweeper: "live"; workerHttpAdmission: "off"; writtenAtMs: number }

const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const count = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const str = (v: unknown): v is string => typeof v === "string" && v.length > 0;
const HEX64 = /^[0-9a-f]{64}$/;
const HEX32 = /^[0-9a-f]{32}$/;
const TOKEN = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const pairOk = (p: unknown): p is [string, string] => Array.isArray(p) && p.length === 2 && str(p[0]) && str(p[1]);
/** A client-supplied id used as a fence map key (origin, reset request): never the prototype setter's name. */
const mapKey = (v: unknown): v is string => str(v) && v !== "__proto__";
/** Own properties only: an origin named `constructor` or `toString` must never read Object.prototype. */
const own = <T>(o: Record<string, T>, k: string): T | undefined => (Object.hasOwn(o, k) ? o[k] : undefined);
const exactKeys = (o: Record<string, unknown>, keys: string[]) => Object.keys(o).length === keys.length && keys.every((k) => Object.hasOwn(o, k));

export function validFence(v: unknown): v is FenceDoc {
  if (!isObject(v) || v.v !== 1 || typeof v.instance !== "string" || !TOKEN.test(v.instance) || !count(v.version) || !count(v.resets) || !count(v.lastResetAtMs)
    || !isObject(v.deleted) || !isObject(v.epochs) || !isObject(v.resetRequests)) return false;
  // A fence from before `resetsCompleted`: it means `resets` when no reset is running, else fail closed (we
  // cannot know which tombstones landed). readFence fills it in; nothing else may treat it as absent.
  if (v.resetsCompleted === undefined) { if (Object.values(v.resetRequests).includes(null)) return false; }
  else if (!count(v.resetsCompleted) || v.resetsCompleted > v.resets) return false;
  if (Object.hasOwn(v.deleted, "__proto__") || Object.hasOwn(v.epochs, "__proto__") || Object.hasOwn(v.resetRequests, "__proto__")) return false;
  if (!Object.values(v.deleted).every((list) => Array.isArray(list) && list.every(pairOk))) return false;
  if (!Object.values(v.epochs).every((m) => isObject(m) && Object.entries(m).every(([k, n]) => HEX32.test(k) && count(n)))) return false;
  if (!Object.values(v.resetRequests).every((n) => n === null || count(n))) return false;
  const o = v.ordered;
  return o === undefined || (isObject(o) && str(o.epoch) && typeof o.version === "string" && /^[1-9][0-9]{0,18}$/.test(o.version) && str(o.digest)
    && (o.kind === "selection" || o.kind === "reset") && (o.result === null || count(o.result)) && (o.requestId === undefined || mapKey(o.requestId)));
}
export function validBatch(v: unknown): v is BatchDoc {
  return isObject(v) && v.v === 1 && str(v.originId) && str(v.batchId) && typeof v.digest === "string" && HEX64.test(v.digest)
    && typeof v.receiptId === "string" && /^rhr_[0-9a-f]{64}$/.test(v.receiptId) && (v.state === "pending" || v.state === "committed")
    && (v.claimToken === null || (typeof v.claimToken === "string" && TOKEN.test(v.claimToken))) && count(v.fenceVersion)
    && (v.fenceInstance === null || (typeof v.fenceInstance === "string" && TOKEN.test(v.fenceInstance))) && count(v.resets)
    && isObject(v.epochs) && Object.entries(v.epochs).every(([k, n]) => HEX32.test(k) && count(n));
}
const entryOk = (r: unknown): r is IndexEntry => Array.isArray(r) && r.length === 6 && Number.isSafeInteger(r[0]) && (r[0] as number) >= 1 && (r[1] === null || str(r[1]))
  && (r[2] === null || (typeof r[2] === "string" && HEX64.test(r[2]))) && (r[3] === null || (typeof r[3] === "string" && HEX32.test(r[3]))) && count(r[4]) && count(r[5]);
export function validIndex(v: unknown): v is IndexDoc {
  return isObject(v) && v.v === 1 && str(v.originId) && typeof v.shard === "string" && SHARD.test(v.shard) && isObject(v.records)
    && Object.values(v.records).every(entryOk); // record ids are read with own() and Maps only, so any id is a plain key
}
export function validOwner(v: unknown): v is OwnerDoc {
  return isObject(v) && v.v === 1 && str(v.originId) && str(v.source) && str(v.sessionId) && count(v.epoch) && count(v.resets);
}
export function validRearm(v: unknown): v is RearmDoc {
  return isObject(v) && exactKeys(v, ["v", "originId", "source", "sessionId", "machineId", "atMs"]) && v.v === 1 && mapKey(v.originId) && str(v.source) && str(v.sessionId)
    && typeof v.machineId === "string" && /^srv_[0-9a-f]{32}$/.test(v.machineId) && count(v.atMs);
}
/** True when `key` is where ledger-s3 writes `doc`'s marker (its origin hash and session key), under `dir`. */
export function rearmKeyMatches(dir: string, key: string, doc: RearmDoc): boolean {
  return key === `${dir}/ledger/rearm/o=${h(doc.originId)}/${sessionKeyHash(doc.source, doc.sessionId)}.json`;
}
export function validFenceAuthority(v: unknown): v is FenceAuthorityDoc {
  return isObject(v) && exactKeys(v, ["v", "fenceBackfill", "revisionBackfill", "ownershipBackfill", "controlMirror", "rearmSweeper", "workerHttpAdmission", "writtenAtMs"]) && v.v === 1
    && v.fenceBackfill === "complete" && v.revisionBackfill === "complete" && v.ownershipBackfill === "complete" && v.controlMirror === "on" && v.rearmSweeper === "live"
    && v.workerHttpAdmission === "off" && count(v.writtenAtMs) && v.writtenAtMs > 0;
}

// ---- fence rules (shared with the backfill and the drain: never reimplement them) ----

/**
 * A valid fence with every field present: a fence written before `resetsCompleted` existed (validFence accepts it
 * only with no reset running) gets `resetsCompleted = resets`. readFence applies it; call it on any other read.
 */
export function normalizedFence(f: FenceDoc): FenceDoc {
  return (f as Partial<FenceDoc>).resetsCompleted === undefined ? { ...f, resetsCompleted: f.resets } : f;
}

/** The ONLY way a fence is created: empty, version 0, a fresh random instance id. */
export function newFence(newId: () => string = randomUUID): FenceDoc {
  const instance = newId();
  if (!TOKEN.test(instance)) throw new LedgerUnavailableError("ledger_fence_instance_invalid");
  return { v: 1, instance, version: 0, resets: 0, resetsCompleted: 0, lastResetAtMs: 0, deleted: {}, epochs: {}, resetRequests: {} };
}

/** What a missing fence means to ADMISSION (claim / commit) only: nothing deleted, never reset. Never written. */
const ABSENT_VIEW: FenceDoc = Object.freeze({ v: 1, instance: "", version: 0, resets: 0, resetsCompleted: 0, lastResetAtMs: 0, deleted: {}, epochs: {}, resetRequests: {} }) as FenceDoc;
const admissionView = (read: FenceRead): FenceDoc => (read.state === "present" ? read.fence : ABSENT_VIEW);
const instanceOf = (read: FenceRead): string | null => (read.state === "present" ? read.fence.instance : null);

/** A session's current epoch in the fence (0 when it never had one). */
export function fenceEpoch(fence: FenceDoc, originId: string, sessionKey: string): number {
  const m = own(fence.epochs, originId);
  return (m && own(m, sessionKey)) ?? 0;
}
export function isDeleted(fence: FenceDoc, originId: string, source: string, sessionId: string): boolean {
  return (own(fence.deleted, originId) ?? []).some(([s, id]) => s === source && id === sessionId);
}

export interface FenceChange {
  /** Tombstone these sessions. A NEW deletion bumps the session's epoch; an existing one changes nothing. */
  deletions?: { originId: string; sessions: [string, string][] }[];
  /**
   * Record a reset. A NEW request id is recorded (`result`: null while it runs, or its final count), increments
   * `resets` and raises `lastResetAtMs` to `atMs` (never lowers it). A known request id changes nothing. With a
   * final count (an already finished reset, e.g. the backfill copying SQL's) and no other reset still running,
   * `resetsCompleted` follows `resets`: the caller applies that reset's tombstones in the same edit.
   */
  reset?: { requestId: string; atMs: number; result: number | null };
}
export interface FenceChangeResult { changed: boolean; newlyDeleted: [string, string, string][]; reset: "recorded" | "known" | null }

/**
 * ledger-s3's exact fence rules, applied in place to `fence` (a copy you own; editFence passes one). The backfill
 * and the control routes call this; nothing else edits `deleted`, `epochs`, `resets`, `lastResetAtMs` or
 * `resetRequests`. Bad input throws `ledger_input_refused`.
 */
export function applyFenceChange(fence: FenceDoc, change: FenceChange): FenceChangeResult {
  const newlyDeleted: [string, string, string][] = [];
  let reset: FenceChangeResult["reset"] = null;
  for (const d of change.deletions ?? []) {
    if (!mapKey(d.originId) || !Array.isArray(d.sessions) || !d.sessions.every(pairOk)) throw new LedgerUnavailableError("ledger_input_refused");
    const have = new Map((own(fence.deleted, d.originId) ?? []).map((p) => [pairId(p[0], p[1]), p]));
    let epochs = own(fence.epochs, d.originId);
    for (const [s, id] of d.sessions) {
      if (have.has(pairId(s, id))) continue;
      have.set(pairId(s, id), [s, id]);
      if (!epochs) epochs = fence.epochs[d.originId] = {};
      const k = sessionKeyHash(s, id);
      epochs[k] = (own(epochs, k) ?? 0) + 1;
      newlyDeleted.push([d.originId, s, id]);
    }
    if (have.size) fence.deleted[d.originId] = [...have.values()].sort(pairCompare);
  }
  if (change.reset) {
    const { requestId, atMs, result } = change.reset;
    if (!mapKey(requestId) || !count(atMs) || !(result === null || count(result))) throw new LedgerUnavailableError("ledger_input_refused");
    if (Object.hasOwn(fence.resetRequests, requestId)) reset = "known";
    else {
      fence.resetRequests[requestId] = result;
      fence.resets += 1;
      fence.lastResetAtMs = Math.max(fence.lastResetAtMs, atMs);
      if (result !== null && !Object.values(fence.resetRequests).includes(null)) fence.resetsCompleted = fence.resets;
      reset = "recorded";
    }
  }
  return { changed: newlyDeleted.length > 0 || reset === "recorded", newlyDeleted, reset };
}

/** The generation a record (or owner stub) written now for (source, sessionId) is stamped with: [sessionKey, epoch, resets BEGUN]. */
export function recordGeneration(fence: FenceDoc, originId: string, source: string, sessionId: string | null): [string | null, number, number] {
  if (sessionId === null) return [null, 0, 0];
  const k = sessionKeyHash(source, sessionId);
  return [k, fenceEpoch(fence, originId, k), fence.resets];
}

/**
 * True when the fence has forgotten this entry: it was written before a reset that has COMPLETED (its tombstones
 * landed), or under an older epoch of its session (a disable, or a reset's tombstone, since). A reset that is begun
 * but unfinished forgets nothing. An entry newer than this fence knows was written after it was read, so it
 * counts. Session-less entries are never forgotten.
 */
export function entryForgotten(fence: FenceDoc, originId: string, e: IndexEntry): boolean {
  const [, , , k, epoch, resets] = e;
  if (k === null) return false;
  return resets < fence.resetsCompleted || epoch < fenceEpoch(fence, originId, k);
}
const genNewer = (a: { epoch: number; resets: number }, b: { epoch: number; resets: number }) => a.resets > b.resets || (a.resets === b.resets && a.epoch > b.epoch);

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
  /**
   * "required": every claim first checks `<keyRoot>/.ledger/fence-authority.json` (FenceAuthorityDoc) and
   * answers a retry (`ledger_fence_authority_missing` / `_invalid`) without it. The hosted Lambda always sets
   * it; self-host always sets it with INGEST_LEDGER=s3 (every admission path). Controls are never gated.
   */
  fenceAuthority?: "required";
  /** How long a VALID marker read is trusted (default 60 s). A missing or invalid one is never cached. */
  fenceAuthorityTtlMs?: number;
  /**
   * false: a claim never completes a reset another attempt left running; it admits against the fence as it is
   * (a running reset is a state claims already handle) and only the reset's own retry completes it. Self-host
   * sets false, because its SQL mirror runs on the control path only: a claim-side completion would land the
   * tombstones in S3 and never reach SQL. Default true (hosted: no SQL; the reset retry replays the count).
   */
  claimCompletesResets?: boolean;
  /**
   * true: a reset only BEGINS (records the request id, resets + 1, lastResetAtMs: one fence CAS) and answers at
   * once; it never lists owner stubs. For a caller with a hard subrequest budget (the Worker's fence mirror,
   * INGEST_S3_FENCE_MIRROR): the next claim that completes resets, or a reset retry through a full ledger, finishes
   * it, and until then every entry is judged as before (resetsCompleted). Its answer counts nothing (0).
   */
  resetBeginOnly?: boolean;
  nowMs?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  newToken?: () => string;
}

const PARALLEL = 6;
/** Reset: ownership stubs read at once per LIST page. */
const RESET_READS = 8;

/**
 * One entry to merge. `revisionId` / `digest` null: a revision-only entry (IndexEntry), for the backfill of SQL
 * rows that have none (seeded proven records: both null; lazy native slots: revisionId null). Stamp the generation
 * with recordGeneration(fence, origin, source, sessionId); a row with no session takes [null, 0, 0].
 */
export interface IndexEntryInput { recordId: string; revision: number; revisionId: string | null; digest: string | null; sessionKey: string | null; epoch: number; resets: number }

/** Validates one IndexEntryInput (the backfill builds them from SQL rows): `ledger_input_refused` when malformed. */
export function checkIndexEntryInput(r: IndexEntryInput): void {
  if (!isObject(r) || !str(r.recordId) || !entryOk([r.revision, r.revisionId, r.digest, r.sessionKey, r.epoch, r.resets])) throw new LedgerUnavailableError("ledger_input_refused");
}

/**
 * The ledger's objects with their exact semantics, shared so other S3-native jobs (the backlog drain, the fence
 * backfill, the control routes' fence-first write) read and merge the SAME formats rather than their own:
 *
 *   keysFor(tenant)                     the per-user keys (refuses a non-segment org)
 *   readFence(key)                      FenceRead: {state: "absent"} or {state: "present", fence, version};
 *                                       unreadable throws (fail closed). The drain must refuse "absent".
 *   editFence(key, edit)                CAS edit; an absent fence starts from newFence(); `edit` returns
 *                                       {next: null} for no change; version + 1 per write. Change the fence
 *                                       only through applyFenceChange inside `edit`.
 *   readIndex(key)                      a revision index shard, or null when absent; unreadable throws
 *   mergeIndex(key, fence, origin, entries)  per record: a same-session newer generation wins, else the max
 *                                       revision; an equal revision keeps the stored entry; entries the fence
 *                                       has forgotten are dropped (both stored and incoming). CAS-retried.
 *   registerOwner(key, doc)           the ownership stub, kept at its oldest generation of its current epoch
 *   writeRearm(key, marker)             the re-arm marker for the eraser: created, or re-armed (If-Match) when present
 *   readBatch(key)                      a batch record, or null when absent
 */
export function s3LedgerObjects(store: ObjectStore, keyRoot: string, opts: Omit<S3LedgerOptions, "acceptanceMode" | "newToken" | "fenceAuthority" | "fenceAuthorityTtlMs"> & { newFenceId?: () => string } = {}) {
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
  async function readFence(key: string): Promise<FenceRead> {
    const got = await readDoc(store, key, validFence, "ledger_fence_invalid");
    return got ? { state: "present", fence: normalizedFence(got.doc), version: got.version } : { state: "absent" };
  }

  /** CAS edit of the fence, as async-admission.ts editJournal. `edit` returns null for "no change". */
  async function editFence<T>(key: string, edit: (f: FenceDoc) => { next: FenceDoc | null; result: T }): Promise<T> {
    for (let attempt = 0; attempt < attempts; attempt++) {
      const read = await readFence(key);
      const base = read.state === "present" ? structuredClone(read.fence) : newFence(opts.newFenceId);
      const prior = base.version;
      const { next, result } = edit(base);
      if (!next) return result;
      next.version = prior + 1;
      if (await casPut(store, key, next, read.state === "present" ? read.version : undefined)) return result;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_fence_contended");
  }

  const readIndex = (key: string) => readDoc(store, key, validIndex, "ledger_revision_index_invalid");

  /** Merge entries into one origin's index shard (see the header). `fence` is the caller's latest fence read. */
  async function mergeIndex(key: string, fence: FenceDoc, originId: string, entries: IndexEntryInput[]): Promise<void> {
    const best = new Map<string, IndexEntryInput>();
    for (const r of entries) {
      const cur = best.get(r.recordId);
      checkIndexEntryInput(r);
      // Highest revision; at an equal one a full entry before a revision-only one, then revisionId byte order.
      const better = !cur || r.revision > cur.revision || (r.revision === cur.revision && (cur.revisionId === null
        ? r.revisionId !== null || (r.digest !== null && cur.digest === null)
        : r.revisionId !== null && byteCompare(r.revisionId, cur.revisionId) < 0));
      if (better) best.set(r.recordId, r);
    }
    const shard = /\/r=([0-9a-f]{2})\.json$/.exec(key)?.[1];
    if (!shard) throw new LedgerUnavailableError("ledger_input_refused");
    for (const r of best.values()) if (revisionShard(r.recordId) !== shard) throw new LedgerUnavailableError("ledger_input_refused");
    for (let attempt = 0; attempt < attempts; attempt++) {
      const got = await readIndex(key);
      if (got && (got.doc.originId !== originId || got.doc.shard !== shard)) throw new LedgerUnavailableError("ledger_revision_index_invalid");
      const map = new Map<string, IndexEntry>(got ? Object.entries(got.doc.records) : []);
      let changed = false;
      for (const [id, e] of map) if (entryForgotten(fence, originId, e)) { map.delete(id); changed = true; }
      for (const [id, r] of best) {
        const n: IndexEntry = [r.revision, r.revisionId, r.digest, r.sessionKey, r.epoch, r.resets];
        if (entryForgotten(fence, originId, n)) continue; // these records predate a control: never stored
        const s = map.get(id);
        let take: boolean;
        if (!s) take = true;
        else if (s[3] === n[3] && (s[4] !== n[4] || s[5] !== n[5])) take = genNewer({ epoch: n[4], resets: n[5] }, { epoch: s[4], resets: s[5] });
        else take = n[0] > s[0]; // #250: an equal revision keeps the stored entry, whichever session wrote it
        if (take) { map.set(id, n); changed = true; }
      }
      if (!changed) return;
      const doc: IndexDoc = { v: 1, originId, shard, records: Object.fromEntries(map) };
      if (await casPut(store, key, doc, got?.version)) return;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_revision_contended");
  }

  const readOwner = (key: string) => readDoc(store, key, validOwner, "ledger_owner_invalid");

  /**
   * Register a session's ownership (ingest_session_ownership) at `doc`'s generation. The stub keeps the OLDEST
   * generation of its current epoch: it moves only to a newer epoch (a disable, or a finished reset, made the old
   * one irrelevant), never forward in resets alone, so an unfinished reset still lists the pre-reset claim.
   */
  async function registerOwner(key: string, doc: OwnerDoc): Promise<void> {
    for (let attempt = 0; attempt < attempts; attempt++) {
      const got = await readOwner(key);
      if (got) {
        if (got.doc.originId !== doc.originId || got.doc.source !== doc.source || got.doc.sessionId !== doc.sessionId) throw new LedgerUnavailableError("ledger_owner_invalid");
        if (got.doc.epoch >= doc.epoch) return;
      }
      if (await casPut(store, key, doc, got?.version)) return;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_owner_contended");
  }

  /**
   * Arm the session's marker. A marker already there may be one the sweeper has finished (its `.done/<hash>` record
   * written), and the canonical objects this commit wrote came AFTER that erase, so a present marker is re-armed: its
   * content is replaced (If-Match) with a strictly later `atMs`, which is a new content hash the sweeper has no
   * completion for. Re-arming one the sweeper has not reached yet costs one extra erase pass, never a lost one.
   */
  async function writeRearm(key: string, marker: { originId: string; source: string; sessionId: string; machineId: string }): Promise<void> {
    for (let attempt = 0; attempt < attempts; attempt++) {
      let got: { doc: RearmDoc; version: ObjectVersion } | null;
      try {
        got = await readDoc(store, key, validRearm, "ledger_rearm_invalid");
      } catch (error) {
        if (!(error instanceof LedgerUnavailableError) || error.code !== "ledger_rearm_invalid") throw error;
        // A corrupt marker (the sweeper reports it, `corrupt`) is replaced by a valid one, so it is erased again and
        // never wedges this session's commits. Still If-Match on the version just seen.
        let meta;
        try { meta = await store.head(objectKey(key)); } catch (e) { throw storeFailure(e); }
        if (meta && await casPut(store, key, { v: 1, ...marker, atMs: nowMs() }, meta.version)) return;
        await backoff(attempt);
        continue;
      }
      const atMs = got ? Math.max(nowMs(), got.doc.atMs + 1) : nowMs();
      if (await casPut(store, key, { v: 1, ...marker, atMs }, got?.version)) return;
      await backoff(attempt);
    }
    throw new LedgerUnavailableError("ledger_rearm_contended");
  }

  const readBatch = (key: string) => readDoc(store, key, validBatch, "ledger_batch_record_invalid");

  /** Every owner stub of a user (LIST + GET, 8 at a time). A corrupt stub is `ledger_owner_invalid`; a store error is coded. */
  async function listOwners(keys: ReturnType<typeof s3LedgerKeys>): Promise<OwnerDoc[]> {
    const out: OwnerDoc[] = [];
    const prefix = keys.ownersRoot + "/";
    let cursor: string | undefined;
    try {
      do {
        const page = await store.list(objectKey(keys.ownersRoot), cursor);
        const docs = await mapLimit(page.items.map((i) => String(i.key)).filter((k) => k.startsWith(prefix)), RESET_READS, async (k) => (await readOwner(k))?.doc);
        for (const d of docs) if (d) out.push(d);
        cursor = page.nextCursor;
      } while (cursor);
    } catch (error) {
      throw storeFailure(error);
    }
    return out;
  }

  return { attempts, backoff, nowMs, keysFor, readFence, editFence, readIndex, mergeIndex, readOwner, registerOwner, listOwners, writeRearm, readBatch };
}

export function s3Ledger(store: ObjectStore, keyRoot: string, opts: S3LedgerOptions = {}): Ledger {
  if (opts.acceptanceMode === "buffered") throw new LedgerUnavailableError("ledger_s3_mode_refused");
  const io = s3LedgerObjects(store, keyRoot, opts);
  const { attempts, backoff, nowMs, keysFor, readFence, editFence, readIndex, mergeIndex, readOwner, registerOwner, writeRearm } = io;
  const newToken = opts.newToken ?? randomUUID;
  const claimCompletesResets = opts.claimCompletesResets ?? true;
  const authorityTtl = opts.fenceAuthorityTtlMs ?? 60_000;
  if (!count(authorityTtl)) throw new LedgerUnavailableError("ledger_s3_options_refused");
  let authorityUntil = -Infinity;

  /** Fail closed: no admission without a valid fence-authority marker (FenceAuthorityDoc). */
  async function requireFenceAuthority(): Promise<void> {
    if (opts.fenceAuthority !== "required") return;
    const now = nowMs();
    if (now < authorityUntil) return;
    const got = await readDoc(store, fenceAuthorityKey(keyRoot), validFenceAuthority, "ledger_fence_authority_invalid");
    if (!got) throw new LedgerUnavailableError("ledger_fence_authority_missing");
    authorityUntil = now + authorityTtl;
  }

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

  /** Claim steps (b) and (c): the deletion filter, then #250's revision rules over the origin's stored index. */
  async function judge(input: ClaimInput, keys: ReturnType<typeof s3LedgerKeys>, fence: FenceDoc) {
    const fenced = new Set((own(fence.deleted, input.originId) ?? []).map(([s, id]) => pairId(s, id)));
    const deleted = batchPairs(input.sessions, input.records).filter(([s, id]) => fenced.has(pairId(s, id)));
    const deletedSet = new Set(deleted.map(([s, id]) => pairId(s, id)));
    const live = input.records.map((r, ord) => ({ ...r, ord })).filter((r) => r.sessionId === null || !deletedSet.has(pairId(r.source, r.sessionId)));
    const shardKeys = [...new Set(live.map((r) => keys.revisions(input.originId, r.recordId)))];
    const loaded = await mapLimit(shardKeys, PARALLEL, async (k) => {
      const doc = (await readIndex(k))?.doc;
      if (doc && doc.originId !== input.originId) throw new LedgerUnavailableError("ledger_revision_index_invalid");
      return [k, doc?.records ?? {}] as const;
    });
    const shards = new Map(loaded.map(([k, recs]) => [k, recs]));
    // Per (origin, record id), whichever session stored it; an entry the fence has forgotten counts as absent.
    const storedOf = (r: (typeof live)[number]) => {
      const e = own(shards.get(keys.revisions(input.originId, r.recordId))!, r.recordId);
      return e && !entryForgotten(fence, input.originId, e) ? e[0] : undefined;
    };
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
    return { deleted, deletedSet, stale, conflicts, twins, fast: stale.length === live.length };
  }

  /** A selection's fence effect (ingest_apply_selection). `mark` decides an ordered control inside the same fence edit. */
  async function applySelection(input: SelectionInput, control?: OrderedControl): Promise<{ outcome: "applied" | "replay" | "stale"; updated: number }> {
    const mark = control ? orderedMark(control) : undefined;
    if (!mapKey(input.originId) || !Array.isArray(input.sessions) || !input.sessions.every(pairOk) || typeof input.enabled !== "boolean") throw new LedgerUnavailableError("ledger_input_refused");
    const keys = keysFor(input.tenant);
    const pairs = batchPairs(input.sessions, []);
    if (input.enabled) {
      // Lift the tombstone. The disable that set it bumped the session's epoch, so its old revisions stay
      // forgotten (a stale entry generation) and the next upload writes them again.
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
      const { changed } = applyFenceChange(f, { deletions: [{ originId: input.originId, sessions: pairs }] });
      if (control) finishOrdered(f, control, pairs.length);
      return { next: changed || control ? f : null, result: "apply" as const };
    });
    if (decided !== "apply") return { outcome: decided.outcome, updated: decided.result };
    // The epoch bump already forgot the sessions' revisions (their entries are now a stale generation). The
    // count is the sessions fenced: SQL's revision-row count has no S3 equivalent without reading every shard.
    return { outcome: "applied", updated: pairs.length };
  }

  /**
   * A reset (ingest_apply_reset). Begin: record the request id, resets + 1, lastResetAtMs (applyFenceChange).
   * Then finishResets. A finished request id replays its count.
   */
  async function applyReset(tenant: LedgerTenant, requestId: string, control?: OrderedControl): Promise<{ outcome: "applied" | "replay" | "stale"; deleted: number }> {
    const mark = control ? orderedMark(control, requestId) : undefined;
    if (!mapKey(requestId)) throw new LedgerUnavailableError("ledger_input_refused");
    const keys = keysFor(tenant);
    const atMs = nowMs();
    const begun = await editFence<Decision>(keys.fence, (f) => {
      const d = mark?.(f);
      if (d && d !== "apply") return { next: null, result: d };
      const prior = own(f.resetRequests, requestId);
      // Finished (by this request's earlier attempt, or by a claim or another reset): replay, closing the ordered mark.
      if (typeof prior === "number") return { next: control && finishOrdered(f, control, prior) ? f : null, result: { outcome: "replay" as const, result: prior } };
      // Begun by an earlier attempt that stopped part way: finish it (the ordered mark, if any, is kept).
      if (prior === null) return { next: control ? f : null, result: "apply" as const };
      applyFenceChange(f, { reset: { requestId, atMs, result: null } });
      return { next: f, result: "apply" as const };
    });
    if (begun !== "apply") return { outcome: begun.outcome, deleted: begun.result };
    if (opts.resetBeginOnly) return { outcome: "applied", deleted: 0 };
    const n = await finishResets(keys, requestId, control);
    return { outcome: "applied", deleted: n };
  }

  /**
   * Finish every reset that was running when the owner stubs are listed: tombstone each session claimed at a
   * reset count BELOW the begun count whose epoch has not moved (not disabled or already tombstoned), give each of
   * those resets the count, and advance `resetsCompleted` in the SAME CAS, so no entry is forgotten before its
   * session is tombstoned. Overlapping or interrupted resets all converge here; a reset begun after the listing
   * stays running for its own retry or the next claim. Returns `requestId`'s count (or the count tombstoned).
   */
  async function finishResets(keys: ReturnType<typeof s3LedgerKeys>, requestId?: string, control?: OrderedControl): Promise<number> {
    const read = await readFence(keys.fence);
    if (read.state === "absent") throw new LedgerUnavailableError("ledger_fence_invalid"); // a begun reset wrote it
    const now = read.fence;
    const runningAtList = Object.entries(now.resetRequests).filter(([, v]) => v === null).map(([k]) => k);
    const owned: OwnerDoc[] = [];
    let cursor: string | undefined;
    const prefix = keys.ownersRoot + "/";
    try {
      do {
        const page = await store.list(objectKey(keys.ownersRoot), cursor);
        const docs = await mapLimit(page.items.map((i) => String(i.key)).filter((k) => k.startsWith(prefix)), RESET_READS, async (k) => {
          // A corrupt stub fails the completion (fail closed: it could be an owned session), never silently skipped.
          try { return (await readOwner(k))?.doc; } catch (error) {
            if (error instanceof LedgerUnavailableError && error.code === "ledger_owner_invalid") throw new LedgerUnavailableError("ledger_reset_owner_invalid");
            throw error;
          }
        });
        for (const d of docs) {
          if (!d || !mapKey(d.originId)) continue;
          if (d.resets < now.resets && d.epoch === fenceEpoch(now, d.originId, sessionKeyHash(d.source, d.sessionId)) && !isDeleted(now, d.originId, d.source, d.sessionId)) owned.push(d);
        }
        cursor = page.nextCursor;
      } while (cursor);
    } catch (error) {
      throw storeFailure(error);
    }
    return editFence<number>(keys.fence, (f) => {
      const prior = requestId === undefined ? undefined : own(f.resetRequests, requestId);
      if (typeof prior === "number") return { next: control && finishOrdered(f, control, prior) ? f : null, result: prior };
      // Only what is still as listed: a session disabled, re-enabled or tombstoned since moved its epoch.
      const still = owned.filter((d) => fenceEpoch(f, d.originId, sessionKeyHash(d.source, d.sessionId)) === d.epoch);
      const byOrigin = new Map<string, [string, string][]>();
      for (const d of still) byOrigin.set(d.originId, [...(byOrigin.get(d.originId) ?? []), [d.source, d.sessionId]]);
      const { newlyDeleted } = applyFenceChange(f, { deletions: [...byOrigin].map(([originId, sessions]) => ({ originId, sessions })) });
      const n = new Set(newlyDeleted.map(([, s, id]) => pairId(s, id))).size;
      let changed = newlyDeleted.length > 0;
      for (const id of runningAtList) if (own(f.resetRequests, id) === null) { f.resetRequests[id] = n; changed = true; }
      if (f.resetsCompleted < now.resets) { f.resetsCompleted = now.resets; changed = true; }
      if (control) changed = finishOrdered(f, control, n) || changed;
      // Another reset's ordered mark: close it only if THAT reset was running at this listing (its tombstones are
      // the ones just landed). A mark without a request id, or of a reset begun after the listing, stays open.
      else if (f.ordered && f.ordered.kind === "reset" && f.ordered.result === null && f.ordered.requestId !== undefined
        && runningAtList.includes(f.ordered.requestId) && typeof own(f.resetRequests, f.ordered.requestId) === "number") { f.ordered.result = n; changed = true; }
      const mine = requestId === undefined ? n : own(f.resetRequests, requestId) ?? n;
      return { next: changed ? f : null, result: typeof mine === "number" ? mine : n };
    });
  }

  /**
   * The ordered-control decision kept in the fence. SQL stays the ordering authority while controls
   * dual-write (decision 0005 "Controls"), so this only dedupes: a lower version of the same epoch is stale,
   * the same version replays (another digest refuses), anything higher applies.
   */
  const orderedMark = (control: OrderedControl, requestId?: string) => (f: FenceDoc): "apply" | { outcome: "replay" | "stale"; result: number } => {
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
    f.ordered = { epoch: control.epoch, version: control.version, digest: control.digest, kind: control.kind, result: null, ...(requestId !== undefined ? { requestId } : {}) };
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
      await requireFenceAuthority();
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
        const read = await readFence(keys.fence);
        const fence = admissionView(read);
        // A reset begun but not finished (it crashed, or is still running): finish it before judging, so a reset is
        // never left half done by a client that stopped retrying, then claim again (backed off, with jitter: many
        // claims may race to finish it). A completion that fails (a corrupt owner stub, a store error) fails only
        // the completion: this claim goes on against the fence as it is, and the reset's own retry reports the code.
        if (claimCompletesResets && fence.resets > fence.resetsCompleted && attempt < attempts - 1) {
          let finished = false;
          try { await finishResets(keys); finished = true; } catch (error) { if (!(error instanceof LedgerUnavailableError)) throw error; }
          if (finished) { await backoff(attempt); continue; }
        }
        const judged = await judge(input, keys, fence);
        if (judged.twins > 0) return { kind: "revision_conflict", conflicts: judged.conflicts, count: judged.twins };
        const pairs = batchPairs(input.sessions, input.records);
        if (!judged.fast) {
          // (c') Ownership (ingest_session_ownership) BEFORE any canonical write: a reset that begins after this
          // lists these sessions. Then the fence again: if it moved meanwhile (a reset could have listed before
          // the stubs landed), claim again under the new fence rather than carry a generation it outran.
          const live = pairs.filter(([s, id]) => !judged.deletedSet.has(pairId(s, id)));
          await mapLimit(live, PARALLEL, ([s, id]) => {
            const [, epoch, resets] = recordGeneration(fence, input.originId, s, id);
            return registerOwner(keys.owner(input.originId, s, id), { v: 1, originId: input.originId, source: s, sessionId: id, epoch, resets });
          });
          // Claim again only if the fence moved in a way that touches THIS claim (a reset, or one of its sessions'
          // epoch or deletion), never for an unrelated edit, so other sessions' controls cannot starve it.
          // A recreated fence (another instance) is rechecked in full at commit, as any snapshot mismatch is.
          const again = await readFence(keys.fence);
          const next = admissionView(again);
          if (next.resets !== fence.resets || pairs.some(([s, id]) => {
            const k = sessionKeyHash(s, id);
            return fenceEpoch(next, input.originId, k) !== fenceEpoch(fence, input.originId, k) || isDeleted(next, input.originId, s, id) !== isDeleted(fence, input.originId, s, id);
          })) { await backoff(attempt); continue; }
        }
        // (d) The claim; committed at once when nothing needs writing (replay_fast).
        const token = judged.fast ? null : newToken();
        const epochs: Record<string, number> = {};
        for (const [s, id] of pairs) { const k = sessionKeyHash(s, id); epochs[k] = fenceEpoch(fence, input.originId, k); }
        const doc: BatchDoc = { v: 1, originId: input.originId, batchId: input.batchId, digest: input.batchDigest, receiptId: input.receiptId,
          state: judged.fast ? "committed" : "pending", claimToken: token, fenceVersion: fence.version, fenceInstance: instanceOf(read), resets: fence.resets, epochs };
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
      if (pairs.some(([s, id]) => !Object.hasOwn(snapshot.epochs, sessionKeyHash(s, id)))) throw new LedgerUnavailableError("ledger_attempt_mismatch");

      // ingest_attempt_fenced: a session deleted now, or whose epoch or the reset count moved since the claim.
      // Nothing to recheck only when this is the SAME fence object (instance) at the same version.
      const fencedBy = (read: FenceRead) => {
        const fence = admissionView(read);
        const deleted = new Set((own(fence.deleted, input.originId) ?? []).map(([s, id]) => pairId(s, id)));
        if (instanceOf(read) === snapshot.fenceInstance && fence.version === snapshot.fenceVersion) return { fence, fenced: [] as [string, string][], deleted };
        const fenced = pairs.filter(([s, id]) => deleted.has(pairId(s, id)) || fence.resets !== snapshot.resets
          || fenceEpoch(fence, input.originId, sessionKeyHash(s, id)) !== snapshot.epochs[sessionKeyHash(s, id)]);
        return { fence, fenced, deleted };
      };
      const rearm = (list: [string, string][]) => mapLimit(list, PARALLEL, ([s, id]) =>
        writeRearm(keys.rearm(input.originId, s, id), { originId: input.originId, source: s, sessionId: id, machineId: input.machineId }));

      // 1. The fence, and re-arm markers for what it reached, BEFORE any index merge.
      const first = fencedBy(await readFence(keys.fence));
      await rearm(first.fenced);
      // 2. The written revisions become stored (see mergeIndex), stamped with the CLAIM's generation: a disable or
      //    reset that lands after the fence read makes them stale at once.
      const firstSet = new Set(first.fenced.map(([s, id]) => pairId(s, id)));
      const groups = new Map<string, IndexEntryInput[]>();
      for (const r of input.records) {
        if (r.sessionId !== null && firstSet.has(pairId(r.source, r.sessionId))) continue;
        const key = keys.revisions(input.originId, r.recordId);
        const sk = r.sessionId === null ? null : sessionKeyHash(r.source, r.sessionId);
        const g = groups.get(key) ?? [];
        g.push({ recordId: r.recordId, revision: r.revision, revisionId: r.revisionId, digest: r.digest, sessionKey: sk,
          epoch: sk === null ? 0 : snapshot.epochs[sk]!, resets: sk === null ? 0 : snapshot.resets });
        groups.set(key, g);
      }
      await mapLimit([...groups], PARALLEL, ([key, entries]) => mergeIndex(key, first.fence, input.originId, entries));
      // A fenced session the fence no longer deletes (re-enabled since the claim): SQL's invalidated.
      const fenced = first.fenced;
      if (fenced.some(([s, id]) => !first.deleted.has(pairId(s, id)))) throw new LedgerUnavailableError("ledger_attempt_superseded");
      // 3. pending -> committed.
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

/**
 * Self-host with INGEST_LEDGER=s3, until the read side moves (decision 0005 "Read side"): the read API and the
 * eraser still read SQL `upload_deletions`, so every control writes the S3 fence FIRST (the authority) and then
 * RECONCILES SQL to the fence: each session the control touched is disabled in SQL if the fence deletes it and
 * enabled if not. After the SQL writes the fence is read again; if it moved (another control landed), SQL is
 * reconciled again, up to `rounds` times, else the control fails (`ledger_control_mirror_unstable`; its retry is
 * idempotent). A failure of either side fails the control. This is transitional deletion state for readers, not
 * upload accounting: claims and commits never touch SQL.
 *
 *   selection          S3, then SQL for its sessions per the fence.
 *   reset              S3, then SQL resetUploadedSessions (its request record, which the read side's reset
 *                      gates read), then the reconcile over every session the fence deletes AND every session
 *                      with an owner stub. SQL's own reset tombstones everything SQL owns, including a session
 *                      S3 keeps live (disabled then re-enabled with no upload since: its stub is an older epoch,
 *                      so the S3 reset does not own it); the reconcile lifts those again, so SQL ends with exactly
 *                      the fence's deletions. Sessions only SQL owned are covered by the ownership backfill, a
 *                      precondition of the fence-authority marker (the ingest role cannot read SQL ownership
 *                      without a migration).
 *   ordered controls   S3's applyOrderedControl, then the same reconcile (even when S3 judged it stale: S3 decides);
 *                      a stale reset writes no SQL reset record.
 */
export function withSqlControlMirror(s3: Ledger, sql: Pick<Ledger, "setSessionUploads" | "resetUploadedSessions">, store: ObjectStore, keyRoot: string, opts: { rounds?: number } = {}): Ledger {
  const io = s3LedgerObjects(store, keyRoot);
  const rounds = opts.rounds ?? 4;
  const same = (a: FenceRead, b: FenceRead) => a.state === "absent" ? b.state === "absent"
    : b.state === "present" && a.fence.instance === b.fence.instance && a.fence.version === b.fence.version;
  async function reconcile(tenant: LedgerTenant, sessions: { originId: string; pair: [string, string] }[], everyDeleted: boolean): Promise<void> {
    const key = io.keysFor(tenant).fence;
    const touched = new Map(sessions.map((t) => [JSON.stringify([t.originId, ...t.pair]), t]));
    let read = await io.readFence(key);
    for (let round = 0; round < rounds; round++) {
      const f = admissionView(read);
      if (everyDeleted) for (const [originId, pairs] of Object.entries(f.deleted)) for (const pair of pairs) touched.set(JSON.stringify([originId, ...pair]), { originId, pair });
      const groups = new Map<string, { originId: string; enabled: boolean; sessions: [string, string][] }>();
      for (const { originId, pair } of touched.values()) {
        const enabled = !isDeleted(f, originId, pair[0], pair[1]);
        const g = groups.get(JSON.stringify([originId, enabled])) ?? { originId, enabled, sessions: [] };
        g.sessions.push(pair);
        groups.set(JSON.stringify([originId, enabled]), g);
      }
      for (const g of groups.values())
        for (let i = 0; i < g.sessions.length; i += 2048) await sql.setSessionUploads({ tenant, originId: g.originId, sessions: g.sessions.slice(i, i + 2048), enabled: g.enabled });
      const again = await io.readFence(key);
      if (same(read, again)) return;
      read = again;
    }
    throw new LedgerUnavailableError("ledger_control_mirror_unstable");
  }
  const of = (originId: string, sessions: [string, string][]) => sessions.map((pair) => ({ originId, pair }));
  /** After SQL's reset: every session S3 knows (owner stubs) and every one it deletes, set in SQL as the fence says. */
  const reconcileAfterReset = async (tenant: LedgerTenant) => {
    const owners = await io.listOwners(io.keysFor(tenant));
    await reconcile(tenant, owners.map((d) => ({ originId: d.originId, pair: [d.source, d.sessionId] as [string, string] })), true);
  };
  return {
    ...s3,
    async setSessionUploads(input) {
      await s3.setSessionUploads(input);
      await reconcile(input.tenant, of(input.originId, input.sessions), false);
    },
    async resetUploadedSessions(tenant, requestId) {
      const n = await s3.resetUploadedSessions(tenant, requestId);
      await sql.resetUploadedSessions(tenant, requestId);
      await reconcileAfterReset(tenant);
      return n;
    },
    async applyOrderedControl(input) {
      const r = await s3.applyOrderedControl!(input);
      if ("requestId" in input) {
        // SQL's request record only for a reset S3 applied (or replays): a stale one never resets in SQL.
        if (r.control.outcome !== "stale") await sql.resetUploadedSessions(input.tenant, input.requestId);
        await reconcileAfterReset(input.tenant);
      } else await reconcile(input.tenant, of(input.originId, input.sessions), false);
      return r;
    },
  };
}
