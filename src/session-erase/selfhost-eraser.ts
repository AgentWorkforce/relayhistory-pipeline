/**
 * Self-host per-session byte eraser (operator per-session hard delete, lane C).
 *
 * Deletes EVERY version and delete marker of one session's bytes in an S3-compatible,
 * versioned self-host store, then lists again to prove it:
 *
 *   {keyRoot}/{org}/v1/ws=…/m=…/h=…/d=yyyy/mm/dd/s=…/            transcript dir (manifest + raw/…)
 *   {keyRoot}/{org}/live-sessions/v1/ws=…/m=…/h=…/s=….json       live location pointer (erased last)
 *   {keyRoot}/{org}/live-batches/v1/ws=…/m=srv_…/b=….staged.ndjson.zst   staged batches (listed by the caller)
 *
 * Inputs mirror `session_index` rows (one `SessionEraseLocation` per row) so the operator
 * orchestrator (lane D) passes row contents. A staged key carries no session id, so the
 * eraser cannot attribute staged objects itself (that needs the DB ledger or a GetObject of
 * the staged header, which the eraser key does not hold): the orchestrator supplies them.
 *
 * Safety: every target prefix and every doomed key is checked against the allow-list computed
 * from the request before any delete (on top of the shared primitive's own per-page prefix
 * check); `buffered-batches` objects are refused (the buffered-GC redaction protocol owns them);
 * staged versions younger than the 48 h fan-out redrive window are kept and reported pending.
 *
 * Deletes go through the shared strict paged primitive (neon-s3-erasure/eraser.ts eraseStep /
 * verifyErased): by version id, paged, bounded per run, resumable by checkpoint.
 *
 * Completion means bytes gone: `verified` is true only when a fresh strict listing of every
 * target finds zero versions and zero delete markers. Log lines carry counts and codes only,
 * never a key, a session id or any other identifier.
 */
import { createHash } from "node:crypto";
import { parseUploadGenerationPrefix } from "../../packages/object-store/src/layout.ts";
import type { EraseCheckpoint, ListedVersion, PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import { layoutComponent } from "../shared/layout-component.ts";

/** The fan-out redrive window (neon-s3-erasure/job.ts STAGED_ERASE_AFTER_MS, pinned by a test). */
export const STAGED_MIN_AGE_MS = 48 * 60 * 60 * 1000;
export const DEFAULT_CONCURRENCY = 4;
export const MAX_CONCURRENCY = 16;
/** Bounded eraseStep runs per target (each run is capped at 10k versions / 100 pages). */
export const DEFAULT_MAX_RUNS_PER_TARGET = 50;
const MAX_LOCATIONS = 64;
const MAX_STAGED_KEYS = 1000;
const MAX_DATES = 32;

/** The operator erase key (canonical, pre-layout-encoding source ids). */
export interface SessionEraseKey {
  orgId: string;
  /** The canonical workspace id, as stored on publication rows (`tenant.workspaceId`). */
  workspaceId: string;
  /** The harness/source, e.g. `claude-code`. */
  source: string;
  sessionId: string;
}

/**
 * One `session_index` row's storage coordinates. `workspaceId`, `machineId`, `harness` and
 * `sessionId` are the row's values; each must equal `layoutComponent` of the erase key's id
 * (the physical path segment). Rows store the canonical workspace id; a layout-safe id is
 * identical to its physical value, and any other id is refused (`location_not_this_session`,
 * fail closed). Through `layoutComponent` two raw ids (e.g. `a:b` and `a_b`) can share one
 * physical directory, and erasing one erases that shared directory. At least one date source
 * is required: the row's `manifestKey` (authoritative: it fixes `d=`; required unless the row
 * has none) and/or explicit `dates` (`yyyy-mm-dd`). A `manifestKey` under the upload-generation
 * layout (`{keyRoot}/{org}/upload-generations/g=<uuid>/…`) is a publication generation, which
 * this eraser does not cover: it is refused with `location_publication_generation`.
 */
export interface SessionEraseLocation {
  workspaceId: string;
  machineId: string;
  harness: string;
  sessionId: string;
  manifestKey?: string | null;
  dates?: readonly string[];
  /** The row's `storage_provider`: only `s3` / `r2` (S3-compatible, versioned) are accepted. */
  storageProvider?: string | null;
  /** The row's `bucket`: must equal the eraser's bucket when present. */
  bucket?: string | null;
}

export interface SessionEraseBytesRequest {
  key: SessionEraseKey;
  locations: readonly SessionEraseLocation[];
  /** Staged batch keys attributed to this session by the orchestrator (DB ledger / header read). */
  stagedKeys?: readonly string[];
}

export interface SessionEraseBytesOptions {
  keyRoot: string;
  /** The eraser's bucket (checked against each location's `bucket`). */
  bucket: string;
  dryRun?: boolean;
  /** Targets processed at once (default 4, max 16). */
  concurrency?: number;
  maxRunsPerTarget?: number;
  /** Staged versions younger than this are kept and reported pending (default 48 h). */
  stagedMinAgeMs?: number;
  now?: () => number;
  /** Re-checks the caller's authority before every page's deletes (default: none). */
  authorize?: () => Promise<void>;
  /**
   * Successor cutoff (ms since epoch): only versions last modified BEFORE it are erased (a version without a
   * timestamp is never erased), as the hosted live lane keeps versions newer than its fence re-read (neon-s3-erasure
   * job.ts LIVE_SKEW_MS). Kept versions still count against verification, so `verified` stays false while any remain.
   * Default: none (every version).
   */
  modifiedBefore?: number;
  log?: (line: Record<string, unknown>) => void;
}

export interface SessionEraseBytesResult {
  dryRun: boolean;
  /** Distinct transcript-dir keys deleted (dry run: found). */
  transcriptObjects: number;
  /** Distinct live pointer keys deleted (dry run: found). */
  pointerObjects: number;
  /** Distinct staged keys deleted (dry run: found). */
  stagedObjects: number;
  /** Versions and delete markers deleted (dry run: found). */
  versionsDeleted: number;
  /** Staged versions kept because they are inside the redrive window. */
  stagedPendingVersions: number;
  /** Targets whose bounded runs ended before a full traversal. */
  incompleteTargets: number;
  /**
   * Live pointer targets left in place this run because another target was incomplete or had
   * staged versions still inside the redrive window (always 0 on a dry run).
   */
  deferredPointerTargets: number;
  /** True only when a fresh listing found nothing left under every target (always false on a dry run). */
  verified: boolean;
}

export class SessionEraseRefused extends Error {
  constructor(readonly code: string) {
    super(`session_erase_${code}`);
  }
}
const refuse = (code: string): never => {
  throw new SessionEraseRefused(code);
};

type TargetKind = "transcript" | "pointer" | "staged";
export interface EraseTarget {
  kind: TargetKind;
  /** Listing prefix (ends in `/`). */
  prefix: string;
  /** Exact-key mode: only this key under `prefix`. */
  exactKey?: string;
  /** Internally constructed scope filter; never accepted from an operator request. */
  match?: RegExp;
}

const escapePattern = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const matchesTarget = (t: EraseTarget, k: string) => k.startsWith(t.prefix)
  && (t.exactKey !== undefined ? k === t.exactKey : k.length > t.prefix.length && (!t.match || t.match.test(k)));

/**
 * Find every date and machine, including objects whose pointer was never indexed.
 * A scope scan selects only exact layout segments. It also covers publication generations;
 * unrelated sessions in the same listing are never destructive authority.
 * The caller must first fence uploads and drain its write obligations.
 */
export function planDiscoveryTargets(key: SessionEraseKey, opts: Pick<SessionEraseBytesOptions, "keyRoot">): EraseTarget[] {
  if (!KEY_ROOT.test(opts.keyRoot)) refuse("key_root_invalid");
  for (const id of [key.orgId, key.workspaceId, key.source, key.sessionId]) {
    // The legacy layout encoder is lossy. Refuse ambiguous ids instead of deleting a neighbour's bytes.
    if (typeof id !== "string" || !SEGMENT.test(id) || id.startsWith(".") || layoutComponent(id) !== id)
      refuse("discovery_key_ambiguous");
  }
  const org = `${opts.keyRoot}/${key.orgId}`;
  const ws = escapePattern(key.workspaceId), h = escapePattern(key.source), s = escapePattern(key.sessionId);
  const machine = "[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}";
  const transcript = `v1/ws=${ws}/m=${machine}/h=${h}/d=\\d{4}/\\d{2}/\\d{2}/s=${s}/.+`;
  return [
    { kind: "transcript", prefix: `${org}/v1/ws=${key.workspaceId}/`, match: new RegExp(`^${escapePattern(org)}/${transcript}$`) },
    { kind: "transcript", prefix: `${org}/upload-generations/`, match: new RegExp(`^${escapePattern(org)}/upload-generations/g=[0-9a-f-]{36}/${transcript}$`) },
    { kind: "pointer", prefix: `${org}/live-sessions/v1/ws=${key.workspaceId}/`, match: new RegExp(`^${escapePattern(org)}/live-sessions/v1/ws=${ws}/m=${machine}/h=${h}/s=${s}\\.json$`) },
  ];
}

const KEY_ROOT = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
const SEGMENT = /^[A-Za-z0-9._-]{1,128}$/;
const DATE = /^(\d{4})-(\d{2})-(\d{2})$/;
const STAGED_FILE = /^b=[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{32}\.staged\.ndjson\.zst$/;
const S3_PROVIDERS = new Set(["s3", "r2"]);

function physical(value: unknown): string {
  if (typeof value !== "string" || !SEGMENT.test(value) || value.startsWith(".")) return refuse("location_invalid");
  return value as string;
}
function nonEmpty(value: unknown, code: string): string {
  if (typeof value !== "string" || !value || value.length > 1024) return refuse(code);
  return value as string;
}
function validDate(raw: string): [string, string, string] {
  const m = DATE.exec(raw);
  if (!m) return refuse("date_invalid");
  const [, y, mo, d] = m as unknown as [string, string, string, string];
  const t = new Date(`${y}-${mo}-${d}T00:00:00Z`);
  if (Number.isNaN(t.getTime()) || t.toISOString().slice(0, 10) !== raw) refuse("date_invalid");
  return [y, mo, d];
}

/**
 * The exact set of prefixes / keys this request may delete. Pure: refuses (SessionEraseRefused,
 * code only) any malformed id, a location that is not this key's session, a manifest key that
 * is not this location's session dir, a non-S3 or other-bucket row, and any staged key that is
 * not a `live-batches` staged object of one of the request's machines.
 */
export function planTargets(req: SessionEraseBytesRequest, opts: Pick<SessionEraseBytesOptions, "keyRoot" | "bucket">): EraseTarget[] {
  if (!KEY_ROOT.test(opts.keyRoot)) refuse("key_root_invalid");
  const { key } = req;
  if (!key || typeof key.orgId !== "string" || !SEGMENT.test(key.orgId) || key.orgId.startsWith(".")) refuse("org_invalid");
  const ws = layoutComponent(nonEmpty(key.workspaceId, "key_invalid"));
  const harness = layoutComponent(nonEmpty(key.source, "key_invalid"));
  const sid = layoutComponent(nonEmpty(key.sessionId, "key_invalid"));
  if (!Array.isArray(req.locations) || req.locations.length < 1 || req.locations.length > MAX_LOCATIONS) refuse("locations_invalid");
  const staged = req.stagedKeys ?? [];
  if (!Array.isArray(staged) || staged.length > MAX_STAGED_KEYS) refuse("staged_invalid");
  const org = `${opts.keyRoot}/${key.orgId}`;

  const targets = new Map<string, EraseTarget>();
  const add = (t: EraseTarget) => targets.set(JSON.stringify([t.prefix, t.exactKey ?? null]), t);
  const stagedDirs = new Set<string>();
  for (const loc of req.locations) {
    if (!loc || typeof loc !== "object") refuse("location_invalid");
    const lws = physical(loc.workspaceId), m = physical(loc.machineId), h = physical(loc.harness), s = physical(loc.sessionId);
    // A row of another session (or another workspace / source) is never erased under this key.
    if (lws !== ws || h !== harness || s !== sid) refuse("location_not_this_session");
    if (loc.storageProvider != null && !S3_PROVIDERS.has(loc.storageProvider)) refuse("store_not_versioned_s3");
    if (loc.bucket != null && loc.bucket !== opts.bucket) refuse("bucket_mismatch");
    const machineBase = `${org}/v1/ws=${lws}/m=${m}/h=${h}/`;
    const dates = new Set<string>();
    if (loc.dates !== undefined) {
      if (!Array.isArray(loc.dates) || loc.dates.length > MAX_DATES) refuse("date_invalid");
      for (const d of loc.dates) dates.add(validDate(String(d)).join("/"));
    }
    if (loc.manifestKey != null) {
      const mk = String(loc.manifestKey);
      if (isPublicationGenerationManifest(mk, org, lws, m, h, s)) refuse("location_publication_generation");
      if (!mk.startsWith(machineBase)) refuse("manifest_key_mismatch");
      const m2 = /^d=(\d{4})\/(\d{2})\/(\d{2})\/s=([^/]+)\/manifest\.json$/.exec(mk.slice(machineBase.length));
      if (!m2 || m2[4] !== s) refuse("manifest_key_mismatch");
      dates.add(validDate(`${m2![1]}-${m2![2]}-${m2![3]}`).join("/"));
    }
    if (!dates.size) refuse("date_required");
    for (const d of dates) add({ kind: "transcript", prefix: `${machineBase}d=${d}/s=${s}/` });
    const pointerDir = `${org}/live-sessions/v1/ws=${lws}/m=${m}/h=${h}/`;
    add({ kind: "pointer", prefix: pointerDir, exactKey: `${pointerDir}s=${s}.json` });
    if (m.startsWith("srv_")) stagedDirs.add(`${org}/live-batches/v1/ws=${lws}/m=${m}/`);
  }
  for (const raw of staged) {
    if (typeof raw !== "string") refuse("staged_invalid");
    if (raw.startsWith(`${org}/buffered-batches/`)) refuse("staged_buffered_owned_by_gc");
    const dir = raw.slice(0, raw.lastIndexOf("/") + 1);
    if (!stagedDirs.has(dir) || !STAGED_FILE.test(raw.slice(dir.length))) refuse("staged_outside_session");
    add({ kind: "staged", prefix: dir, exactKey: raw });
  }
  const list = [...targets.values()];
  for (const t of list) assertAllowed(list, t.exactKey ? [t.exactKey] : [t.prefix + "x"]);
  return list;
}

/**
 * True when `mk` is this location's session manifest under the upload-generation layout
 * (`{org}/upload-generations/g=<uuid>/v1/ws=…/m=…/h=…/d=…/s=<sid>/publications/<uuid>/manifest.json`).
 * Those bytes are a publication generation, a separate erase step: the row is refused with a
 * distinct code so lane D can track it as not erased (a row of another session stays
 * `manifest_key_mismatch`).
 */
function isPublicationGenerationManifest(mk: string, org: string, ws: string, m: string, h: string, s: string): boolean {
  const marker = `${org}/upload-generations/g=`;
  if (!mk.startsWith(marker)) return false;
  const end = mk.indexOf("/", marker.length);
  if (end < 0 || parseUploadGenerationPrefix(mk.slice(0, end))?.base !== org) return false;
  const rest = mk.slice(end + 1);
  const at = /^v1\/ws=([^/]+)\/m=([^/]+)\/h=([^/]+)\/d=\d{4}\/\d{2}\/\d{2}\/s=([^/]+)\/publications\/[^/]+\/manifest\.json$/.exec(rest);
  return !!at && at[1] === ws && at[2] === m && at[3] === h && at[4] === s;
}

/**
 * The eraser's own guard (in addition to the primitive's per-page prefix check): every key
 * must lie inside a transcript dir of the plan or BE one of the plan's exact keys. Throws
 * `session_erase_key_outside_allowed_prefixes` (no key in the message).
 */
export function assertAllowed(targets: readonly EraseTarget[], keys: readonly string[]): void {
  for (const k of keys) {
    const ok = typeof k === "string" && !k.split("/").some((x) => x === "." || x === "..")
      && targets.some((t) => matchesTarget(t, k));
    if (!ok) refuse("key_outside_allowed_prefixes");
  }
}

function scopeOf(key: SessionEraseKey, t: EraseTarget): string {
  return "session_erase_bytes:" + createHash("sha256").update(JSON.stringify([key.orgId, key.workspaceId, key.source, key.sessionId, t.prefix, t.exactKey ?? null])).digest("hex");
}

/**
 * Runs `fn` over `items`, at most `width` at once. The first failure sets a shared `stopped`
 * flag: no lane takes another item, `fn` can poll `stopped()` between its own steps, and the
 * pool waits for every lane to settle before rethrowing that first error (so no lane keeps
 * deleting after the call has failed).
 */
async function pool<T>(items: readonly T[], width: number, fn: (item: T, stopped: () => boolean) => Promise<void>): Promise<void> {
  let next = 0, stopped = false;
  let first: { error: unknown } | undefined;
  const isStopped = () => stopped;
  const lanes = Array.from({ length: Math.min(width, items.length) }, async () => {
    while (!stopped && next < items.length) {
      try {
        await fn(items[next++]!, isStopped);
      } catch (error) {
        if (!first) first = { error };
        stopped = true;
      }
    }
  });
  await Promise.allSettled(lanes);
  if (first) throw first.error;
}

/** Erase (or, dry run, count) one session's bytes, then verify by listing again. */
export async function eraseSessionBytes(eraser: PagedEraser, req: SessionEraseBytesRequest, opts: SessionEraseBytesOptions): Promise<SessionEraseBytesResult> {
  const targets = planTargets(req, opts);
  return executeTargets(eraser, req.key, targets, opts);
}

/** One (workspace, machine, source, session)'s canonical bytes: physical layout segments, as the writer derives them. */
export interface MachineSessionKey {
  orgId: string;
  /** The PHYSICAL workspace segment (`layoutComponent(workspaceId)`, as it appears in `ws=`). */
  workspaceSegment: string;
  /** The writer's machine id (`srv_` + 32 hex: writer.ts liveMachineId). */
  machineId: string;
  /** Canonical (pre-layout) source and session ids; mapped with `layoutComponent` exactly as the writer does. */
  source: string;
  sessionId: string;
}

/**
 * The targets of ONE machine's copy of a session (the S3 ledger's re-arm sweep, ledger-s3.ts): the transcript dir under
 * every date and the live pointer, exactly where writer.ts writeGroup puts them. Unlike planDiscoveryTargets it is
 * fixed to one machine (one user's origin), so another user's session with the same id is never in scope. Ids map
 * through `layoutComponent` like planTargets: two raw ids sharing a physical dir share its erasure.
 */
export function planMachineSessionTargets(key: MachineSessionKey, opts: Pick<SessionEraseBytesOptions, "keyRoot">): EraseTarget[] {
  if (!KEY_ROOT.test(opts.keyRoot)) refuse("key_root_invalid");
  if (!key || typeof key.orgId !== "string" || !SEGMENT.test(key.orgId) || key.orgId.startsWith(".")) refuse("org_invalid");
  const ws = physical(key.workspaceSegment);
  if (typeof key.machineId !== "string" || !/^srv_[0-9a-f]{32}$/.test(key.machineId)) refuse("location_invalid");
  const h = layoutComponent(nonEmpty(key.source, "key_invalid")), s = layoutComponent(nonEmpty(key.sessionId, "key_invalid"));
  const org = `${opts.keyRoot}/${key.orgId}`;
  const base = `${org}/v1/ws=${ws}/m=${key.machineId}/h=${h}/`;
  const pointerDir = `${org}/live-sessions/v1/ws=${ws}/m=${key.machineId}/h=${h}/`;
  const list: EraseTarget[] = [
    { kind: "transcript", prefix: base, match: new RegExp(`^${escapePattern(base)}d=\\d{4}/\\d{2}/\\d{2}/s=${escapePattern(s)}/.+$`) },
    { kind: "pointer", prefix: pointerDir, exactKey: `${pointerDir}s=${s}.json` },
  ];
  for (const t of list) assertAllowed(list, t.exactKey ? [t.exactKey] : []);
  return list;
}

/** Erase (or, dry run, count) one machine's copy of a session through the same executor as every other path. */
export async function eraseMachineSessionBytes(eraser: PagedEraser, key: MachineSessionKey, opts: SessionEraseBytesOptions): Promise<SessionEraseBytesResult> {
  const targets = planMachineSessionTargets(key, opts);
  return executeTargets(eraser, { orgId: key.orgId, workspaceId: key.workspaceSegment, source: key.source, sessionId: key.sessionId }, targets,
    { ...opts, log: opts.log ?? (() => {}) });
}

/** Automatic self-host path: no supplied locations, staged keys or user-built prefixes. */
export async function eraseDiscoveredSessionBytes(eraser: PagedEraser, key: SessionEraseKey, opts: SessionEraseBytesOptions): Promise<SessionEraseBytesResult> {
  return executeTargets(eraser, key, planDiscoveryTargets(key, opts), opts);
}

async function executeTargets(eraser: PagedEraser, key: SessionEraseKey, targets: EraseTarget[], opts: SessionEraseBytesOptions): Promise<SessionEraseBytesResult> {
  const dryRun = opts.dryRun === true;
  const width = opts.concurrency ?? DEFAULT_CONCURRENCY;
  const maxRuns = opts.maxRunsPerTarget ?? DEFAULT_MAX_RUNS_PER_TARGET;
  const minAge = opts.stagedMinAgeMs ?? STAGED_MIN_AGE_MS;
  if (!Number.isInteger(width) || width < 1 || width > MAX_CONCURRENCY) refuse("concurrency_invalid");
  if (!Number.isInteger(maxRuns) || maxRuns < 1 || maxRuns > 1000) refuse("budget_invalid");
  if (!Number.isFinite(minAge) || minAge < 0) refuse("staged_min_age_invalid");
  const now = opts.now ?? Date.now;
  const authorize = opts.authorize ?? (async () => {});
  const log = opts.log ?? ((line) => console.log(JSON.stringify(line)));
  const keys: Record<TargetKind, Set<string>> = { transcript: new Set(), pointer: new Set(), staged: new Set() };
  // Dry run: a page boundary inside one key relists that key's versions, so count each once.
  const seen = new Set<string>();
  let versions = 0, stagedPending = 0, incomplete = 0, stagedKept = false;
  const cutoff = now() - minAge;
  const stagedOld = (v: ListedVersion) => v.lastModified !== undefined && v.lastModified < cutoff;
  const before = opts.modifiedBefore;
  if (before !== undefined && !Number.isFinite(before)) refuse("modified_before_invalid");
  // Successors (written after the caller's authority read) are never erased; they keep `verified` false.
  const settled = (v: ListedVersion) => before === undefined || (v.lastModified !== undefined && v.lastModified < before);

  const run = async (t: EraseTarget, stopped: () => boolean) => {
    const keep = t.kind === "staged"
      ? (v: ListedVersion) => {
          if (stagedOld(v) && settled(v)) return true;
          if (t.exactKey === undefined || v.key === t.exactKey) stagedKept = true;
          return false;
        }
      : settled;
    const select = dryRun
      ? (v: ListedVersion) => {
          if (!matchesTarget(t, v.key)) return false;
          assertAllowed(targets, [v.key]);
          const id = JSON.stringify([v.key, v.versionId]);
          if ((t.exactKey === undefined || v.key === t.exactKey) && !seen.has(id)) {
            seen.add(id);
            if (keep(v)) { versions++; keys[t.kind].add(v.key); } else if (t.kind === "staged") stagedPending++;
          }
          return false;
        }
      : (v: ListedVersion) => matchesTarget(t, v.key) && keep(v);
    let checkpoint: EraseCheckpoint | null = null;
    for (let r = 0; r < maxRuns; r++) {
      if (stopped()) return; // a sibling lane failed: the pool rethrows its error
      const res = await eraser.eraseStep(t.prefix, {
        scope: scopeOf(key, t), checkpoint, select, authorize, now,
        ...(t.exactKey !== undefined ? { exactKey: t.exactKey } : {}),
        beforeDelete: async (doomed) => {
          assertAllowed(targets, doomed.map((v) => v.key));
          for (const v of doomed) keys[t.kind].add(v.key);
        },
      });
      if (!dryRun) versions += res.deleted;
      if (res.status === "traversed") return;
      checkpoint = res.checkpoint;
    }
    incomplete++;
  };

  // Staged batches and transcript dirs first; the live pointer last. Keeping the pointer does
  // not stop a writer re-creating bytes (a writer that finds it writes into the `d=` dir it
  // names), but it means any such re-creation lands in a PLANNED dir that verify catches,
  // instead of a fresh pointer and a new `d=` dir outside the plan.
  // A pointer is only erased once every other target was fully traversed and no staged
  // version is still pending inside the redrive window (as the cloud eraser erases it only
  // after the session dir); otherwise it stays for the rerun.
  const pointers = targets.filter((t) => t.kind === "pointer");
  await pool(targets.filter((t) => t.kind !== "pointer"), width, run);
  const deferPointers = !dryRun && (incomplete > 0 || stagedKept);
  if (!deferPointers) await pool(pointers, width, run);
  const deferredPointers = deferPointers ? pointers.length : 0;

  let remaining = 0, pendingBudget = 0;
  if (!dryRun) {
    await pool(targets, width, async (t) => {
      if (t.match) {
        // A workspace may exceed verifyErased's single-invocation budget. Compose the same strict
        // paged primitive into a fresh, read-only full pass; never infer zero from a partial listing.
        let checkpoint: EraseCheckpoint | null = null;
        let found = 0;
        for (let r = 0; r < maxRuns; r++) {
          const proof = await eraser.eraseStep(t.prefix, {
            scope: `${scopeOf(key, t)}:verify`, checkpoint, authorize, now,
            select: (v) => { if (matchesTarget(t, v.key)) found++; return false; },
          });
          if (found > 0) { remaining += found; return; }
          if (proof.status === "traversed") return;
          checkpoint = proof.checkpoint;
        }
        pendingBudget++;
        return;
      }
      const proof = await eraser.verifyErased(t.prefix, { authorize, now, ...(t.exactKey !== undefined ? { exactKey: t.exactKey } : {}) });
      if (proof.status === "pending") {
        if (proof.reason === "budget") pendingBudget++;
        remaining += proof.remaining;
        if (t.kind === "staged") stagedPending += proof.remaining;
      }
    });
  }
  const result: SessionEraseBytesResult = {
    dryRun,
    transcriptObjects: keys.transcript.size,
    pointerObjects: keys.pointer.size,
    stagedObjects: keys.staged.size,
    versionsDeleted: versions,
    stagedPendingVersions: stagedPending,
    incompleteTargets: incomplete,
    deferredPointerTargets: deferredPointers,
    verified: !dryRun && remaining === 0 && pendingBudget === 0 && incomplete === 0 && deferredPointers === 0,
  };
  log({
    event: "session_erase_bytes", dry_run: dryRun, targets: targets.length, transcript_objects: result.transcriptObjects,
    pointer_objects: result.pointerObjects, staged_objects: result.stagedObjects, versions: result.versionsDeleted,
    staged_pending_versions: stagedPending, incomplete_targets: incomplete,
    deferred_pointer_targets: deferredPointers, remaining_versions: remaining,
    verify_budget_exhausted: pendingBudget, verified: result.verified,
  });
  return result;
}
