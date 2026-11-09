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
  workspaceId: string;
  /** The harness/source, e.g. `claude-code`. */
  source: string;
  sessionId: string;
}

/**
 * One `session_index` row's storage coordinates. `workspaceId`, `machineId`, `harness` and
 * `sessionId` are the PHYSICAL (layout-encoded) values stored in the row; they must equal
 * `layoutComponent` of the erase key's ids. At least one date source is required: the row's
 * `manifestKey` (authoritative: it fixes `d=`) and/or explicit `dates` (`yyyy-mm-dd`).
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
 * The eraser's own guard (in addition to the primitive's per-page prefix check): every key
 * must lie inside a transcript dir of the plan or BE one of the plan's exact keys. Throws
 * `session_erase_key_outside_allowed_prefixes` (no key in the message).
 */
export function assertAllowed(targets: readonly EraseTarget[], keys: readonly string[]): void {
  for (const k of keys) {
    const ok = typeof k === "string" && !k.split("/").some((x) => x === "." || x === "..")
      && targets.some((t) => (t.exactKey !== undefined ? k === t.exactKey : k.startsWith(t.prefix) && k.length > t.prefix.length));
    if (!ok) refuse("key_outside_allowed_prefixes");
  }
}

function scopeOf(key: SessionEraseKey, t: EraseTarget): string {
  return "session_erase_bytes:" + createHash("sha256").update(JSON.stringify([key.orgId, key.workspaceId, key.source, key.sessionId, t.prefix, t.exactKey ?? null])).digest("hex");
}

async function pool<T>(items: readonly T[], width: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  const lanes = Array.from({ length: Math.min(width, items.length) }, async () => {
    while (next < items.length) await fn(items[next++]!);
  });
  await Promise.all(lanes);
}

/** Erase (or, dry run, count) one session's bytes, then verify by listing again. */
export async function eraseSessionBytes(eraser: PagedEraser, req: SessionEraseBytesRequest, opts: SessionEraseBytesOptions): Promise<SessionEraseBytesResult> {
  const targets = planTargets(req, opts);
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
  let versions = 0, stagedPending = 0, incomplete = 0;
  const cutoff = now() - minAge;
  const stagedOld = (v: ListedVersion) => v.lastModified !== undefined && v.lastModified < cutoff;

  const run = async (t: EraseTarget) => {
    const keep = t.kind === "staged" ? stagedOld : () => true;
    const select = dryRun
      ? (v: ListedVersion) => {
          assertAllowed(targets, [v.key]);
          const id = JSON.stringify([v.key, v.versionId]);
          if ((t.exactKey === undefined || v.key === t.exactKey) && !seen.has(id)) {
            seen.add(id);
            if (keep(v)) { versions++; keys[t.kind].add(v.key); } else if (t.kind === "staged") stagedPending++;
          }
          return false;
        }
      : keep;
    let checkpoint: EraseCheckpoint | null = null;
    for (let r = 0; r < maxRuns; r++) {
      const res = await eraser.eraseStep(t.prefix, {
        scope: scopeOf(req.key, t), checkpoint, select, authorize, now,
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

  // Staged batches and transcript dirs first; the live pointer last, so a writer that still
  // finds its pointer never re-creates a dir that was already erased behind it.
  // A pointer is only erased once every other target was fully traversed (as the cloud eraser
  // erases it only after the session dir); otherwise it stays for the rerun.
  await pool(targets.filter((t) => t.kind !== "pointer"), width, run);
  if (incomplete === 0 || dryRun) await pool(targets.filter((t) => t.kind === "pointer"), width, run);

  let remaining = 0, pendingBudget = 0;
  if (!dryRun) {
    await pool(targets, width, async (t) => {
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
    verified: !dryRun && remaining === 0 && pendingBudget === 0 && incomplete === 0,
  };
  log({
    event: "session_erase_bytes", dry_run: dryRun, targets: targets.length, transcript_objects: result.transcriptObjects,
    pointer_objects: result.pointerObjects, staged_objects: result.stagedObjects, versions: result.versionsDeleted,
    staged_pending_versions: stagedPending, incomplete_targets: incomplete, remaining_versions: remaining,
    verify_budget_exhausted: pendingBudget, verified: result.verified,
  });
  return result;
}
