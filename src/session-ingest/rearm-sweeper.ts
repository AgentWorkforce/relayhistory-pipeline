/**
 * The S3 upload ledger's re-arm sweeper (decision 0005; ledger-s3.ts RearmDoc). A commit that a deletion, disable or
 * reset reached after its claim writes `<admission user dir>/ledger/rearm/o=<h(origin)>/<sessionKey>.json`: "this
 * session's canonical objects may hold data written after its erasure; erase it again". This job consumes them. It is
 * the `rearmSweeper: "live"` precondition of the fence-authority marker (ledger-s3.ts FenceAuthorityDoc).
 *
 * Per marker, in key order, a bounded number at once:
 *
 *   1. read it; a malformed marker, or one not at the key ledger-s3 would write it to, is `corrupt` (counted, never
 *      fatal, never erased from).
 *   2. its completion record `<marker>.done/<h(marker bytes)>.json` exists: `alreadyDone`. A re-arm after that
 *      rewrites the marker with a later `atMs` (ledger-s3 writeRearm), a new hash, so it is processed again.
 *   3. read the user's fence (ledger-s3 s3LedgerObjects readFence: normalizedFence, fail closed):
 *        deletes the session                         -> erase
 *        a reset begun but unfinished                -> `resetRunning` (its tombstones have not landed: retry later)
 *        otherwise (re-enabled since, or never
 *        tombstoned by a finished reset)             -> `superseded`: nothing erased, completion recorded
 *        absent or unreadable                        -> `fenceAbsent` / `fenceUnreadable`: nothing erased or recorded
 *   4. erase that machine's copy of the session through the SAME executor as the operator and self-host erasers
 *      (session-erase/selfhost-eraser.ts eraseMachineSessionBytes -> the shared strict paged primitive,
 *      neon-s3-erasure/eraser.ts): every version of the transcript dir under every date, then the live pointer, then a
 *      fresh listing proves nothing selected is left. As the hosted live lane (neon-s3-erasure job.ts #eraseLiveFenced):
 *      the fence is re-read before every page's deletes (a re-enable stops it: `reenabled`), and versions modified
 *      after the fence read less REARM_SKEW_MS (= LIVE_SKEW_MS) are kept (and keep it unverified: `pending`, next pass).
 *      Index and search rows: the version deletes are `ObjectRemoved:Delete` events, which the session pipeline's
 *      indexer already turns into index and search removals, exactly as for the live lane's deletes. Nothing here
 *      writes SQL or search.
 *   5. verified: write the completion record (create-only). The marker itself is never deleted.
 *
 * Dry run: classify and count (the eraser in dry run lists and counts versions), no write and no delete.
 * Output is counts and error codes only: never a key, a tenant id or a session id.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import type { PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import { eraseMachineSessionBytes, SessionEraseRefused } from "../session-erase/selfhost-eraser.ts";
import { INBOX_NAMESPACE } from "./async-admission.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import { isDeleted, rearmKeyMatches, s3LedgerObjects, validRearm, type FenceDoc, type RearmDoc } from "./ledger-s3.ts";
import { assertTenantSegment } from "./writer.ts";

export const REARM_DEFAULT_CONCURRENCY = 4;
export const REARM_MAX_CONCURRENCY = 16;
export const REARM_DEFAULT_MAX_MARKERS = 500;
/** neon-s3-erasure/job.ts LIVE_SKEW_MS (pinned by a test; not imported, so this job does not load the Neon job). */
export const REARM_SKEW_MS = 2 * 60 * 1000;

/** A marker's completion: one per marker CONTENT (hash), create-only. */
export interface RearmDoneDoc { v: 1; outcome: "erased" | "superseded"; marker: string; atMs: number }

export interface RearmSweepOptions {
  keyRoot: string;
  /** The bucket the eraser deletes from (the transcripts bucket that holds both the ledger and the canonical layout). */
  bucket: string;
  /** Org scope: only these orgs' admission directories are listed. Absent: every org under the admission namespace. */
  orgs?: readonly string[];
  dryRun?: boolean;
  /** Markers processed at once (1..16, default 4). Each erase lists its two targets in parallel. */
  concurrency?: number;
  /** Markers that needed work (anything but `alreadyDone`) after which the pass stops (before its next marker). */
  maxMarkers?: number;
  /** Absolute ms: no new listing page starts at or after it. */
  deadlineAt?: number;
  /** Versions modified within this of the fence read are kept (default REARM_SKEW_MS). */
  skewMs?: number;
  now?: () => number;
}

/** Where to continue: the scope index (org) and the store's list cursor. Opaque to callers; never logged. */
export interface RearmSweepResume { scope: number; cursor?: string }

export interface RearmSweepCounts {
  /** Markers read this pass. */
  listed: number;
  erased: number;
  superseded: number;
  alreadyDone: number;
  /** Erased what was there, but a fresh listing still shows versions (successors, budget): next pass. */
  pending: number;
  resetRunning: number;
  reenabled: number;
  fenceAbsent: number;
  fenceUnreadable: number;
  corrupt: number;
  /** The marker was gone when read (a user erase removed the directory meanwhile). */
  gone: number;
  failed: number;
  wouldErase: number;
  wouldSupersede: number;
  /** Versions deleted (dry run: versions that would be). */
  versions: number;
}

export interface RearmSweepResult {
  dryRun: boolean;
  /** True when every scope was listed to its end in this pass. */
  complete: boolean;
  counts: RearmSweepCounts;
  /** Failure codes by count (codes only). */
  failures: Record<string, number>;
  /** Where the next pass continues (null: start over). */
  resume: RearmSweepResume | null;
}

export class RearmSweepRefused extends Error {
  constructor(readonly code: string) {
    super(`rearm_sweep_${code}`);
    this.name = "RearmSweepRefused";
  }
}
class Reenabled extends Error {}

const SEGMENT = /^[A-Za-z0-9._-]{1,128}$/;
const zero = (): RearmSweepCounts => ({ listed: 0, erased: 0, superseded: 0, alreadyDone: 0, pending: 0, resetRunning: 0, reenabled: 0,
  fenceAbsent: 0, fenceUnreadable: 0, corrupt: 0, gone: 0, failed: 0, wouldErase: 0, wouldSupersede: 0, versions: 0 });

/** A marker key's parts, or null when the key is not a re-arm marker (the namespace also holds inbox objects). */
export function parseRearmKey(keyRoot: string, key: string): { dir: string; orgId: string; workspaceSegment: string } | null {
  const ns = `${keyRoot}/${INBOX_NAMESPACE}/`;
  if (!key.startsWith(ns)) return null;
  const m = /^([^/]+)\/v1\/ws=([^/]+)\/m=srv_[0-9a-f]{32}\/ledger\/rearm\/o=[0-9a-f]{32}\/[0-9a-f]{32}\.json$/.exec(key.slice(ns.length));
  if (!m || !SEGMENT.test(m[1]!) || m[1]!.startsWith(".") || !SEGMENT.test(m[2]!) || m[2]!.startsWith(".")) return null;
  return { dir: key.slice(0, key.indexOf("/ledger/rearm/")), orgId: m[1]!, workspaceSegment: m[2]! };
}

/** The completion record of a marker whose bytes hash to `digest`. */
export const rearmDoneKey = (markerKey: string, digest: string) => `${markerKey.slice(0, -".json".length)}.done/${digest}.json`;

const safeCode = (e: unknown): string => {
  const c = (e as { code?: unknown } | null)?.code;
  return typeof c === "string" && /^[a-z0-9_]{1,64}$/i.test(c) ? c : "error";
};

/** The fence's verdict for one marker's session. */
function verdict(fence: FenceDoc, m: RearmDoc): "erase" | "reset_running" | "superseded" {
  if (isDeleted(fence, m.originId, m.source, m.sessionId)) return "erase";
  return fence.resets > fence.resetsCompleted ? "reset_running" : "superseded";
}

async function mapLimit<T>(items: readonly T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) await fn(items[next++]!);
  }));
}

/**
 * One pass: lists the scopes from `resume` (or the start), processes the markers of each page, and stops before the
 * next marker once `maxMarkers` markers needed work or `deadlineAt` passed (markers in flight finish). Throws only for a refused configuration or a
 * failed listing (the loop retries); a failure on one marker is counted and the pass goes on.
 */
export async function sweepRearmMarkers(deps: { store: ObjectStore; eraser: PagedEraser }, opts: RearmSweepOptions, resume: RearmSweepResume | null = null): Promise<RearmSweepResult> {
  const { store, eraser } = deps;
  const dryRun = opts.dryRun === true;
  const concurrency = opts.concurrency ?? REARM_DEFAULT_CONCURRENCY;
  const maxMarkers = opts.maxMarkers ?? REARM_DEFAULT_MAX_MARKERS;
  const skewMs = opts.skewMs ?? REARM_SKEW_MS;
  const now = opts.now ?? Date.now;
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > REARM_MAX_CONCURRENCY) throw new RearmSweepRefused("concurrency_invalid");
  if (!Number.isInteger(maxMarkers) || maxMarkers < 1) throw new RearmSweepRefused("budget_invalid");
  if (!Number.isFinite(skewMs) || skewMs < 0) throw new RearmSweepRefused("skew_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new RearmSweepRefused("key_root_invalid");
  const orgs = opts.orgs ?? null;
  if (orgs !== null) {
    if (!Array.isArray(orgs) || orgs.length < 1) throw new RearmSweepRefused("orgs_invalid");
    for (const org of orgs) { try { assertTenantSegment(opts.keyRoot, org); } catch { throw new RearmSweepRefused("orgs_invalid"); } }
  }
  const ns = `${opts.keyRoot}/${INBOX_NAMESPACE}`;
  const scopes = orgs === null ? [ns] : [...new Set(orgs)].sort().map((org) => `${ns}/${org}/v1`);
  const io = s3LedgerObjects(store, opts.keyRoot);
  const counts = zero();
  const failures: Record<string, number> = {};
  const fail = (code: string) => { counts.failed++; failures[code] = (failures[code] ?? 0) + 1; };

  async function writeDone(markerKey: string, digest: string, outcome: RearmDoneDoc["outcome"]): Promise<void> {
    const doc: RearmDoneDoc = { v: 1, outcome, marker: digest, atMs: now() };
    try {
      await store.put(objectKey(rearmDoneKey(markerKey, digest)), new TextEncoder().encode(JSON.stringify(doc)), { ifNoneMatch: true, contentType: "application/json" });
    } catch (error) {
      // Another pass recorded the same content's completion first: the same fact.
      if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
    }
  }

  type Outcome = Exclude<keyof RearmSweepCounts, "listed" | "failed" | "versions">;
  async function one(key: string): Promise<Outcome> {
    const at = parseRearmKey(opts.keyRoot, key)!;
    let raw: Uint8Array;
    try {
      raw = await readAll((await store.getObject(objectKey(key))).body);
    } catch (error) {
      if (isObjectStoreError(error, "not_found")) return "gone";
      throw error;
    }
    let marker: unknown;
    try { marker = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw)); } catch { return "corrupt"; }
    if (!validRearm(marker) || !rearmKeyMatches(at.dir, key, marker)) return "corrupt";
    const digest = createHash("sha256").update(raw).digest("hex").slice(0, 32);
    if (await store.head(objectKey(rearmDoneKey(key, digest)))) return "alreadyDone";
    const fenceKey = `${at.dir}/upload-fences.json`;
    const readFence = async () => {
      try { return await io.readFence(fenceKey); } catch (error) {
        if (error instanceof LedgerUnavailableError && error.code === "ledger_fence_invalid") return null;
        throw error;
      }
    };
    const read = await readFence();
    if (read === null) return "fenceUnreadable";
    if (read.state === "absent") return "fenceAbsent";
    const first = verdict(read.fence, marker);
    if (first === "reset_running") return "resetRunning";
    if (first === "superseded") {
      if (dryRun) return "wouldSupersede";
      await writeDone(key, digest, "superseded");
      return "superseded";
    }
    const checkedAt = now();
    // Before every page's deletes: the session must still be deleted (a re-enable since stops the erase).
    const authorize = async () => {
      const again = await readFence();
      if (again === null || again.state !== "present" || verdict(again.fence, marker) !== "erase") throw new Reenabled();
    };
    let result;
    try {
      result = await eraseMachineSessionBytes(eraser, { orgId: at.orgId, workspaceSegment: at.workspaceSegment, machineId: marker.machineId, source: marker.source, sessionId: marker.sessionId },
        { keyRoot: opts.keyRoot, bucket: opts.bucket, dryRun, concurrency: 2, authorize, modifiedBefore: checkedAt - skewMs, now });
    } catch (error) {
      if (error instanceof Reenabled) return "reenabled";
      throw error;
    }
    counts.versions += result.versionsDeleted;
    if (dryRun) return "wouldErase";
    if (!result.verified) return "pending";
    await writeDone(key, digest, "erased");
    return "erased";
  }

  let worked = 0;
  const spent = () => worked >= maxMarkers || (opts.deadlineAt !== undefined && now() >= opts.deadlineAt);
  if (resume !== null && (!Number.isInteger(resume.scope) || resume.scope < 0)) throw new RearmSweepRefused("resume_invalid");
  for (let scope = resume?.scope ?? 0, cursor = resume?.cursor; scope < scopes.length; scope++, cursor = undefined) {
    const prefix = scopes[scope]!;
    do {
      const stop = { scope, ...(cursor !== undefined ? { cursor } : {}) };
      if (spent()) return { dryRun, complete: false, counts, failures, resume: stop };
      const page = await store.list(objectKey(prefix), cursor);
      const markers = page.items.map((i) => String(i.key)).filter((k) => k.startsWith(`${prefix}/`) && parseRearmKey(opts.keyRoot, k) !== null);
      // Out of budget part way through a page: the rest is left, and the next pass lists THIS page again (its finished
      // markers cost a GET and a HEAD each), so one page of many markers can never run far past a deadline.
      let cut = false;
      await mapLimit(markers, concurrency, async (key) => {
        if (cut || spent()) { cut = true; return; }
        counts.listed++;
        let outcome: Outcome | null = null;
        try { outcome = await one(key); } catch (error) {
          fail(error instanceof SessionEraseRefused || error instanceof LedgerUnavailableError ? error.code : isObjectStoreError(error) ? `store_${error.code}` : safeCode(error));
        }
        if (outcome !== null) counts[outcome]++;
        if (outcome !== "alreadyDone") worked++;
      });
      if (cut) return { dryRun, complete: false, counts, failures, resume: stop };
      cursor = page.nextCursor;
    } while (cursor);
  }
  return { dryRun, complete: true, counts, failures, resume: null };
}
