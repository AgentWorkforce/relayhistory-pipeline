/**
 * The S3 upload ledger's re-arm sweeper (decision 0005; ledger-s3.ts RearmDoc). A commit that a deletion, disable or
 * reset reached after its claim writes `<admission user dir>/ledger/rearm/o=<h(origin)>/<sessionKey>.json`: "this
 * session's canonical objects may hold data written after its erasure; erase it again". This job consumes them. It is
 * the `rearmSweeper: "live"` precondition of the fence-authority marker (ledger-s3.ts FenceAuthorityDoc).
 *
 * Per marker, in key order, a bounded number at once:
 *
 *   1. read it; a malformed marker, or one not at the key ledger-s3 would write it to, is `corrupt` (see "Corrupt
 *      markers" below): counted, recorded, never fatal, never erased from.
 *   2. its completion record `<marker>.done/<h(marker bytes)>.json` exists: `alreadyDone`. A re-arm after that
 *      rewrites the marker with a later `atMs` (ledger-s3 writeRearm), a new hash, so it is processed again.
 *   3. read the user's fence (ledger-s3 s3LedgerObjects readFence: normalizedFence, fail closed):
 *        deletes the session                         -> erase (step 4)
 *        a reset begun but unfinished                -> `resetRunning` (its tombstones have not landed: retry later)
 *        otherwise (re-enabled since, or never
 *        tombstoned by a finished reset)             -> `superseded`: nothing erased, completion recorded
 *        absent or unreadable                        -> `fenceAbsent` / `fenceUnreadable`: nothing erased or recorded
 *   4. before any delete, bind the marker to its user: the session's ownership stub (same admission directory, same
 *      origin and session key) must exist (`ownerMissing`) and carry the SAME writer machine (`machineMismatch`; a stub
 *      with none yet: `machineUnbound`). The ledger computes that machine from the tenant at claim and refuses a commit
 *      whose machine is not the tenant's, so a marker can only name its own user's canonical prefix. Then refuse a
 *      layout collision (`layoutCollision`): `layoutComponent` maps e.g. `a:b` and `a_b` to one physical `s=a_b` dir,
 *      so when another live (not deleted) session of the same origin shares this one's physical source and session
 *      segments, erasing would destroy its bytes too; nothing is erased and the marker waits for an operator.
 *   5. erase that machine's copy of the session through the SAME executor as the operator and self-host erasers
 *      (session-erase/selfhost-eraser.ts eraseMachineSessionBytes -> the shared strict paged primitive,
 *      neon-s3-erasure/eraser.ts): every version of the transcript dir under every date, then the live pointer, then a
 *      fresh listing proves nothing selected is left. As the hosted live lane (neon-s3-erasure job.ts #eraseLiveFenced):
 *      the fence is re-read before every page's deletes (a re-enable stops it: `reenabled`), and versions modified
 *      after the fence read less REARM_SKEW_MS (= LIVE_SKEW_MS) are kept (and keep it unverified: `pending`, next pass).
 *      Index and search rows: the version deletes are `ObjectRemoved:Delete` events, which the session pipeline's
 *      indexer already turns into index and search removals, exactly as for the live lane's deletes. Nothing here
 *      writes SQL or search.
 *   6. verified: write the completion record (create-only). The marker itself is never deleted.
 *
 * Budget and progress: `maxMarkers` counts only markers whose ERASE ran (erased, pending, reenabled, wouldErase), so
 * markers that cannot progress (corrupt, unbound, fence absent...) cost a few reads and never starve the ones after
 * them; `deadlineAt` bounds the time. A pass that stops returns where it stopped: the scope, the page's list cursor
 * and the last key it finished (`afterKey`), so the next pass continues at the next marker, never the same page from
 * its start. The entry (rearm-sweeper-main.ts) keeps that position durably in the bucket, so successive Lambda
 * invocations walk the whole namespace and then wrap around to the start.
 *
 * Access: a 401/403 from the store or the eraser aborts the pass (RearmSweepRefused `access_denied`): the role is
 * misconfigured (for instance `s3:ListBucket` conditioned on a prefix makes every HEAD of a missing completion record a
 * 403, not a 404), and that must page an operator, not count as a per-marker failure forever.
 *
 * Corrupt markers: a marker that does not parse, does not validate, or sits at another session's key cannot be
 * attributed, so it is never erased from. The pass counts it (`corrupt`: alarm on it) and writes a create-only
 * `<marker>.corrupt/<h(marker bytes)>.json` record next to it ({v, reason, atMs}), so it can be found without any key
 * in a log. Repair: the session's next re-arm replaces it with a valid marker (ledger-s3 writeRearm). To repair by
 * hand, find the `.corrupt/` record under the namespace, read the ownership stub at the same `o=`/`s=` path under the
 * same directory's `ledger/owners/` (it carries originId, source, sessionId and machineId), and overwrite the marker
 * If-Match with `{v: 1, originId, source, sessionId, machineId, atMs: <now ms>}`; the next pass erases it. With no stub
 * the marker cannot be attributed: escalate to a per-user erase.
 *
 * Dry run: classify and count (the eraser in dry run lists and counts versions), no write and no delete.
 * Output is counts and error codes only: never a key, a tenant id or a session id.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import type { PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { eraseMachineSessionBytes, SessionEraseRefused } from "../session-erase/selfhost-eraser.ts";
import { INBOX_NAMESPACE } from "./async-admission.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import { isDeleted, ownerKeyIn, ownersOriginPrefix, rearmKeyMatches, s3LedgerObjects, validRearm, type FenceDoc, type OwnerDoc, type RearmDoc } from "./ledger-s3.ts";
import { assertTenantSegment } from "./writer.ts";

export const REARM_DEFAULT_CONCURRENCY = 4;
export const REARM_MAX_CONCURRENCY = 16;
export const REARM_DEFAULT_MAX_MARKERS = 500;
/** neon-s3-erasure/job.ts LIVE_SKEW_MS (pinned by a test; not imported, so this job does not load the Neon job). */
export const REARM_SKEW_MS = 2 * 60 * 1000;

/** A marker's completion: one per marker CONTENT (hash), create-only. */
export interface RearmDoneDoc { v: 1; outcome: "erased" | "superseded"; marker: string; atMs: number }
/** A corrupt marker's record: one per marker content, create-only, so an operator can find it (see the header). */
export interface RearmCorruptDoc { v: 1; reason: "unparseable" | "invalid" | "key_mismatch"; atMs: number }

export interface RearmSweepOptions {
  keyRoot: string;
  /** The bucket the eraser deletes from (the transcripts bucket that holds both the ledger and the canonical layout). */
  bucket: string;
  /** Org scope: only these orgs' admission directories are listed. Absent: every org under the admission namespace. */
  orgs?: readonly string[];
  dryRun?: boolean;
  /** Markers processed at once (1..16, default 4). Each erase lists its two targets in parallel. */
  concurrency?: number;
  /** Markers whose erase ran, after which the pass stops (before its next marker). Cheap outcomes never count. */
  maxMarkers?: number;
  /** Absolute ms: no new marker or listing page starts at or after it. */
  deadlineAt?: number;
  /** Versions modified within this of the fence read are kept (default REARM_SKEW_MS). */
  skewMs?: number;
  now?: () => number;
}

/**
 * Where to continue: the scope (org) index, the list cursor of the page to list, and the last key already finished on
 * it (keys at or before it are skipped). Opaque to callers; never logged.
 */
export interface RearmSweepResume { scope: number; cursor?: string; afterKey?: string }

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
  /** Unattributable markers (alarm on this): recorded next to the marker, never erased from. */
  corrupt: number;
  ownerMissing: number;
  machineMismatch: number;
  machineUnbound: number;
  layoutCollision: number;
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
  fenceAbsent: 0, fenceUnreadable: 0, corrupt: 0, ownerMissing: 0, machineMismatch: 0, machineUnbound: 0, layoutCollision: 0, gone: 0,
  failed: 0, wouldErase: 0, wouldSupersede: 0, versions: 0 });
type Outcome = Exclude<keyof RearmSweepCounts, "listed" | "failed" | "versions">;
/** The outcomes whose erase ran: only these spend the budget. */
const ERASE_RAN: ReadonlySet<Outcome> = new Set(["erased", "pending", "reenabled", "wouldErase"]);

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
/** The corrupt-marker record of a marker whose bytes hash to `digest`. */
export const rearmCorruptKey = (markerKey: string, digest: string) => `${markerKey.slice(0, -".json".length)}.corrupt/${digest}.json`;

/** The listing scopes, in order: one per org, or the whole admission namespace. */
export function rearmScopes(keyRoot: string, orgs: readonly string[] | null): string[] {
  const ns = `${keyRoot}/${INBOX_NAMESPACE}`;
  return orgs === null ? [ns] : [...new Set(orgs)].sort().map((org) => `${ns}/${org}/v1`);
}

const safeCode = (e: unknown): string => {
  const c = (e as { code?: unknown } | null)?.code;
  return typeof c === "string" && /^[a-z0-9_]{1,64}$/i.test(c) ? c : "error";
};
/** A 401/403 from the object store (`unauthorized`) or the eraser (`s3_401` / `s3_403`): the role is wrong. */
export const isAccessDenied = (e: unknown): boolean =>
  isObjectStoreError(e, "unauthorized") || (e instanceof LedgerUnavailableError && e.code === "ledger_store_unauthorized")
  || /^s3_40[13]$/.test(String((e as { code?: unknown } | null)?.code ?? ""));
/** UTF-8 byte order, as S3 lists. */
const byteAfter = (a: string, b: string) => Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8")) > 0;

/** The fence's verdict for one marker's session. */
function verdict(fence: FenceDoc, m: RearmDoc): "erase" | "reset_running" | "superseded" {
  if (isDeleted(fence, m.originId, m.source, m.sessionId)) return "erase";
  return fence.resets > fence.resetsCompleted ? "reset_running" : "superseded";
}

/**
 * One pass: lists the scopes from `resume` (or the start), processes the markers of each page, and stops before the
 * next marker once `maxMarkers` erases ran or `deadlineAt` passed (markers in flight finish). Throws for a refused
 * configuration, denied access (`access_denied`) or a failed listing; a failure on one marker is counted and the pass
 * goes on.
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
  const scopes = rearmScopes(opts.keyRoot, orgs);
  if (resume !== null && (!Number.isInteger(resume.scope) || resume.scope < 0 || (resume.afterKey !== undefined && typeof resume.afterKey !== "string")))
    throw new RearmSweepRefused("resume_invalid");
  const io = s3LedgerObjects(store, opts.keyRoot);
  const counts = zero();
  const failures: Record<string, number> = {};
  const fail = (code: string) => { counts.failed++; failures[code] = (failures[code] ?? 0) + 1; };

  async function createOnly(key: string, doc: unknown): Promise<void> {
    try {
      await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(doc)), { ifNoneMatch: true, contentType: "application/json" });
    } catch (error) {
      // Another pass recorded the same content first: the same fact.
      if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
    }
  }
  const digestOf = (raw: Uint8Array) => createHash("sha256").update(raw).digest("hex").slice(0, 32);

  async function corrupt(key: string, raw: Uint8Array, reason: RearmCorruptDoc["reason"]): Promise<Outcome> {
    if (!dryRun) await createOnly(rearmCorruptKey(key, digestOf(raw)), { v: 1, reason, atMs: now() } satisfies RearmCorruptDoc);
    return "corrupt";
  }

  /** Step 4: the marker's machine is its user's (the ownership stub), and no live session shares its physical dir. */
  async function bound(dir: string, fence: FenceDoc, m: RearmDoc): Promise<Outcome | null> {
    const stub = await io.readOwner(ownerKeyIn(dir, m.originId, m.source, m.sessionId));
    if (!stub || stub.doc.originId !== m.originId || stub.doc.source !== m.source || stub.doc.sessionId !== m.sessionId) return "ownerMissing";
    if (stub.doc.machineId === undefined) return "machineUnbound";
    if (stub.doc.machineId !== m.machineId) return "machineMismatch";
    const h = layoutComponent(m.source), s = layoutComponent(m.sessionId);
    const prefix = ownersOriginPrefix(dir, m.originId);
    let cursor: string | undefined;
    do {
      const page = await store.list(objectKey(prefix), cursor);
      for (const item of page.items) {
        const k = String(item.key);
        if (!k.startsWith(`${prefix}/`) || !k.endsWith(".json")) continue;
        const other = await readOwnerLoose(k);
        // An unreadable stub could be a colliding live session: fail closed.
        if (other === "invalid") return "layoutCollision";
        if (other === null || (other.source === m.source && other.sessionId === m.sessionId)) continue;
        if (layoutComponent(other.source) === h && layoutComponent(other.sessionId) === s && !isDeleted(fence, m.originId, other.source, other.sessionId)) return "layoutCollision";
      }
      cursor = page.nextCursor;
    } while (cursor);
    return null;
  }
  async function readOwnerLoose(key: string): Promise<OwnerDoc | null | "invalid"> {
    try { return (await io.readOwner(key))?.doc ?? null; } catch (error) {
      if (error instanceof LedgerUnavailableError && error.code === "ledger_owner_invalid") return "invalid";
      throw error;
    }
  }

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
    try { marker = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw)); } catch { return corrupt(key, raw, "unparseable"); }
    if (!validRearm(marker)) return corrupt(key, raw, "invalid");
    if (!rearmKeyMatches(at.dir, key, marker)) return corrupt(key, raw, "key_mismatch");
    const digest = digestOf(raw);
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
      await createOnly(rearmDoneKey(key, digest), { v: 1, outcome: "superseded", marker: digest, atMs: now() } satisfies RearmDoneDoc);
      return "superseded";
    }
    const refused = await bound(at.dir, read.fence, marker);
    if (refused) return refused;
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
    await createOnly(rearmDoneKey(key, digest), { v: 1, outcome: "erased", marker: digest, atMs: now() } satisfies RearmDoneDoc);
    return "erased";
  }

  let erases = 0;
  let denied: unknown = null;
  const spent = () => erases >= maxMarkers || (opts.deadlineAt !== undefined && now() >= opts.deadlineAt);
  const stopAt = (scope: number, cursor: string | undefined, afterKey: string | undefined): RearmSweepResult =>
    ({ dryRun, complete: false, counts, failures, resume: { scope, ...(cursor !== undefined ? { cursor } : {}), ...(afterKey !== undefined ? { afterKey } : {}) } });
  const list = async (prefix: string, cursor: string | undefined) => {
    try { return await store.list(objectKey(prefix), cursor); } catch (error) {
      if (isAccessDenied(error)) throw new RearmSweepRefused("access_denied");
      throw error;
    }
  };

  for (let scope = resume?.scope ?? 0, cursor = resume?.cursor, afterKey = resume?.afterKey; scope < scopes.length; scope++, cursor = undefined, afterKey = undefined) {
    const prefix = scopes[scope]!;
    if (afterKey !== undefined && !afterKey.startsWith(`${prefix}/`)) afterKey = undefined; // another scope's key: ignore
    let resumed = cursor !== undefined;
    for (;;) {
      if (spent()) return stopAt(scope, cursor, afterKey);
      let page;
      try { page = await list(prefix, cursor); } catch (error) {
        // A stale saved cursor: list the scope again from its start; `afterKey` still skips what was finished.
        if (!resumed || error instanceof RearmSweepRefused) throw error;
        resumed = false;
        cursor = undefined;
        continue;
      }
      resumed = false;
      const markers = page.items.map((i) => String(i.key))
        .filter((k) => k.startsWith(`${prefix}/`) && parseRearmKey(opts.keyRoot, k) !== null && (afterKey === undefined || byteAfter(k, afterKey)));
      // In key order, `concurrency` at once. Out of budget part way: everything before `next` has finished, so the next
      // pass lists this page again and continues after the last of them.
      let next = 0;
      await Promise.all(Array.from({ length: Math.min(concurrency, markers.length) }, async () => {
        while (next < markers.length && !spent() && denied === null) {
          const key = markers[next++]!;
          counts.listed++;
          let outcome: Outcome | null = null;
          try { outcome = await one(key); } catch (error) {
            if (isAccessDenied(error)) { denied = error; return; }
            fail(error instanceof SessionEraseRefused || error instanceof LedgerUnavailableError ? error.code : isObjectStoreError(error) ? `store_${error.code}` : safeCode(error));
          }
          if (outcome !== null) { counts[outcome]++; if (ERASE_RAN.has(outcome)) erases++; }
        }
      }));
      if (denied !== null) throw new RearmSweepRefused("access_denied");
      if (next < markers.length) return stopAt(scope, cursor, markers[next - 1] ?? afterKey);
      // `afterKey` stays for the rest of the scope: later pages sort after it anyway, and after a stale-cursor restart
      // it keeps skipping every page that was already finished.
      if (!page.nextCursor) break;
      cursor = page.nextCursor;
    }
  }
  return { dryRun, complete: true, counts, failures, resume: null };
}

// ---- the durable position (successive Lambda invocations and loop passes walk the namespace, then wrap around) ----

/** `<keyRoot>/.ledger/rearm-sweeper/cursor-<h(scopes)>.json`: one position per scope set, so differently scoped sweepers never share one. */
export const rearmCursorKey = (keyRoot: string, scopes: readonly string[]) =>
  `${keyRoot}/.ledger/rearm-sweeper/cursor-${createHash("sha256").update(JSON.stringify(scopes)).digest("hex").slice(0, 32)}.json`;
/** `resume` null: the next pass starts over (the last one finished the namespace). Never logged: it holds a key. */
export interface RearmCursorDoc { v: 1; resume: RearmSweepResume | null; atMs: number }

const validResume = (r: unknown): r is RearmSweepResume => {
  if (r === null || typeof r !== "object" || Array.isArray(r)) return false;
  const v = r as Record<string, unknown>;
  return Object.keys(v).every((k) => k === "scope" || k === "cursor" || k === "afterKey") && Number.isInteger(v.scope) && (v.scope as number) >= 0 && (v.scope as number) < 100_000
    && (v.cursor === undefined || (typeof v.cursor === "string" && v.cursor.length > 0 && v.cursor.length <= 8192))
    && (v.afterKey === undefined || (typeof v.afterKey === "string" && v.afterKey.length > 0 && v.afterKey.length <= 2048));
};

/** The saved position, or a start-over (null) when there is none or it is unreadable. `version`: for the CAS write. */
export async function readRearmCursor(store: ObjectStore, key: string): Promise<{ resume: RearmSweepResume | null; version?: ObjectVersion }> {
  let got;
  try { got = await store.getObject(objectKey(key)); } catch (error) {
    if (isObjectStoreError(error, "not_found")) return { resume: null };
    if (isAccessDenied(error)) throw new RearmSweepRefused("access_denied");
    throw error;
  }
  const version = got.meta.version;
  try {
    const doc = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(await readAll(got.body))) as RearmCursorDoc;
    if (doc?.v === 1 && (doc.resume === null || validResume(doc.resume))) return { resume: doc.resume, version };
  } catch { /* unreadable: start over; the next save replaces it */ }
  return { resume: null, version };
}

/** Save the position: create-only, or If-Match the version read. false: another sweeper saved first (its position stands). */
export async function writeRearmCursor(store: ObjectStore, key: string, resume: RearmSweepResume | null, version: ObjectVersion | undefined, atMs: number): Promise<boolean> {
  const body = new TextEncoder().encode(JSON.stringify({ v: 1, resume, atMs } satisfies RearmCursorDoc));
  try {
    await store.put(objectKey(key), body, version ? { ifMatch: version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
    return true;
  } catch (error) {
    if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists")) return false;
    if (isAccessDenied(error)) throw new RearmSweepRefused("access_denied");
    throw error;
  }
}
