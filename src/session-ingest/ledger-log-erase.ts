/**
 * Erasing log payloads (docs/design/s3-ledger-log-commit.md, Remaining 3). A log-mode stream keeps each admitted
 * batch's raw records in a payload object (`<stream>/p/`) until the materializer applies the batch's entry. Before
 * this module, a privacy reset left a user's unapplied payloads in place until the stream's next apply.
 *
 * Two erasures, both scoped to ONE (org, workspace, user), never the deployment-wide stream registry: no other user's
 * stream is ever read or touched, and every stream root is rebuilt under that user's own hashed directory.
 *
 *   eraseSupersededLogPayloads   the payloads of every unapplied batch the materializer will skip as superseded by a
 *                                reset: its admission snapshot's `resets` is below the fence's `resets` (exactly the
 *                                materializer's rule, ledger-log-materialize.ts `fence.resets > b.admitted.resets`).
 *                                A payload a live batch also names is kept. Used by the S3 ledger's reset
 *                                (ledger-s3.ts `resetLogErase`, INGEST_LEDGER_LOG_ERASE=on) BEFORE the reset is
 *                                completed or answered, on every answer (applied, replay, stale), so a retry of a
 *                                reset whose first erase failed erases again.
 *   eraseLogPayloads             every payload of the user, unconditionally (a whole-user erase; not wired to a route
 *                                yet, Remaining 3). The caller must already hold the user's admissions (see "Ordering"
 *                                below), or a racing admission can write a new payload after the listing; the
 *                                verification listing then finds it and the erase fails (retryable), never success.
 *
 * Cost (the reset is on a request path, so it never lists a stream's entries, markers or whole directory):
 *   - the user's streams come from the user's stream INDEX (ledger-log.ts logUserIndex): one LIST page per 1000
 *     streams of the user. `list` is recursive on every backend and the ObjectStore has no delimiter, so listing the
 *     user's log directory itself would list every entry, checkpoint, marker and payload (unbounded until compaction);
 *   - per stream, one LIST of `p/` (pages of the payloads still present: unapplied, in flight or orphaned, never the
 *     applied history). A stream with none costs nothing more;
 *   - only then, a lower bound on the materializer's cursor in O(log n) marker HEADs (materializedAtLeast; never a
 *     listing of `m/`), the stream's unapplied entries from the nearest verified checkpoint (readEntries), and a DELETE
 *     plus a verifying HEAD per superseded payload.
 * The whole-user erase is rare and must be complete, so it ALSO lists the user's log directory (a stream the index does
 * not name, switched before the index existed and never taken since, is still erased); it is not on a request path.
 *
 * No fixed cap on one reset's work, on purpose: a cap would either report the erase done with payloads left (a privacy
 * miss) or fail that user's every reset until an operator stepped in. The work is bounded by the user's own data: the
 * number of their streams (each costs one `p/` LIST when it holds no payload), and only for a stream that still holds
 * payloads, its unapplied entries and superseded payloads, which the materializer drains on its own.
 *
 * Fail closed: any store fault, an unreadable stream (LogCorrupt, LogUnavailable) or a payload still present after its
 * delete throws `LedgerUnavailableError` (`ledger_log_erase_failed` / `ledger_log_erase_unverified`): a retry, never
 * success. A failing stream does not stop the others: every stream is attempted, and the first failure is thrown once
 * all ran. Each failure emits one structured line through `opts.log` (route `ingest.ledger_log_erase`, outcome
 * `ledger_log_erase_failed`, a closed `error_code`, `kind`, and `key_sha256` = sha256 of the stream root: never an id or
 * a key), one per failing stream, so a stream that keeps failing its user's resets is visible (runbook: the design
 * doc's slice 8). A sink that throws is ignored: the erase's own error still surfaces.
 *
 * Ordering (what a reset relies on): log admission refuses a user while a reset is begun and not completed
 * (ledger-log-admit.ts: `ledger_reset_in_flight`, `ledger_control_pending`), and the reset's erase runs after its
 * begin CAS. So every batch appended by an admission that read the fence after the begin carries the new `resets`
 * and is not touched. An admission that read the fence BEFORE the begin and appends AFTER this erase listed its
 * stream carries the old `resets`: its payload is not deleted by the reset, but the materializer never writes it
 * (the same snapshot rule) and deletes it when it reaches the entry (or, if the append never happens, orphan
 * collection takes it after ORPHAN_GRACE_MS). That window is bounded by one admission's fence-read -> append time.
 *
 * Deletes remove the current object version only, as the materializer's own payload drop does: on a versioned
 * bucket, noncurrent versions are the bucket lifecycle's (a separate retention concern, Remaining 5).
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, type ObjectStore } from "../shared/vendor.ts";
import { LedgerUnavailableError, type LedgerTenant } from "./ledger.ts";
import { listKeysUnder, LogCorrupt, LogUnavailable, logKeysAt, logUserIndex, logUserRoot, readEntries, type LogKeys } from "./ledger-log.ts";
import { materializedAtLeast } from "./ledger-log-materialize.ts";

export const INGEST_LEDGER_LOG_ERASE_VAR = "INGEST_LEDGER_LOG_ERASE";

/** Unset, "" or "off": off (today, byte for byte). Exactly "on" otherwise; anything else throws. */
export function ledgerLogEraseMode(raw: string | undefined): "off" | "on" {
  const v = raw?.trim();
  if (v === undefined || v === "" || v === "off") return "off";
  if (v === "on") return "on";
  throw new Error(`session ingest: ${INGEST_LEDGER_LOG_ERASE_VAR} must be off or on`);
}

/** Where an erase reports a failure: one structured line (shared/log.ts allowlisted fields only). */
export interface LogEraseOptions { log?: (fields: Record<string, unknown>) => void }

const STREAM_ROOT = /^(.*\/ledger\/log\/o=[0-9a-f]{32})\//;
const INDEX_ENTRY = /^o=([0-9a-f]{32})\.json$/;
const CODE = /^[a-z0-9_]{1,64}$/;
const sha256 = (v: string) => createHash("sha256").update(v).digest("hex");

/** The structured failure line: closed codes and a hash of the stream root only. */
function failureFields(error: unknown, root?: string): Record<string, unknown> {
  const kind = error instanceof LogCorrupt ? "log_corrupt" : error instanceof LogUnavailable ? "log_unavailable"
    : isObjectStoreError(error) ? "store"
    : error instanceof LedgerUnavailableError ? (error.code === "ledger_log_erase_unverified" ? "unverified" : "ledger") : "other";
  const raw = (error as { code?: unknown } | null)?.code;
  return { route: "ingest.ledger_log_erase", outcome: "ledger_log_erase_failed", kind,
    error_code: typeof raw === "string" && CODE.test(raw) ? raw : "erase_failed", ...(root ? { key_sha256: sha256(root) } : {}) };
}

/** Store faults and unreadable streams become one retryable code; an erase never half-succeeds silently. */
function asRetry(error: unknown): unknown {
  if (error instanceof LedgerUnavailableError) return error;
  if (error instanceof LogCorrupt || error instanceof LogUnavailable || isObjectStoreError(error)) return new LedgerUnavailableError("ledger_log_erase_failed");
  return error;
}

/** Emit the failure line. A throwing sink never replaces the erase's own error (the reset must still fail as a retry). */
function report(opts: LogEraseOptions, error: unknown, root: string | undefined): void {
  try { opts.log?.(failureFields(error, root)); } catch { /* the sink's own fault: the erase error below still surfaces */ }
}

/** Run `work` for one stream (or the discovery, `root` undefined): a failure is logged once, then thrown as a retry. */
async function reported<T>(opts: LogEraseOptions, root: string | undefined, work: () => Promise<T>): Promise<T> {
  try { return await work(); }
  catch (error) { report(opts, error, root); throw asRetry(error); }
}

/**
 * Run `work` on EVERY stream, even after one fails: one unreadable stream never leaves the user's other streams'
 * payloads in place for this attempt. Each failing stream logs its own line (its `key_sha256`); if any failed, the
 * first failure is thrown (as a retry) once all streams ran, so the erase is still never reported done.
 */
async function eachStream(opts: LogEraseOptions, streams: readonly LogKeys[], work: (keys: LogKeys) => Promise<number>): Promise<number> {
  let n = 0, failed = false, first: unknown;
  for (const keys of streams) {
    try { n += await work(keys); }
    catch (error) { report(opts, error, keys.root); if (!failed) { failed = true; first = error; } }
  }
  if (failed) throw asRetry(first);
  return n;
}

/** A stream root found for the user, rechecked: exactly one stream directory directly under the user's own log root. */
function ownRoot(userRoot: string, root: string): string | undefined {
  return STREAM_ROOT.exec(`${root}/`)?.[1] === root && root.startsWith(userRoot) && !root.slice(userRoot.length).includes("/") ? root : undefined;
}

/**
 * Every log stream of the user, from the user's stream index (logUserIndex): one flat LIST, no registry read, never
 * the streams' own objects. An object in the index that is not `o=<32 hex>.json` names nothing.
 */
export async function userLogStreams(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<LogKeys[]> {
  const index = logUserIndex(keyRoot, tenant), userRoot = logUserRoot(keyRoot, tenant);
  const roots = new Set<string>();
  for (const k of await listKeysUnder(store, index)) {
    const m = INDEX_ENTRY.exec(k.slice(index.length));
    const root = m ? ownRoot(userRoot, `${userRoot}o=${m[1]}`) : undefined;
    if (root) roots.add(root);
  }
  return [...roots].sort().map(logKeysAt);
}

/** The index's streams plus every stream directory a listing of the user's whole log directory finds (complete, O(objects)). */
async function everyUserLogStream(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<LogKeys[]> {
  const userRoot = logUserRoot(keyRoot, tenant);
  const roots = new Set((await userLogStreams(store, keyRoot, tenant)).map((k) => k.root));
  for (const k of await listKeysUnder(store, userRoot)) {
    const root = STREAM_ROOT.exec(k)?.[1];
    if (root && ownRoot(userRoot, root)) roots.add(root);
  }
  return [...roots].sort().map(logKeysAt);
}

async function deleteVerified(store: ObjectStore, keys: readonly string[]): Promise<void> {
  for (const k of keys) await store.delete(objectKey(k));
  for (const k of keys) if ((await store.head(objectKey(k))) !== undefined) throw new LedgerUnavailableError("ledger_log_erase_unverified");
}

/**
 * Delete every payload of the user's log streams. Returns how many were deleted. Throws (retryable) on any fault or if
 * a fresh listing still finds a payload (a write that raced the erase): the caller must not report the erase done.
 * A batch whose payload is gone is skipped by the materializer (`payload_erased`), never written.
 */
export async function eraseLogPayloads(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, opts: LogEraseOptions = {}): Promise<number> {
  return eachStream(opts, await reported(opts, undefined, () => everyUserLogStream(store, keyRoot, tenant)), async (keys) => {
    const found = await listKeysUnder(store, keys.payloads);
    for (const k of found) await store.delete(objectKey(k));
    // A fresh listing, not the deletes' answers: a payload PUT that raced the erase fails it.
    if ((await listKeysUnder(store, keys.payloads)).length > 0) throw new LedgerUnavailableError("ledger_log_erase_unverified");
    return found.length;
  });
}

/** One stream's superseded payloads (the module header's cost list): `p/` first, and nothing more when it is empty. */
async function eraseSupersededIn(store: ObjectStore, keys: LogKeys, resets: number): Promise<number> {
  const present = new Set(await listKeysUnder(store, keys.payloads));
  if (present.size === 0) return 0;
  // A lower bound: reading from below the true cursor reads more entries, never skips an unapplied one. A superseded
  // batch that is in fact applied already had its payload written (or skipped) and dropped (or about to be): deleting
  // it is the materializer's own next step.
  // -1 (no marker 0: nothing applied yet, or marker 0 removed by a future compaction) falls back to reading from entry
  // 0 and skipping nothing: more entries read, never one missed, and never a negative fromSeq.
  const through = await materializedAtLeast(store, keys);
  const doomed = new Set<string>(), live = new Set<string>();
  for await (const e of readEntries(store, keys, through < 0 ? 0 : through)) {
    if (e.seq <= through || e.kind !== "batches") continue;
    for (const b of e.batches) {
      const superseded = b.admitted !== undefined && b.admitted.resets < resets;
      for (const p of b.payloads) {
        // An entry naming a key outside its own stream's payload directory is not something admission writes.
        if (!p.key.startsWith(keys.payloads)) throw new LedgerUnavailableError("ledger_log_erase_failed");
        (superseded ? doomed : live).add(p.key);
      }
    }
  }
  // Only payloads the listing found: one written after it is the "Ordering" window (never written, deleted at apply).
  const targets = [...doomed].filter((k) => !live.has(k) && present.has(k));
  await deleteVerified(store, targets);
  return targets.length;
}

/**
 * Delete the payloads of every unapplied batch of the user whose admission snapshot predates `resets` (the fence's
 * reset count, this reset included). Batches without a snapshot (library tests only; admission always writes one)
 * are left to the materializer's journal re-check. Returns how many payloads were deleted.
 */
export async function eraseSupersededLogPayloads(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, resets: number, opts: LogEraseOptions = {}): Promise<number> {
  if (!Number.isSafeInteger(resets) || resets < 0) throw new LedgerUnavailableError("ledger_input_refused");
  if (resets === 0) return 0;
  return eachStream(opts, await reported(opts, undefined, () => userLogStreams(store, keyRoot, tenant)), (keys) => eraseSupersededIn(store, keys, resets));
}
