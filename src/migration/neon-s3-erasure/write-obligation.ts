/**
 * THE shared unresolved-write obligation primitive (lead ruling 2026-11-07, "no
 * unresolved-PUT proof"). F1 (analysis result / backfill PUTs), lane A (E3) and Babysitter
 * retention compose this one implementation; each lane keeps its OWN obligation table (schemas
 * differ per slot) behind `ObligationStore`. The design is F1's reference model (#292
 * b608ea3b / #296 494272c6) with the lane-agnostic parts extracted.
 *
 * Model. One durable obligation per object key a writer may PUT:
 *   publishing  recorded BEFORE the PUT (same transaction as the writer's lease/generation);
 *   unknown     the PUT aborted, timed out, errored or lost its response: it MAY still commit,
 *               however late (no timer, grace or wall time is ever a commit bound);
 *   forgotten   the outcome is known and every unowned version at the key must be erased.
 * A writer that gets an acknowledged PUT records the exact owned version id in its own terminal
 * transaction and retires the obligation there. Purge / forget never downgrade publishing or
 * unknown to forgotten.
 *
 * Reconcile (`reconcileObligation`), always through the shared strict paged eraser
 * (eraser.ts eraseStep / verifyErased):
 *   - selects, by exact version id, every version at the key that no completed owner holds
 *     (successor-safe: an owned version is never selected, never counted as remaining);
 *   - the checkpoint scope digest binds operation, the lane's identity parts, the key, the
 *     reason and the owned versions; a foreign checkpoint restarts;
 *   - authority (the obligation re-read) is re-checked before every page's deletes;
 *   - "an unowned version was found" is recorded DURABLY before that page's deletes
 *     (`markStray`), so a lost DELETE ack never loses the evidence.
 * Completion (`isObligationComplete`), the strict predicate every lane uses:
 *   forgotten            -> a fresh strict verifyErased shows zero unowned versions;
 *   unknown / publishing -> an unowned version WAS found and erased (the write is accounted
 *                           for) AND a fresh strict verifyErased shows zero;
 *   anything else keeps the obligation: a clean sweep only reschedules (backoff 1, 2, 4 ...
 *   minutes, capped at a day: `nextSweepDelayMs`); a failed step keeps the obligation, its
 *   checkpoint and stray evidence; an owner whose exact version is unrecorded (an unversioned
 *   store) cannot be told apart from a stray, so nothing is erased and it is rescheduled.
 * A never-landing unknown PUT therefore stays tracked (rescheduled) until a ruling defines a
 * provider-fencing terminal proof; none is assumed here.
 */
import { createHash } from "node:crypto";
import type { EraseBudget, EraseCheckpoint, ErasedProof, ListedVersion, PagedEraser } from "./eraser.ts";

export type ObligationReason = "publishing" | "unknown" | "forgotten";

/** The obligation as the lane's table holds it (one row per object key). */
export interface WriteObligation {
  /** The exact object key a writer may PUT (the erase scope is that key only). */
  key: string;
  reason: ObligationReason;
  /** The lane's identity bound into the checkpoint scope (operation, tenant/session, owner, generation, writer...). */
  identity: readonly unknown[];
  checkpoint: EraseCheckpoint | null;
  /** Durable evidence that an unowned version was seen (and selected for erasure) at the key. */
  strayFound: boolean;
}

/** The obligation's CURRENT authority, re-read before every page's deletes. */
export interface ObligationAuthority {
  reason: ObligationReason;
  /** The lane's identity now (must equal the obligation's, or the sweep stops). */
  identity: readonly unknown[];
  /** Exact version ids completed owners hold at the key (never erased). */
  ownedVersions: readonly string[];
  /** Completed owners at the key whose exact version id is unrecorded (blocks any erase). */
  ownersWithoutVersion: number;
}

/**
 * The lane's durable adapter over its own obligation table, sweep side; every method is
 * idempotent. DURABILITY CONTRACT: `markStray` must be durable when it returns (commit it on
 * its own, or commit the caller's per-page transaction) because the page's deletes follow
 * immediately; a rolled-back in-transaction mark would lose the only proof of the write.
 */
export interface ObligationStore {
  /** The current authority, or null when the obligation is gone (retired elsewhere). */
  load(): Promise<ObligationAuthority | null>;
  /** DURABLY, before that page's deletes: an unowned non-marker version was found. */
  markStray(): Promise<void>;
  /** Durably: the bounded run's checkpoint (bound to the scope). */
  saveCheckpoint(checkpoint: EraseCheckpoint): Promise<void>;
  /** Not complete: clear the checkpoint, keep stray evidence, back off (`nextSweepDelayMs`). */
  reschedule(strayFound: boolean): Promise<void>;
  /** Complete: delete the obligation (only after `isObligationComplete`). */
  retire(): Promise<void>;
}

/**
 * The lane's writer side (its own SQL, inside ITS transaction `Tx`), for a uniform contract:
 *   record      BEFORE the PUT, in the same transaction as the writer's lease/heartbeat/generation;
 *   markUnknown on abort / timeout / error / lost response (writer-guarded; publishing -> unknown);
 *   publish     the terminal success: records the exact owned version id and retires the
 *               obligation in the SAME transaction (proof "published"); the version stays.
 * Purge / forget may set forgotten only on an obligation that is neither publishing nor unknown
 * (those stay, or become unknown); nothing ever downgrades an unresolved write.
 */
export interface ObligationWriter<Tx> {
  record(tx: Tx, key: string, identity: readonly unknown[]): Promise<void>;
  markUnknown(tx: Tx, key: string, identity: readonly unknown[]): Promise<void>;
  publish(tx: Tx, key: string, identity: readonly unknown[], versionId: string): Promise<void>;
}

export type ReconcileOutcome = "retired" | "rescheduled" | "incomplete";

/** The re-sweep backoff: 1, 2, 4 ... minutes, at most a day; never a retirement timer. */
export function nextSweepDelayMs(sweeps: number): number {
  const n = Number.isSafeInteger(sweeps) && sweeps > 0 ? Math.min(sweeps, 11) : 0;
  return Math.min(86_400_000, 60_000 * 2 ** n);
}

/**
 * The strict completion predicate (see the header): zero UNOWNED versions remain (owned ones
 * may stay), and for an unresolved write the write is accounted for: an unowned version was
 * found and erased, or the key's version is held by a completed owner (resolved by ownership).
 */
export function isObligationComplete(reason: ObligationReason, accounted: { strayFound: boolean; ownedSeen: boolean }, proof: ErasedProof): boolean {
  if (proof.status !== "erased") return false;
  return reason === "forgotten" || accounted.strayFound || accounted.ownedSeen;
}

/** The checkpoint scope digest: operation, lane identity, key, reason and the owned versions. */
export function obligationScope(o: Pick<WriteObligation, "key" | "identity">, authority: Pick<ObligationAuthority, "reason" | "ownedVersions">): string {
  const owned = [...authority.ownedVersions].sort();
  return "write-obligation:" + createHash("sha256").update(JSON.stringify(["write-obligation-v1", o.identity, o.key, authority.reason, owned])).digest("hex");
}

const same = (a: readonly unknown[], b: readonly unknown[]) => JSON.stringify(a) === JSON.stringify(b);
const coded = (code: string) => Object.assign(new Error(`write obligation: ${code}`), { code });

/**
 * One bounded reconcile of one obligation. Returns `incomplete` (checkpoint saved, run again),
 * `rescheduled` (not provably complete) or `retired`. Throws on a failed step: the lane keeps
 * the obligation, its last checkpoint and its (already durable) stray evidence.
 */
export async function reconcileObligation(
  eraser: PagedEraser,
  store: ObligationStore,
  o: WriteObligation,
  opts: { budget?: EraseBudget; now?: () => number } = {},
): Promise<ReconcileOutcome> {
  if (typeof o.key !== "string" || !o.key.includes("/") || o.key.endsWith("/")) throw coded("key_shape");
  const first = await store.load();
  if (!first) return "rescheduled";
  if (first.reason !== o.reason || !same(first.identity, o.identity)) throw coded("authority_changed");
  // An owner whose exact version is unknown cannot be told apart from a stray: never erase.
  if (first.ownersWithoutVersion > 0) { await store.reschedule(o.strayFound); return "rescheduled"; }
  const owned = new Set(first.ownedVersions);
  const prefix = o.key.slice(0, o.key.lastIndexOf("/") + 1);
  const scope = obligationScope(o, first);
  const authorize = async () => {
    const now = await store.load();
    if (!now || now.reason !== first.reason || !same(now.identity, first.identity) || now.ownersWithoutVersion > 0
        || !same([...now.ownedVersions].sort(), [...first.ownedVersions].sort()))
      throw coded("authority_changed");
  };
  const unowned = (v: ListedVersion) => v.key === o.key && !owned.has(v.versionId);
  let strayFound = o.strayFound, ownedSeen = false;
  // Markers are never a write; an owned listed version accounts for an unresolved write.
  const select = (v: ListedVersion) => {
    if (v.key === o.key && owned.has(v.versionId) && !v.deleteMarker) ownedSeen = true;
    return unowned(v);
  };
  const beforeDelete = async (doomed: readonly ListedVersion[]) => {
    if (!strayFound && doomed.some((v) => !v.deleteMarker)) { await store.markStray(); strayFound = true; }
  };
  const checkpoint = o.checkpoint && o.checkpoint.scope === scope ? o.checkpoint : null;
  const step = await eraser.eraseStep(prefix, { scope, checkpoint, select, authorize, beforeDelete, ...(opts.budget ? { budget: opts.budget } : {}), ...(opts.now ? { now: opts.now } : {}) });
  if (step.status === "incomplete") { await store.saveCheckpoint(step.checkpoint); return "incomplete"; }
  const proof = await eraser.verifyErased(prefix, { select, authorize, ...(opts.budget ? { budget: opts.budget } : {}), ...(opts.now ? { now: opts.now } : {}) });
  if (isObligationComplete(first.reason, { strayFound, ownedSeen }, proof)) { await store.retire(); return "retired"; }
  await store.reschedule(strayFound);
  return "rescheduled";
}

/**
 * The writer's bounded PUT (default 120 s): the abort signal reaches the request; on abort,
 * timeout, error or a lost response `onUnknown` durably marks the obligation unknown (writer-
 * guarded in the lane's SQL) BEFORE the error propagates. Never retires anything.
 */
export async function boundedPut<T>(put: (signal: AbortSignal) => Promise<T>, onUnknown: () => Promise<void>, timeoutMs = 120_000): Promise<T> {
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    return await put(signal);
  } catch (error) {
    await onUnknown();
    throw error;
  }
}
