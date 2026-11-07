/**
 * THE shared unresolved-write obligation primitive (lead rulings 2026-11-07: "no unresolved-PUT
 * proof", shared-unknown-put-nonterminal, and the 3e8b914d / 8b00f44b reviews). F1 (analysis
 * result / backfill PUTs), lane A (E3) and Babysitter retention compose this one implementation;
 * each lane keeps its OWN tables behind `ObligationStore` / `ObligationWriter`.
 *
 * Model: one obligation per object KEY with the full set of DISPATCHED invocations at that key.
 * Each invocation (one PUT attempt) gets a durable id recorded BEFORE it is sent. Its outcome:
 *   dispatched  sent; outcome unknown (lost/timed-out response, crash): it MAY still commit;
 *   acked       its OWN confirmed response, with the exact version id it created;
 *   refused     a definite provider refusal of THAT request (e.g. 412 If-None-Match): no write.
 * Terminal evidence clears exactly the invocation it belongs to, never another:
 *   refused                                -> resolved;
 *   acked v, v owned as its own publish    -> resolved (the version stays);
 *   acked v, v NOT owned                   -> resolved once a fresh strict, COMPLETE inventory
 *                                             shows that exact version absent (version ids are
 *                                             never reused);
 *   dispatched                             -> UNRESOLVED, indefinitely.
 * There is NO content attribution: an ETag (even a body MD5, which SSE-KMS does not give) is
 * CONTENT identity, not REQUEST identity, so a listed version never resolves a dispatched
 * invocation (a pre-existing identical-content stray, or another invocation's identical body,
 * would otherwise retire a PUT that can still commit). Strays are erased as cleanup only.
 *
 * NONTERMINAL UNKNOWN (shared-unknown-put-nonterminal ruling): without its own terminal
 * evidence an invocation stays unresolved indefinitely, re-swept on the bounded backoff
 * (capped at 1 day, so a stuck key costs one strict listing per day). It is never resolved by a
 * timeout, a lease expiry, a generation change, a zero inventory or a matching listed version,
 * and the lane never reports the scope fully erased while it remains. There is NO
 * credential-rotation / bucket-policy / deny-observation shortcut, and an error from a
 * different later request never resolves an earlier lost acknowledgement.
 *
 * Deletion reservations (distinct from any outcome evidence): before ANY version is deleted,
 * EVERY doomed version of the page is reserved atomically (`reserve`) under the same row lock
 * the lane's publish takes, so publish and reserve conflict in either order: a version owned
 * before the reserve is refused (nothing in that page is deleted); a publish after the reserve
 * must refuse the reserved version. A reservation proves nothing about which PUT created it.
 *
 * Atomicity: every sweep-side mutation is a compare-and-set on the row version (`rev`) the sweep
 * loaded; a changed row refuses and the reconcile returns "refused" without retiring anything.
 *
 * Reconcile (`reconcileObligation`) always goes through the shared strict paged eraser
 * (eraser.ts eraseStep / verifyErased) in EXACT-KEY mode (only the key's own versions are listed,
 * so a directory of any number of kept siblings stays within budget): every unowned version at the key is erased, by exact
 * version id, successor-safe (owned versions are never selected); the checkpoint scope binds
 * the lane identity, key, owned set and invocation set; the row version is re-checked before
 * every page's deletes; an owner with an unrecorded version id blocks every erase.
 */
import { createHash } from "node:crypto";
import type { EraseBudget, EraseCheckpoint, ListedVersion, PagedEraser } from "./eraser.ts";

export type InvocationOutcome = "dispatched" | "acked" | "refused";

export interface WriteInvocation {
  /** Durable id recorded BEFORE the PUT is sent (never reused). */
  id: string;
  outcome: InvocationOutcome;
  /** acked: the version id its OWN response confirmed. */
  versionId?: string;
}

/** The obligation row as the sweep loads it; `rev` is the compare-and-set token. */
export interface ObligationState {
  key: string;
  /** The lane's identity (operation, tenant/session, generation, fence, writer...). */
  identity: readonly unknown[];
  rev: number;
  invocations: readonly WriteInvocation[];
  /** Exact version ids completed owners hold at the key (never erased). */
  ownedVersions: readonly string[];
  /** Completed owners at the key with an unrecorded version id (blocks every erase). */
  ownersWithoutVersion: number;
  checkpoint: EraseCheckpoint | null;
}

export type Cas = { ok: true; rev: number } | { ok: false };

/**
 * The lane's sweep-side adapter over its own tables. Every mutation is a compare-and-set on
 * `rev` and returns the new rev, or { ok: false } when the row changed (or is gone). Each must
 * be durable when it returns (commit on its own) because remote deletes follow.
 */
export interface ObligationStore {
  load(): Promise<ObligationState | null>;
  /**
   * Atomically reserve EVERY version about to be deleted, under the row lock the lane's publish
   * takes: refused ({ ok: false }) if the row changed or any version is owned (published) now;
   * after it succeeds, a publish of any reserved version must be refused.
   */
  reserve(rev: number, versionIds: readonly string[]): Promise<Cas>;
  saveCheckpoint(rev: number, checkpoint: EraseCheckpoint): Promise<Cas>;
  /** Clear exactly these invocations (each on its own terminal evidence). */
  resolve(rev: number, invocationIds: readonly string[]): Promise<Cas>;
  /** Not complete: clear the checkpoint, back off (`nextSweepDelayMs`). */
  reschedule(rev: number): Promise<Cas>;
  /** Complete: no invocation left and zero unowned versions; delete the obligation. */
  retire(rev: number): Promise<Cas>;
}

/**
 * The lane's writer side (its own SQL, inside ITS transaction `Tx`):
 *   dispatch  BEFORE the PUT, with the writer's lease/generation: a NEW invocation id;
 *   acked     its OWN confirmed response: the exact version id (resolves only this invocation);
 *   refused   a definite refusal of this very request (no write happened);
 *   publish   an owner's terminal success: records the owned version in the same tx, under the
 *             same row lock `reserve` takes, and MUST refuse a version reserved for deletion.
 * A timeout / abort / lost response leaves the invocation dispatched (unknown); nothing else may
 * downgrade or clear it.
 */
export interface ObligationWriter<Tx> {
  dispatch(tx: Tx, key: string, identity: readonly unknown[], invocationId: string): Promise<void>;
  acked(tx: Tx, key: string, invocationId: string, versionId: string): Promise<void>;
  refused(tx: Tx, key: string, invocationId: string): Promise<void>;
  publish(tx: Tx, key: string, identity: readonly unknown[], versionId: string): Promise<void>;
}

export type ReconcileOutcome = "retired" | "rescheduled" | "incomplete" | "refused" | "gone";

/** The re-sweep backoff: 1, 2, 4 ... minutes, at most a day; never a retirement timer. */
export function nextSweepDelayMs(sweeps: number): number {
  const n = Number.isSafeInteger(sweeps) && sweeps > 0 ? Math.min(sweeps, 11) : 0;
  return Math.min(86_400_000, 60_000 * 2 ** n);
}

/** The checkpoint scope: lane identity, key, owned set and the invocation set. */
export function obligationScope(s: Pick<ObligationState, "key" | "identity" | "ownedVersions" | "invocations">): string {
  const owned = [...s.ownedVersions].sort();
  const inv = [...s.invocations].map((i) => [i.id, i.outcome, i.versionId ?? null]).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
  return "write-obligation:" + createHash("sha256").update(JSON.stringify(["write-obligation-v3", s.identity, s.key, owned, inv])).digest("hex");
}

/**
 * Which invocations have their OWN terminal evidence, given a COMPLETE strict inventory's
 * versions at the key (`present`, or null when the inventory is incomplete) and the owned set.
 * Pure; a dispatched invocation is never terminal.
 */
export function terminalInvocations(invocations: readonly WriteInvocation[], owned: ReadonlySet<string>, present: ReadonlySet<string> | null): string[] {
  const out: string[] = [];
  for (const i of invocations) {
    if (i.outcome === "refused") { out.push(i.id); continue; }
    if (i.outcome !== "acked" || !i.versionId) continue;          // dispatched: nonterminal
    if (owned.has(i.versionId)) { out.push(i.id); continue; }     // its own publish
    if (present !== null && !present.has(i.versionId)) out.push(i.id);   // that exact version is gone
  }
  return out;
}

const coded = (code: string) => Object.assign(new Error(`write obligation: ${code}`), { code });

/**
 * One bounded reconcile of one key's obligation. Returns "incomplete" (checkpoint saved),
 * "rescheduled" (not provably complete), "retired", "refused" (a compare-and-set or reservation
 * lost: the row changed and survives, nothing of that page was deleted) or "gone". Throws on a
 * failed step (the obligation, its checkpoint and its reservations stay).
 */
export async function reconcileObligation(
  eraser: PagedEraser,
  store: ObligationStore,
  opts: { budget?: EraseBudget; now?: () => number } = {},
): Promise<ReconcileOutcome> {
  const first = await store.load();
  if (!first) return "gone";
  const key = first.key;
  if (typeof key !== "string" || !key.includes("/") || key.endsWith("/")) throw coded("key_shape");
  let rev = first.rev;
  const cas = async (p: Promise<Cas>) => { const r = await p; if (!r.ok) return false; rev = r.rev; return true; };
  if (first.ownersWithoutVersion > 0) return (await cas(store.reschedule(rev))) ? "rescheduled" : "refused";
  const owned = new Set(first.ownedVersions);
  const prefix = key.slice(0, key.lastIndexOf("/") + 1);
  const scope = obligationScope(first);
  let lost = false;
  const authorize = async () => {
    const now = await store.load();
    if (!now || now.rev !== rev) { lost = true; throw coded("authority_changed"); }
  };
  const unowned = (v: ListedVersion) => v.key === key && !owned.has(v.versionId);
  // Reserve EVERY doomed version (attributable or not) under the publish lock before its delete.
  const beforeDelete = async (doomed: readonly ListedVersion[]) => {
    const ids = doomed.filter((v) => v.key === key).map((v) => v.versionId);
    if (!ids.length) return;
    if (!(await cas(store.reserve(rev, ids)))) { lost = true; throw coded("reservation_refused"); }
  };
  // Progress selection (local, not a durable change): a checkpoint is reused only for this scope
  // AND this listing mode (exactKey). Any other one (a legacy directory-mode checkpoint, or one
  // whose scope changed because the obligation changed: a new invocation, a new owned version)
  // is simply ignored and the traversal restarts with a fresh strict listing; nothing of the
  // obligation (invocations, reservations, history) is touched by that choice. The low-level
  // eraseStep/verifyErased API still refuses a mismatched checkpoint passed to it directly.
  const checkpoint: EraseCheckpoint | null = first.checkpoint && first.checkpoint.scope === scope && first.checkpoint.prefix === prefix
    && (first.checkpoint.exactKey ?? null) === key ? first.checkpoint : null;
  let step;
  try {
    step = await eraser.eraseStep(prefix, { scope, checkpoint, select: unowned, authorize, beforeDelete, exactKey: key, ...(opts.budget ? { budget: opts.budget } : {}), ...(opts.now ? { now: opts.now } : {}) });
  } catch (error) {
    if (lost) return "refused";
    throw error;
  }
  if (step.status === "incomplete") return (await cas(store.saveCheckpoint(rev, step.checkpoint))) ? "incomplete" : "refused";
  // A fresh strict COMPLETE inventory of the key: which exact versions are present.
  const present = new Set<string>();
  let proof;
  try {
    proof = await eraser.verifyErased(prefix, { select: (v) => { if (v.key === key && !v.deleteMarker) present.add(v.versionId); return unowned(v); },
      authorize, exactKey: key, ...(opts.budget ? { budget: opts.budget } : {}), ...(opts.now ? { now: opts.now } : {}) });
  } catch (error) {
    if (lost) return "refused";
    throw error;
  }
  const current = await store.load();
  if (!current || current.rev !== rev) return "refused";
  const done = terminalInvocations(current.invocations, owned, proof.status === "erased" ? present : null);
  if (done.length && !(await cas(store.resolve(rev, done)))) return "refused";
  const left = current.invocations.length - done.length;
  if (proof.status === "erased" && left === 0) return (await cas(store.retire(rev))) ? "retired" : "refused";
  return (await cas(store.reschedule(rev))) ? "rescheduled" : "refused";
}

export class PutTimeout extends Error {
  readonly code = "put_timeout";
  constructor() { super("write obligation: PUT exceeded its wall deadline (outcome unknown)"); }
}

/** Runs an auxiliary hook so that neither a synchronous throw nor an async rejection escapes. */
function contained(run: () => unknown): void {
  try {
    const r = run();
    if (r && typeof (r as Promise<unknown>).then === "function") (r as Promise<unknown>).then(undefined, () => {});
  } catch { /* an auxiliary late-evidence hook never crashes the caller or changes the outcome */ }
}

/**
 * The writer's bounded PUT with an ENFORCED wall deadline (the PUT is raced against an
 * independent timer; a port that ignores the abort cannot hang the caller). On the timeout,
 * or any error the caller does not classify as a definite refusal, `onUnknown` durably keeps
 * the invocation dispatched/unknown BEFORE the failure propagates; a definite refusal calls
 * `onRefused`. A late result or rejection after the deadline is contained and reported to
 * `onLate` only (its own throws/rejections contained too): it never resolves the invocation,
 * is never treated as proof of no commit, and the PUT is never retried here.
 */
export async function boundedPut<T>(
  put: (signal: AbortSignal) => Promise<T>,
  hooks: { onUnknown: () => Promise<void>; onRefused?: () => Promise<void>; isRefusal?: (error: unknown) => boolean; onLate?: (outcome: { ok: true; value: T } | { ok: false; error: unknown }) => unknown },
  timeoutMs = 120_000,
): Promise<T> {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 900_000) throw coded("timeout_bounds");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const attempt = Promise.resolve().then(() => put(controller.signal));
  const deadline = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new PutTimeout()), timeoutMs); });
  try {
    return await Promise.race([attempt, deadline]);
  } catch (error) {
    if (error instanceof PutTimeout) {
      contained(() => controller.abort(error));
      attempt.then(
        (value) => contained(() => hooks.onLate?.({ ok: true, value })),
        (e) => contained(() => hooks.onLate?.({ ok: false, error: e })),
      );
      await hooks.onUnknown();
      throw error;
    }
    if (hooks.isRefusal?.(error) && hooks.onRefused) await hooks.onRefused();
    else await hooks.onUnknown();
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
