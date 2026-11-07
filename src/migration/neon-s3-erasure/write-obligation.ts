/**
 * THE shared unresolved-write obligation primitive (lead rulings 2026-11-07: "no unresolved-PUT
 * proof", shared-unknown-put-nonterminal, and the 3e8b914d review). F1 (analysis result /
 * backfill PUTs), lane A (E3) and Babysitter retention compose this one implementation; each
 * lane keeps its OWN obligation table behind `ObligationStore` / `ObligationWriter`.
 *
 * Model: one obligation per object KEY, holding the full set of DISPATCHED invocations at that
 * key. Each invocation (one PUT attempt) gets a durable id recorded BEFORE it is sent, with the
 * attribution evidence the provider will list back: the body's expected ETag (the MD5 of a
 * single-part, non-KMS PutObject body). An invocation's outcome is one of
 *   dispatched  sent; outcome unknown (lost/timed-out response, crash): it MAY still commit;
 *   acked       its OWN confirmed response, with the exact version id it created;
 *   refused     a definite provider refusal of THAT request (e.g. 412 If-None-Match): no write.
 * Terminal evidence clears exactly the invocation it belongs to, never another:
 *   refused                                     -> resolved;
 *   acked v, v owned as its own publish         -> resolved (the version stays);
 *   acked v / observed v, v NOT owned           -> resolved once a fresh strict, COMPLETE
 *                                                  inventory shows that exact version absent
 *                                                  (version ids are never reused);
 *   dispatched, nothing attributed              -> UNRESOLVED (nonterminal), however old.
 * Attribution: a listed, unowned, non-marker version whose ETag equals a dispatched
 * invocation's expected ETag is recorded on ONE such invocation (`observe`, durable BEFORE that
 * version's delete); each version is attributed at most once, so two attempts with one visible
 * version leave the other unresolved. Another owner's success, a different stray, a zero
 * inventory, time, lease expiry or a generation change are never terminal for an invocation.
 * A version that cannot be attributed (no expected ETag recorded, or a provider whose ETag is
 * not the body MD5) is still erased if unowned, but resolves nothing.
 *
 * NONTERMINAL UNKNOWN (shared-unknown-put-nonterminal ruling): without its own terminal
 * evidence an invocation stays unresolved indefinitely, re-swept on the bounded backoff
 * (capped at 1 day, so a stuck key costs one strict listing per day). It is never marked
 * erased by a timeout or any number of zero inventories, and the lane never reports the scope
 * fully erased while it remains. There is NO credential-rotation / bucket-policy / deny
 * observation terminal shortcut: preventing new requests does not prove an already-dispatched
 * one cannot commit; an error from a different later request never resolves an earlier one.
 *
 * Atomicity: every sweep-side mutation is a compare-and-set on the row version (`rev`) the
 * sweep loaded; a changed row (a new invocation, a publish, a new owner) refuses the mutation
 * and the reconcile returns "refused" without retiring anything. The lane's publish path must
 * refuse to adopt a version an obligation has OBSERVED for deletion.
 *
 * Reconcile (`reconcileObligation`) always goes through the shared strict paged eraser
 * (eraser.ts eraseStep / verifyErased): every unowned version at the key is erased (cleanup),
 * by exact version id, successor-safe (owned versions are never selected); the checkpoint scope
 * binds the lane identity, key, owned set and invocation set; authority (rev) is re-checked
 * before every page's deletes; an owner with an unrecorded version id blocks every erase.
 */
import { createHash } from "node:crypto";
import type { EraseBudget, EraseCheckpoint, ListedVersion, PagedEraser } from "./eraser.ts";

export type InvocationOutcome = "dispatched" | "acked" | "refused";

export interface WriteInvocation {
  /** Durable id recorded BEFORE the PUT is sent (never reused). */
  id: string;
  outcome: InvocationOutcome;
  /** acked: the version id its own response confirmed. */
  versionId?: string;
  /** dispatched: the version a sweep attributed to it (recorded before deleting it). */
  observedVersion?: string;
  /** The body's expected ETag (lowercase hex MD5, no quotes), for attribution. */
  expectedEtag?: string;
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
 * The lane's sweep-side adapter over its own table. Every mutation is a compare-and-set on
 * `rev` and returns the new rev, or { ok: false } when the row changed (or is gone). Each must
 * be durable when it returns (commit on its own) because remote deletes follow.
 */
export interface ObligationStore {
  load(): Promise<ObligationState | null>;
  /** Attribute `versionId` to dispatched invocation `invocationId` (it becomes unadoptable). */
  observe(rev: number, invocationId: string, versionId: string): Promise<Cas>;
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
 *   dispatch  BEFORE the PUT, with the writer's lease/generation: a NEW invocation id and the
 *             body's expected ETag (never reuse an id);
 *   acked     its own confirmed response: the exact version id (resolves only this invocation);
 *   refused   a definite refusal of this very request (no write happened);
 *   publish   the terminal success of an owner: records the owned version in the same tx; it
 *             must refuse a version any obligation has observed for deletion.
 * A timeout / abort / lost response leaves the invocation dispatched (unknown); nothing else
 * may downgrade or clear it.
 */
export interface ObligationWriter<Tx> {
  dispatch(tx: Tx, key: string, identity: readonly unknown[], invocationId: string, expectedEtag: string | null): Promise<void>;
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

/** The body ETag a single-part, non-KMS PutObject reports (lowercase hex MD5). */
export function expectedEtag(body: Uint8Array): string {
  return createHash("md5").update(body).digest("hex");
}

/** The checkpoint scope: lane identity, key, owned set and the invocation set. */
export function obligationScope(s: Pick<ObligationState, "key" | "identity" | "ownedVersions" | "invocations">): string {
  const owned = [...s.ownedVersions].sort();
  const inv = [...s.invocations].map((i) => [i.id, i.outcome, i.versionId ?? null, i.observedVersion ?? null]).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
  return "write-obligation:" + createHash("sha256").update(JSON.stringify(["write-obligation-v2", s.identity, s.key, owned, inv])).digest("hex");
}

/**
 * Which invocations have terminal evidence, given a COMPLETE strict inventory's versions at the
 * key (`present`) and the owned set. Pure; exported for the lanes' own SQL-side checks/tests.
 */
export function terminalInvocations(invocations: readonly WriteInvocation[], owned: ReadonlySet<string>, present: ReadonlySet<string> | null): string[] {
  const out: string[] = [];
  for (const i of invocations) {
    if (i.outcome === "refused") { out.push(i.id); continue; }
    const v = i.outcome === "acked" ? i.versionId : i.observedVersion;
    if (!v) continue;                                   // dispatched, nothing attributed: nonterminal
    if (owned.has(v)) { if (i.outcome === "acked") out.push(i.id); continue; }   // its own publish
    if (present !== null && !present.has(v)) out.push(i.id);                    // that exact version is gone
  }
  return out;
}

const coded = (code: string) => Object.assign(new Error(`write obligation: ${code}`), { code });
const normEtag = (e: string | undefined) => (e ?? "").replace(/^"|"$/g, "").toLowerCase();

/**
 * One bounded reconcile of one key's obligation. Returns "incomplete" (checkpoint saved),
 * "rescheduled" (not provably complete), "retired", "refused" (a compare-and-set lost: the row
 * changed and survives) or "gone". Throws on a failed step (the obligation, its checkpoint and
 * every durable observation stay).
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
  // Attribution state: each listed version is attributed to at most one dispatched invocation.
  const pendingAttribution = new Map<string, WriteInvocation[]>();
  for (const i of first.invocations)
    if (i.outcome === "dispatched" && !i.observedVersion && i.expectedEtag) {
      const e = normEtag(i.expectedEtag);
      pendingAttribution.set(e, [...(pendingAttribution.get(e) ?? []), i]);
    }
  const attributed = new Set(first.invocations.map((i) => i.observedVersion).filter((v): v is string => !!v));
  const unowned = (v: ListedVersion) => v.key === key && !owned.has(v.versionId);
  const beforeDelete = async (doomed: readonly ListedVersion[]) => {
    for (const v of doomed) {
      if (v.deleteMarker || attributed.has(v.versionId)) continue;
      const queue = pendingAttribution.get(normEtag(v.etag));
      const inv = queue?.shift();
      if (!inv) continue;
      if (!(await cas(store.observe(rev, inv.id, v.versionId)))) { lost = true; throw coded("authority_changed"); }
      attributed.add(v.versionId);
    }
  };
  const checkpoint = first.checkpoint && first.checkpoint.scope === scope ? first.checkpoint : null;
  let step;
  try {
    step = await eraser.eraseStep(prefix, { scope, checkpoint, select: unowned, authorize, beforeDelete, ...(opts.budget ? { budget: opts.budget } : {}), ...(opts.now ? { now: opts.now } : {}) });
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
      authorize, ...(opts.budget ? { budget: opts.budget } : {}), ...(opts.now ? { now: opts.now } : {}) });
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

/**
 * The writer's bounded PUT with an ENFORCED wall deadline (the PUT is raced against an
 * independent timer; a port that ignores the abort cannot hang the caller). On the timeout,
 * or any error the caller does not classify as a definite refusal, `onUnknown` durably keeps
 * the invocation dispatched/unknown BEFORE the failure propagates; a definite refusal calls
 * `onRefused`. A late result or rejection after the deadline is contained and reported to
 * `onLate` only: it is never treated as proof of no commit, and the PUT is never retried here.
 */
export async function boundedPut<T>(
  put: (signal: AbortSignal) => Promise<T>,
  hooks: { onUnknown: () => Promise<void>; onRefused?: () => Promise<void>; isRefusal?: (error: unknown) => boolean; onLate?: (outcome: { ok: true; value: T } | { ok: false; error: unknown }) => void },
  timeoutMs = 120_000,
): Promise<T> {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 900_000) throw coded("timeout_bounds");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let settled = false;
  const attempt = Promise.resolve().then(() => put(controller.signal));
  const deadline = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new PutTimeout()), timeoutMs); });
  try {
    const value = await Promise.race([attempt, deadline]);
    settled = true;
    return value;
  } catch (error) {
    settled = true;
    if (error instanceof PutTimeout) {
      controller.abort(error);
      attempt.then((value) => hooks.onLate?.({ ok: true, value }), (e) => hooks.onLate?.({ ok: false, error: e }));
      await hooks.onUnknown();
      throw error;
    }
    if (hooks.isRefusal?.(error) && hooks.onRefused) await hooks.onRefused();
    else await hooks.onUnknown();
    throw error;
  } finally {
    clearTimeout(timer);
    void settled;
  }
}
