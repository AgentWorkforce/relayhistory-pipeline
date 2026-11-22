/**
 * INGEST_INFLIGHT_LEASE: a per-isolate in-flight lease for HTTP sync deliveries, keyed by the signed
 * (orgId, workspaceId, userId). Taken in app.ts acceptBatch before the first call that can take a ledger or
 * fence lock and released in `finally`, so a second overlapping delivery for the same key can be refused
 * cheaply (429 delivery_busy, lock `tenant_inflight`) instead of queueing behind the first one's fence row.
 *
 * Modes (inflightLeaseMode): unset / empty / anything else = off (the app never calls this module);
 * `observe` = every delivery is tracked and nothing is refused (the app logs a would-refuse line);
 * `on` = at most one holder per key, a second acquire is refused.
 *
 * Scope and limits:
 *   - PER ISOLATE. Each Worker isolate has its own map, so two overlapping deliveries that land in different
 *     isolates are not seen. Observe mode measures the share it does see; it is never an exact global limit.
 *   - No timers, no IO: the clock is passed in, and acquire / release never wait on anything.
 *   - A holder expires after INFLIGHT_LEASE_TTL_MS, so a release lost to a promise that never settles frees
 *     itself. Release is token-guarded: a late release after expiry and re-acquire never frees the newer holder.
 *   - When the key already has live holders, acquire / track also report the age of the oldest one
 *     (oldestHolderAgeMs, from its expiry and the TTL), so a holder whose release never ran (its age keeps
 *     growing towards the TTL) can be told apart from a real overlap (a young holder).
 *   - Each holder also remembers the origin id it was taken with, only to report how many of the live holders
 *     share the arriving origin (sameOriginHolders: observe data for whether a per-origin key would refuse less).
 *     The origin is not part of the key: holders of one key are still counted and refused together.
 *   - Entries are capped (INFLIGHT_LEASE_MAX_ENTRIES). Expired holders of the acquired key are swept on every
 *     acquire, and all expired holders are swept when the map is full. A map still full of live holders FAILS
 *     OPEN (the delivery proceeds untracked) and is counted, so the lease can never refuse for its own memory.
 */
import { DELIVERY_BUSY_RETRY_AFTER } from "./vendor/delivery/delivery-contracts.ts";

export type InflightLeaseMode = "observe" | "on";

/** The DeliveryBusyError lock name of a lease refusal: a pre-ledger refusal, never a database lock alert. */
export const INFLIGHT_LEASE_LOCK = "tenant_inflight";

/**
 * Holder expiry: 90 s, the SQL slot lease TTL (worker.ts deliverDepsFor, "past the Worker's wall-clock
 * budget") and the self-host tenant limiter's. It is a backstop for a release that never ran (for example a
 * request cancelled by its caller before `finally`), not a claim about how long a delivery takes: a delivery
 * runs several statements (the role's statement_timeout is 5 s each) plus retries and lock waits, so its own
 * bound is a multiple of that timeout.
 */
export const INFLIGHT_LEASE_TTL_MS = 90_000;

/**
 * Most holders one isolate tracks. One holder is one delivery in flight in this isolate, so a full map of live
 * holders is far above any real concurrency; the cap only bounds memory if releases were lost faster than they
 * expire. Worst case per holder: a few hundred bytes plus its origin id (a parsed batch identifier, at most 512
 * bytes), so a full map is a few MB per isolate.
 */
export const INFLIGHT_LEASE_MAX_ENTRIES = 10_000;

export interface InflightLeaseKey { orgId: string; workspaceId: string; userId: string }

export interface InflightGrant {
  ok: true;
  /** Live holders of the same key when this one was taken (observe: > 0 means on would have refused). */
  holdersBefore: number;
  /** Present only when holdersBefore > 0: age in ms of the oldest live holder the key already had. */
  oldestHolderAgeMs?: number;
  /** Present only when holdersBefore > 0: how many of those live holders were taken with this origin id (0 when
   *  the arriving origin id is undefined; a holder taken without an origin never matches). */
  sameOriginHolders?: number;
  /** The map was full of live holders: proceeded untracked. */
  failOpen: boolean;
  /** Idempotent; frees only this grant's own holder. */
  release(): void;
}
export interface InflightRefusal {
  ok: false;
  holdersBefore: number;
  /** Age in ms of the oldest live holder of the key (holdersBefore is always > 0 here). */
  oldestHolderAgeMs: number;
  /** How many of those live holders were taken with this origin id (see InflightGrant.sameOriginHolders). */
  sameOriginHolders: number;
}

export interface InflightLeaseCounters { granted: number; refused: number; failOpen: number }

export interface InflightLease {
  /** At most one holder per key; a second live holder is refused (the app's `on` for delivery batches). */
  acquire(key: InflightLeaseKey, nowMs: number, originId?: string): InflightGrant | InflightRefusal;
  /** Always granted (counted), reporting how many live holders the key already had (the app's `observe`, and
   *  the turns route in `on`). */
  track(key: InflightLeaseKey, nowMs: number, originId?: string): InflightGrant;
  /** Live holders of `key` at `nowMs`. Tests and diagnostics. */
  holders(key: InflightLeaseKey, nowMs: number): number;
  /** Tracked holders, live or not yet swept. */
  entries(): number;
  readonly counters: Readonly<InflightLeaseCounters>;
}

export interface InflightLeaseOptions { ttlMs?: number; maxEntries?: number }

/** Unset, empty or anything else (after trim, any case) is off: undefined. */
export function inflightLeaseMode(value: string | undefined): InflightLeaseMode | undefined {
  const v = (value ?? "").trim().toLowerCase();
  return v === "observe" || v === "on" ? v : undefined;
}

/** The refusal's Retry-After in seconds, jittered inside DELIVERY_BUSY_RETRY_AFTER (2-10). */
export function inflightRetryAfter(random: () => number): number {
  const { min, max } = DELIVERY_BUSY_RETRY_AFTER;
  const r = random();
  return min + Math.floor((Number.isFinite(r) && r >= 0 && r < 1 ? r : 0) * (max - min + 1));
}

/** Collision-free: a JSON array of three strings is injective, whatever characters the ids contain. */
export function inflightLeaseKey(key: InflightLeaseKey): string {
  return JSON.stringify([key.orgId, key.workspaceId, key.userId]);
}

const NOOP = () => {};

/** One holder: its expiry and the origin id it was taken with (reported as sameOriginHolders, never keyed on). */
interface Holder { expiresAt: number; originId?: string }
/** A key's live holders at a moment: their count and, when count > 0, the oldest's age and the same-origin count. */
interface Live { count: number; oldestHolderAgeMs?: number; sameOriginHolders?: number }

export function createInflightLease(opts: InflightLeaseOptions = {}): InflightLease {
  const ttlMs = opts.ttlMs ?? INFLIGHT_LEASE_TTL_MS;
  const maxEntries = opts.maxEntries ?? INFLIGHT_LEASE_MAX_ENTRIES;
  if (!Number.isSafeInteger(ttlMs) || ttlMs < 1) throw new Error("inflight lease: ttlMs must be a positive integer");
  if (!Number.isSafeInteger(maxEntries) || maxEntries < 1) throw new Error("inflight lease: maxEntries must be a positive integer");
  /** key -> token -> holder (expiresAt in ms, and the origin id it was taken with). A key with no holder has no entry. */
  const keys = new Map<string, Map<number, Holder>>();
  let size = 0;
  let nextToken = 1;
  const counters: InflightLeaseCounters = { granted: 0, refused: 0, failOpen: 0 };

  const prune = (k: string, at: number): Map<number, Holder> | undefined => {
    const holders = keys.get(k);
    if (!holders) return undefined;
    for (const [token, { expiresAt }] of holders) if (expiresAt <= at) { holders.delete(token); size--; }
    if (holders.size === 0) { keys.delete(k); return undefined; }
    return holders;
  };
  const sweepAll = (at: number) => { for (const k of [...keys.keys()]) prune(k, at); };

  /** Live holders of `k` at `at` and, when there are any, the age of the oldest (expiresAt = taken + ttlMs) and
   *  how many were taken with `originId` (strict equality; an undefined arriving origin matches none). */
  const live = (k: string, at: number, originId: string | undefined): Live => {
    const holders = prune(k, at);
    if (!holders) return { count: 0 };
    let oldest = Infinity;
    let same = 0;
    for (const h of holders.values()) {
      if (h.expiresAt < oldest) oldest = h.expiresAt;
      if (originId !== undefined && h.originId === originId) same++;
    }
    return { count: holders.size, oldestHolderAgeMs: Math.max(0, at - (oldest - ttlMs)), sameOriginHolders: same };
  };

  const grant = (k: string, at: number, originId: string | undefined, before: Live): InflightGrant => {
    const holdersBefore = before.count;
    const age = before.oldestHolderAgeMs === undefined ? {} : { oldestHolderAgeMs: before.oldestHolderAgeMs, sameOriginHolders: before.sameOriginHolders! };
    if (size >= maxEntries) sweepAll(at);
    if (size >= maxEntries) {
      counters.failOpen++;
      return { ok: true, holdersBefore, ...age, failOpen: true, release: NOOP };
    }
    const token = nextToken++;
    let holders = keys.get(k);
    if (!holders) { holders = new Map(); keys.set(k, holders); }
    holders.set(token, originId === undefined ? { expiresAt: at + ttlMs } : { expiresAt: at + ttlMs, originId });
    size++;
    counters.granted++;
    let released = false;
    return {
      ok: true,
      holdersBefore,
      ...age,
      failOpen: false,
      release() {
        if (released) return;
        released = true;
        // Token-guarded: after expiry the token is gone (swept) and a newer holder has its own token.
        const current = keys.get(k);
        if (current?.delete(token)) {
          size--;
          if (current.size === 0) keys.delete(k);
        }
      },
    };
  };

  return {
    acquire(key, nowMs, originId) {
      const k = inflightLeaseKey(key);
      const before = live(k, nowMs, originId);
      if (before.count > 0) {
        counters.refused++;
        return { ok: false, holdersBefore: before.count, oldestHolderAgeMs: before.oldestHolderAgeMs!, sameOriginHolders: before.sameOriginHolders! };
      }
      return grant(k, nowMs, originId, before);
    },
    track(key, nowMs, originId) {
      const k = inflightLeaseKey(key);
      return grant(k, nowMs, originId, live(k, nowMs, originId));
    },
    holders(key, nowMs) {
      return prune(inflightLeaseKey(key), nowMs)?.size ?? 0;
    },
    entries: () => size,
    counters,
  };
}
