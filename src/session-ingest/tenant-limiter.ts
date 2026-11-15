/**
 * Per-tenant upload limits for INGEST_LEDGER=s3 (decision 0005 amendment): the S3 ledger has no slot table, so
 * this process counts its own in-flight admissions. One limiter per ingest-edge replica (selfhostS3Ledger), keyed
 * by org and by (org, user), capping CONCURRENT in-flight admissions at INGEST_MAX_PER_USER / INGEST_MAX_PER_ORG
 * (defaults 4 / 32, the SQL lease's). The caps are PER REPLICA: N replicas admit up to N x the cap for one
 * tenant. No database, no network: an acquire or a release never waits on anything and never fails for a
 * store fault, so Supabase can never block or bottleneck an upload through it.
 *
 * Semantics are the SQL lease's (`ingest_acquire_slot`, supabase/migrations/20261104022000):
 *   - a lease per holder (deliver.ts: one per request or admission attempt), counted per user and per org;
 *   - the same holder re-acquiring refreshes its lease instead of taking a second one;
 *   - a refusal carries relayhistory's jittered Retry-After, 2-10 s (DELIVERY_BUSY_RETRY_AFTER);
 *   - a lease expires by TTL (deliver deps: 90 s), so a release lost to a hang frees itself.
 * Unlike the SQL lease it is exact, not soft: acquire is synchronous, so two racing acquires cannot both pass.
 * deliver.ts releases in `finally` on every path (success, refusal, throw); the TTL covers a promise that never
 * settles. Memory is bounded by the leases alive: expired ones are swept on every acquire of their org and in a
 * full sweep at most once a TTL floor, and an org with no lease has no entry.
 */
import type { LedgerTenant, SlotRequest, SlotResult } from "./ledger.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import { DELIVERY_BUSY_RETRY_AFTER } from "./vendor/delivery/delivery-contracts.ts";

export interface TenantLimiter {
  acquire(request: SlotRequest): SlotResult;
  release(tenant: LedgerTenant, holder: string): void;
  /** Live (unexpired) leases: for the org, and for one of its users when given. Tests and diagnostics. */
  inFlight(orgId: string, userId?: string): number;
}

export interface TenantLimiterOptions {
  nowMs?: () => number;
  random?: () => number;
}

interface Lease { userId: string; expiresAt: number }

const FULL_SWEEP_MS = 60_000;
const int = (v: unknown, min: number, max: number) => typeof v === "number" && Number.isSafeInteger(v) && v >= min && v <= max;

export function tenantLimiter(opts: TenantLimiterOptions = {}): TenantLimiter {
  const now = opts.nowMs ?? Date.now;
  const random = opts.random ?? Math.random;
  /** org -> (user \0 holder) -> lease. */
  const orgs = new Map<string, Map<string, Lease>>();
  let lastFullSweep = now();

  const sweep = (orgId: string, leases: Map<string, Lease>, at: number) => {
    for (const [k, l] of leases) if (l.expiresAt <= at) leases.delete(k);
    if (leases.size === 0) orgs.delete(orgId);
  };
  const retryAfter = () => {
    const { min, max } = DELIVERY_BUSY_RETRY_AFTER;
    const r = random();
    return min + Math.floor((Number.isFinite(r) && r >= 0 && r < 1 ? r : 0) * (max - min + 1));
  };

  return {
    acquire(request) {
      const { tenant, holder, maxPerUser, maxPerOrg, ttlSeconds } = request;
      // The SQL function's bounds; a bad request is a coded ledger fault (503), never an unlimited pass.
      if (!tenant || typeof tenant.orgId !== "string" || !tenant.orgId || typeof tenant.userId !== "string" || !tenant.userId
        || typeof holder !== "string" || !holder || !int(maxPerUser, 1, 1000) || !int(maxPerOrg, 1, 100_000) || !int(ttlSeconds, 5, 900))
        throw new LedgerUnavailableError("ledger_input_refused");
      const at = now();
      if (at - lastFullSweep >= FULL_SWEEP_MS) {
        lastFullSweep = at;
        for (const [org, leases] of orgs) sweep(org, leases, at);
      }
      const leases = orgs.get(tenant.orgId) ?? new Map<string, Lease>();
      sweep(tenant.orgId, leases, at);
      const key = `${tenant.userId}\u0000${holder}`;
      const lease = { userId: tenant.userId, expiresAt: at + ttlSeconds * 1000 };
      if (leases.has(key)) {
        leases.set(key, lease);
        orgs.set(tenant.orgId, leases);
        return { ok: true };
      }
      let user = 0;
      for (const l of leases.values()) if (l.userId === tenant.userId) user++;
      if (user >= maxPerUser || leases.size >= maxPerOrg) return { ok: false, retryAfterSeconds: retryAfter() };
      leases.set(key, lease);
      orgs.set(tenant.orgId, leases);
      return { ok: true };
    },
    release(tenant, holder) {
      const leases = orgs.get(tenant?.orgId);
      if (!leases) return;
      leases.delete(`${tenant.userId}\u0000${holder}`);
      if (leases.size === 0) orgs.delete(tenant.orgId);
    },
    inFlight(orgId, userId) {
      const leases = orgs.get(orgId);
      if (!leases) return 0;
      const at = now();
      let n = 0;
      for (const l of leases.values()) if (l.expiresAt > at && (userId === undefined || l.userId === userId)) n++;
      return n;
    },
  };
}
