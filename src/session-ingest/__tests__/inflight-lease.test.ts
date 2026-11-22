/**
 * INGEST_INFLIGHT_LEASE (default off): the per-isolate in-flight lease taken in acceptBatch before the first
 * lock-capable call. Module: one holder per (org, workspace, user), token-guarded idempotent release, expiry,
 * collision-free keys, capped entries that fail open. App: off unchanged, observe logs one would-refuse line,
 * on refuses an overlapping delivery with a 429 before any ledger, consent or turn-register call, the turns
 * route is tracked (never refused) in on, the oldest holder's age and the same-origin overlap label ride on
 * would_refuse / refused lines (and survive the production log filter, with no origin id), the lease is released
 * on every exit, and the queued ACK is never leased.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { logLine } from "../../shared/log.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER } from "../async-admission.ts";
import {
  createInflightLease, INFLIGHT_LEASE_LOCK, INFLIGHT_LEASE_MAX_ENTRIES, INFLIGHT_LEASE_TTL_MS, inflightLeaseKey, inflightLeaseMode, inflightRetryAfter,
  type InflightLease, type InflightLeaseMode,
} from "../inflight-lease.ts";
import type { ClaimInput, ClaimResult } from "../ledger.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../turns.ts";
import { DELIVERY_BUSY_RETRY_AFTER } from "../vendor/delivery/delivery-contracts.ts";
import { batch, MemoryLedger, record, scratchStore, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const T0 = 1_800_000_000_000;

describe("inflight lease module", () => {
  test("mode parsing: observe / on (trimmed, any case); everything else is off", () => {
    for (const v of ["observe", "OBSERVE", " Observe\t"]) expect(inflightLeaseMode(v)).toBe("observe");
    for (const v of ["on", "ON", " on "]) expect(inflightLeaseMode(v)).toBe("on");
    for (const v of [undefined, "", " ", "off", "1", "true", "yes", "o n", "enforce"]) expect(inflightLeaseMode(v)).toBeUndefined();
  });

  test("acquire, refuse while held, release frees, release is idempotent", () => {
    const lease = createInflightLease();
    const a = lease.acquire(TENANT, T0);
    expect(a.ok).toBe(true);
    expect(lease.holders(TENANT, T0)).toBe(1);
    const b = lease.acquire(TENANT, T0 + 1);
    expect(b).toEqual({ ok: false, holdersBefore: 1, oldestHolderAgeMs: 1, sameOriginHolders: 0 });
    if (!a.ok) throw new Error("unreachable");
    a.release();
    expect(lease.holders(TENANT, T0 + 2)).toBe(0);
    expect(lease.entries()).toBe(0);
    const c = lease.acquire(TENANT, T0 + 3);
    expect(c.ok).toBe(true);
    a.release(); // idempotent: must not free c
    expect(lease.holders(TENANT, T0 + 4)).toBe(1);
    expect(lease.counters).toEqual({ granted: 2, refused: 1, failOpen: 0 });
  });

  test("expiry frees a holder whose release never ran; its late release never frees the newer holder", () => {
    const lease = createInflightLease({ ttlMs: 1_000 });
    const old = lease.acquire(TENANT, T0);
    expect(lease.acquire(TENANT, T0 + 999).ok).toBe(false);
    const fresh = lease.acquire(TENANT, T0 + 1_000);
    expect(fresh.ok).toBe(true);
    if (!old.ok) throw new Error("unreachable");
    old.release();
    expect(lease.holders(TENANT, T0 + 1_001)).toBe(1);
    expect(lease.acquire(TENANT, T0 + 1_002).ok).toBe(false);
  });

  test("default expiry is the 90 s slot lease bound", () => {
    expect(INFLIGHT_LEASE_TTL_MS).toBe(90_000);
    const lease = createInflightLease();
    lease.acquire(TENANT, T0);
    expect(lease.holders(TENANT, T0 + 89_999)).toBe(1);
    expect(lease.holders(TENANT, T0 + 90_000)).toBe(0);
  });

  test("key isolation: workspace and user are separate; an empty workspace is its own value; separators cannot collide", () => {
    const lease = createInflightLease();
    expect(lease.acquire(TENANT, T0).ok).toBe(true);
    expect(lease.acquire({ ...TENANT, userId: "user-2" }, T0).ok).toBe(true);
    expect(lease.acquire({ ...TENANT, workspaceId: "ws-b" }, T0).ok).toBe(true);
    expect(lease.acquire({ ...TENANT, workspaceId: "" }, T0).ok).toBe(true);
    expect(lease.acquire({ ...TENANT, workspaceId: "" }, T0).ok).toBe(false);
    // Joined with a separator these would be equal; the JSON key keeps them apart.
    const x = { orgId: "a", workspaceId: "b\u001fc", userId: "d" };
    const y = { orgId: "a\u001fb", workspaceId: "c", userId: "d" };
    const z = { orgId: "a", workspaceId: "", userId: "b\u001fc\u001fd" };
    const w = { orgId: 'a","b', workspaceId: "c", userId: "d" };
    const keys = [x, y, z, w, { orgId: "a", workspaceId: '","b', userId: "c\",\"d" }].map(inflightLeaseKey);
    expect(new Set(keys).size).toBe(keys.length);
    for (const k of [x, y, z, w]) expect(lease.acquire(k, T0).ok).toBe(true);
  });

  test("cap: a full map sweeps expired holders first; full of live holders it fails open and counts it", () => {
    expect(INFLIGHT_LEASE_MAX_ENTRIES).toBe(10_000);
    const lease = createInflightLease({ ttlMs: 100, maxEntries: 2 });
    expect(lease.acquire({ ...TENANT, userId: "u1" }, T0).ok).toBe(true);
    expect(lease.acquire({ ...TENANT, userId: "u2" }, T0 + 50).ok).toBe(true);
    const open = lease.acquire({ ...TENANT, userId: "u3" }, T0 + 60);
    expect(open).toMatchObject({ ok: true, failOpen: true, holdersBefore: 0 });
    expect(lease.entries()).toBe(2);
    expect(lease.holders({ ...TENANT, userId: "u3" }, T0 + 60)).toBe(0);
    if (open.ok) open.release(); // no-op
    expect(lease.entries()).toBe(2);
    expect(lease.counters.failOpen).toBe(1);
    // u1 expired at T0+100: the sweep on a full map makes room.
    const swept = lease.acquire({ ...TENANT, userId: "u3" }, T0 + 100);
    expect(swept).toMatchObject({ ok: true, failOpen: false });
    expect(lease.entries()).toBe(2);
    // observe (track) fails open the same way.
    expect(lease.track({ ...TENANT, userId: "u4" }, T0 + 101)).toMatchObject({ ok: true, failOpen: true });
    expect(lease.counters.failOpen).toBe(2);
  });

  test("track never refuses and reports the live holders already present", () => {
    const lease = createInflightLease();
    const a = lease.track(TENANT, T0);
    const b = lease.track(TENANT, T0);
    expect(a.holdersBefore).toBe(0);
    expect(b.holdersBefore).toBe(1);
    a.release();
    b.release();
    expect(lease.entries()).toBe(0);
    expect(lease.counters).toEqual({ granted: 2, refused: 0, failOpen: 0 });
  });

  test("oldest holder age: on refusals and on grants with holders, from the clock; absent with no holder", () => {
    const lease = createInflightLease({ ttlMs: 10_000 });
    const first = lease.acquire(TENANT, T0);
    expect(first.ok).toBe(true);
    expect(Object.hasOwn(first, "oldestHolderAgeMs")).toBe(false);
    expect(lease.acquire(TENANT, T0 + 1_234)).toEqual({ ok: false, holdersBefore: 1, oldestHolderAgeMs: 1_234, sameOriginHolders: 0 });
    expect(lease.acquire(TENANT, T0 + 9_999)).toEqual({ ok: false, holdersBefore: 1, oldestHolderAgeMs: 9_999, sameOriginHolders: 0 });
    // track: the oldest of several holders, not the newest.
    const t1 = lease.track(TENANT, T0 + 500);
    expect(t1).toMatchObject({ holdersBefore: 1, oldestHolderAgeMs: 500 });
    const t2 = lease.track(TENANT, T0 + 2_000);
    expect(t2).toMatchObject({ holdersBefore: 2, oldestHolderAgeMs: 2_000 });
    // The first holder expires at T0+10000: the oldest live one is then t1 (taken at T0+500).
    expect(lease.track(TENANT, T0 + 10_000)).toMatchObject({ holdersBefore: 2, oldestHolderAgeMs: 9_500 });
    // A key with no holder reports no age.
    const other = lease.track({ ...TENANT, userId: "user-2" }, T0 + 3_000);
    expect(other.holdersBefore).toBe(0);
    expect(Object.hasOwn(other, "oldestHolderAgeMs")).toBe(false);
  });

  test("oldest holder age is clamped at 0 when the clock runs backwards", () => {
    const lease = createInflightLease({ ttlMs: 10_000 });
    expect(lease.acquire(TENANT, T0).ok).toBe(true);
    expect(lease.track(TENANT, T0 - 5)).toMatchObject({ holdersBefore: 1, oldestHolderAgeMs: 0 });
    expect(lease.acquire(TENANT, T0 - 5)).toEqual({ ok: false, holdersBefore: 2, oldestHolderAgeMs: 0, sameOriginHolders: 0 });
  });

  test("same origin: an overlap with a holder of the same origin counts it; a different origin is still refused by acquire", () => {
    const same = createInflightLease();
    expect(same.acquire(TENANT, T0, "origin-a").ok).toBe(true);
    expect(same.track(TENANT, T0 + 1, "origin-a")).toMatchObject({ ok: true, holdersBefore: 1, sameOriginHolders: 1 });
    expect(same.acquire(TENANT, T0 + 2, "origin-a")).toEqual({ ok: false, holdersBefore: 2, oldestHolderAgeMs: 2, sameOriginHolders: 2 });

    const other = createInflightLease();
    expect(other.acquire(TENANT, T0, "origin-a").ok).toBe(true);
    // The key is per user: a different origin of the same user is refused exactly as before, with no same-origin holder.
    expect(other.acquire(TENANT, T0 + 1, "origin-b")).toEqual({ ok: false, holdersBefore: 1, oldestHolderAgeMs: 1, sameOriginHolders: 0 });
    expect(other.track(TENANT, T0 + 1, "origin-b")).toMatchObject({ ok: true, holdersBefore: 1, sameOriginHolders: 0 });
    expect(other.counters).toEqual({ granted: 2, refused: 1, failOpen: 0 });
  });

  test("same origin: mixed holders count only the matching ones; strict equality", () => {
    const lease = createInflightLease();
    lease.track(TENANT, T0, "origin-a");
    lease.track(TENANT, T0, "origin-b");
    // A third origin: two live holders, none of them its own.
    expect(lease.acquire(TENANT, T0 + 1, "origin-c")).toEqual({ ok: false, holdersBefore: 2, oldestHolderAgeMs: 1, sameOriginHolders: 0 });
    expect(lease.acquire(TENANT, T0 + 1, "origin-a")).toEqual({ ok: false, holdersBefore: 2, oldestHolderAgeMs: 1, sameOriginHolders: 1 });
    expect(lease.acquire(TENANT, T0 + 1, "ORIGIN-A")).toMatchObject({ ok: false, holdersBefore: 2, sameOriginHolders: 0 });
    expect(lease.track(TENANT, T0 + 1, "origin-a")).toMatchObject({ ok: true, holdersBefore: 2, sameOriginHolders: 1 });
  });

  test("same origin: an arrival without an origin matches nothing, and a holder taken without one never matches", () => {
    const lease = createInflightLease();
    lease.track(TENANT, T0);
    expect(lease.track(TENANT, T0 + 1)).toMatchObject({ holdersBefore: 1, sameOriginHolders: 0 });
    expect(lease.track(TENANT, T0 + 2, "origin-a")).toMatchObject({ holdersBefore: 2, sameOriginHolders: 0 });
    expect(lease.acquire(TENANT, T0 + 3)).toMatchObject({ ok: false, holdersBefore: 3, sameOriginHolders: 0 });
    expect(lease.acquire(TENANT, T0 + 3, "origin-a")).toMatchObject({ ok: false, holdersBefore: 3, sameOriginHolders: 1 });
  });

  test("same origin: an expired or released same-origin holder no longer counts while a different-origin holder stays", () => {
    const expiring = createInflightLease({ ttlMs: 1_000 });
    expiring.track(TENANT, T0, "origin-a");
    expiring.track(TENANT, T0 + 500, "origin-b");
    // acquire refuses without adding a holder: just before expiry both count, one of them the same origin.
    expect(expiring.acquire(TENANT, T0 + 999, "origin-a")).toEqual({ ok: false, holdersBefore: 2, oldestHolderAgeMs: 999, sameOriginHolders: 1 });
    // At T0+1000 the origin-a holder has expired; only the origin-b holder (taken at T0+500) is live.
    expect(expiring.acquire(TENANT, T0 + 1_000, "origin-a")).toEqual({ ok: false, holdersBefore: 1, oldestHolderAgeMs: 500, sameOriginHolders: 0 });

    const releasing = createInflightLease();
    const a = releasing.track(TENANT, T0, "origin-a");
    releasing.track(TENANT, T0, "origin-b");
    a.release();
    expect(releasing.acquire(TENANT, T0 + 1, "origin-a")).toEqual({ ok: false, holdersBefore: 1, oldestHolderAgeMs: 1, sameOriginHolders: 0 });
    expect(releasing.track(TENANT, T0 + 1, "origin-a")).toMatchObject({ holdersBefore: 1, sameOriginHolders: 0 });
  });

  test("same origin: a grant with no prior holder (or failing open with none) has no sameOriginHolders", () => {
    const lease = createInflightLease({ maxEntries: 1 });
    const first = lease.acquire(TENANT, T0, "origin-a");
    expect(first).toMatchObject({ ok: true, holdersBefore: 0 });
    expect(Object.hasOwn(first, "sameOriginHolders")).toBe(false);
    const tracked = lease.track({ ...TENANT, userId: "user-2" }, T0, "origin-a");
    expect(tracked).toMatchObject({ ok: true, holdersBefore: 0, failOpen: true });
    expect(Object.hasOwn(tracked, "sameOriginHolders")).toBe(false);
    expect(Object.hasOwn(lease.track({ ...TENANT, userId: "user-3" }, T0), "sameOriginHolders")).toBe(false);
  });

  test("options must be positive integers", () => {
    for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => createInflightLease({ ttlMs: bad })).toThrow(/ttlMs/);
      expect(() => createInflightLease({ maxEntries: bad })).toThrow(/maxEntries/);
    }
  });

  test("Retry-After is jittered inside DELIVERY_BUSY_RETRY_AFTER (2-10); a bad random falls back to the minimum", () => {
    expect(DELIVERY_BUSY_RETRY_AFTER).toEqual({ min: 2, max: 10 } as never);
    expect(inflightRetryAfter(() => 0)).toBe(2);
    expect(inflightRetryAfter(() => 0.999999)).toBe(10);
    for (let i = 0; i < 200; i++) {
      const s = inflightRetryAfter(Math.random);
      expect(s).toBeGreaterThanOrEqual(2);
      expect(s).toBeLessThanOrEqual(10);
    }
    for (const r of [1, -0.1, Number.NaN, 7]) expect(inflightRetryAfter(() => r)).toBe(2);
  });
});

/** One valid relay turns batch for TENANT (the shape of async-admission.test.ts). */
async function turnBatch() {
  const SESSION = "7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a5b";
  const record_id = turnRecordId(TENANT.workspaceId, "claude", SESSION, 0);
  const revision = 1_793_000_000_000_000;
  const turn = { schema_version: 1, origin_id: TURN_ORIGIN_ID, record_id, revision_id: `turnrev:${record_id.slice(5)}:${revision}`, revision, kind: TURN_KIND, source: "claude", session_id: SESSION, operation: "upsert",
    payload: { id: "00000000-aaaa-4bbb-8ccc-dddddddddddd", org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, source: "claude", session_id: SESSION, turn_index: 0, session_owner: TENANT.userId,
      role: "user", content: "turn 0", actor_name: "Ada", actor_role: "owner", metadata_json: JSON.stringify({ nativeCli: "claude", relaySession: { sessionId: SESSION } }), ts: "2026-11-05T12:00:00.123Z", tagged: true } };
  const base = await batch([]);
  return { schema_version: 1, origin_id: TURN_ORIGIN_ID, batch_id: "turns-1", job_id: "relayhistory-turns", generation: 1, destination_id: "relayhistory", instance_id: "relayhistory-api",
    account_id: base.account_id, mapping_version: base.mapping_version, records: [turn] };
}

/** A MemoryLedger whose claim can be held open, so a second delivery overlaps the first. */
class GatedLedger extends MemoryLedger {
  entered = 0;
  hold: Promise<void> | undefined;
  reached: (() => void) | undefined;
  throwNext = false;
  override async claim(input: ClaimInput): Promise<ClaimResult> {
    this.entered++;
    if (this.throwNext) { this.throwNext = false; throw new Error("claim exploded"); }
    if (this.hold) {
      const h = this.hold;
      this.hold = undefined;
      this.reached?.();
      await h;
    }
    return super.claim(input);
  }
  /** Hold the next claim; resolves `entered` when the claim is reached, `open()` lets it continue. */
  gate() {
    let open!: () => void;
    this.hold = new Promise<void>((r) => { open = r; });
    const entered = new Promise<void>((r) => { this.reached = r; });
    return { entered, open };
  }
}

function harness(mode: InflightLeaseMode | undefined, opts: { slots?: true; lease?: InflightLease; deps?: Record<string, unknown> } = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new GatedLedger();
  const lease = opts.lease ?? createInflightLease();
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: {
      ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} },
      ...(opts.slots ? { slots: { maxPerUser: 1, maxPerOrg: 32, ttlSeconds: 90 } } : {}),
    } as never,
    log: (fields) => logged.push(fields),
    identify: (r) => bindingIdentity(r),
    ...(mode ? { inflightLease: { mode, lease, now: () => T0, random: () => 0.5 } } : {}),
    ...opts.deps,
  });
  let n = 0;
  const post = async (over: { userId?: string; path?: string; batch?: unknown; origin?: string } = {}) => {
    const id = `r${++n}`;
    const origin = over.origin ? { origin_id: over.origin } : {};
    const body = JSON.stringify({ protocolVersion: 1, batch: over.batch ?? await batch([record({ record_id: id, ...origin })], { batch_id: `batch-${id}`, ...origin }) });
    const res = await app.fetch(new Request(`https://x${over.path ?? "/v1/delivery/batches"}`, {
      method: "POST", body,
      headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, ...(over.userId ? { userId: over.userId } : {}), scopes: [SYNC_SCOPE] }), "x-correlation-id": `corr-${id}` },
    }));
    return { status: res.status, retryAfter: res.headers.get("retry-after"), json: (await res.json()) as { error?: { code: string } } };
  };
  const leaseLines = () => logged.filter((f) => f.route === "ingest.inflight_lease");
  return { app, ledger, lease, logged, post, leaseLines };
}

/** Delivery A held at its claim, then B; returns both answers after A is let through. */
async function overlap(h: ReturnType<typeof harness>, bUser?: string, origins: { a?: string; b?: string } = {}) {
  const g = h.ledger.gate();
  const a = h.post(origins.a ? { origin: origins.a } : {});
  await g.entered;
  const b = await h.post({ ...(bUser ? { userId: bUser } : {}), ...(origins.b ? { origin: origins.b } : {}) });
  g.open();
  return { a: await a, b };
}

describe("acceptBatch with INGEST_INFLIGHT_LEASE", () => {
  test("off (no dep): both overlapping deliveries reach the ledger and no lease line is logged", async () => {
    const h = harness(undefined);
    const { a, b } = await overlap(h);
    expect(a.status).toBe(200);
    expect(b.status).toBe(200);
    expect(h.ledger.claims).toHaveLength(2);
    expect(h.leaseLines()).toHaveLength(0);
    expect(h.logged.some((f) => JSON.stringify(f).includes(INFLIGHT_LEASE_LOCK))).toBe(false);
    expect(h.lease.counters).toEqual({ granted: 0, refused: 0, failOpen: 0 });
  });

  test("observe: both proceed; exactly one would-refuse line with the documented fields", async () => {
    const h = harness("observe");
    const { a, b } = await overlap(h);
    expect(a.status).toBe(200);
    expect(b.status).toBe(200);
    expect(h.ledger.claims).toHaveLength(2);
    const lines = h.leaseLines();
    expect(lines).toHaveLength(1);
    expect(lines[0]).toEqual({
      route: "ingest.inflight_lease", kind: "delivery.batches", outcome: "would_refuse", state: "observe", reason: "tenant_inflight", count: 1, age_ms: 0, same_origin: true,
      org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, correlation_id: "corr-r2", client_kind: "other",
    });
    expect(h.lease.entries()).toBe(0);
    expect(h.lease.counters).toEqual({ granted: 2, refused: 0, failOpen: 0 });
  });

  test("on: the overlapping delivery is a 429 delivery_busy with Retry-After in [2,10] and makes no ledger call", async () => {
    const h = harness("on");
    const { a, b } = await overlap(h);
    expect(a.status).toBe(200);
    expect(b.status).toBe(429);
    expect(b.json.error?.code).toBe("delivery_busy");
    const ra = Number(b.retryAfter);
    expect(ra).toBeGreaterThanOrEqual(2);
    expect(ra).toBeLessThanOrEqual(10);
    expect(h.ledger.entered).toBe(1);
    expect(h.ledger.claims).toHaveLength(1);
    const lines = h.leaseLines();
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ outcome: "refused", state: "on", reason: "tenant_inflight", count: 1, age_ms: 0, same_origin: true, kind: "delivery.batches" });
    // The refused request's route line is a plain delivery_busy: no lock alert, no database lock fields.
    const route = h.logged.find((f) => f.route === "delivery.batches" && f.outcome === "delivery_busy")!;
    expect(route).toBeDefined();
    expect(route.db_error_code).toBeUndefined();
    expect(h.logged.some((f) => f.route === "ingest.lock_alert")).toBe(false);
    expect(h.lease.entries()).toBe(0);
    // Free again afterwards.
    expect((await h.post()).status).toBe(200);
  });

  test("on: the turns route shares the key with delivery but is tracked, never refused", async () => {
    const h = harness("on");
    const held = h.lease.acquire(TENANT, T0 - 3_000);
    const res = await h.post({ path: "/v1/turns/batches", batch: await turnBatch() });
    expect(res.status).toBe(200);
    expect(h.ledger.entered).toBe(1);
    expect(h.leaseLines()).toEqual([expect.objectContaining({ kind: "turns.batches", outcome: "would_refuse", state: "on", reason: "tenant_inflight", count: 1, age_ms: 3_000, same_origin: false })]);
    // The turns batch held the key while it ran (granted, not refused) and released it.
    expect(h.lease.counters).toEqual({ granted: 2, refused: 0, failOpen: 0 });
    expect(h.lease.holders(TENANT, T0)).toBe(1);
    if (held.ok) held.release();
    expect(h.lease.entries()).toBe(0);
  });

  test("on: a turns batch overlapping an in-flight delivery proceeds and logs would_refuse with state on", async () => {
    const h = harness("on");
    const g = h.ledger.gate();
    const a = h.post();
    await g.entered;
    const t = await h.post({ path: "/v1/turns/batches", batch: await turnBatch() });
    g.open();
    expect((await a).status).toBe(200);
    expect(t.status).toBe(200);
    expect(h.ledger.entered).toBe(2);
    expect(h.leaseLines()).toEqual([{
      route: "ingest.inflight_lease", kind: "turns.batches", outcome: "would_refuse", state: "on", reason: "tenant_inflight", count: 1, age_ms: 0, same_origin: false,
      org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, correlation_id: "corr-r2", client_kind: "other",
    }]);
    expect(h.lease.counters).toEqual({ granted: 2, refused: 0, failOpen: 0 });
    expect(h.lease.entries()).toBe(0);
  });

  test("on: a delivery overlapping an in-flight turns batch is refused", async () => {
    const h = harness("on");
    const g = h.ledger.gate();
    const t = h.post({ path: "/v1/turns/batches", batch: await turnBatch() });
    await g.entered;
    const d = await h.post();
    g.open();
    expect((await t).status).toBe(200);
    expect(d.status).toBe(429);
    expect(d.json.error?.code).toBe("delivery_busy");
    expect(h.ledger.entered).toBe(1);
    expect(h.leaseLines()).toEqual([expect.objectContaining({ kind: "delivery.batches", outcome: "refused", state: "on", count: 1, age_ms: 0, same_origin: false })]);
    expect(h.lease.entries()).toBe(0);
  });

  test("on: a refused delivery never reaches the consent check, the turn register or the ledger", async () => {
    let consentCalls = 0;
    const consentStore = {
      snapshot: async () => { consentCalls++; throw Object.assign(new Error("unread"), { code: "probe" }); },
      apply: async () => { throw new Error("unused"); },
    };
    const h = harness("on", { deps: { uploadConsent: { enforcement: "report", store: consentStore, nowMs: () => T0 }, turnFenceRegister: true } });
    let turnRegisters = 0;
    (h.ledger as unknown as { registerTurnSessions: (i: unknown) => Promise<[string, string][]> }).registerTurnSessions = async () => { turnRegisters++; return []; };
    // A is held at its claim, past its own consent precheck; B overlaps it and is refused.
    const g = h.ledger.gate();
    const a = h.post();
    await g.entered;
    expect(consentCalls).toBe(1);
    const b = await h.post();
    expect(b.status).toBe(429);
    // The refused delivery reached neither the consent check, the turn register nor the ledger. The turn-register
    // count is expected to be zero for any delivery; the consent counter and the ledger entry carry the assertion.
    expect(consentCalls).toBe(1);
    expect(turnRegisters).toBe(0);
    expect(h.ledger.entered).toBe(1);
    expect(h.leaseLines()).toEqual([expect.objectContaining({ outcome: "refused" })]);
    g.open();
    expect((await a).status).toBe(200);
    // The counters are live: an admitted turns batch reaches both.
    const before = consentCalls;
    expect((await h.post({ path: "/v1/turns/batches", batch: await turnBatch() })).status).toBe(200);
    expect(consentCalls).toBeGreaterThan(before);
    expect(turnRegisters).toBe(1);
  });

  test("age_ms: the oldest holder's age on would_refuse (observe) and refused (on), never on fail_open", async () => {
    const observe = harness("observe");
    const stuck = observe.lease.acquire(TENANT, T0 - 4_321);
    expect((await observe.post()).status).toBe(200);
    expect(observe.leaseLines()).toEqual([expect.objectContaining({ outcome: "would_refuse", state: "observe", count: 1, age_ms: 4_321, same_origin: false })]);
    if (stuck.ok) stuck.release();

    const on = harness("on");
    on.lease.acquire(TENANT, T0 - 60_000);
    const refused = await on.post();
    expect(refused.status).toBe(429);
    expect(on.leaseLines()).toEqual([expect.objectContaining({ outcome: "refused", state: "on", count: 1, age_ms: 60_000, same_origin: false })]);

    // A full map with a live holder of the same key: the grant carries an age, the fail_open line does not.
    const full = createInflightLease({ maxEntries: 1 });
    full.acquire(TENANT, T0 - 1_000);
    const open = harness("observe", { lease: full });
    expect((await open.post()).status).toBe(200);
    const lines = open.leaseLines();
    expect(lines).toEqual([expect.objectContaining({ outcome: "fail_open", state: "observe", count: 1 })]);
    expect(Object.hasOwn(lines[0]!, "age_ms")).toBe(false);
    expect(Object.hasOwn(lines[0]!, "same_origin")).toBe(false);
  });

  test("same_origin: observe labels a same-origin overlap true and a different-origin overlap false, with no origin id", async () => {
    const same = harness("observe");
    const s = await overlap(same, undefined, { a: "origin-x1", b: "origin-x1" });
    expect([s.a.status, s.b.status]).toEqual([200, 200]);
    expect(same.leaseLines()).toEqual([expect.objectContaining({ kind: "delivery.batches", outcome: "would_refuse", state: "observe", count: 1, age_ms: 0, same_origin: true })]);
    expect(JSON.stringify(same.leaseLines())).not.toContain("origin-x1");

    const diff = harness("observe");
    const d = await overlap(diff, undefined, { a: "origin-x1", b: "origin-y2" });
    expect([d.a.status, d.b.status]).toEqual([200, 200]);
    expect(diff.ledger.claims).toHaveLength(2);
    expect(diff.leaseLines()).toEqual([{
      route: "ingest.inflight_lease", kind: "delivery.batches", outcome: "would_refuse", state: "observe", reason: "tenant_inflight", count: 1, age_ms: 0, same_origin: false,
      org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, correlation_id: "corr-r2", client_kind: "other",
    }]);
    const text = JSON.stringify(diff.leaseLines());
    for (const o of ["origin-x1", "origin-y2"]) expect(text).not.toContain(o);
  });

  test("same_origin: on still refuses a different-origin overlap (the key is per user) and labels it false; same origin true", async () => {
    const diff = harness("on");
    const d = await overlap(diff, undefined, { a: "origin-x1", b: "origin-y2" });
    expect(d.a.status).toBe(200);
    expect(d.b.status).toBe(429);
    expect(d.b.json.error?.code).toBe("delivery_busy");
    expect(diff.ledger.entered).toBe(1);
    expect(diff.leaseLines()).toEqual([{
      route: "ingest.inflight_lease", kind: "delivery.batches", outcome: "refused", state: "on", reason: "tenant_inflight", count: 1, age_ms: 0, same_origin: false,
      org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, correlation_id: "corr-r2", client_kind: "other",
    }]);
    const text = JSON.stringify(diff.leaseLines());
    for (const o of ["origin-x1", "origin-y2"]) expect(text).not.toContain(o);

    const same = harness("on");
    const s = await overlap(same, undefined, { a: "origin-x1", b: "origin-x1" });
    expect([s.a.status, s.b.status]).toEqual([200, 429]);
    expect(same.leaseLines()).toEqual([expect.objectContaining({ outcome: "refused", state: "on", count: 1, age_ms: 0, same_origin: true })]);
    expect(JSON.stringify(same.leaseLines())).not.toContain("origin-x1");
  });

  test("same_origin on turns lines (observe): another turns batch is true, a delivery holder is false", async () => {
    // Every turns batch carries the one fixed turns origin, so two overlapping turns batches always match.
    const turns = harness("observe");
    const g = turns.ledger.gate();
    const first = turns.post({ path: "/v1/turns/batches", batch: await turnBatch() });
    await g.entered;
    await turns.post({ path: "/v1/turns/batches", batch: { ...(await turnBatch()), batch_id: "turns-2" } });
    g.open();
    expect((await first).status).toBe(200);
    expect(turns.leaseLines()).toEqual([expect.objectContaining({ kind: "turns.batches", outcome: "would_refuse", state: "observe", count: 1, same_origin: true })]);
    expect(JSON.stringify(turns.leaseLines())).not.toContain(TURN_ORIGIN_ID);

    // A turns batch behind a live delivery holder (origin-1) does not match it.
    const mixed = harness("observe");
    const h = mixed.ledger.gate();
    const delivery = mixed.post();
    await h.entered;
    const t = await mixed.post({ path: "/v1/turns/batches", batch: await turnBatch() });
    h.open();
    expect((await delivery).status).toBe(200);
    expect(t.status).toBe(200);
    expect(mixed.leaseLines()).toEqual([expect.objectContaining({ kind: "turns.batches", outcome: "would_refuse", state: "observe", count: 1, same_origin: false })]);
  });

  test("same_origin, age_ms and count survive the production log filter on would_refuse and refused lines", async () => {
    const observe = harness("observe");
    await overlap(observe, undefined, { a: "origin-x1", b: "origin-x1" });
    const on = harness("on");
    await overlap(on, undefined, { a: "origin-x1", b: "origin-y2" });
    const lines = [...observe.leaseLines(), ...on.leaseLines()];
    expect(lines.map((l) => l.outcome)).toEqual(["would_refuse", "refused"]);
    for (const [i, l] of lines.entries()) {
      // The default emit (no deps.log) writes through log.ts: the same filter as logLine.
      const written = JSON.parse(logLine("info", "session ingest", l));
      expect(written).toMatchObject({ route: "ingest.inflight_lease", outcome: l.outcome, count: 1, age_ms: 0, same_origin: i === 0 });
      for (const key of Object.keys(l)) expect(Object.hasOwn(written, key)).toBe(true);
    }
  });

  test("on: a different user in the same workspace is not refused", async () => {
    const h = harness("on");
    const { a, b } = await overlap(h, "user-2");
    expect(a.status).toBe(200);
    expect(b.status).toBe(200);
    expect(h.ledger.claims).toHaveLength(2);
    expect(h.leaseLines()).toHaveLength(0);
  });

  test("on: released after success, after a thrown error, and after another gate's 429", async () => {
    const h = harness("on", { slots: true });
    expect((await h.post()).status).toBe(200);
    expect(h.lease.holders(TENANT, T0)).toBe(0);

    h.ledger.throwNext = true;
    const thrown = await h.post();
    expect(thrown.status).toBe(503);
    expect(h.lease.holders(TENANT, T0)).toBe(0);

    h.ledger.slots.set(TENANT.userId, new Set(["someone-else"]));
    const slots = await h.post();
    expect(slots.status).toBe(429);
    expect(h.logged.some((f) => f.route === "delivery.batches" && f.outcome === "delivery_busy")).toBe(true);
    expect(h.leaseLines()).toHaveLength(0);
    expect(h.lease.holders(TENANT, T0)).toBe(0);
    expect(h.lease.entries()).toBe(0);
    expect(h.lease.counters).toEqual({ granted: 3, refused: 0, failOpen: 0 });
  });

  test("on: a map full of live holders fails open (delivery proceeds) and logs fail_open", async () => {
    const lease = createInflightLease({ maxEntries: 1 });
    lease.acquire({ ...TENANT, userId: "other" }, T0);
    const h = harness("on", { lease });
    expect((await h.post()).status).toBe(200);
    expect(h.ledger.claims).toHaveLength(1);
    expect(h.leaseLines()).toEqual([expect.objectContaining({ outcome: "fail_open", state: "on", count: 0 })]);
    expect(lease.entries()).toBe(1);
  });

  test("assertHttpAdmissionOpen stays first: a stopped org is refused without touching the lease", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const lease = createInflightLease();
    const ledger = new MemoryLedger();
    const app = createIngestApp({
      deliver: { ledger, target: { store: s.store, keyRoot: "96bulls" } } as never,
      log: () => {},
      identify: (r) => bindingIdentity(r),
      httpAdmissionStoppedOrgs: new Set([TENANT.orgId]),
      inflightLease: { mode: "on", lease, now: () => T0 },
    } as never);
    const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
      method: "POST", body: JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "s1" })]) }),
      headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }) },
    }));
    expect(res.status).not.toBe(200);
    expect(lease.counters).toEqual({ granted: 0, refused: 0, failOpen: 0 });
  });
});

describe("queued ACK path", () => {
  test("the queued path is neither leased nor refused, even with a live holder for the same key", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const lease = createInflightLease();
    const held = lease.acquire(TENANT, T0);
    const sent: unknown[] = [];
    const logged: Record<string, unknown>[] = [];
    const app = createIngestApp({
      identify: (req) => bindingIdentity(req), log: (f) => logged.push(f),
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "buffered", target: { store: s.store, keyRoot: "96bulls" } },
      asyncAdmission: { mode: "on", enqueue: async (m) => { sent.push(m); }, nowMs: () => T0 },
      inflightLease: { mode: "on", lease, now: () => T0 },
    });
    const res = await app.request("/v1/delivery/batches", {
      method: "POST",
      headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE], admission: ADMISSION_ACCEPT_VALUE }) },
      body: JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "q1" })]) }),
    });
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBe('{"version":1,"state":"queued"}');
    expect(sent).toHaveLength(1);
    expect(lease.counters).toEqual({ granted: 1, refused: 0, failOpen: 0 });
    expect(lease.holders(TENANT, T0)).toBe(1);
    expect(logged.filter((f) => f.route === "ingest.inflight_lease")).toHaveLength(0);
    if (held.ok) held.release();
  });
});

describe("Worker settings", () => {
  test("INGEST_INFLIGHT_LEASE: observe / on (any case, trimmed) reach the settings; anything else leaves them exactly as before", async () => {
    const { ingestSettings } = await import("../worker.ts");
    const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
    const off = (await ingestSettings(full))!;
    expect(Object.hasOwn(off, "inflightLease")).toBe(false);
    for (const [v, mode] of [["observe", "observe"], [" OBSERVE ", "observe"], ["on", "on"], ["ON", "on"], ["on\t", "on"]] as const) {
      expect(await ingestSettings({ ...full, INGEST_INFLIGHT_LEASE: v })).toEqual({ ...off, inflightLease: mode } as never);
    }
    for (const v of ["", " ", "off", "OFF", "1", "true", "yes", "o n", "enforce"]) {
      expect(await ingestSettings({ ...full, INGEST_INFLIGHT_LEASE: v })).toEqual(off);
    }
  });
});
