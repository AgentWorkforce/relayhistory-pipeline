/** The in-process per-tenant upload limit for INGEST_LEDGER=s3 (tenant-limiter.ts), and its wiring into ledger-s3.ts. */
import { describe, expect, test } from "bun:test";
import { s3Ledger } from "../ledger-s3.ts";
import type { SlotRequest } from "../ledger.ts";
import { tenantLimiter } from "../tenant-limiter.ts";
import { scratchStore } from "./support.ts";

const A1 = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const A2 = { orgId: "org-a", workspaceId: "ws-a", userId: "user-2" };
const B1 = { orgId: "org-b", workspaceId: "ws-b", userId: "user-1" };
const req = (tenant: SlotRequest["tenant"], holder: string, over: Partial<SlotRequest> = {}): SlotRequest =>
  ({ tenant, holder, maxPerUser: 2, maxPerOrg: 3, ttlSeconds: 90, ...over });

function clock(start = 1_000_000) {
  let t = start;
  return { now: () => t, advance: (ms: number) => { t += ms; } };
}

describe("tenantLimiter", () => {
  test("caps concurrent leases per (org, user) and per org; another org is independent", () => {
    const l = tenantLimiter({ random: () => 0 });
    expect(l.acquire(req(A1, "h1"))).toEqual({ ok: true });
    expect(l.acquire(req(A1, "h2"))).toEqual({ ok: true });
    // user-1 is at maxPerUser (2).
    expect(l.acquire(req(A1, "h3"))).toEqual({ ok: false, retryAfterSeconds: 2 });
    // user-2 of the same org still gets one: the org has 2 of 3.
    expect(l.acquire(req(A2, "h4"))).toEqual({ ok: true });
    // The org is at maxPerOrg (3): user-2's second is refused even though user-2 is under its own cap.
    expect(l.acquire(req(A2, "h5")).ok).toBe(false);
    // Another org is not affected; the same user id in another org is a different tenant.
    expect(l.acquire(req(B1, "h6"))).toEqual({ ok: true });
    expect([l.inFlight("org-a"), l.inFlight("org-a", "user-1"), l.inFlight("org-a", "user-2"), l.inFlight("org-b")]).toEqual([3, 2, 1, 1]);
    // A release frees exactly that holder's lease.
    l.release(A1, "h1");
    expect(l.acquire(req(A1, "h3"))).toEqual({ ok: true });
    expect(l.acquire(req(A2, "h5")).ok).toBe(false);
  });

  test("the same holder re-acquiring refreshes its lease (the SQL lease's ON CONFLICT), it never takes a second", () => {
    const c = clock();
    const l = tenantLimiter({ nowMs: c.now });
    expect(l.acquire(req(A1, "h1", { maxPerUser: 1, ttlSeconds: 10 }))).toEqual({ ok: true });
    c.advance(9_000);
    expect(l.acquire(req(A1, "h1", { maxPerUser: 1, ttlSeconds: 10 }))).toEqual({ ok: true });
    expect(l.inFlight("org-a", "user-1")).toBe(1);
    c.advance(9_000); // 18 s after the first acquire, 9 s after the refresh: still held.
    expect(l.acquire(req(A1, "h2", { maxPerUser: 1, ttlSeconds: 10 })).ok).toBe(false);
  });

  test("Retry-After is relayhistory's jittered 2..10 s (DELIVERY_BUSY_RETRY_AFTER), whatever the random source", () => {
    for (const [r, want] of [[0, 2], [0.5, 6], [0.9999, 10], [1, 2], [-1, 2], [Number.NaN, 2]] as const) {
      const l = tenantLimiter({ random: () => r });
      l.acquire(req(A1, "h1", { maxPerUser: 1 }));
      expect(l.acquire(req(A1, "h2", { maxPerUser: 1 }))).toEqual({ ok: false, retryAfterSeconds: want });
    }
  });

  test("TTL: a leaked lease (never released) frees itself after ttlSeconds", () => {
    const c = clock();
    const l = tenantLimiter({ nowMs: c.now });
    expect(l.acquire(req(A1, "leaked", { maxPerUser: 1 })).ok).toBe(true);
    c.advance(89_999);
    expect(l.acquire(req(A1, "h2", { maxPerUser: 1 })).ok).toBe(false);
    c.advance(1);
    expect(l.inFlight("org-a")).toBe(0);
    expect(l.acquire(req(A1, "h2", { maxPerUser: 1 })).ok).toBe(true);
    // Releasing the expired holder later is harmless: it never frees another holder's lease.
    l.release(A1, "leaked");
    expect(l.inFlight("org-a", "user-1")).toBe(1);
  });

  test("release in finally frees the lease when the guarded work throws", async () => {
    const l = tenantLimiter();
    const guarded = async (holder: string, work: () => Promise<void>) => {
      const s = l.acquire(req(A1, holder, { maxPerUser: 1 }));
      if (!s.ok) throw new Error("busy");
      try { await work(); } finally { l.release(A1, holder); }
    };
    await expect(guarded("h1", async () => { throw new Error("store down"); })).rejects.toThrow("store down");
    expect(l.inFlight("org-a")).toBe(0);
    await guarded("h2", async () => {});
    expect(l.inFlight("org-a")).toBe(0);
  });

  test("an out-of-bounds request is a coded ledger fault, never an unlimited pass", () => {
    const l = tenantLimiter();
    for (const over of [{ maxPerUser: 0 }, { maxPerOrg: 0 }, { maxPerUser: 1.5 }, { ttlSeconds: 1 }, { ttlSeconds: 901 }, { holder: "" }, { tenant: { ...A1, orgId: "" } }] as Partial<SlotRequest>[])
      expect(() => l.acquire(req(A1, "h", over))).toThrow(expect.objectContaining({ code: "ledger_input_refused" }));
    expect(l.inFlight("org-a")).toBe(0);
  });

  test("expired leases of an idle org are swept and never counted; a released lease leaves nothing", () => {
    const c = clock();
    const l = tenantLimiter({ nowMs: c.now });
    l.acquire(req(B1, "gone", { ttlSeconds: 5 }));
    c.advance(61_000);
    l.acquire(req(A1, "h1"));
    l.release(A1, "h1");
    expect([l.inFlight("org-a"), l.inFlight("org-b")]).toEqual([0, 0]);
  });
});

describe("ledger-s3.ts acquireSlot / releaseSlot", () => {
  test("with a tenant limiter: the limiter decides, with no store request; without one: unchanged (always ok)", async () => {
    const s = scratchStore();
    try {
      let requests = 0;
      const counted = new Proxy(s.store, { get(t, p, r) { const v = Reflect.get(t, p, r); return typeof v === "function" ? (...a: unknown[]) => { requests++; return v.apply(t, a); } : v; } });
      const limited = s3Ledger(counted, "96bulls", { tenantLimiter: tenantLimiter() });
      expect(await limited.acquireSlot(req(A1, "h1", { maxPerUser: 1 }))).toEqual({ ok: true });
      expect((await limited.acquireSlot(req(A1, "h2", { maxPerUser: 1 }))).ok).toBe(false);
      await limited.releaseSlot(A1, "h1");
      expect(await limited.acquireSlot(req(A1, "h2", { maxPerUser: 1 }))).toEqual({ ok: true });
      expect(requests).toBe(0);
      const unlimited = s3Ledger(s.store, "96bulls");
      for (const h of ["a", "b", "c"]) expect(await unlimited.acquireSlot(req(A1, h, { maxPerUser: 1 }))).toEqual({ ok: true });
    } finally {
      s.done();
    }
  });
});

describe("INGEST_S3_TENANT_LIMIT / INGEST_ASYNC_MAX_PER_ORG config (selfhost-config.ts)", () => {
  const enabled = {
    RELAYHISTORY_SELFHOST_INGEST: "true", RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
    RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test", RELAYCONTROL_INTROSPECT_SECRET: "s",
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
    SESSION_POSTGRES_TLS_PROFILE: "supabase", LOCKER_BUCKET: "transcripts", INGEST_LEDGER: "s3",
  };
  const async = { ...enabled, SELFHOST_INGEST_ADMISSION: "async" };

  test("off: unset, empty or off leaves the settings exactly as before (no s3TenantLimit field)", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    for (const env of [enabled, async]) {
      const base = selfhostIngestConfig(env);
      expect(Object.hasOwn(base, "s3TenantLimit")).toBe(false);
      for (const v of ["", "off"]) expect(selfhostIngestConfig({ ...env, INGEST_S3_TENANT_LIMIT: v })).toEqual(base);
    }
  });

  test("on: exactly on; the async per-org cap defaults to max(1, floor(concurrency / 2)) only when unset", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    expect(selfhostIngestConfig({ ...enabled, INGEST_S3_TENANT_LIMIT: "on" }).s3TenantLimit).toEqual({});
    expect(selfhostIngestConfig({ ...async, INGEST_S3_TENANT_LIMIT: "on" }).s3TenantLimit).toEqual({ asyncMaxPerOrg: 2 });
    for (const [c, want] of [["1", 1], ["3", 1], ["8", 4], ["32", 16]] as const)
      expect(selfhostIngestConfig({ ...async, INGEST_S3_TENANT_LIMIT: "on", SELFHOST_ADMISSION_CONCURRENCY: c }).s3TenantLimit).toEqual({ asyncMaxPerOrg: want });
    expect(selfhostIngestConfig({ ...async, INGEST_S3_TENANT_LIMIT: "on", SELFHOST_ADMISSION_CONCURRENCY: "8", INGEST_ASYNC_MAX_PER_ORG: "8" }).s3TenantLimit).toEqual({ asyncMaxPerOrg: 8 });
  });

  test("invalid values refuse startup, naming the variable only", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    for (const v of ["ON", "On", " on", "true", "1", "yes"]) expect(() => selfhostIngestConfig({ ...enabled, INGEST_S3_TENANT_LIMIT: v })).toThrow(/^96bulls: INGEST_S3_TENANT_LIMIT must be off or on$/);
    expect(() => selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "sql", INGEST_S3_TENANT_LIMIT: "on" })).toThrow(/INGEST_S3_TENANT_LIMIT=on needs INGEST_LEDGER=s3/);
    // INGEST_ASYNC_MAX_PER_ORG sits behind the flag and async admission, and is bounded 1..concurrency.
    expect(() => selfhostIngestConfig({ ...async, INGEST_ASYNC_MAX_PER_ORG: "2" })).toThrow(/INGEST_ASYNC_MAX_PER_ORG needs INGEST_S3_TENANT_LIMIT=on/);
    expect(() => selfhostIngestConfig({ ...enabled, INGEST_S3_TENANT_LIMIT: "on", INGEST_ASYNC_MAX_PER_ORG: "2" })).toThrow(/INGEST_ASYNC_MAX_PER_ORG needs SELFHOST_INGEST_ADMISSION=async or drain/);
    for (const v of ["0", "5", "-1", "2.5", "two"])
      expect(() => selfhostIngestConfig({ ...async, INGEST_S3_TENANT_LIMIT: "on", INGEST_ASYNC_MAX_PER_ORG: v })).toThrow(/^96bulls: INGEST_ASYNC_MAX_PER_ORG must be an integer from 1 to 4$/);
  });

  test("the runtime refuses the flag without the S3 ledger (direct callers)", async () => {
    const { selfhostIngestRuntime } = await import("../selfhost-server.ts");
    expect(() => selfhostIngestRuntime({ keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 32, admission: undefined, s3TenantLimit: {} },
      { run: (async () => { throw new Error("unused"); }) as never, store: {} as never, verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {} }))
      .toThrow(/INGEST_S3_TENANT_LIMIT=on needs INGEST_LEDGER=s3/);
  });
});
