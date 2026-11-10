/**
 * INGEST_CLAIM_LOCK_BUSY on the self-host ingest edge (selfhost-config.ts -> selfhost-server.ts DeliverDeps),
 * without a database: the env parse mirrors the Worker's (routingFlag), and over real loopback node:http
 * a claim whose ledger statement loses its lock wait (55P03 / 40P01) is a 429 delivery_busy with
 * Retry-After 5..10 with the flag on and the unchanged 503 without it. 57014 stays 503. The async
 * consumer (hosted admitMessage over the same deliver deps) behaves identically either way.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import { selfhostIngestRuntime, startSelfhostIngestService, type RuntimeBuilder } from "../selfhost-server.ts";
import type { SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

const enabled: NodeJS.ProcessEnv = {
  RELAYHISTORY_SELFHOST_INGEST: "true",
  RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
  RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test",
  RELAYCONTROL_INTROSPECT_SECRET: "s",
  RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
  SESSION_POSTGRES_TLS_PROFILE: "supabase",
  LOCKER_BUCKET: "transcripts",
};

describe("INGEST_CLAIM_LOCK_BUSY config (self-host)", () => {
  test("only on (any case, trimmed) enables it; unset/empty/off/other values leave the settings exactly as before", () => {
    const base = selfhostIngestConfig(enabled);
    expect(Object.hasOwn(base, "claimLockBusy")).toBe(false);
    for (const v of ["on", "ON", "On", "ON ", " on\t"]) expect(selfhostIngestConfig({ ...enabled, INGEST_CLAIM_LOCK_BUSY: v })).toEqual({ ...base, claimLockBusy: true } as never);
    // The Worker's routingFlag never refuses a value: anything but "on" is off (claim-lock-busy.test.ts).
    for (const v of [undefined, "", " ", "off", "OFF", "1", "true", "yes", "o n"]) {
      const c = selfhostIngestConfig({ ...enabled, INGEST_CLAIM_LOCK_BUSY: v });
      expect(c).toEqual(base);
      expect(Object.hasOwn(c, "claimLockBusy")).toBe(false);
    }
    // Dormant: nothing but the port is parsed, as before.
    expect(selfhostIngestConfig({ INGEST_CLAIM_LOCK_BUSY: "on" })).toEqual({ enabled: false, port: 8081 });
  });
});

/** The edge over node:http with a ledger whose every statement fails with `sqlstate`, and a fixed verified identity. */
async function start(sqlstate: string, over: NodeJS.ProcessEnv) {
  const fs = scratchStore();
  cleanups.push(fs.done);
  const sql: string[] = [];
  const logged: Record<string, unknown>[] = [];
  let consumer: SelfhostAdmissionConsumer | undefined;
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] } as unknown as IngestIdentity;
  const build: RuntimeBuilder = async (settings) => selfhostIngestRuntime(settings, {
    run: async (text) => { sql.push(text); throw Object.assign(new Error(`canceling statement ${TENANT.orgId}`), { code: sqlstate }); },
    store: fs.store,
    verify: async () => identity,
    ready: async () => true,
    close: async () => {},
  }, {
    startConsumer: false,
    onConsumer: (c) => void (consumer = c),
    log: { info: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}), warn: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}), error: (_m: string, f?: Record<string, unknown>) => void logged.push(f ?? {}) } as never,
  });
  const service = await startSelfhostIngestService({ ...enabled, ...over }, { host: "127.0.0.1", port: 0, build });
  cleanups.push(() => service.close());
  const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "clb1" })]) });
  const res = await fetch(`http://127.0.0.1:${service.port}/v1/delivery/batches`, {
    method: "POST", body, headers: { "content-type": "application/json", authorization: "Bearer sh_at_t" },
  });
  const json = (await res.json()) as { error?: { code: string } };
  return { status: res.status, retryAfter: res.headers.get("retry-after"), code: json.error?.code, sql, logged, consumer: () => consumer! };
}

describe("INGEST_CLAIM_LOCK_BUSY over the self-host edge", () => {
  for (const admission of ["sync", "drain"] as const) {
    test(`${admission}: on -> a claim lock wait (55P03 / 40P01) is 429 delivery_busy, Retry-After 5..10; unset -> the same failure is 503`, async () => {
      for (const sqlstate of ["55P03", "40P01"]) {
        const on = await start(sqlstate, { SELFHOST_INGEST_ADMISSION: admission, INGEST_CLAIM_LOCK_BUSY: "on" });
        expect([on.status, on.code]).toEqual([429, "delivery_busy"]);
        const after = Number(on.retryAfter);
        expect(Number.isInteger(after) && after >= 5 && after <= 10).toBe(true);
        expect(on.sql.length).toBe(1); // the claim statement only: no wait or retry in the edge

        const off = await start(sqlstate, { SELFHOST_INGEST_ADMISSION: admission });
        expect([off.status, off.code, off.retryAfter]).toEqual([503, "delivery_unavailable", null]);
        expect(off.sql.length).toBe(1);
      }
    });
  }

  test("on: 57014 (statement timeout) and a connection failure on the claim stay 503", async () => {
    for (const sqlstate of ["57014", "08006"]) {
      const r = await start(sqlstate, { INGEST_CLAIM_LOCK_BUSY: "on" });
      expect([r.status, r.code, r.retryAfter]).toEqual([503, "delivery_unavailable", null]);
    }
  });

  test("async: the upload is queued without SQL, and the consumer's retry of a claim lock wait is identical with the flag on or off", async () => {
    const lines: Record<string, unknown>[][] = [];
    for (const flag of [{ INGEST_CLAIM_LOCK_BUSY: "on" }, {}]) {
      const r = await start("55P03", { SELFHOST_INGEST_ADMISSION: "async", ...flag });
      expect(r.status).toBeLessThan(300);
      expect(r.sql.length).toBe(0);
      await r.consumer().scan();
      await r.consumer().settle();
      expect(r.sql.length).toBe(1);
      lines.push(r.logged.filter((f) => f.route === "ingest.admission").map(({ duration_ms: _d, ...rest }) => rest));
    }
    expect(lines[0]).toEqual(lines[1]!);
    expect(lines[0]).toEqual([{ route: "ingest.admission", outcome: "retry", error_code: "ledger_lock_timeout", stage: "claim", attempt: 1, db_error_class: expect.any(String), db_error_code: "55P03", db_error_basis: expect.any(String) }]);
  });
});
