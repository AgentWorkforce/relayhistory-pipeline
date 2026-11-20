/**
 * INGEST_ADVERTISED_MAX_RECORDS (advertised-limits.ts): the advertised records-per-batch limit, independent of the
 * accepted limit. Default off: unset publishes exactly what main publishes.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { deliverBatch } from "../deliver.ts";
import { advertisedDeliveryLimits, parseAdvertisedMaxRecords } from "../advertised-limits.ts";
import { DELIVERY_LIMITS, MAX_DELIVERY_RECORDS } from "../vendor/delivery/delivery-contracts.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const noSleep = async () => {};
function setup(advertisedMaxRecords?: number) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const deps = { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: noSleep }, ...(advertisedMaxRecords === undefined ? {} : { advertisedMaxRecords }) };
  return { deps, app: createIngestApp({ deliver: deps, log: () => {}, signingKey: SIGNING_KEY }) };
}
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
const enc = (s: string) => new TextEncoder().encode(s);
const getLimits = (app: ReturnType<typeof createIngestApp>) =>
  app.fetch(new Request("https://x/v1/delivery/limits", { headers: signedHeaders(identity, "GET", "/v1/delivery/limits", new Uint8Array()) }));
const post = async (app: ReturnType<typeof createIngestApp>, b: unknown) => {
  const body = JSON.stringify({ protocolVersion: 1, batch: b });
  return app.fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: signedHeaders(identity, "POST", "/v1/delivery/batches", enc(body)) }));
};
const manyRecords = (n: number, prefix: string) => Array.from({ length: n }, (_, i) => record({ record_id: `${prefix}-${i}`, session_id: `${prefix}-s${i % 7}` }));

describe("parseAdvertisedMaxRecords: a strict decimal integer 1..500, anything else ignored (never a refusal)", () => {
  test("valid values", () => {
    for (const [raw, value] of [["1", 1], ["50", 50], ["100", 100], ["250", 250], ["500", 500]] as const)
      expect(parseAdvertisedMaxRecords(raw)).toEqual({ value });
  });
  test("unset or empty is off, not invalid", () => {
    expect(parseAdvertisedMaxRecords(undefined)).toEqual({});
    expect(parseAdvertisedMaxRecords("")).toEqual({});
  });
  test("everything else is invalid and ignored", () => {
    for (const raw of ["0", "501", "999", "1000", "0100", "+5", "-1", " 100", "100 ", "1e2", "1.0", "100.", "0x64", "abc", "١٠٠", "10 0"])
      expect({ raw, parsed: parseAdvertisedMaxRecords(raw) }).toEqual({ raw, parsed: { invalid: true } });
  });
});

describe("advertisedDeliveryLimits", () => {
  test("unset and 500 return the very same DELIVERY_LIMITS object (byte-identical publication)", () => {
    expect(advertisedDeliveryLimits(undefined)).toBe(DELIVERY_LIMITS);
    expect(advertisedDeliveryLimits(MAX_DELIVERY_RECORDS)).toBe(DELIVERY_LIMITS);
  });
  test("a lower value changes maxRecords only, frozen", () => {
    const l = advertisedDeliveryLimits(100);
    expect(l).toEqual({ ...DELIVERY_LIMITS, maxRecords: 100 });
    expect(Object.isFrozen(l)).toBe(true);
    expect(DELIVERY_LIMITS.maxRecords).toBe(500);
  });
});

describe("publication: every site reads the one resolved value", () => {
  test("golden: unset, GET /v1/delivery/limits is byte-identical to main's body", async () => {
    const { app } = setup();
    const res = await getLimits(app);
    expect(res.status).toBe(200);
    const text = await res.text();
    const { contentEncodings } = await import("../app.ts");
    expect(text).toBe(JSON.stringify({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings() }));
  });
  test("golden: unset, a batch receipt's limits are byte-identical to main's", async () => {
    const { app } = setup();
    const res = await post(app, await batch([record({ record_id: "g1" })]));
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    const { contentEncodings } = await import("../app.ts");
    expect(JSON.stringify(body.limits)).toBe(JSON.stringify({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings() }));
  });
  test("advertised 100: the limits route and the receipt both publish 100; maxRequestBytes unchanged", async () => {
    const { app } = setup(100);
    const limits = await (await getLimits(app)).json() as any;
    expect(limits.maxRecords).toBe(100);
    expect(limits.maxRequestBytes).toBe(DELIVERY_LIMITS.maxRequestBytes);
    const receipt = await (await post(app, await batch([record({ record_id: "p1" })]))).json() as any;
    expect(receipt.limits.maxRecords).toBe(100);
    expect(receipt.limits.maxRequestBytes).toBe(DELIVERY_LIMITS.maxRequestBytes);
  });
  test("deliver.ts's receipt (every outcome path) carries the advertised value", async () => {
    const { deps } = setup(50);
    const b = await batch([record({ record_id: "d1" })]);
    const first = await deliverBatch(deps as never, TENANT, b);
    const replay = await deliverBatch(deps as never, TENANT, b);
    expect(first.receipt.limits).toEqual({ ...DELIVERY_LIMITS, maxRecords: 50 });
    expect(replay.outcome).toMatch(/^replay/);
    expect(replay.receipt.limits).toEqual({ ...DELIVERY_LIMITS, maxRecords: 50 });
  });
});

describe("acceptance is unchanged: advertised 100 never refuses or strands a 500-record batch", () => {
  test("a 500-record batch is accepted, and its replay (a persisted batch retried with the same body) gets the same receipt", async () => {
    const { app } = setup(100);
    const b = await batch(manyRecords(MAX_DELIVERY_RECORDS, "big"), { batch_id: "persisted-500" });
    const first = await post(app, b);
    expect(first.status).toBe(200);
    const r1 = await first.json() as any;
    const again = await post(app, b);
    expect(again.status).toBe(200);
    const r2 = await again.json() as any;
    expect(r2.acceptedRevisionIds).toEqual(r1.acceptedRevisionIds);
    expect(r2.acceptedRevisionIds.length).toBe(MAX_DELIVERY_RECORDS);
    expect(r2).toEqual(r1);
    expect(r1.limits.maxRecords).toBe(100);
  });
  test("501 records are still refused exactly as before (the accepted limit is MAX_DELIVERY_RECORDS, not the advertised one)", async () => {
    const off = setup(), on = setup(100);
    const b = await batch(manyRecords(MAX_DELIVERY_RECORDS + 1, "over"));
    const [a, c] = [await post(off.app, b), await post(on.app, b)];
    expect(c.status).toBe(a.status);
    expect(a.status).toBe(400); // prepare.ts: "Delivery batch must contain 1..500 records"
    const strip = ({ correlationId: _id, ...rest }: any) => rest; // per-request id
    expect(strip(await c.json())).toEqual(strip(await a.json()));
  });
});

describe("Worker settings", () => {
  const full = { HYPERDRIVE: { connectionString: "postgres://x" }, INGEST_FANOUT: {} as never, S3_ACCESS_KEY_ID: "k", S3_SECRET_ACCESS_KEY: "s", INGEST_SIGNING_KEY: "sign", INGEST_S3_BUCKET: "b", INGEST_KMS_KEY_ARN: "arn" };
  const target = { store: {} as never, keyRoot: "96bulls" };
  test("unset: settings and deliver deps exactly as before", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const off = (await ingestSettings(full))!;
    expect(Object.hasOwn(off, "advertisedMaxRecords")).toBe(false);
    expect(Object.hasOwn(off, "advertisedMaxRecordsInvalid")).toBe(false);
    expect(Object.hasOwn(deliverDepsFor(off, async () => ({ rows: [] }), target as never), "advertisedMaxRecords")).toBe(false);
    expect(await ingestSettings({ ...full, INGEST_ADVERTISED_MAX_RECORDS: "" })).toEqual(off);
  });
  test("a valid value reaches the deliver deps; an invalid one is ignored (settings flag it, never null/refused)", async () => {
    const { ingestSettings, deliverDepsFor } = await import("../worker.ts");
    const off = (await ingestSettings(full))!;
    const on = (await ingestSettings({ ...full, INGEST_ADVERTISED_MAX_RECORDS: "100" }))!;
    expect(on).toEqual({ ...off, advertisedMaxRecords: 100 } as never);
    expect(deliverDepsFor(on, async () => ({ rows: [] }), target as never).advertisedMaxRecords).toBe(100);
    for (const bad of ["0", "501", "abc", " 100", "1e2"]) {
      const s = (await ingestSettings({ ...full, INGEST_ADVERTISED_MAX_RECORDS: bad }))!;
      expect(s).toEqual({ ...off, advertisedMaxRecordsInvalid: true } as never);
      expect(Object.hasOwn(deliverDepsFor(s, async () => ({ rows: [] }), target as never), "advertisedMaxRecords")).toBe(false);
    }
  });
});
