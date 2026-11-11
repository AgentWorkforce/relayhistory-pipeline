/**
 * Step 8 parity contract (scoping 4.3, "what Desktop observes"): a response that comes THROUGH the Node ingest
 * edge equals the handler's DIRECT response in status, body bytes and every header (content-type, Retry-After,
 * x-relay-admission included). On pass-through the edge owns no response header (ingest-edge.ts returns
 * `handler.fetch(forwarded)` as is); it adds Retry-After and `www-authenticate` only to its OWN refusals.
 *
 * What ingest-edge.test.ts already proves is not repeated here (the 401/403 code table, the jitter bounds at
 * both ends of the random source, the route 404/501/405 table, the trust-header strip with a gzip body).
 * This file adds the response-side matrix, the real createIngestApp error paths compared both ways, and
 * the request-side gaps: an opaque zstd body with its Content-Encoding, every stripped name by prefix, and
 * unrelated headers surviving. In-process only: no network, no clock, a fixed jitter source.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createIngestApp, encodeIdentity, IDENTITY_HEADER, SIGNATURE_HEADER, SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { ADMISSION_HEADER, ADMISSION_QUEUED } from "../async-admission.ts";
import { AuthError } from "../../session-read-api/app.ts";
import {
  EDGE_STRIPPED_HEADERS, EDGE_STRIPPED_PREFIX, INGEST_EDGE_RETRY_AFTER_MAX_S, INGEST_EDGE_RETRY_AFTER_MIN_S, createIngestEdge,
  type EdgeHandlerIdentify, type EdgeVerify,
} from "../ingest-edge.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { MAX_DELIVERY_BYTES } from "../vendor/delivery/delivery-contracts.ts";
import { batch, MemoryLedger, record, scratchStore, TENANT } from "./support.ts";

const ORIGIN = "https://history.edge.test";
const IDENTITY: IngestIdentity = { ...TENANT, scopes: [SYNC_SCOPE] };
const VERIFY_OK: EdgeVerify = async () => IDENTITY;
/** A fixed jitter source: every edge-own 503 here answers Retry-After 7. */
const RANDOM = () => 0.4;

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

type Snapshot = { status: number; body: number[]; headers: [string, string][] };
async function snapshot(res: Response, drop: string[] = []): Promise<Snapshot> {
  const headers: [string, string][] = [];
  res.headers.forEach((v, k) => { if (!drop.includes(k)) headers.push([k, v]); });
  headers.sort(([a], [b]) => a.localeCompare(b));
  return { status: res.status, body: [...new Uint8Array(await res.arrayBuffer())], headers };
}

/** Each matrix case is a fresh Response per call, exactly as a handler would build it. */
type Case = { name: string; make: () => Response; expect: { status: number; retryAfter: string | null; admission: string | null } };
const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  () => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "x-request-id": "req-fixed", "x-correlation-id": "corr-1", ...headers } });
const MATRIX: Case[] = [
  { name: "200 queued with x-relay-admission", make: json(200, { protocolVersion: 1, receiptId: `rhr_${"a".repeat(64)}` }, { [ADMISSION_HEADER]: ADMISSION_QUEUED }), expect: { status: 200, retryAfter: null, admission: ADMISSION_QUEUED } },
  { name: "202 queued with x-relay-admission", make: json(202, { ok: true }, { [ADMISSION_HEADER]: ADMISSION_QUEUED }), expect: { status: 202, retryAfter: null, admission: ADMISSION_QUEUED } },
  { name: "413 delivery_too_large", make: json(413, { error: { code: "delivery_too_large", message: `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes` }, correlationId: "corr-1" }), expect: { status: 413, retryAfter: null, admission: null } },
  { name: "429 delivery_busy with Retry-After", make: json(429, { error: { code: "delivery_busy", message: "busy" }, correlationId: "corr-1" }, { "retry-after": "8" }), expect: { status: 429, retryAfter: "8", admission: null } },
  { name: "503 with Retry-After", make: json(503, { error: { code: "delivery_unavailable", message: "x" }, correlationId: "corr-1" }, { "retry-after": "3" }), expect: { status: 503, retryAfter: "3", admission: null } },
  { name: "503 without Retry-After stays without", make: json(503, { error: { code: "delivery_unavailable", message: "x" }, correlationId: "corr-1" }), expect: { status: 503, retryAfter: null, admission: null } },
  { name: "409 untyped body", make: () => new Response("conflict, no envelope", { status: 409, headers: { "content-type": "text/plain; charset=utf-8" } }), expect: { status: 409, retryAfter: null, admission: null } },
  { name: "400 JSON error body", make: json(400, { error: { code: "invalid_delivery", message: "bad", conflicts: [{ recordId: "r1" }] }, correlationId: "corr-1" }), expect: { status: 400, retryAfter: null, admission: null } },
  { name: "401 from the handler keeps no www-authenticate", make: json(401, { error: { code: "missing_authorization", message: "m" }, correlationId: "corr-1" }), expect: { status: 401, retryAfter: null, admission: null } },
  { name: "204 with no body and no content-type", make: () => new Response(null, { status: 204 }), expect: { status: 204, retryAfter: null, admission: null } },
];

/** A scripted in-process handler reached through the edge's identify seam. */
function scripted(make: () => Response) {
  const seen: Request[] = [];
  const handler = { fetch: async (r: Request) => { seen.push(r); return make(); } };
  const edge = createIngestEdge(VERIFY_OK, () => handler, { random: RANDOM });
  return { handler, edge, seen };
}

const post = (path = "/v1/delivery/batches", init: { body?: BodyInit | null; headers?: Record<string, string> } = {}) =>
  new Request(`${ORIGIN}${path}`, { method: "POST", body: init.body ?? "{}", headers: { authorization: "Bearer sh_at_t", ...(init.headers ?? {}) } });

describe("response pass-through: through the edge == the handler direct", () => {
  for (const c of MATRIX) {
    test(c.name, async () => {
      const { handler, edge } = scripted(c.make);
      const direct = await snapshot(await handler.fetch(post()));
      const through = await edge.fetch(post());
      expect(through.headers.get("retry-after")).toBe(c.expect.retryAfter);
      expect(through.headers.get(ADMISSION_HEADER)).toBe(c.expect.admission);
      expect(through.headers.get("www-authenticate")).toBeNull();
      expect(await snapshot(through)).toEqual(direct);
      expect(direct.status).toBe(c.expect.status);
    });
  }

  test("GET /v1/delivery/limits passes through the same way (status, bytes, every header)", async () => {
    const make = json(200, { limits: { maxBytes: MAX_DELIVERY_BYTES } }, { "cache-control": "max-age=60" });
    const { handler, edge } = scripted(make);
    const get = () => new Request(`${ORIGIN}/v1/delivery/limits`, { headers: { authorization: "Bearer sh_at_t" } });
    expect(await snapshot(await edge.fetch(get()))).toEqual(await snapshot(await handler.fetch(get())));
  });
});

describe("the real createIngestApp error paths, direct vs through the edge", () => {
  /** Two apps over equal deps: one identified directly, one only through the edge's seam. */
  async function pair(deliver: () => Record<string, unknown>) {
    const s = scratchStore();
    cleanups.push(s.done);
    const deps = () => ({ deliver: { target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, ...deliver() } as never, log: () => {} });
    const direct = createIngestApp({ ...deps(), identify: () => IDENTITY });
    const edge = createIngestEdge(VERIFY_OK, (identify: EdgeHandlerIdentify) => createIngestApp({ ...deps(), identify: (r) => identify(r) }), { random: RANDOM });
    return { direct, edge };
  }
  const body = async () => JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "p1" })]) });
  const request = (b: BodyInit, headers: Record<string, string> = {}) =>
    new Request(`${ORIGIN}/v1/delivery/batches`, { method: "POST", body: b, headers: { authorization: "Bearer sh_at_t", "content-type": "application/json", "x-correlation-id": "corr-parity", ...headers } });
  /** X-Request-Id is a fresh UUID per app response (app.ts middleware), so it alone is compared by shape. */
  async function same(direct: Response, through: Response) {
    for (const r of [direct, through]) expect(r.headers.get("x-request-id")).toMatch(/^[0-9a-f-]{36}$/);
    const a = await snapshot(direct, ["x-request-id"]), b = await snapshot(through, ["x-request-id"]);
    expect(b).toEqual(a);
    return { ...b, json: JSON.parse(new TextDecoder().decode(new Uint8Array(b.body))) as { error: { code: string } } };
  }

  test("413 delivery_too_large (over the wire cap)", async () => {
    const { direct, edge } = await pair(() => ({ ledger: new MemoryLedger() }));
    const big = () => new Uint8Array(MAX_DELIVERY_BYTES + 1).fill(0x20);
    const r = await same(await direct.fetch(request(big())), await edge.fetch(request(big())));
    expect(r.status).toBe(413);
    expect(r.json.error.code).toBe("delivery_too_large");
  });

  test("429 delivery_busy keeps the app's Retry-After (tenant slots full)", async () => {
    const { direct, edge } = await pair(() => ({ ledger: new MemoryLedger(), slots: { maxPerUser: 0, maxPerOrg: 0, ttlSeconds: 90 } }));
    const r = await same(await direct.fetch(request(await body())), await edge.fetch(request(await body())));
    expect(r.status).toBe(429);
    expect(r.json.error.code).toBe("delivery_busy");
    expect(new Map(r.headers).get("retry-after")).toBe("5");
  });

  test("429 delivery_busy under INGEST_CLAIM_LOCK_BUSY keeps the app's jittered Retry-After, not the edge's", async () => {
    const failing = () => sqlLedger(async () => { throw Object.assign(new Error("lock"), { code: "55P03" }); });
    const { direct, edge } = await pair(() => ({ ledger: failing(), claimLockBusy: true, random: () => 0.99 }));
    const r = await same(await direct.fetch(request(await body())), await edge.fetch(request(await body())));
    expect(r.status).toBe(429);
    expect(new Map(r.headers).get("retry-after")).toBe("10"); // the edge's own source (0.4) would say 7
  });

  test("503 delivery_unavailable stays without Retry-After", async () => {
    const failing = () => sqlLedger(async () => { throw Object.assign(new Error("timeout"), { code: "57014" }); });
    const { direct, edge } = await pair(() => ({ ledger: failing() }));
    const r = await same(await direct.fetch(request(await body())), await edge.fetch(request(await body())));
    expect(r.status).toBe(503);
    expect(r.json.error.code).toBe("delivery_unavailable");
    expect(new Map(r.headers).has("retry-after")).toBe(false);
  });

  test("409 delivery_conflict (a batch id reused with another digest)", async () => {
    const ledger = () => Object.assign(new MemoryLedger(), { script: [{ kind: "batch_conflict", currentDigest: "d".repeat(64) }] });
    const { direct, edge } = await pair(() => ({ ledger: ledger() }));
    const r = await same(await direct.fetch(request(await body())), await edge.fetch(request(await body())));
    expect(r.status).toBe(409);
  });

  test("4xx with a JSON error body (malformed delivery JSON)", async () => {
    const { direct, edge } = await pair(() => ({ ledger: new MemoryLedger() }));
    const r = await same(await direct.fetch(request("{not json")), await edge.fetch(request("{not json")));
    expect(r.status).toBeGreaterThanOrEqual(400);
    expect(r.status).toBeLessThan(500);
    expect(r.headers.find(([k]) => k === "content-type")?.[1]).toContain("application/json");
    expect(typeof r.json.error.code).toBe("string");
  });
});

describe("request pass-through: what reaches the handler", () => {
  test("an opaque zstd-looking body and its Content-Encoding arrive byte for byte; method and query preserved", async () => {
    const { edge, seen } = scripted(() => new Response(null, { status: 204 }));
    // zstd frame magic (28 b5 2f fd) then bytes that are not a valid frame: the edge never decodes.
    const opaque = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0xff, 0x80, 0x0d, 0x0a, 0x00, 0x7f, 0xfe]);
    await edge.fetch(new Request(`${ORIGIN}/v1/turns/batches?trace=1&b=%2F&b=2`, {
      method: "POST", body: opaque, headers: { authorization: "Bearer sh_at_t", "content-encoding": "zstd", "content-type": "application/json" },
    }));
    expect(seen).toHaveLength(1);
    const r = seen[0]!;
    expect(r.method).toBe("POST");
    expect(r.url).toBe(`${ORIGIN}/v1/turns/batches?trace=1&b=%2F&b=2`);
    expect(r.headers.get("content-encoding")).toBe("zstd");
    expect(r.headers.get("content-type")).toBe("application/json");
    expect([...new Uint8Array(await r.arrayBuffer())]).toEqual([...opaque]);
  });

  test("GET keeps its method and query and carries no body", async () => {
    const { edge, seen } = scripted(() => new Response(null, { status: 204 }));
    await edge.fetch(new Request(`${ORIGIN}/v1/delivery/limits?x=1`, { headers: { authorization: "Bearer sh_at_t" } }));
    expect(seen[0]!.method).toBe("GET");
    expect(new URL(seen[0]!.url).search).toBe("?x=1");
    expect(seen[0]!.body).toBeNull();
  });

  test("every EDGE_STRIPPED_HEADERS entry and every x-relay-ingest-* name is gone; unrelated headers survive", async () => {
    const { edge, seen } = scripted(() => new Response(null, { status: 204 }));
    const spoof: Record<string, string> = {
      "proxy-authorization": "Basic Zm9vOmJhcg==",
      cookie: "session=1",
      [IDENTITY_HEADER]: encodeIdentity({ orgId: "org-evil", workspaceId: "ws-evil", userId: "u", scopes: [SYNC_SCOPE] }),
      [SIGNATURE_HEADER]: "t=1,v1=00",
      [`${EDGE_STRIPPED_PREFIX}admission`]: "queued-v1",
      "X-Relay-Ingest-Anything": "1",
    };
    const keep: Record<string, string> = {
      "content-type": "application/json", "user-agent": "relay-desktop/1.2.3", accept: "application/json",
      "x-relay-client": "desktop", "x-correlation-id": "corr-keep", "x-custom-trace": "abc", "if-none-match": "\"e\"",
    };
    await edge.fetch(post("/v1/delivery/batches", { headers: { ...spoof, ...keep } }));
    const got = seen[0]!.headers;
    for (const name of EDGE_STRIPPED_HEADERS) expect(got.has(name)).toBe(false);
    for (const name of Object.keys(spoof)) if (name.toLowerCase() !== IDENTITY_HEADER) expect(got.has(name)).toBe(false);
    const prefixed = [...got.keys()].filter((k) => k.startsWith(EDGE_STRIPPED_PREFIX));
    // The only x-relay-ingest-* left is the edge's own identity header, naming the verified tenant.
    expect(prefixed).toEqual([IDENTITY_HEADER]);
    expect(got.get(IDENTITY_HEADER)).toBe(encodeIdentity(IDENTITY));
    for (const [k, v] of Object.entries(keep)) expect(got.get(k)).toBe(v);
  });

  test("Authorization never reaches the handler on any route", async () => {
    const { edge, seen } = scripted(() => new Response(null, { status: 204 }));
    await edge.fetch(post("/v1/delivery/batches"));
    await edge.fetch(post("/v1/turns/batches"));
    await edge.fetch(new Request(`${ORIGIN}/v1/delivery/limits`, { headers: { authorization: "Bearer sh_at_t" } }));
    expect(seen).toHaveLength(3);
    for (const r of seen) expect(r.headers.has("authorization")).toBe(false);
  });
});

describe("the edge's own refusals (only what ingest-edge.test.ts does not already cover)", () => {
  const edgeWith = (verify: EdgeVerify) => {
    const seen: Request[] = [];
    return { seen, edge: createIngestEdge(verify, () => ({ fetch: async (r: Request) => { seen.push(r); return new Response(null, { status: 204 }); } }), { random: RANDOM }) };
  };

  test("any non-AuthError thrown by verify is 503 relaycontrol_unavailable with Retry-After 5..10, never 401, handler never called", async () => {
    for (const thrown of [new Error("boom"), new TypeError("fetch failed"), "a string", undefined]) {
      const { edge, seen } = edgeWith(async () => { throw thrown; });
      const res = await edge.fetch(post());
      expect(res.status).toBe(503);
      const after = Number(res.headers.get("retry-after"));
      expect(after).toBe(7);
      expect(after >= INGEST_EDGE_RETRY_AFTER_MIN_S && after <= INGEST_EDGE_RETRY_AFTER_MAX_S).toBe(true);
      expect(res.headers.get("www-authenticate")).toBeNull();
      expect(((await res.json()) as { error: { code: string } }).error.code).toBe("relaycontrol_unavailable");
      expect(seen).toHaveLength(0);
    }
  });

  test("the edge's own refusal shape: JSON, private no-store, correlation echoed; Retry-After only on 503, www-authenticate only on 401", async () => {
    const cases: [AuthError, number][] = [
      [new AuthError(401, "missing_authorization", "m"), 401],
      [new AuthError(401, "unsupported_token", "m"), 401],
      [new AuthError(401, "invalid_token", "m"), 401],
      [new AuthError(401, "token_expired", "m"), 401],
      [new AuthError(403, "forbidden", "m"), 403],
      [new AuthError(503, "relaycontrol_unavailable", "m"), 503],
    ];
    for (const [error, status] of cases) {
      const { edge, seen } = edgeWith(async () => { throw error; });
      const res = await edge.fetch(post("/v1/delivery/batches", { headers: { "x-correlation-id": "corr-own" } }));
      expect(res.status).toBe(status);
      expect(res.headers.get("content-type")).toBe("application/json");
      expect(res.headers.get("cache-control")).toBe("private, no-store");
      expect(res.headers.get("x-correlation-id")).toBe("corr-own");
      expect(res.headers.get("retry-after")).toBe(status === 503 ? "7" : null);
      expect(res.headers.get("www-authenticate")).toBe(status === 401 ? "Bearer" : null);
      expect(await res.json()).toEqual({ error: { code: error.code, message: "m" }, correlationId: "corr-own" });
      expect(seen).toHaveLength(0);
    }
  });

  test("a handler 503 is distinguishable from the edge's own: the edge adds nothing to it", async () => {
    const { edge } = scripted(json(503, { error: { code: "delivery_unavailable", message: "x" }, correlationId: "corr-1" }));
    const res = await edge.fetch(post());
    expect(res.headers.get("retry-after")).toBeNull();
    expect(res.headers.get("cache-control")).toBeNull();
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("delivery_unavailable");
  });
});
