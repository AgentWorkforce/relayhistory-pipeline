// Q3 private Cloud publication receiver (design §9): POST /v1/acquisition/cloud/batches on the
// ops listener, HELD behind ACQUISITION_CLOUD_PUBLICATION_ENABLED. One injectable fetch serves
// both relaycontrol introspection (the real acquisition verifier) and the private Ingest
// destination, so "zero upstream calls" is an exact count of requests to the Ingest origin.
import { expect, test } from "bun:test";
import { acquisitionGrantDigest } from "../../session-ingest/acquisition-assertion.ts";
import { encodeIdentity, signedBindingIdentity } from "../../session-ingest/app.ts";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";
import { ALLOWED_LOG_FIELDS } from "../../shared/log.ts";
import { createAcquisitionApp, type CloudPublicationGate } from "../app.ts";
import { CLOUD_BATCHES_PATH, MAX_CLOUD_BATCH_BYTES, MAX_CLOUD_ENVELOPE_BYTES, createCloudBatchReceiver } from "../cloud-batches.ts";
import { registryOfClass } from "../key-registry.ts";
import { createAcquisitionVerifier } from "../verifier.ts";
import { ORG, ORIGIN, USER, WS, captureLog, cloudInstall, envelope, leaks, nativeHarness, nativeKey, secrets } from "./helpers.ts";
import { registry } from "./helpers.ts";

const G = "rhg_" + "0123456789abcdef".repeat(4);
const OTHER_G = "rhg_" + "fedcba9876543210".repeat(4);
const SIGNING_KEY = "test-ingest-signing-key-not-real";
const INTROSPECT_SECRET = "test-introspect-secret-not-real";
const INGEST = "https://ingest.example.test";
const INTROSPECT = "https://relaycontrol.example.test";
const NOW = 1_800_000_000;
const TOKEN = "sh_wk_cloudoperator";
const OTHER_WS_TOKEN = "sh_wk_otherworkspace";
for (const s of [G, OTHER_G, SIGNING_KEY, INTROSPECT_SECRET, TOKEN, OTHER_WS_TOKEN]) secrets.add(s);

const ACCOUNT = await deliveryAccount({ orgId: ORG, workspaceId: WS });
const SID = "sess-0001";
const GRANT_ID = "00000000-0000-4000-8000-000000000001";

test("fixture: the helpers' install grantSha256 is the §3 hash of G", () => {
  expect(acquisitionGrantDigest({ orgId: ORG, workspaceId: WS, source: "droid", sessionId: SID }, GRANT_ID, G)).toBe(cloudInstall().grantSha256);
});

// ---------------------------------------------------------------- fixtures

type Rec = Record<string, unknown>;
const record = (i: number, over: Rec = {}): Rec => ({
  revision_id: `rev-${i}`, source: "droid", session_id: SID, origin_id: "origin_test", content_sha256: "a".repeat(64), ...over,
});
function request(over: { batch?: Rec; entry?: Rec; records?: Rec[]; top?: Rec } = {}): Rec {
  return {
    protocolVersion: 1,
    batch: { batch_id: "batch-0001", account_id: ACCOUNT, origin_id: "origin_test", instance_id: "origin_test", records: over.records ?? [record(1), record(2)], ...over.batch },
    acquisition: [{ source: "droid", sessionId: SID, grantId: GRANT_ID, ownerEpoch: "1", grant: G, ...over.entry }],
    ...over.top,
  };
}
const bytesOf = (value: unknown) => Buffer.from(JSON.stringify(value), "utf8");
function wrap(batchBytes: Uint8Array | string, install: unknown = envelope("install", cloudInstall())): Rec {
  const b64 = typeof batchBytes === "string" ? batchBytes : Buffer.from(batchBytes).toString("base64url");
  return { v: 1, install, batchBodyBase64url: b64 };
}

// ---------------------------------------------------------------- harness

type UpstreamCall = { url: string; init: RequestInit; headers: Headers; body: Buffer };
type Answer = (call: UpstreamCall) => Response | Promise<Response>;
const receiptFor = (call: UpstreamCall) => {
  const req = JSON.parse(call.body.toString("utf8"));
  // Odd spacing and key order: the pass-through must be byte-exact, never re-serialized.
  return `{ "receiptId":"rcpt-1",  "protocolVersion":1,"batchId":${JSON.stringify(req.batch.batch_id)},"acceptedRevisionIds":${JSON.stringify(req.batch.records.map((r: Rec) => r.revision_id))},"unsupportedRevisionIds":[],"acceptanceLevel":"durable" }`;
};
const ok: Answer = (c) => new Response(receiptFor(c), { status: 200, headers: { "content-type": "application/json" } });

function harness(opts: { gate?: "disabled" | "unconfigured" | "absent"; upstream?: Answer; master?: boolean; timeoutMs?: number } = {}) {
  const introspections: Rec[] = [];
  const upstream: UpstreamCall[] = [];
  const other: string[] = [];
  let answer = opts.upstream ?? ok;
  const fakeFetch = (async (input: string | URL | Request, init: RequestInit = {}) => {
    const url = String(input);
    if (url.startsWith(INTROSPECT + "/")) {
      const body = JSON.parse(String(init.body)) as Rec;
      introspections.push(body);
      const token = body.token;
      if (token === "sh_wk_down") throw new Error("connect ECONNREFUSED");
      const workspace = token === TOKEN ? WS : token === OTHER_WS_TOKEN ? "ws_other" : null;
      if (!workspace) return Response.json({ active: false });
      return Response.json({
        active: true, principal_type: "service", org: ORG, workspace, scopes: ["acquisition:operate"], aud: ORIGIN,
        iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 600, token_id: "tok_operator",
      });
    }
    if (url.startsWith(INGEST + "/")) {
      const body = Buffer.from(init.body as Uint8Array);
      const call = { url, init, headers: new Headers(init.headers), body };
      upstream.push(call);
      const signal = init.signal;
      return Promise.race([
        answer(call),
        new Promise<Response>((_, reject) => signal?.addEventListener("abort", () => reject(signal.reason ?? new Error("aborted")))),
      ]);
    }
    other.push(url);
    throw new Error("unexpected destination");
  }) as unknown as typeof fetch;
  const authenticator = createAcquisitionVerifier({ url: INTROSPECT + "/", secret: INTROSPECT_SECRET, origin: ORIGIN, fetch: fakeFetch, timeoutMs: 200 });
  const receiver = createCloudBatchReceiver({
    registry: registryOfClass(registry, "cloud"), ingestOrigin: INGEST, signingKey: { reveal: () => SIGNING_KEY },
    fetch: fakeFetch, nowSeconds: () => NOW, timeoutMs: opts.timeoutMs ?? 200,
  });
  const gate: CloudPublicationGate | undefined = opts.gate === "absent" ? undefined
    : opts.gate === "disabled" ? { state: "disabled" } : opts.gate === "unconfigured" ? { state: "unconfigured" } : { state: "ready", receiver };
  const captured = captureLog();
  // groupEnabled false: the Q3 route is independent of ACQUISITION_OPS_ENABLED (P2).
  const app = opts.master === false
    ? createAcquisitionApp({ enabled: false, group: "ops", log: captured.log })
    : createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: false, authenticator, installer: null, log: captured.log, ...(gate ? { cloud: gate } : {}) });
  const texts: string[] = [];
  async function post(body: unknown, o: { token?: string | null; headers?: Record<string, string> } = {}) {
    const headers: Record<string, string> = { "content-type": "application/json", ...(o.headers ?? {}) };
    const token = o.token === undefined ? TOKEN : o.token;
    if (token) headers.authorization = `Bearer ${token}`;
    const raw = typeof body === "string" || body instanceof Uint8Array ? body : JSON.stringify(body);
    const res = await app.request(CLOUD_BATCHES_PATH, { method: "POST", headers, body: raw });
    const text = await res.text();
    texts.push(text);
    return { status: res.status, text, json: (() => { try { return JSON.parse(text); } catch { return null; } })(), headers: res.headers };
  }
  return {
    app, post, introspections, upstream, other, captured, texts,
    setUpstream: (a: Answer) => { answer = a; },
    /** Every response body and log line, for the secret scan. */
    allText: () => [...texts, ...captured.lines.map((l) => JSON.stringify(l))],
  };
}

const refusal = (code: string) => ({ error: { code } });

function expectClean(h: ReturnType<typeof harness>, extra: string[] = []) {
  for (const s of extra) secrets.add(s);
  expect(leaks(h.allText())).toEqual([]);
  for (const line of h.captured.lines) for (const k of Object.keys(line.fields)) expect(ALLOWED_LOG_FIELDS.has(k)).toBe(true);
  expect(h.other).toEqual([]);
}

// ---------------------------------------------------------------- gate

test("flag off (default), absent receiver and master off: static 503 acquisition_disabled, zero introspection or upstream calls", async () => {
  for (const o of [{ gate: "disabled" as const }, { gate: "absent" as const }, { master: false }]) {
    const h = harness(o);
    const r = await h.post(wrap(bytesOf(request())));
    expect([r.status, r.json, r.headers.get("retry-after")]).toEqual([503, refusal("acquisition_disabled"), "60"]);
    expect([h.introspections.length, h.upstream.length]).toEqual([0, 0]);
    expectClean(h);
  }
});

test("flag on but config missing: 503 acquisition_cloud_unconfigured, never a fallback publisher or credential", async () => {
  const h = harness({ gate: "unconfigured" });
  const r = await h.post(wrap(bytesOf(request())));
  expect([r.status, r.json, r.headers.get("retry-after")]).toEqual([503, refusal("acquisition_cloud_unconfigured"), "5"]);
  expect([h.introspections.length, h.upstream.length]).toEqual([0, 0]);
});

test("receiver refuses a non-origin ingest destination at construction", () => {
  expect(() => createCloudBatchReceiver({ registry, ingestOrigin: INGEST + "/v1", signingKey: { reveal: () => SIGNING_KEY } })).toThrow();
});

test("the route exists only on the ops listener: the native (Desktop) listener answers 404", async () => {
  const n = nativeHarness();
  const r = await n.app.request(CLOUD_BATCHES_PATH, { method: "POST", headers: { "content-type": "application/json", authorization: "Bearer sh_at_alice" }, body: "{}" });
  expect(r.status).toBe(404);
  // Other methods on the ops path fall through to the ops catch-all, never the receiver.
  const h = harness();
  expect((await h.app.request(CLOUD_BATCHES_PATH, { method: "GET", headers: { authorization: `Bearer ${TOKEN}` } })).status).not.toBe(200);
  expect(h.upstream.length).toBe(0);
});

// ---------------------------------------------------------------- authentication

test("authentication precedes decoding: missing, wrong-class or refused bearer is 403 not_authorized even for a garbage batch", async () => {
  const h = harness();
  const garbage = { v: 1, install: { payload: {}, signature: "x" }, batchBodyBase64url: "!!!not-base64!!!" };
  for (const token of [null, "sh_at_desktopuser", "sh_wk_unknown"]) {
    const r = await h.post(garbage, { token });
    expect([r.status, r.json]).toEqual([403, refusal("not_authorized")]);
  }
  expect(h.upstream.length).toBe(0);
  expectClean(h);
});

test("introspection unavailable is 503 introspection_unavailable, not a grant refusal; zero upstream calls", async () => {
  const h = harness();
  const r = await h.post(wrap(bytesOf(request())), { token: "sh_wk_down" });
  expect([r.status, r.json]).toEqual([503, refusal("introspection_unavailable")]);
  expect(h.upstream.length).toBe(0);
});

test("cross-workspace operator: a valid install for ws_test presented with a ws_other operator is 403 target_mismatch, zero upstream", async () => {
  const h = harness();
  const r = await h.post(wrap(bytesOf(request())), { token: OTHER_WS_TOKEN });
  expect([r.status, r.json]).toEqual([403, refusal("target_mismatch")]);
  // And an install signed for another workspace, presented by the ws_test operator.
  const other = envelope("install", cloudInstall({ workspaceId: "ws_other" }));
  const r2 = await h.post(wrap(bytesOf(request()), other));
  expect([r2.status, r2.json]).toEqual([403, refusal("target_mismatch")]);
  expect(h.upstream.length).toBe(0);
});

test("body-supplied identity, key, class or headers authorize nothing", async () => {
  const h = harness();
  const install = envelope("install", cloudInstall());
  // Extra outer or install keys (identity, public key, issuer class) are refused outright.
  for (const body of [
    { ...wrap(bytesOf(request()), install), identity: { orgId: ORG, workspaceId: WS, userId: "attacker" } },
    { ...wrap(bytesOf(request()), install), publicKey: "MCowBQYDK2VwAyEA" },
    wrap(bytesOf(request()), { ...install, issuerClass: "cloud" }),
  ]) {
    const r = await h.post(body);
    expect([r.status, r.json]).toEqual([400, refusal("invalid_request")]);
  }
  // An unknown issuer key id is not trusted however the body describes it.
  const unknown = envelope("install", cloudInstall({ issuerKeyId: "attacker-key" }), nativeKey);
  expect((await h.post(wrap(bytesOf(request()), unknown))).status).toBe(403);
  // Extra keys inside the decoded request (an identity sibling) are refused.
  const r = await h.post(wrap(bytesOf(request({ top: { identity: { userId: "attacker" } } }))));
  expect([r.status, r.json]).toEqual([400, refusal("invalid_request")]);
  expect(h.upstream.length).toBe(0);
  // Forged ingest identity/signature headers on the caller request are never forwarded.
  const forged = encodeIdentity({ userId: "attacker", orgId: "org_attacker", workspaceId: "ws_attacker", scopes: ["rth:sync"] });
  const ok200 = await h.post(wrap(bytesOf(request())), { headers: { "x-relay-ingest-identity": forged, "x-relay-ingest-signature": "t=1,v1=" + "0".repeat(64), cookie: "session=caller", "x-forwarded-for": "203.0.113.9" } });
  expect(ok200.status).toBe(200);
  const sent = h.upstream[0]!;
  expect(sent.headers.get("x-relay-ingest-identity")).not.toBe(forged);
  const identity = signedBindingIdentity(SIGNING_KEY, () => NOW)(new Request(sent.url, { method: "POST", headers: sent.headers }), sent.body);
  expect(identity).toEqual({ userId: USER, orgId: ORG, workspaceId: WS, scopes: ["rth:sync"] });
});

// ---------------------------------------------------------------- assertion and binding refusals (all zero upstream)

test("wrong issuer class / signature / G / epoch / SID / origin / account: refused with zero upstream calls", async () => {
  const h = harness();
  const good = envelope("install", cloudInstall());
  const flipped = { ...good, signature: (good.signature[0] === "A" ? "B" : "A") + good.signature.slice(1) };
  const nativeClass = envelope("install", cloudInstall({ ownerKind: "native", bindingUuid: "", installationId: "00000000-0000-4000-8000-000000000003", issuerKeyId: "native-key-1" }), nativeKey);
  const cloudKeyClaimsNative = envelope("install", cloudInstall({ ownerKind: "native", bindingUuid: "", installationId: "00000000-0000-4000-8000-000000000003" }));
  const tamperedA = { ...good, payload: { ...good.payload, ownerOriginId: "origin_attacker" } };
  const cases: Array<[string, Rec, number, string]> = [
    ["native issuer class", wrap(bytesOf(request()), nativeClass), 403, "not_authorized"],
    ["cloud key claiming native class", wrap(bytesOf(request()), cloudKeyClaimsNative), 403, "not_authorized"],
    ["flipped signature", wrap(bytesOf(request()), flipped), 403, "not_authorized"],
    ["A edited after signing", wrap(bytesOf(request({ batch: { origin_id: "origin_attacker", instance_id: "origin_attacker" }, records: [record(1, { origin_id: "origin_attacker" })] })), tamperedA), 403, "not_authorized"],
    ["wrong G", wrap(bytesOf(request({ entry: { grant: OTHER_G } }))), 403, "not_authorized"],
    ["epoch not expected+1", wrap(bytesOf(request({ entry: { ownerEpoch: "2" } }))), 403, "target_mismatch"],
    ["epoch equal to expected", wrap(bytesOf(request({ entry: { ownerEpoch: "0" } }))), 400, "invalid_request"],
    ["grantId not operationUuid", wrap(bytesOf(request({ entry: { grantId: "00000000-0000-4000-8000-000000000099" } }))), 403, "target_mismatch"],
    ["entry SID", wrap(bytesOf(request({ entry: { sessionId: "sess-other" } }))), 403, "target_mismatch"],
    ["entry source", wrap(bytesOf(request({ entry: { source: "claude" } }))), 403, "target_mismatch"],
    ["record SID", wrap(bytesOf(request({ records: [record(1), record(2, { session_id: "sess-other" })] }))), 403, "target_mismatch"],
    ["record source", wrap(bytesOf(request({ records: [record(1, { source: "claude" })] }))), 403, "target_mismatch"],
    ["record origin", wrap(bytesOf(request({ records: [record(1, { origin_id: "origin_other" })] }))), 403, "target_mismatch"],
    ["batch origin", wrap(bytesOf(request({ batch: { origin_id: "origin_other" } }))), 403, "target_mismatch"],
    ["batch instance", wrap(bytesOf(request({ batch: { instance_id: "origin_other" } }))), 403, "target_mismatch"],
    ["account", wrap(bytesOf(request({ batch: { account_id: "relayhistory:" + "0".repeat(64) } }))), 403, "target_mismatch"],
    ["install source not droid", wrap(bytesOf(request({ entry: { source: "claude" }, records: [record(1, { source: "claude" })] })),
      envelope("install", cloudInstall({ source: "claude" }))), 403, "target_mismatch"],
    ["duplicate revision ids", wrap(bytesOf(request({ records: [record(1), record(1)] }))), 400, "invalid_request"],
    ["two acquisition entries", wrap(bytesOf({ ...request(), acquisition: [request().acquisition, request().acquisition].flat() })), 400, "invalid_request"],
    ["malformed G", wrap(bytesOf(request({ entry: { grant: "rhg_ABC" } }))), 400, "invalid_request"],
    ["number epoch", wrap(bytesOf(request({ entry: { ownerEpoch: 1 } }))), 400, "invalid_request"],
    ["protocolVersion 2", wrap(bytesOf(request({ top: { protocolVersion: 2 } }))), 400, "invalid_request"],
    ["acquisition nested in batch", wrap(bytesOf((() => { const r = request(); (r.batch as Rec).acquisition = r.acquisition; delete r.acquisition; return r; })())), 400, "invalid_request"],
  ];
  for (const [name, body, status, code] of cases) {
    const r = await h.post(body);
    expect([name, r.status, r.json]).toEqual([name, status, refusal(code)]);
  }
  expect(h.upstream.length).toBe(0);
  expectClean(h, [good.signature]);
});

test("bigint epochs above 2^53 are exact: expected+1 accepted, the Number-rounded answer refused", async () => {
  const h = harness();
  const install = envelope("install", cloudInstall({ expectedOwnerEpoch: "9007199254740993" }));
  // Number("9007199254740993") is ...992 and ...992 + 1 rounds back to ...992: that is what Number
  // arithmetic would accept; ...993 is "no increment"; ...995 is off by one.
  for (const wrong of ["9007199254740992", "9007199254740993", "9007199254740995"]) {
    const r = await h.post(wrap(bytesOf(request({ entry: { ownerEpoch: wrong } })), install));
    expect([wrong, r.status, r.json]).toEqual([wrong, 403, refusal("target_mismatch")]);
  }
  expect(h.upstream.length).toBe(0);
  const r = await h.post(wrap(bytesOf(request({ entry: { ownerEpoch: "9007199254740994" } })), install));
  expect(r.status).toBe(200);
  const max = envelope("install", cloudInstall({ expectedOwnerEpoch: "9223372036854775806" }));
  expect((await h.post(wrap(bytesOf(request({ entry: { ownerEpoch: "9223372036854775807" } })), max))).status).toBe(200);
  expect((await h.post(wrap(bytesOf(request({ entry: { ownerEpoch: "9223372036854775808" } })), max))).status).toBe(400);
  expect(h.upstream.length).toBe(2);
});

// ---------------------------------------------------------------- envelope, base64url, UTF-8, JSON

/** A request whose byte length is 1 mod 3, so its unpadded base64url ends in a 2-char group with 4 spare bits. */
function oddBytes(): Buffer {
  let pad = 0;
  for (;;) {
    const b = bytesOf(request({ records: [record(1, { note: " ".repeat(pad) })] }));
    if (b.byteLength % 3 === 1) return b;
    pad++;
  }
}

test("padded, noncanonical, standard-alphabet or whitespace base64url is 400; nothing is forwarded", async () => {
  const h = harness();
  const bytes = oddBytes();
  const canonical = bytes.toString("base64url");
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const last = canonical.at(-1)!;
  const nonzeroTail = canonical.slice(0, -1) + alphabet[alphabet.indexOf(last) + 1];
  expect(Buffer.from(nonzeroTail, "base64url").equals(bytes)).toBe(true); // Node decodes both spellings to the same bytes
  for (const b64 of [canonical + "==", nonzeroTail, bytes.toString("base64"), canonical.slice(0, 8) + " " + canonical.slice(8), canonical + "A"]) {
    const r = await h.post(wrap(b64));
    expect([r.status, r.json]).toEqual([400, refusal("invalid_request")]);
  }
  expect(h.upstream.length).toBe(0);
  expect((await h.post(wrap(canonical))).status).toBe(200);
});

test("outer envelope: exact keys, v:1, strict UTF-8 and JSON; empty or malformed batch is 400", async () => {
  const h = harness();
  const install = envelope("install", cloudInstall());
  const b64 = bytesOf(request()).toString("base64url");
  for (const body of [
    { v: 1, install },
    { v: 2, install, batchBodyBase64url: b64 },
    { v: "1", install, batchBodyBase64url: b64 },
    { v: 1, install, batchBodyBase64url: b64, extra: true },
    [1, install, b64],
    "{not json",
    new Uint8Array([0x7b, 0xff, 0x7d]),
    Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), bytesOf({ v: 1, install, batchBodyBase64url: b64 })]),
    wrap(""),
    wrap(Buffer.from([0xff, 0xfe, 0x7b, 0x7d])),
    wrap(Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), bytesOf(request())])),
    wrap(Buffer.from("{\"protocolVersion\":1,")),
    wrap(bytesOf(request({ records: [] }))),
    wrap(bytesOf(request({ records: ["rev-1" as unknown as Rec] }))),
  ]) {
    const r = await h.post(body);
    expect([r.status, r.json]).toEqual([400, refusal("invalid_request")]);
  }
  expect(h.upstream.length).toBe(0);
});

test("Content-Type must be application/json and Content-Encoding identity (415), before authentication", async () => {
  const h = harness();
  const body = wrap(bytesOf(request()));
  expect((await h.post(body, { headers: { "content-type": "text/plain" } })).status).toBe(415);
  expect((await h.post(body, { headers: { "content-encoding": "gzip" } })).status).toBe(415);
  expect([h.introspections.length, h.upstream.length]).toEqual([0, 0]);
});

// ---------------------------------------------------------------- bounds

test("declared outer body over 2,100,000 bytes is 413 before authentication", async () => {
  const h = harness();
  for (const declared of [String(MAX_CLOUD_ENVELOPE_BYTES + 1), "abc", "-1"]) {
    const res = await h.app.fetch(new Request("http://localhost" + CLOUD_BATCHES_PATH, {
      method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}`, "content-length": declared }, body: "{}",
    }));
    expect([res.status, await res.json()]).toEqual([413, refusal("payload_too_large")]);
  }
  expect([h.introspections.length, h.upstream.length]).toEqual([0, 0]);
});

test("streamed outer body over 2,100,000 bytes (no Content-Length) is 413 and the stream is cancelled", async () => {
  const h = harness();
  let sent = 0;
  let cancelled = false;
  const chunk = new Uint8Array(64 * 1024).fill(0x20);
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (sent > 8 * MAX_CLOUD_ENVELOPE_BYTES) return controller.close();
      sent += chunk.byteLength;
      controller.enqueue(chunk);
    },
    cancel() { cancelled = true; },
  });
  const req = new Request("http://localhost" + CLOUD_BATCHES_PATH, {
    method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` }, body: stream, duplex: "half",
  } as RequestInit);
  expect(req.headers.get("content-length")).toBeNull();
  const res = await h.app.fetch(req);
  expect([res.status, await res.json()]).toEqual([413, refusal("payload_too_large")]);
  expect(cancelled).toBe(true);
  expect(sent).toBeLessThan(MAX_CLOUD_ENVELOPE_BYTES + 2 * chunk.byteLength);
  expect(h.upstream.length).toBe(0);
});

/** A request padded with one filler field to exactly `size` bytes. */
function sized(size: number): Buffer {
  const base = bytesOf(request({ records: [record(1, { note: "" })] })).byteLength;
  const b = bytesOf(request({ records: [record(1, { note: "x".repeat(size - base) })] }));
  expect(b.byteLength).toBe(size);
  return b;
}

test("decoded batch: exactly 1,500,000 bytes forwards, 1,500,001 is 413 with zero upstream calls", async () => {
  const h = harness();
  const over = await h.post(wrap(sized(MAX_CLOUD_BATCH_BYTES + 1)));
  expect([over.status, over.json]).toEqual([413, refusal("payload_too_large")]);
  // Oversized encoded text is refused before decoding.
  const huge = await h.post(wrap("A".repeat(2_000_004)));
  expect([huge.status, huge.json]).toEqual([413, refusal("payload_too_large")]);
  expect(h.upstream.length).toBe(0);
  const exact = sized(MAX_CLOUD_BATCH_BYTES);
  const r = await h.post(wrap(exact));
  expect(r.status).toBe(200);
  expect(h.upstream[0]!.body.equals(exact)).toBe(true);
});

test("500 records forward; 501 records is 413 with zero upstream calls", async () => {
  const h = harness();
  const many = (n: number) => Array.from({ length: n }, (_, i) => record(i));
  const r501 = await h.post(wrap(bytesOf(request({ records: many(501) }))));
  expect([r501.status, r501.json]).toEqual([413, refusal("payload_too_large")]);
  expect(h.upstream.length).toBe(0);
  const r500 = await h.post(wrap(bytesOf(request({ records: many(500) }))));
  expect(r500.status).toBe(200);
  expect(h.upstream.length).toBe(1);
});

// ---------------------------------------------------------------- forwarding

test("happy path: exact decoded bytes forwarded with rebuilt internal headers only; the 200 receipt passes through verbatim", async () => {
  const h = harness();
  // Unusual but valid JSON spelling: the upstream must receive these bytes, not a re-serialization.
  const exact = Buffer.from(JSON.stringify(request(), null, 1).replace(/\n/g, "\r\n"), "utf8");
  const body = wrap(exact);
  secrets.add(body.batchBodyBase64url as string);
  const r = await h.post(body, { headers: { "x-correlation-id": "corr-q3-1" } });
  expect(r.status).toBe(200);
  expect(h.upstream.length).toBe(1);
  const sent = h.upstream[0]!;
  expect(sent.url).toBe(INGEST + "/v1/delivery/batches");
  expect(sent.init.method).toBe("POST");
  expect(sent.init.redirect).toBe("manual");
  expect(sent.body.equals(exact)).toBe(true);
  expect([...sent.headers.keys()].sort()).toEqual(["accept", "content-type", "x-correlation-id", "x-relay-ingest-identity", "x-relay-ingest-signature"]);
  expect(sent.headers.get("x-correlation-id")).toBe("corr-q3-1");
  // The ingest's own verifier accepts the signature, and the identity comes from A + operator scope.
  const identity = signedBindingIdentity(SIGNING_KEY, () => NOW)(new Request(sent.url, { method: "POST", headers: sent.headers }), sent.body);
  expect(identity).toEqual({ userId: USER, orgId: ORG, workspaceId: WS, scopes: ["rth:sync"] });
  // Verbatim receipt bytes and JSON content type; no wrapper.
  expect(r.text).toBe(receiptFor(sent));
  expect(r.headers.get("content-type")).toContain("application/json");
  expect(r.headers.get("x-correlation-id")).toBe("corr-q3-1");
  expect(r.headers.get("cache-control")).toBe("no-store");
  const line = h.captured.lines.at(-1)!;
  expect(line.fields).toMatchObject({ route: "cloud POST batches", status: 200, outcome: "ok", org_id: ORG, workspace_id: WS, count: 2, reason: "upstream_200" });
  expectClean(h, [exact.toString("base64url")]);
});

test("byte-identical retry after a lost response forwards the same bytes again (claim identity is the idempotency key)", async () => {
  const h = harness();
  const body = wrap(bytesOf(request()));
  const first = await h.post(body);
  const second = await h.post(body);
  expect([first.status, second.status]).toEqual([200, 200]);
  expect(h.upstream.length).toBe(2);
  expect(h.upstream[0]!.body.equals(h.upstream[1]!.body)).toBe(true);
  expect(second.text).toBe(first.text);
});

test("ordinary ingest errors pass through verbatim, including 409 acquisition_refused and a numeric Retry-After", async () => {
  const refused = `{"error":{"code":"acquisition_refused"},"refused":[{"source":"droid","sessionId":"${SID}","reason":"owner_changed"}]}`;
  const h = harness({ upstream: () => new Response(refused, { status: 409, headers: { "retry-after": "7", "content-type": "application/json" } }) });
  const r = await h.post(wrap(bytesOf(request())));
  expect([r.status, r.text, r.headers.get("retry-after")]).toEqual([409, refused, "7"]);
  expect(h.captured.lines.at(-1)!.fields).toMatchObject({ status: 409, outcome: "refused", error_code: "acquisition_refused", reason: "upstream_409" });
  for (const [status, retry] of [[429, "3"], [503, "11"], [400, null], [422, null], [413, null]] as const) {
    const text = `{"error":{"code":"x_${status}"}}`;
    h.setUpstream(() => new Response(text, { status, headers: retry ? { "retry-after": retry } : {} }));
    const e = await h.post(wrap(bytesOf(request())));
    expect([e.status, e.text, e.headers.get("retry-after")]).toEqual([status, text, retry ?? (status === 503 ? "5" : null)]);
  }
  // A non-numeric Retry-After is not forwarded.
  h.setUpstream(() => new Response("{}", { status: 429, headers: { "retry-after": "Wed, 21 Oct 2015 07:28:00 GMT" } }));
  expect((await h.post(wrap(bytesOf(request())))).headers.get("retry-after")).toBeNull();
});

test("unknown outcomes are 503: transport failure, timeout, redirect, internal-hop 401, invalid or oversized receipt", async () => {
  const cases: Array<[string, Answer, string]> = [
    ["transport", () => { throw new TypeError("fetch failed: ECONNRESET"); }, "ingest_unavailable"],
    ["timeout", () => new Promise<Response>(() => {}), "ingest_unavailable"],
    ["redirect", () => new Response(null, { status: 307, headers: { location: "https://elsewhere.example.test/" } }), "invalid_upstream_receipt"],
    ["202", () => new Response("{}", { status: 202 }), "invalid_upstream_receipt"],
    ["401", () => new Response(`{"error":{"code":"invalid_token"}}`, { status: 401 }), "ingest_unavailable"],
    ["not json", () => new Response("<html>", { status: 200 }), "invalid_upstream_receipt"],
    ["wrong batch id", (c) => new Response(receiptFor(c).replace("batch-0001", "batch-9999"), { status: 200 }), "invalid_upstream_receipt"],
    ["foreign revision", () => new Response(`{"batchId":"batch-0001","acceptedRevisionIds":["rev-1","rev-other"]}`, { status: 200 }), "invalid_upstream_receipt"],
    ["duplicate revision", () => new Response(`{"batchId":"batch-0001","acceptedRevisionIds":["rev-1","rev-1"]}`, { status: 200 }), "invalid_upstream_receipt"],
    ["oversized", () => new Response("x".repeat(1024 * 1024 + 1), { status: 200 }), "ingest_unavailable"],
    ["oversized declared", () => new Response("{}", { status: 409, headers: { "content-length": String(1024 * 1024 + 1) } }), "ingest_unavailable"],
  ];
  for (const [name, answer, code] of cases) {
    const h = harness({ upstream: answer, timeoutMs: 50 });
    const r = await h.post(wrap(bytesOf(request())));
    expect([name, r.status, r.json, r.headers.get("retry-after")]).toEqual([name, 503, refusal(code), "5"]);
    expect(h.upstream.length).toBe(1);
    expectClean(h);
  }
});

test("no secret in any error body or log line across the whole matrix", async () => {
  const h = harness();
  const install = envelope("install", cloudInstall());
  secrets.add(install.signature);
  const body = wrap(bytesOf(request()), install);
  secrets.add(body.batchBodyBase64url as string);
  await h.post(body);
  await h.post(wrap(bytesOf(request({ entry: { grant: OTHER_G } })), install));
  await h.post(body, { token: OTHER_WS_TOKEN });
  await h.post({ ...body, extra: G });
  h.setUpstream(() => { throw new Error(`boom ${SIGNING_KEY} ${G}`); });
  await h.post(body);
  expect(h.captured.lines.length).toBe(5);
  expectClean(h);
  // Introspection carried the bearer only to relaycontrol; the ingest never saw it.
  for (const c of h.upstream) expect([...c.headers.values()].some((v) => v.includes(TOKEN))).toBe(false);
});
