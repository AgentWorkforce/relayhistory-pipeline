import { describe, expect, test } from "bun:test";
import { gzipSync } from "node:zlib";
import {
  INGEST_UPSTREAM_ASSERTION_PREFIX, READ_API_UPSTREAM_ASSERTION_PREFIX, assertUpstreamAssertionOff, deliveryBatchScope, ingestRecordsScope,
  sessionScope, sessionSelectionScope, sha256Hex, upstreamAssertionBypass, upstreamAssertionConfig, upstreamAssertionMode, upstreamAssertionRefusal,
  verifyUpstreamAssertionBody, verifyUpstreamAssertionPhase1, verifyUpstreamAssertionScope, type UpstreamAssertionConfig, type UpstreamAssertionPhase1,
} from "../upstream-assertion.ts";
import { claimsFor, signAssertion, signSegment, testKey, tokenFromJson, type SignInput, type TestKey } from "./upstream-assertion-signer.ts";

const NOW_S = 1_900_000_000;
const NOW_MS = NOW_S * 1000 + 999; // whole seconds: the fraction is dropped
const AUTH = "Bearer sh_at_reader-token";

const current = testKey("k-current");
const previous = testKey("k-previous");

function configOf(...keys: TestKey[]): UpstreamAssertionConfig {
  const env: NodeJS.ProcessEnv = { READ_API_UPSTREAM_ASSERTION_MODE: "enforce", READ_API_UPSTREAM_ASSERTION_PUBLIC_KEY: keys[0]!.publicB64, READ_API_UPSTREAM_ASSERTION_KID: keys[0]!.kid };
  if (keys[1]) Object.assign(env, { READ_API_UPSTREAM_ASSERTION_PUBLIC_KEY_PREVIOUS: keys[1].publicB64, READ_API_UPSTREAM_ASSERTION_KID_PREVIOUS: keys[1].kid });
  return upstreamAssertionConfig(env, READ_API_UPSTREAM_ASSERTION_PREFIX)!;
}
const config = configOf(current, previous);

type Req = { method?: string; target?: string; authorization?: string | undefined };
function readToken(over: Partial<SignInput> = {}, req: Req = {}) {
  const target = req.target ?? "/v1/sessions/s-1/events";
  return signAssertion({ key: current, aud: "session-read-api", method: req.method ?? "GET", path: target, authorization: AUTH, access: "read", sessions: ["s-1"], nowS: NOW_S, ...over });
}
function phase1(token: string | undefined, req: Req = {}, audience: "session-read-api" | "ingest-edge" = "session-read-api", cfg = config, nowMs = NOW_MS): UpstreamAssertionPhase1 {
  return verifyUpstreamAssertionPhase1(cfg, audience, {
    method: req.method ?? "GET",
    target: req.target ?? "/v1/sessions/s-1/events",
    authorization: "authorization" in req ? req.authorization : AUTH,
    assertion: token,
  }, nowMs);
}
const reason = (r: UpstreamAssertionPhase1) => (r.ok ? "ok" : r.reason);

describe("phase 1: token shape and signature", () => {
  test("a valid read assertion passes", () => {
    expect(reason(phase1(readToken()))).toBe("ok");
  });
  test("missing and empty headers are 'missing'", () => {
    expect(reason(phase1(undefined))).toBe("missing");
    expect(reason(phase1(""))).toBe("missing");
  });
  test("bad shapes are bad_format", () => {
    const good = readToken();
    const [, payload, sig] = good.split(".");
    for (const bad of [
      "v2." + payload + "." + sig, `v1.${payload}`, `v1..${sig}`, `v1.${payload}.${sig}.x`, `v1.${payload}.${sig!.slice(1)}`, `v1.${payload}.${sig}A`,
      `v1.${payload}=.${sig}`, `v1.${payload}.${sig!.slice(0, 85)}=`, ` ${good}`, `${good} `,
    ]) expect(reason(phase1(bad))).toBe("bad_format");
    expect(reason(phase1([good, good] as unknown as string))).toBe("bad_format");
  });
  test("a non-canonical base64url signature (trailing bits set) is bad_format", () => {
    const [, payload, sig] = readToken().split(".");
    const last = sig!.at(-1)!;
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    // 86 chars carry 516 bits for 512: the last char's low 4 bits must be zero; flip one of them.
    const tweaked = alphabet[alphabet.indexOf(last) ^ 1]!;
    expect(reason(phase1(`v1.${payload}.${sig!.slice(0, 85)}${tweaked}`))).toBe("bad_format");
  });
  test("an oversize payload segment is bad_format", () => {
    const json = JSON.stringify({ ...claimsFor({ key: current, aud: "session-read-api", method: "GET", path: "/v1/sessions/s-1/events", authorization: AUTH, access: "read", sessions: ["s-1"], nowS: NOW_S }), pad: "x".repeat(13_000) });
    const token = tokenFromJson(current, json);
    expect(token.split(".")[1]!.length).toBeGreaterThan(16_384);
    expect(reason(phase1(token))).toBe("bad_format");
  });
  test("a payload that is not strict JSON claims is bad_format", () => {
    const base = claimsFor({ key: current, aud: "session-read-api", method: "GET", path: "/v1/sessions/s-1/events", authorization: AUTH, access: "read", sessions: ["s-1"], nowS: NOW_S });
    for (const broken of [
      "not json", "[]", "null", JSON.stringify({ ...base, v: "1" }), JSON.stringify({ ...base, iat: 1.5 }), JSON.stringify({ ...base, method: "get" }),
      JSON.stringify({ ...base, body_sha256: "AB".repeat(32) }), JSON.stringify({ ...base, credential_sha256: "00" }), JSON.stringify({ ...base, jti: 7 }),
      JSON.stringify({ ...base, principal: { agent_id: "a", gid: 1, groups: [-1] } }), JSON.stringify({ ...base, principal: { agent_id: "a", gid: "1", groups: [] } }),
      JSON.stringify({ ...base, access: "admin" }), JSON.stringify({ ...base, session_ids: [1] }), JSON.stringify({ ...base, session_count: -1 }),
      JSON.stringify({ ...base, kid: undefined }),
    ]) expect(reason(phase1(tokenFromJson(current, broken)))).toBe("bad_format");
  });
  test("unknown kid is refused; previous kid accepted; a removed kid is refused", () => {
    expect(reason(phase1(readToken({ key: testKey("k-other") })))).toBe("unknown_kid");
    expect(reason(phase1(readToken({ key: previous })))).toBe("ok");
    expect(reason(phase1(readToken({ key: previous }), {}, "session-read-api", configOf(current)))).toBe("unknown_kid");
  });
  test("a signature over a different payload, a different key with the same kid, or another prefix is bad_signature", () => {
    const a = readToken().split(".");
    const b = readToken({ sessions: ["s-2"] }, { target: "/v1/sessions/s-2/events" }).split(".");
    expect(reason(phase1(`v1.${a[1]}.${b[2]}`))).toBe("bad_signature");
    expect(reason(phase1(readToken({ key: { ...testKey("k-current") } })))).toBe("bad_signature");
    const payload = a[1]!;
    const wrongPrefix = { ...current };
    const sig = (prefix: string) => (require("node:crypto") as typeof import("node:crypto")).sign(null, Buffer.from(prefix + payload, "utf8"), wrongPrefix.privateKey).toString("base64url");
    expect(reason(phase1(`v1.${payload}.${sig("relaygate-upstream-v2\n")}`))).toBe("bad_signature");
    expect(reason(phase1(`v1.${payload}.${sig("")}`))).toBe("bad_signature");
    expect(reason(phase1(`v1.${payload}.${signSegment(current, payload)}`))).toBe("ok");
  });
  test("duplicate claim keys: the parsed (last) value is what is checked; unknown extra claims are ignored", () => {
    const json = JSON.stringify(claimsFor({ key: current, aud: "session-read-api", method: "GET", path: "/v1/sessions/s-1/events", authorization: AUTH, access: "read", sessions: ["s-1"], nowS: NOW_S }));
    expect(reason(phase1(tokenFromJson(current, json.replace(/^\{/, '{"aud":"ingest-edge",'))))).toBe("ok");
    expect(reason(phase1(tokenFromJson(current, json.replace(/\}$/, ',"aud":"ingest-edge"}'))))).toBe("aud");
    expect(reason(phase1(readToken({ claims: { extra: { nested: [1] }, scope_hint: "x" } })))).toBe("ok");
  });
});

describe("phase 1: claims, time and bindings", () => {
  test("iss, alg and v must be exact", () => {
    expect(reason(phase1(readToken({ claims: { iss: "other" } })))).toBe("claims");
    expect(reason(phase1(readToken({ claims: { alg: "EdDSA" } })))).toBe("claims");
    expect(reason(phase1(readToken({ claims: { v: 2 } })))).toBe("claims");
  });
  test("aud is fixed per service: a read assertion is refused by the ingest edge and the reverse", () => {
    expect(reason(phase1(readToken(), {}, "ingest-edge"))).toBe("aud");
    expect(reason(phase1(readToken({ aud: "ingest-edge" })))).toBe("aud");
  });
  test("lifetime must be 5..60 seconds", () => {
    expect(reason(phase1(readToken({ ttl: 4 })))).toBe("lifetime");
    expect(reason(phase1(readToken({ ttl: 61 })))).toBe("lifetime");
    expect(reason(phase1(readToken({ ttl: 5 })))).toBe("ok");
    expect(reason(phase1(readToken({ ttl: 60 })))).toBe("ok");
  });
  test("iat up to 5 s ahead is accepted, 6 s is refused; exp == now is expired", () => {
    expect(reason(phase1(readToken({ nowS: NOW_S + 5 })))).toBe("ok");
    expect(reason(phase1(readToken({ nowS: NOW_S + 6 })))).toBe("not_yet_valid");
    expect(reason(phase1(readToken({ nowS: NOW_S - 30, ttl: 30 })))).toBe("expired");
    expect(reason(phase1(readToken({ nowS: NOW_S - 29, ttl: 30 })))).toBe("ok");
  });
  test("method must equal the request method", () => {
    expect(reason(phase1(readToken({ method: "POST" })))).toBe("method");
    expect(reason(phase1(readToken(), { method: "HEAD" }))).toBe("method");
  });
  test("path is the exact target: re-ordered query, an added parameter, or a decoded form is refused", () => {
    const target = "/v1/events?session=s-1&limit=5";
    const token = readToken({}, { target });
    expect(reason(phase1(token, { target }))).toBe("ok");
    expect(reason(phase1(token, { target: "/v1/events?limit=5&session=s-1" }))).toBe("path");
    expect(reason(phase1(token, { target: `${target}&cursor=x` }))).toBe("path");
    expect(reason(phase1(token, { target: "/v1/events?session=s%2D1&limit=5" }))).toBe("path");
  });
  test("pathname hygiene: dot segments, //, %2f and %5C are refused; %2F in the query passes", () => {
    for (const target of ["/v1/sessions/../sessions/s-1/events", "/v1/sessions/./s-1/events", "/v1/sessions/%2e%2e/events", "/v1//sessions/s-1/events", "/v1/sessions/a%2fb/events", "/v1/sessions/a%5Cb/events", "/v1/sessions/a%5cb/events", "/v1/sessions/a%2Fb/events"]) {
      const token = readToken({}, { target });
      expect(reason(phase1(token, { target }))).toBe("path");
    }
    const target = "/v1/events?q=a%2Fb%5Cc";
    expect(reason(phase1(readToken({ access: "filter", sessions: [] }, { target }), { target }))).toBe("ok");
  });
  test("credential is sha256 of the exact Authorization value; spacing differences and a missing header fail", () => {
    const token = readToken();
    expect(reason(phase1(token, { authorization: "Bearer  sh_at_reader-token" }))).toBe("credential");
    expect(reason(phase1(token, { authorization: "bearer sh_at_reader-token" }))).toBe("credential");
    expect(reason(phase1(token, { authorization: undefined }))).toBe("credential");
    expect(reason(phase1(token, { authorization: "" }))).toBe("credential");
  });
  test("a token bound to the empty credential (sha256 of \"\") is refused when the Authorization header is missing", () => {
    const token = readToken({ authorization: "" });
    expect(reason(phase1(token, { authorization: "" }))).toBe("ok");
    expect(reason(phase1(token, { authorization: undefined }))).toBe("credential");
  });
});

describe("phase 1: route table", () => {
  const ok = (target: string, access: string, sessions: string[] = [], method = "GET", audience: "session-read-api" | "ingest-edge" = "session-read-api") =>
    reason(phase1(signAssertion({ key: current, aud: audience, method, path: target, authorization: AUTH, access, sessions, nowS: NOW_S }), { target, method }, audience));

  test("read API classes: read / filter / aggregate / passthrough, and access mismatch per class", () => {
    for (const leaf of ["events", "turns", "metadata", "thread", "catalog", "timing", "analysis", "brief", "summary"]) expect(ok(`/v1/sessions/s-1/${leaf}`, "read", ["s-1"])).toBe("ok");
    expect(ok("/v1/sessions/s%3A1/events", "read", ["s:1"])).toBe("ok");
    expect(ok("/v1/events?session=s-9", "read", ["s-9"])).toBe("ok");
    expect(ok("/v1/sessions", "filter")).toBe("ok");
    expect(ok("/v1/sessions?limit=10", "filter")).toBe("ok");
    expect(ok("/v1/events?q=hello", "filter")).toBe("ok");
    for (const p of ["/v1/sessions/users", "/v1/sessions/activity", "/v1/sessions/summaries/activity", "/v1/sessions/output", "/v1/sessions/titles?key=a&key=b", "/v1/delivery/records", "/v1/digest/daily"]) expect(ok(p, "aggregate")).toBe("ok");
    for (const p of ["/v1/relayfacts", "/v1/relayfacts/topic-a", "/v1/coordination/overlaps", "/v1/coordination/advisories"]) expect(ok(p, "passthrough")).toBe("ok");
    expect(ok("/v1/coordination/advisories/a-1/ack", "passthrough", [], "POST")).toBe("ok");
    // Access mismatch per class.
    expect(ok("/v1/sessions/s-1/events", "filter", ["s-1"])).toBe("access");
    expect(ok("/v1/events?session=s-9", "filter", ["s-9"])).toBe("access");
    expect(ok("/v1/sessions", "read")).toBe("access");
    expect(ok("/v1/events?q=hello", "aggregate")).toBe("access");
    expect(ok("/v1/sessions/users", "passthrough")).toBe("access");
    expect(ok("/v1/digest/daily", "read")).toBe("access");
    expect(ok("/v1/relayfacts", "aggregate")).toBe("access");
    expect(ok("/v1/coordination/advisories/a-1/ack", "write", [], "POST")).toBe("access");
  });
  test("read API scope mismatch: wrong id, extra id, and a scoped claim on an empty-scope route", () => {
    expect(ok("/v1/sessions/s-1/events", "read", ["s-2"])).toBe("scope");
    expect(ok("/v1/sessions/s-1/events", "read", ["s-1", "s-2"])).toBe("scope");
    expect(ok("/v1/sessions/s-1/events", "read", [])).toBe("scope");
    expect(ok("/v1/sessions", "filter", ["s-1"])).toBe("scope");
    expect(ok("/v1/sessions/users", "aggregate", ["s-1"])).toBe("scope");
  });
  test("GET /v1/events with two session= values or neither q nor session is no route: refused (route) whatever the claims", () => {
    for (const [access, sessions] of [["read", ["a", "b"]], ["passthrough", ["a", "b"]], ["filter", []], ["passthrough", []]] as const) {
      expect(ok("/v1/events?session=a&session=b", access, [...sessions])).toBe("route");
    }
    expect(ok("/v1/events?limit=3", "aggregate")).toBe("route");
    expect(ok("/v1/events?limit=3", "passthrough")).toBe("route");
    expect(ok("/v1/events", "passthrough")).toBe("route");
  });
  test("GET /v1/events with an empty session= value is not a single-session read: with a non-empty q= it is filter, else no route", () => {
    expect(ok("/v1/events?session=", "read", [""])).toBe("route");
    expect(ok("/v1/events?session=", "read")).toBe("route");
    expect(ok("/v1/events?session=", "passthrough", [""])).toBe("route");
    expect(ok("/v1/events?session=", "passthrough")).toBe("route");
    expect(ok("/v1/events?session=&q=hello", "read", [""])).toBe("access");
    expect(ok("/v1/events?session=&q=hello", "filter", [""])).toBe("scope");
    expect(ok("/v1/events?session=&q=hello", "filter")).toBe("ok");
    expect(ok("/v1/events?q=hello&session=", "filter")).toBe("ok");
    // A non-empty session= with q= stays a single-session read.
    expect(ok("/v1/events?session=s-9&q=hello", "read", ["s-9"])).toBe("ok");
    expect(ok("/v1/events?session=s-9&q=hello", "filter")).toBe("access");
  });
  test("GET /v1/events: an empty q= counts as absent, a repeated q= is no route (the app answers 400), as app.ts reads them", () => {
    // ?q= alone and ?session=&q= : neither parameter is present, no route, whatever the claims.
    for (const target of ["/v1/events?q=", "/v1/events?session=&q=", "/v1/events?q=&session="]) {
      for (const access of ["filter", "passthrough", "read"]) expect(ok(target, access)).toBe("route");
      expect(ok(target, "read", [""])).toBe("route");
    }
    // ?session=X&q= : the single-session read of X (the empty q= is no filter).
    expect(ok("/v1/events?session=s-9&q=", "read", ["s-9"])).toBe("ok");
    expect(ok("/v1/events?session=s-9&q=", "filter")).toBe("access");
    expect(ok("/v1/events?session=s-9&q=", "read", ["s-8"])).toBe("scope");
    // ?q=abc : the workspace search; ?session=X&q=abc : the single-session read.
    expect(ok("/v1/events?q=abc", "filter")).toBe("ok");
    expect(ok("/v1/events?q=abc", "read")).toBe("access");
    expect(ok("/v1/events?session=s-9&q=abc", "read", ["s-9"])).toBe("ok");
    expect(ok("/v1/events?session=s-9&q=abc", "filter")).toBe("access");
    // A repeated q= is never served by the app (400): no route, with or without session=, empty or not.
    for (const target of ["/v1/events?q=a&q=b", "/v1/events?q=abc&q=", "/v1/events?q=&q=", "/v1/events?session=s-9&q=a&q=b", "/v1/events?session=s-9&q=&q=", "/v1/events?session=&q=a&q=b"]) {
      expect(ok(target, "filter")).toBe("route");
      expect(ok(target, "read", ["s-9"])).toBe("route");
      expect(ok(target, "passthrough")).toBe("route");
    }
  });
  test("a route not in the table is refused (route) even with a valid passthrough assertion and an empty scope", () => {
    for (const access of ["read", "write", "filter", "aggregate", "passthrough"]) {
      for (const sessions of [[], ["s-1"]]) {
        expect(ok("/v1/sessions/s-1/sources", access, sessions)).toBe("route");
        expect(ok("/v1/turns/batches", access, sessions, "POST", "ingest-edge")).toBe("route");
        expect(ok("/v1/cli/login", access, sessions, "POST")).toBe("route");
        expect(ok("/v1/custody/pending", access, sessions, "POST", "ingest-edge")).toBe("route");
      }
    }
  });
  test("HEAD is not aliased to GET: HEAD on a GET table path is no route", () => {
    for (const [target, access, sessions] of [["/v1/sessions/s-1/events", "read", ["s-1"]], ["/v1/sessions", "filter", []], ["/v1/events?q=abc", "filter", []], ["/v1/relayfacts", "passthrough", []]] as const) {
      expect(ok(target, access, [...sessions], "HEAD")).toBe("route");
      expect(ok(target, "passthrough", [], "HEAD")).toBe("route");
      expect(ok(target, access, [...sessions], "GET")).toBe("ok");
    }
    for (const p of ["/v1/delivery/limits", "/v1/delivery/admission"]) expect(ok(p, "passthrough", [], "HEAD", "ingest-edge")).toBe("route");
  });
  test("check order on a route not in the table: a missing or bad token reports its own reason, `route` only for a token that passed every binding", () => {
    const target = "/v1/sessions/s-1/sources";
    const t = (over: Partial<SignInput> = {}) => signAssertion({ key: current, aud: "session-read-api", method: "GET", path: target, authorization: AUTH, access: "passthrough", sessions: [], nowS: NOW_S, ...over });
    expect(reason(phase1(undefined, { target }))).toBe("missing");
    expect(reason(phase1("v1.x.y", { target }))).toBe("bad_format");
    expect(reason(phase1(t({ key: testKey("k-other") }), { target }))).toBe("unknown_kid");
    expect(reason(phase1(`v1.${t().split(".")[1]}.${t({ sessions: ["s-2"] }).split(".")[2]}`, { target }))).toBe("bad_signature");
    expect(reason(phase1(t({ aud: "ingest-edge" }), { target }))).toBe("aud");
    expect(reason(phase1(t({ nowS: NOW_S - 30, ttl: 30 }), { target }))).toBe("expired");
    expect(reason(phase1(t({ method: "POST" }), { target }))).toBe("method");
    expect(reason(phase1(t({ path: "/v1/sessions/s-2/sources" }), { target }))).toBe("path");
    expect(reason(phase1(t(), { target, authorization: "Bearer other" }))).toBe("credential");
    expect(reason(phase1(t(), { target }))).toBe("route");
    // The answer body is the same generic one for every reason but `missing`.
    expect(upstreamAssertionRefusal("route")).toEqual(upstreamAssertionRefusal("bad_signature"));
  });
  test("ingest classes: write by path, passthrough, body-scoped routes defer the scope to phase 2", () => {
    expect(ok("/v1/sessions/s-1/turns", "write", ["s-1"], "POST", "ingest-edge")).toBe("ok");
    expect(ok("/v1/sessions/s-1/turns", "write", ["s-2"], "POST", "ingest-edge")).toBe("scope");
    for (const [method, p] of [["GET", "/v1/delivery/admission"], ["POST", "/v1/upload-consent"], ["GET", "/v1/delivery/limits"], ["POST", "/v1/auth/token/refresh"], ["POST", "/v1/onboarding/heartbeat"]] as const) {
      expect(ok(p, "passthrough", [], method, "ingest-edge")).toBe("ok");
      expect(ok(p, "write", [], method, "ingest-edge")).toBe("access");
    }
    for (const p of ["/v1/ingest", "/v1/delivery/batches", "/v1/delivery/sessions/selection"]) {
      expect(ok(p, "passthrough", [], "POST", "ingest-edge")).toBe("access");
      const target = p;
      const r = phase1(signAssertion({ key: current, aud: "ingest-edge", method: "POST", path: target, authorization: AUTH, access: "write", sessions: ["x"], nowS: NOW_S }), { target, method: "POST" }, "ingest-edge");
      expect(r.ok && r.pending?.bodyScope).toBeTruthy();
    }
  });
  test("exempt routes: missing passes only on the exact method + path; a presented one is validated", () => {
    expect(phase1(undefined, { method: "POST", target: "/v1/auth/token/revoke" })).toEqual({ ok: true, pending: null, exempt: true });
    expect(reason(phase1(undefined, { method: "GET", target: "/v1/auth/token/revoke" }))).toBe("missing");
    expect(reason(phase1(undefined, { method: "POST", target: "/v1/auth/token/revoke/" }))).toBe("missing");
    expect(reason(phase1(undefined, { method: "POST", target: "/v1/auth/token/refresh" }))).toBe("missing");
    expect(phase1(undefined, { method: "POST", target: "/v1/auth/token/refresh" }, "ingest-edge")).toEqual({ ok: true, pending: null, exempt: true });
    expect(reason(phase1("v1.x.y", { method: "POST", target: "/v1/auth/token/revoke" }))).toBe("bad_format");
    expect(reason(phase1(readToken({ access: "read" }, { method: "POST", target: "/v1/auth/token/refresh" }), { method: "POST", target: "/v1/auth/token/refresh" }, "ingest-edge"))).toBe("aud");
    // The read API's exempt route is not a table route: a presented assertion is checked as passthrough, empty scope.
    const revoke = { method: "POST", target: "/v1/auth/token/revoke" };
    expect(reason(phase1(readToken({ access: "passthrough", sessions: [], method: "POST" }, revoke), revoke))).toBe("ok");
    expect(reason(phase1(readToken({ access: "read", sessions: [], method: "POST" }, revoke), revoke))).toBe("access");
    expect(reason(phase1(readToken({ access: "passthrough", sessions: ["s-1"], method: "POST" }, revoke), revoke))).toBe("scope");
    expect(reason(phase1(readToken({ access: "passthrough", sessions: [], method: "POST", nowS: NOW_S - 60 }, revoke), revoke))).toBe("expired");
    // Only the exact method + raw pathname: a trailing slash, another method or the other audience's path is no route.
    for (const [method, target] of [["POST", "/v1/auth/token/revoke/"], ["GET", "/v1/auth/token/revoke"], ["POST", "/v1/auth/token/refresh"]]) {
      expect(reason(phase1(readToken({ access: "passthrough", sessions: [], method }, { method, target }), { method, target }))).toBe("route");
    }
  });
  test("bypass: probes and OPTIONS", () => {
    expect(upstreamAssertionBypass("GET", "/healthz")).toBe(true);
    expect(upstreamAssertionBypass("GET", "/readyz?x=1")).toBe(true);
    expect(upstreamAssertionBypass("GET", "/health")).toBe(true);
    expect(upstreamAssertionBypass("OPTIONS", "/v1/sessions")).toBe(true);
    expect(upstreamAssertionBypass("GET", "/healthz/x")).toBe(false);
    expect(upstreamAssertionBypass("GET", "/v1/sessions")).toBe(false);
    expect(upstreamAssertionBypass("HEAD", "/v1/sessions")).toBe(false);
    expect(upstreamAssertionBypass("HEAD", "/v1/sessions/s-1/events")).toBe(false);
  });
});

describe("session scope digest", () => {
  test("empty scope: count 0, digest of [], inline []", () => {
    expect(sessionScope([])).toEqual({ count: 0, digest: sha256Hex("[]"), inline: [] });
  });
  test("de-duplicated, plain sort, JSON.stringify", () => {
    const s = sessionScope(["b", "a", "b", "C"]);
    expect(s).toEqual({ count: 3, digest: sha256Hex('["C","a","b"]'), inline: ["C", "a", "b"] });
  });
  test("inline only at <= 16 ids and <= 2048 bytes", () => {
    const ids = (n: number, len = 4) => Array.from({ length: n }, (_, i) => `${String(i).padStart(2, "0")}${"x".repeat(len - 2)}`);
    expect(sessionScope(ids(16)).inline).toHaveLength(16);
    expect(sessionScope(ids(17)).inline).toEqual([]);
    // 2 ids: JSON = 2*len + 7 bytes; len 1020 gives 2047, 1021 gives 2049.
    const two = (len: number) => [`a${"x".repeat(len - 1)}`, `b${"x".repeat(len - 1)}`];
    expect(Buffer.byteLength(JSON.stringify(two(1020)))).toBe(2047);
    expect(sessionScope(two(1020)).inline).toHaveLength(2);
    expect(Buffer.byteLength(JSON.stringify(two(1021)))).toBe(2049);
    expect(sessionScope(two(1021)).inline).toEqual([]);
    // exactly 2048: one id of 2044 chars -> ["..."] = 2048
    const one = ["y".repeat(2044)];
    expect(Buffer.byteLength(JSON.stringify(one))).toBe(2048);
    expect(sessionScope(one).inline).toEqual(one);
    expect(sessionScope(["y".repeat(2045)]).inline).toEqual([]);
  });
  test("phase-1 scope check: count, digest and the inline list at the 16-id and 2048-byte edges", () => {
    const ids16 = Array.from({ length: 16 }, (_, i) => `s-${String(i).padStart(2, "0")}`);
    const ids17 = [...ids16, "s-16"];
    const target = "/v1/sessions/s-1/events";
    const tok = (claims: Record<string, unknown>) => readToken({ claims });
    const s1 = sessionScope(["s-1"]);
    expect(reason(phase1(tok({ session_count: 2 }), { target }))).toBe("scope");
    expect(reason(phase1(tok({ session_ids_sha256: sha256Hex("[]") }), { target }))).toBe("scope");
    expect(reason(phase1(tok({ session_ids: [] }), { target }))).toBe("scope"); // inline due but empty
    expect(reason(phase1(tok({ session_ids: ["s-1", "s-1"] }), { target }))).toBe("scope");
    expect(s1.inline).toEqual(["s-1"]);
    // Body-scope edges are checked through phase 2 (below) with 16 and 17 ids.
    const pending = (ids: string[], claims: Record<string, unknown> = {}) => {
      const r = phase1(signAssertion({ key: current, aud: "ingest-edge", method: "POST", path: "/v1/ingest", authorization: AUTH, access: "write", sessions: ids, nowS: NOW_S, claims }), { method: "POST", target: "/v1/ingest" }, "ingest-edge");
      if (!r.ok || !r.pending) throw new Error("phase 1 failed");
      return r.pending;
    };
    const body = (ids: string[]) => ({ records: ids.map((sessionId) => ({ sessionId })) });
    expect(verifyUpstreamAssertionScope(pending(ids16), body(ids16))).toBeNull();
    expect(verifyUpstreamAssertionScope(pending(ids17), body(ids17))).toBeNull();
    // inline present when it must be empty (17 ids), and absent when it is due (16 ids).
    expect(verifyUpstreamAssertionScope(pending(ids17, { session_ids: ids16 }), body(ids17))).toBe("scope");
    expect(verifyUpstreamAssertionScope(pending(ids16, { session_ids: [] }), body(ids16))).toBe("scope");
    // the 2048-byte edge: an inline list over the byte cap must be [].
    const big = [`a${"x".repeat(1020)}`, `b${"x".repeat(1020)}`]; // 2049 bytes of JSON
    expect(verifyUpstreamAssertionScope(pending(big), body(big))).toBeNull();
    expect(verifyUpstreamAssertionScope(pending(big, { session_ids: [...big].sort() }), body(big))).toBe("scope");
    // count / digest mismatch, a different set, an unparseable body.
    expect(verifyUpstreamAssertionScope(pending(ids16, { session_count: 15 }), body(ids16))).toBe("scope");
    expect(verifyUpstreamAssertionScope(pending(ids16), body(ids17))).toBe("scope");
    expect(verifyUpstreamAssertionScope(pending(ids16), { records: "x" })).toBe("scope");
    // duplicates in the body collapse.
    expect(verifyUpstreamAssertionScope(pending(["a"]), body(["a", "a"]))).toBeNull();
  });
});

describe("phase 2: body", () => {
  const pendingFor = (body: string | Uint8Array, target = "/v1/delivery/batches") => {
    const r = phase1(signAssertion({ key: current, aud: "ingest-edge", method: "POST", path: target, authorization: AUTH, access: "write", sessions: ["s-1"], body, nowS: NOW_S }), { method: "POST", target }, "ingest-edge");
    if (!r.ok || !r.pending) throw new Error("phase 1 failed");
    return r.pending;
  };
  test("the hash is over the exact wire bytes (compressed stays compressed)", () => {
    const json = JSON.stringify({ batch: { records: [{ session_id: "s-1" }] } });
    const wire = gzipSync(json);
    const p = pendingFor(wire);
    expect(verifyUpstreamAssertionBody(p, wire)).toBeNull();
    expect(verifyUpstreamAssertionBody(p, new TextEncoder().encode(json))).toBe("body");
    expect(verifyUpstreamAssertionScope(p, JSON.parse(json))).toBeNull();
  });
  test("empty body = sha256 of zero bytes", () => {
    const p = pendingFor("");
    expect(verifyUpstreamAssertionBody(p, new Uint8Array())).toBeNull();
    expect(verifyUpstreamAssertionBody(p, new Uint8Array([0]))).toBe("body");
  });
  test("a non-body-scoped route's scope check is a no-op", () => {
    const r = phase1(readToken());
    if (!r.ok || !r.pending) throw new Error("phase 1 failed");
    expect(verifyUpstreamAssertionScope(r.pending, { anything: true })).toBeNull();
  });
  test("body scope extractors", () => {
    expect(ingestRecordsScope({ records: [{ sessionId: "a" }, { sessionId: "b" }] })).toEqual(["a", "b"]);
    expect(deliveryBatchScope({ batch: { records: [{ session_id: "a" }] } })).toEqual(["a"]);
    expect(sessionSelectionScope({ sessions: [{ source: "x", session_id: "a" }] })).toEqual(["a"]);
    expect(deliveryBatchScope({ records: [{ session_id: "a" }] })).toBeNull();
    expect(sessionSelectionScope({ sessions: [{ session_id: 1 }] })).toBeNull();
    expect(ingestRecordsScope(null)).toBeNull();
  });
});

describe("configuration", () => {
  const P = READ_API_UPSTREAM_ASSERTION_PREFIX;
  const valid = (over: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv => ({ [`${P}MODE`]: "log", [`${P}PUBLIC_KEY`]: current.publicB64, [`${P}KID`]: current.kid, ...over });
  const refusal = (env: NodeJS.ProcessEnv, prefix = P) => {
    try {
      upstreamAssertionConfig(env, prefix);
    } catch (error) {
      return (error as Error).message;
    }
    return "accepted";
  };

  test("modes: unset, empty and off are off; log and enforce; anything else refuses naming the variable only", () => {
    expect(upstreamAssertionMode({}, P)).toBe("off");
    expect(upstreamAssertionMode({ [`${P}MODE`]: "" }, P)).toBe("off");
    expect(upstreamAssertionMode({ [`${P}MODE`]: "off" }, P)).toBe("off");
    expect(upstreamAssertionMode({ [`${P}MODE`]: "log" }, P)).toBe("log");
    expect(upstreamAssertionMode({ [`${P}MODE`]: "enforce" }, P)).toBe("enforce");
    for (const bad of ["required", "ENFORCE", " log", "on", "true"]) {
      const message = refusal({ [`${P}MODE`]: bad });
      expect(message).toContain(`${P}MODE`);
      if (bad.trim()) expect(message).not.toContain(`"${bad}"`);
      expect(message).not.toMatch(/required|ENFORCE|\btrue\b|\bon\b/);
    }
  });
  test("off reads no key variable: garbage keys are ignored", () => {
    expect(upstreamAssertionConfig({ [`${P}PUBLIC_KEY`]: "garbage", [`${P}KID`]: "!!", [`${P}KID_PREVIOUS`]: "x" }, P)).toBeNull();
    expect(upstreamAssertionConfig({ [`${P}MODE`]: "off", [`${P}PUBLIC_KEY`]: "garbage" }, P)).toBeNull();
  });
  test("valid single and rotated configs", () => {
    expect([...upstreamAssertionConfig(valid(), P)!.keys.keys()]).toEqual([current.kid]);
    expect(upstreamAssertionConfig(valid({ [`${P}MODE`]: "enforce" }), P)!.mode).toBe("enforce");
    const rotated = upstreamAssertionConfig(valid({ [`${P}PUBLIC_KEY_PREVIOUS`]: previous.publicB64, [`${P}KID_PREVIOUS`]: previous.kid }), P)!;
    expect([...rotated.keys.keys()]).toEqual([current.kid, previous.kid]);
  });
  test("every startup refusal names the variable and never contains the value", () => {
    const { generateKeyPairSync } = require("node:crypto") as typeof import("node:crypto");
    const rsa = generateKeyPairSync("rsa", { modulusLength: 1024 }).publicKey.export({ format: "der", type: "spki" }).toString("base64");
    const x25519 = generateKeyPairSync("x25519").publicKey.export({ format: "der", type: "spki" }).toString("base64");
    const nonCanonical = current.publicB64.slice(0, -1) + (current.publicB64.endsWith("=") ? "" : "") ;
    const cases: [NodeJS.ProcessEnv, string, string | undefined][] = [
      [valid({ [`${P}PUBLIC_KEY`]: undefined }), `${P}PUBLIC_KEY`, undefined],
      [valid({ [`${P}PUBLIC_KEY`]: "" }), `${P}PUBLIC_KEY`, undefined],
      [valid({ [`${P}PUBLIC_KEY`]: current.publicB64.replace(/\+/g, "-").replace(/\//g, "_") + "-_" }), `${P}PUBLIC_KEY`, "-_"],
      [valid({ [`${P}PUBLIC_KEY`]: ` ${current.publicB64}` }), `${P}PUBLIC_KEY`, current.publicB64],
      [valid({ [`${P}PUBLIC_KEY`]: `${current.publicB64}\n` }), `${P}PUBLIC_KEY`, current.publicB64],
      [valid({ [`${P}PUBLIC_KEY`]: nonCanonical }), `${P}PUBLIC_KEY`, nonCanonical],
      [valid({ [`${P}PUBLIC_KEY`]: "AAAA" }), `${P}PUBLIC_KEY`, "AAAA"],
      [valid({ [`${P}PUBLIC_KEY`]: rsa }), `${P}PUBLIC_KEY`, rsa.slice(0, 40)],
      [valid({ [`${P}PUBLIC_KEY`]: x25519 }), `${P}PUBLIC_KEY`, x25519.slice(0, 40)],
      [valid({ [`${P}KID`]: undefined }), `${P}KID`, undefined],
      [valid({ [`${P}KID`]: "bad kid" }), `${P}KID`, "bad kid"],
      [valid({ [`${P}KID`]: "k".repeat(65) }), `${P}KID`, "k".repeat(65)],
      [valid({ [`${P}PUBLIC_KEY_PREVIOUS`]: previous.publicB64 }), `${P}KID_PREVIOUS`, previous.publicB64],
      [valid({ [`${P}KID_PREVIOUS`]: previous.kid }), `${P}PUBLIC_KEY_PREVIOUS`, previous.kid],
      [valid({ [`${P}PUBLIC_KEY_PREVIOUS`]: previous.publicB64, [`${P}KID_PREVIOUS`]: current.kid }), `${P}KID_PREVIOUS`, previous.publicB64],
      [valid({ [`${P}PUBLIC_KEY_PREVIOUS`]: "AAAA", [`${P}KID_PREVIOUS`]: previous.kid }), `${P}PUBLIC_KEY_PREVIOUS`, "AAAA"],
      [valid({ [`${P}PUBLIC_KEY_PREVIOUS`]: previous.publicB64, [`${P}KID_PREVIOUS`]: "bad/kid" }), `${P}KID_PREVIOUS`, "bad/kid"],
    ];
    for (const [env, name, value] of cases) {
      const message = refusal(env);
      expect(message).toContain(name);
      expect(message).not.toContain(current.publicB64);
      expect(message).not.toContain(previous.publicB64);
      if (value !== undefined) expect(message).not.toContain(value);
    }
  });
  test("the ingest prefix is independent", () => {
    const I = INGEST_UPSTREAM_ASSERTION_PREFIX;
    expect(upstreamAssertionConfig(valid(), I)).toBeNull();
    expect(refusal({ [`${I}MODE`]: "enforce" }, I)).toContain(`${I}KID`);
  });
  test("hosted entries: any mode but off refuses, naming the variable", () => {
    for (const ok of [{}, { READ_API_UPSTREAM_ASSERTION_MODE: "" }, { READ_API_UPSTREAM_ASSERTION_MODE: "off" }]) expect(() => assertUpstreamAssertionOff(ok, P)).not.toThrow();
    for (const mode of ["log", "enforce", "bogus"]) {
      let message = "";
      try {
        assertUpstreamAssertionOff({ READ_API_UPSTREAM_ASSERTION_MODE: mode }, P);
      } catch (error) {
        message = (error as Error).message;
      }
      expect(message).toContain("READ_API_UPSTREAM_ASSERTION_MODE");
      expect(message).not.toContain(mode === "log" ? '"log"' : mode);
    }
  });
  test("refusal answers are generic", () => {
    expect(upstreamAssertionRefusal("missing").code).toBe("missing_upstream_assertion");
    for (const r of ["bad_signature", "scope", "body", "expired", "route"] as const) expect(upstreamAssertionRefusal(r)).toEqual({ code: "invalid_upstream_assertion", message: "invalid upstream assertion" });
  });
});
