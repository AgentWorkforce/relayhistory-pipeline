/**
 * Query parity for GET /v1/events between the upstream-assertion verifier (src/shared/upstream-assertion.ts) and the
 * read app (app.ts). The verifier reads `session` and `q` from the raw request target to pick the route rule and the
 * scope it binds; the app reads them through its own framework parser to decide what it serves. If the two readings
 * differ, an assertion could authorize one session while the app serves another.
 *
 * For each raw query this test obtains, independently:
 *  (a) the verifier's reading, through verifyUpstreamAssertionPhase1 only: which /v1/events rule matched (from the
 *      closed reason of probe assertions signed in memory) and whether the session scope it requires is exactly the
 *      expected id list. The q value reaches the verifier's decision only as "empty or absent / non-empty / repeated",
 *      so that class is what the verifier side can show for q.
 *  (b) the read app's reading, through the real createReadApi with an in-memory search adapter that records the
 *      SearchQuery the app builds (sessionId, q), or the app's refusal when it serves nothing.
 * It asserts (a) equals (b) and pins both against the literal table, and finally runs every vector through the real
 * self-host service in enforce mode with the real app behind it.
 *
 * Keys are generated in memory by the test-only signer; nothing is written to disk or logged.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createPublicKey } from "node:crypto";
import type { SearchIndexAdapter, SearchQuery } from "@rhc/search-index/contracts.ts";
import { createReadApi } from "../app.ts";
import type { Liveness } from "../page.ts";
import { startReadApiService, type ReadApiService } from "../server.ts";
import type { SearchServiceBinding } from "../../session-pipeline/search-adapter.ts";
import { verifyUpstreamAssertionPhase1, type UpstreamAssertionConfig, type UpstreamAssertionReason } from "../../shared/upstream-assertion.ts";
import { signAssertion, testKey, type SignInput } from "../../shared/__tests__/upstream-assertion-signer.ts";

const KEY = testKey("parity-k1");
const AUTH = "Bearer synthetic-parity-token";
const TENANT = { orgId: "org-a", workspaceId: "ws-a" };
const CONFIG: UpstreamAssertionConfig = { mode: "enforce", keys: new Map([[KEY.kid, createPublicKey({ key: Buffer.from(KEY.publicB64, "base64"), format: "der", type: "spki" })]]) };

/**
 * The exact raw target both sides receive: the vector, plus the workspace the app requires and a limit its search
 * serves (neither is a rule input).
 */
const target = (raw: string) => `/v1/events?${raw}&workspace=${TENANT.workspaceId}&limit=10`;

type Rule = "read" | "filter" | "none";

interface Vector {
  raw: string;
  /** What the app reads: the SearchQuery it builds, or its refusal when it serves nothing. */
  app: { session: string | undefined; q: string | undefined } | { refused: number; reason: string };
  /** The /v1/events rule the verifier matches, and the scope ids it binds on the read rule. */
  rule: Rule;
  scope: string[];
}

/** Vectors 1-6: both sides must agree. Literal values: a change on either side fails here. */
const PARITY: Vector[] = [
  // 1. percent-encoded parameter NAME: both decode the name, so it is `session`.
  { raw: "%73ession=s&q=x", app: { session: "s", q: "x" }, rule: "read", scope: ["s"] },
  // 2. encoded "&" and "=" inside a VALUE stay inside the value: one session parameter, no q.
  { raw: "session=s%26q%3Dx", app: { session: "s&q=x", q: undefined }, rule: "read", scope: ["s&q=x"] },
  // 3. "+" in a query value is a space on both sides (q is non-empty either way: the filter rule).
  { raw: "q=a+b", app: { session: undefined, q: "a b" }, rule: "filter", scope: [] },
  // 3b. "+" in the session value, where the verifier's reading is observable as the bound scope.
  { raw: "session=a+b&q=x", app: { session: "a b", q: "x" }, rule: "read", scope: ["a b"] },
  // 4. plain control.
  { raw: "session=s&q=x", app: { session: "s", q: "x" }, rule: "read", scope: ["s"] },
  // 5. a repeated parameter: the app answers 400 and serves nothing; the verifier matches no rule.
  { raw: "session=a&session=b&q=x", app: { refused: 400, reason: "bad_request" }, rule: "none", scope: [] },
  // 6. an empty session= is absent on both sides: the workspace search, unscoped.
  { raw: "session=&q=x", app: { session: undefined, q: "x" }, rule: "filter", scope: [] },
];

// --------------------------------------------------------------------------------------------------------------
// (b) the app's reading

const liveness: Liveness = {
  async erased() { return new Set(); },
  async sessionStart() { return { pointers: 1, live: 1, createdAtMs: 1 }; },
  async livePointerSessions(_o, _w, ids) { return new Set(ids); },
};

/** The real read app over an in-memory search adapter that records every SearchQuery and answers no hits. */
function recordingApp(queries: SearchQuery[]) {
  const adapter: SearchServiceBinding = {
    index: { async query(q: SearchQuery) { queries.push(q); return { hits: [] }; } } as unknown as SearchIndexAdapter,
    state: { async fromMs() { return 0; }, async record() {}, async partials() { return { sessions: [], overflow: false }; } },
  };
  return createReadApi({ adapter, liveness, completeSinceMs: 0, verify: async () => ({ ...TENANT, userId: "u", scopes: ["rth:read"] }), log: () => {} });
}

type AppReading = { session: string | undefined; q: string | undefined } | { refused: number; reason: string };

/** Drives the real app with the exact raw target (as server.ts builds the Request) and reads what it queried. */
async function appReading(raw: string): Promise<AppReading> {
  const queries: SearchQuery[] = [];
  const app = recordingApp(queries);
  const response = await app.fetch(new Request(`http://localhost${target(raw)}`, { headers: { authorization: AUTH } }));
  if (response.status !== 200) {
    const body = (await response.json()) as { error?: { code?: string; reason?: string } };
    return { refused: response.status, reason: body.error?.reason ?? body.error?.code ?? "" };
  }
  expect(queries).toHaveLength(1);
  return { session: queries[0]!.sessionId, q: queries[0]!.q };
}

/** The /v1/events rule the app's reading implies (app.ts: a session read, else the workspace search, else nothing). */
const appRule = (reading: AppReading): { rule: Rule; scope: string[] } =>
  "refused" in reading ? { rule: "none", scope: [] }
  : reading.session !== undefined ? { rule: "read", scope: [reading.session] }
  : reading.q !== undefined ? { rule: "filter", scope: [] }
  : { rule: "none", scope: [] };

// --------------------------------------------------------------------------------------------------------------
// (a) the verifier's reading

const probe = (raw: string, over: Partial<SignInput>): UpstreamAssertionReason | "ok" => {
  const path = target(raw);
  const assertion = signAssertion({ key: KEY, aud: "session-read-api", method: "GET", path, authorization: AUTH, access: "read", sessions: [], ...over });
  const result = verifyUpstreamAssertionPhase1(CONFIG, "session-read-api", { method: "GET", target: path, authorization: AUTH, assertion });
  return result.ok ? "ok" : result.reason;
};

/**
 * The verifier's rule, from the closed reason of a filter probe with an empty scope: "ok" = the filter rule,
 * "access" = a rule with another access matched (the read rule), "route" = no rule.
 */
function verifierRule(raw: string): Rule {
  const reason = probe(raw, { access: "filter", sessions: [] });
  if (reason === "ok") return "filter";
  if (reason === "access") return "read";
  if (reason === "route") return "none";
  throw new Error(`unexpected verifier reason ${reason}`);
}

/** Whether the verifier accepts a read assertion scoped to exactly these session ids for this target. */
const verifierAcceptsReadScope = (raw: string, sessions: string[]) => probe(raw, { access: "read", sessions });

// --------------------------------------------------------------------------------------------------------------

describe("GET /v1/events: the verifier and the app read session and q the same way", () => {
  test.each(PARITY.map((v) => [v.raw, v] as const))("%s", async (_raw, v) => {
    // (b) the app, pinned.
    const app = await appReading(v.raw);
    expect(app).toEqual(v.app);
    const fromApp = appRule(app);
    expect(fromApp).toEqual({ rule: v.rule, scope: v.scope });

    // (a) the verifier, pinned, and equal to the app's reading.
    const rule = verifierRule(v.raw);
    expect(rule).toBe(v.rule);
    expect(rule).toBe(fromApp.rule);
    if (rule === "read") {
      // The verifier binds exactly the session the app serves: that scope passes, any other is refused.
      expect(verifierAcceptsReadScope(v.raw, fromApp.scope)).toBe("ok");
      expect(verifierAcceptsReadScope(v.raw, [])).toBe("scope");
      expect(verifierAcceptsReadScope(v.raw, [`${fromApp.scope[0]}-other`])).toBe("scope");
      expect(probe(v.raw, { access: "filter", sessions: [] })).toBe("access");
    } else if (rule === "filter") {
      expect(probe(v.raw, { access: "filter", sessions: [] })).toBe("ok");
      expect(verifierAcceptsReadScope(v.raw, ["s"])).toBe("access");
    } else {
      // No rule: refused whatever access or scope the assertion claims.
      for (const over of [{ access: "read", sessions: ["a"] }, { access: "read", sessions: ["a", "b"] }, { access: "read", sessions: ["b"] }, { access: "filter", sessions: [] }] as const)
        expect(probe(v.raw, { ...over, sessions: [...over.sessions] })).toBe("route");
    }
  });
});

describe("pinned known difference: malformed percent-encoding in a value", () => {
  // KNOWN, ALREADY REPORTED DIFFERENCE (not asserted equal): for `session=s%FFt` the verifier's URLSearchParams
  // decodes the invalid escape to U+FFFD ("s", U+FFFD, "t") while the app's parser keeps the raw text ("s%FFt"). Both
  // readings are pinned here so a change on either side is seen.
  const RAW = "session=s%FFt&q=x";
  const VERIFIER_SESSION = "s\uFFFDt";
  const APP_SESSION = "s%FFt";

  test("each side's reading is pinned; an assertion scoped to the app's reading is refused", async () => {
    expect(await appReading(RAW)).toEqual({ session: APP_SESSION, q: "x" });
    expect(verifierRule(RAW)).toBe("read");
    // The scope the app serves cannot be authorized: the verifier binds a different id.
    expect(verifierAcceptsReadScope(RAW, [APP_SESSION])).toBe("scope");
    // The verifier's own reading, pinned as observed (this is the reported difference: it is not the app's id).
    expect(verifierAcceptsReadScope(RAW, [VERIFIER_SESSION])).toBe("ok");
    expect(VERIFIER_SESSION).not.toBe(APP_SESSION);
  });
});

// --------------------------------------------------------------------------------------------------------------
// End to end: the real self-host service in enforce mode, the real app behind it.

const running: ReadApiService[] = [];
afterEach(async () => {
  for (const s of running.splice(0)) await s.close();
});

describe("through the real service (enforce) with the real app", () => {
  test("an assertion for the pinned rule and scope reaches the app, which serves exactly that reading", async () => {
    const queries: SearchQuery[] = [];
    const service = await startReadApiService(
      { RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: "{}", READ_API_UPSTREAM_ASSERTION_MODE: "enforce", READ_API_UPSTREAM_ASSERTION_PUBLIC_KEY: KEY.publicB64, READ_API_UPSTREAM_ASSERTION_KID: KEY.kid },
      { host: "127.0.0.1", port: 0, build: async () => ({ app: recordingApp(queries), close: async () => {} }) },
    );
    running.push(service);
    const errors: string[] = [];
    const original = process.stderr.write.bind(process.stderr);
    process.stderr.write = ((chunk: string | Uint8Array) => { errors.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8")); return true; }) as typeof process.stderr.write;
    try {
      for (const v of PARITY) {
        const path = target(v.raw);
        const over: Partial<SignInput> = v.rule === "filter" ? { access: "filter", sessions: [] } : { access: "read", sessions: v.rule === "read" ? v.scope : ["a", "b"] };
        const assertion = signAssertion({ key: KEY, aud: "session-read-api", method: "GET", path, authorization: AUTH, access: "read", sessions: [], ...over });
        const before = queries.length;
        const r = await fetch(`http://127.0.0.1:${service.port}${path}`, { headers: { authorization: AUTH, "x-relaygate-assertion": assertion } });
        if (v.rule === "none") {
          expect([v.raw, r.status]).toEqual([v.raw, 401]);
          expect(queries.length).toBe(before);
        } else {
          expect([v.raw, r.status]).toEqual([v.raw, 200]);
          expect(queries.length).toBe(before + 1);
          expect({ session: queries.at(-1)!.sessionId, q: queries.at(-1)!.q }).toEqual(v.app as { session: string | undefined; q: string | undefined });
        }
      }
      // The known difference end to end: an assertion scoped to the app's reading is refused before the app.
      const raw = "session=s%FFt&q=x";
      const before = queries.length;
      const r = await fetch(`http://127.0.0.1:${service.port}${target(raw)}`, {
        headers: { authorization: AUTH, "x-relaygate-assertion": signAssertion({ key: KEY, aud: "session-read-api", method: "GET", path: target(raw), authorization: AUTH, access: "read", sessions: ["s%FFt"] }) },
      });
      expect(r.status).toBe(401);
      expect(queries.length).toBe(before);
    } finally {
      process.stderr.write = original;
    }
    expect(errors.map((l) => (JSON.parse(l) as { reason: string }).reason)).toEqual(["route", "scope"]);
  });
});
