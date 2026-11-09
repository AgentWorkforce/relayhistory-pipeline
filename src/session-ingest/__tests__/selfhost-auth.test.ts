/**
 * The self-host ingest edge's relaycontrol identity (selfhost-auth.ts) with the
 * opt-in positive introspection cache (RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS,
 * selfhost-config.ts). A fake fetch plays relaycontrol (no network); the clock is
 * bun's setSystemTime, which drives the introspector's default `now` (Date.now)
 * through the real selfhostIntrospector wiring. Synthetic ids and secrets only.
 */
import { afterEach, describe, expect, setSystemTime, test } from "bun:test";
import { AuthError } from "../../session-read-api/app.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector, type SelfhostAuthConfig } from "../selfhost-auth.ts";
import {
  SELFHOST_INTROSPECT_POSITIVE_TTL_VAR,
  selfhostIngestConfig,
  selfhostIntrospectPositiveTtlMs,
} from "../selfhost-config.ts";

const ORIGIN = "https://history.selfhost.test";
const SECRET = "introspect-secret-test";
const TOKEN = "sh_at_desktop-token-1";
const OTHER = "sh_at_desktop-token-2";
const T0 = Date.UTC(2026, 10, 9, 12, 0, 0);
const IDENTITY = { userId: "user-1", orgId: "org-a", workspaceId: "ws-a" };

afterEach(() => {
  setSystemTime();
});

let clock = T0;
function at(ms: number) {
  clock = ms;
  setSystemTime(new Date(ms));
}
const sec = (ms: number) => Math.floor(ms / 1000);

type Answer = { status: number; body?: unknown };
/** relaycontrol's introspection endpoint, scripted per call; records each request body. */
function relaycontrol(answer: (body: { token: string; aud: string }) => Answer) {
  const calls: Record<string, unknown>[] = [];
  const fetcher = (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body)) as { token: string; aud: string };
    calls.push(body);
    const a = answer(body);
    return new Response(a.body === undefined ? "" : JSON.stringify(a.body), { status: a.status, headers: { "content-type": "application/json" } });
  }) as unknown as typeof fetch;
  return { calls, fetcher };
}

const granted = (scopes: string[] = ["rth:read", "rth:sync"], over: Record<string, unknown> = {}): Answer => ({
  status: 200,
  body: {
    active: true, sub: IDENTITY.userId, email: null, principal_type: "desktop-user", machine_id: null,
    org: IDENTITY.orgId, workspace: IDENTITY.workspaceId, scopes, aud: ORIGIN,
    iat: sec(clock) - 10, exp: sec(clock) + 600, token_id: "tok-1", ...over,
  },
});
const refused = (reason = "revoked"): Answer => ({ status: 200, body: { active: false, reason } });

function edge(rc: { fetcher: typeof fetch }, ttl?: number) {
  const config: SelfhostAuthConfig = { origin: ORIGIN, introspectUrl: "https://relaycontrol.test", introspectSecret: SECRET, ...(ttl !== undefined ? { introspectPositiveTtlMs: ttl } : {}) };
  return relaycontrolIngestIdentity(config, selfhostIntrospector(config, {}, rc.fetcher));
}

async function refusal(p: Promise<unknown>): Promise<[number, string]> {
  try {
    await p;
  } catch (e) {
    if (e instanceof AuthError) return [e.status, e.code];
    throw e;
  }
  throw new Error("expected a refusal");
}

const bearer = (t = TOKEN) => `Bearer ${t}`;

describe("positive introspection cache: default off", () => {
  for (const ttl of [undefined, 0]) {
    test(`TTL ${ttl === undefined ? "unset" : ttl}: every request introspects (unchanged)`, async () => {
      at(T0);
      const rc = relaycontrol(() => granted());
      const verify = edge(rc, ttl);
      expect(await verify(bearer())).toMatchObject(IDENTITY);
      expect(await verify(bearer())).toMatchObject(IDENTITY);
      expect(rc.calls.length).toBe(2);
      expect(rc.calls[0]).toEqual({ token: TOKEN, aud: ORIGIN });
    });
  }

  test("TTL 0: a revocation is refused on the very next request", async () => {
    at(T0);
    let revoked = false;
    const rc = relaycontrol(() => (revoked ? refused() : granted()));
    const verify = edge(rc, 0);
    await verify(bearer());
    revoked = true;
    expect(await refusal(verify(bearer()))).toEqual([401, "invalid_token"]);
  });
});

describe("positive introspection cache: TTL 30000", () => {
  test("a second identical request within the TTL makes no introspection call and returns the identity", async () => {
    at(T0);
    const rc = relaycontrol(() => granted());
    const verify = edge(rc, 30_000);
    const first = await verify(bearer());
    at(T0 + 29_000);
    const second = await verify(bearer());
    expect(rc.calls.length).toBe(1);
    expect(second).toEqual(first);
    expect(second).toMatchObject({ ...IDENTITY, scopes: ["rth:read", "rth:sync"] });
    // Returned identities are copies: mutating one never changes the cached grant.
    second.scopes.length = 0;
    expect((await verify(bearer())).scopes).toEqual(["rth:read", "rth:sync"]);
    expect(rc.calls.length).toBe(1);
  });

  test("a different token is never served from another token's entry", async () => {
    at(T0);
    const rc = relaycontrol((b) => (b.token === OTHER ? refused() : granted()));
    const verify = edge(rc, 30_000);
    await verify(bearer());
    expect(await refusal(verify(bearer(OTHER)))).toEqual([401, "invalid_token"]);
    expect(rc.calls.map((c) => c.token)).toEqual([TOKEN, OTHER]);
  });

  test("revocation: after the TTL the token is re-introspected and active:false is 401 invalid_token, no stale grant", async () => {
    at(T0);
    let revoked = false;
    const rc = relaycontrol(() => (revoked ? refused() : granted()));
    const verify = edge(rc, 30_000);
    await verify(bearer());
    revoked = true;
    // Inside the window the revocation is not yet seen: the lag is bounded by the TTL.
    at(T0 + 29_999);
    expect(await verify(bearer())).toMatchObject(IDENTITY);
    expect(rc.calls.length).toBe(1);
    at(T0 + 30_001);
    expect(await refusal(verify(bearer()))).toEqual([401, "invalid_token"]);
    expect(rc.calls.length).toBe(2);
    // And it stays refused.
    expect(await refusal(verify(bearer()))).toEqual([401, "invalid_token"]);
  });

  test("exp cap: a token 10 s from exp is re-introspected after 11 s and the expired answer is 401 token_expired", async () => {
    at(T0);
    const exp = sec(T0) + 10;
    const rc = relaycontrol(() => granted(undefined, { exp }));
    const verify = edge(rc, 30_000);
    await verify(bearer());
    at(T0 + 9_000);
    await verify(bearer());
    expect(rc.calls.length).toBe(1);
    at(T0 + 11_000);
    expect(await refusal(verify(bearer()))).toEqual([401, "token_expired"]);
    expect(rc.calls.length).toBe(2);
  });

  test("scope removal: after the TTL an answer without rth:sync is 403 forbidden", async () => {
    at(T0);
    let scopes = ["rth:read", "rth:sync"];
    const rc = relaycontrol(() => granted(scopes));
    const verify = edge(rc, 30_000);
    await verify(bearer());
    scopes = ["rth:read"];
    at(T0 + 30_001);
    expect(await refusal(verify(bearer()))).toEqual([403, "forbidden"]);
    expect(rc.calls.length).toBe(2);
  });

  test("a refusal is cached at most 5 s: refused, then granted 6 s later is accepted", async () => {
    at(T0);
    let ok = false;
    const rc = relaycontrol(() => (ok ? granted() : refused()));
    const verify = edge(rc, 30_000);
    expect(await refusal(verify(bearer()))).toEqual([401, "invalid_token"]);
    ok = true;
    at(T0 + 4_000);
    expect(await refusal(verify(bearer()))).toEqual([401, "invalid_token"]);
    expect(rc.calls.length).toBe(1);
    at(T0 + 6_000);
    expect(await verify(bearer())).toMatchObject(IDENTITY);
    expect(rc.calls.length).toBe(2);
  });

  test("an outage is never cached: 503, then the next request introspects again", async () => {
    at(T0);
    let down = true;
    const rc = relaycontrol(() => (down ? { status: 503 } : granted()));
    const verify = edge(rc, 30_000);
    expect(await refusal(verify(bearer()))).toEqual([503, "relaycontrol_unavailable"]);
    down = false;
    expect(await verify(bearer())).toMatchObject(IDENTITY);
    expect(rc.calls.length).toBe(2);
  });
});

describe(SELFHOST_INTROSPECT_POSITIVE_TTL_VAR, () => {
  const enabled: NodeJS.ProcessEnv = {
    RELAYHISTORY_SELFHOST_INGEST: "true",
    RELAYHISTORY_INGEST_ORIGIN: ORIGIN,
    RELAYCONTROL_INTROSPECT_URL: "http://127.0.0.1:9",
    RELAYCONTROL_INTROSPECT_SECRET: SECRET,
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    LOCKER_BUCKET: "transcripts",
  };
  const ttlOf = (env: NodeJS.ProcessEnv) => {
    const c = selfhostIngestConfig(env);
    if (!c.enabled) throw new Error("expected enabled");
    return c.introspectPositiveTtlMs;
  };

  test("unset, empty or 0 is 0 (off); an integer up to 60000 is taken as is", () => {
    expect(ttlOf(enabled)).toBe(0);
    for (const [raw, value] of [["", 0], ["0", 0], ["1", 1], ["30000", 30_000], ["60000", 60_000]] as const) {
      expect(ttlOf({ ...enabled, [SELFHOST_INTROSPECT_POSITIVE_TTL_VAR]: raw })).toBe(value);
      expect(selfhostIntrospectPositiveTtlMs({ [SELFHOST_INTROSPECT_POSITIVE_TTL_VAR]: raw })).toBe(value);
    }
    expect(selfhostIntrospectPositiveTtlMs({})).toBe(0);
  });

  test("anything else refuses startup, naming the variable and never echoing the value; never clamped", () => {
    for (const raw of ["abc", "-1", "1.5", "60001", "99999", "1e4", "+5", "030000", " 30000", "30000 ", "0x10", "Infinity", "NaN", " "]) {
      let message = "";
      try {
        selfhostIngestConfig({ ...enabled, [SELFHOST_INTROSPECT_POSITIVE_TTL_VAR]: raw });
      } catch (e) {
        message = (e as Error).message;
      }
      expect(message).toBe(`96bulls: ${SELFHOST_INTROSPECT_POSITIVE_TTL_VAR} must be unset or an integer from 0 to 60000`);
      expect(() => selfhostIntrospectPositiveTtlMs({ [SELFHOST_INTROSPECT_POSITIVE_TTL_VAR]: raw })).toThrow(SELFHOST_INTROSPECT_POSITIVE_TTL_VAR);
    }
  });

  test("a dormant edge never reads it", () => {
    expect(selfhostIngestConfig({ [SELFHOST_INTROSPECT_POSITIVE_TTL_VAR]: "abc" })).toEqual({ enabled: false, port: 8081 });
  });
});
