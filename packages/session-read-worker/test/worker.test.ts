/**
 * The Worker's full request path inside workerd: the deployed entry
 * (`exports.default.fetch`), RelayAuth RS256 assertions verified against a live
 * JWKS endpoint, ClickHouse as a SELECT-only user over HTTP, and Supabase as
 * `bulls96_read_api` through the Hyperdrive binding, with the role SQL and RLS
 * policy exactly as the lead applies them (test/global-setup.ts).
 */
import { env, exports } from "cloudflare:workers";
import { createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import type pg from "pg";
import { beforeAll, describe, expect, test } from "vitest";
import { createClickHouseClient, type ClickHouseClient } from "../../../src/session-pipeline/vendor/search-index/clickhouse.ts";
import { row } from "../../../src/session-read-api/__tests__/support.ts";
import { boundedClickHouse, connectPg, handle, type Env } from "../src/index.ts";

type TestEnv = Env & {
  TEST_PG_ADMIN: Hyperdrive;
  TEST_CH_ADMIN_USER: string;
  TEST_CH_ADMIN_PASSWORD: string;
  TEST_SIGNING_JWK: JsonWebKey & { kid: string };
  READ_API_ORIGIN: string;
};
const E = env as unknown as TestEnv;
const ORIGIN = E.READ_API_ORIGIN;

// ---- RelayAuth assertions, minted as relayauth tokens.ts issueRelayhistoryAssertion does.

const b64url = (bytes: Uint8Array) => btoa(String.fromCharCode(...bytes)).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
const enc = (v: unknown) => b64url(new TextEncoder().encode(JSON.stringify(v)));

async function signWith(jwk: JsonWebKey & { kid: string }, claims: Record<string, unknown>): Promise<string> {
  const key = await crypto.subtle.importKey("jwk", jwk, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"]);
  const head = `${enc({ alg: "RS256", typ: "JWT", kid: jwk.kid })}.${enc(claims)}`;
  const sig = new Uint8Array(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(head)));
  return `${head}.${b64url(sig)}`;
}

interface Tenant {
  org: string;
  ws: string;
  user: string;
}

function assertionClaims(t: Tenant, over: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return {
    sub: "agent_relayhistory_assertion",
    org: t.org,
    wks: t.ws,
    scopes: ["rth:read"],
    sponsorId: t.user,
    sponsorChain: [t.user, "agent_relayhistory_assertion"],
    token_type: "access",
    iss: "https://relayauth.dev",
    aud: [ORIGIN],
    exp: now + 60,
    iat: now,
    jti: crypto.randomUUID(),
    meta: { tokenClass: "relayhistory_assertion", workspaceId: t.ws, historyOrigin: ORIGIN },
    ...over,
  };
}

const token = (t: Tenant, over: Record<string, unknown> = {}) => signWith(E.TEST_SIGNING_JWK, assertionClaims(t, over));

async function get(path: string, bearer?: string, init: RequestInit = {}): Promise<Response> {
  return (exports as unknown as { default: Fetcher }).default.fetch(
    new Request(`${ORIGIN}${path}`, { ...init, headers: { ...(bearer ? { authorization: `Bearer ${bearer}` } : {}), ...(init.headers ?? {}) } }),
  );
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;

// ---- Seeding: ClickHouse as admin, Supabase pointers as the database owner.

let admin: ClickHouseClient;
const tenant = (): Tenant => {
  const n = crypto.randomUUID().slice(0, 8);
  return { org: `org-${n}`, ws: `ws-${n}`, user: `u-${n}` };
};

async function insert(rows: ReturnType<typeof row>[]) {
  // One INSERT per row: versions inserted together would collapse on insert.
  for (const r of rows) await admin.insert("transcript_events", [r]);
}

async function owner<T>(work: (c: pg.Client) => Promise<T>): Promise<T> {
  const c = await connectPg(E.TEST_PG_ADMIN.connectionString);
  try {
    return await work(c);
  } finally {
    await c.end();
  }
}

async function pointer(t: Tenant, p: { session: string; harness?: string; machine?: string; created?: string; deleted?: boolean }) {
  await owner((c) =>
    c.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
       VALUES ($1, $2, $3, $4, $5, 'u', $6::timestamptz, $6::timestamptz, 's3', 'bucket', 'k/' || $4 || $5,
               decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2,
               CASE WHEN $7 THEN now() END)`,
      [t.org, t.ws, p.harness ?? "codex", p.session, p.machine ?? "m-1", p.created ?? "2026-10-01T00:00:00Z", p.deleted ?? false],
    ),
  );
}

const ev = (t: Tenant, r: Parameters<typeof row>[0]) => row({ org: t.org, ws: t.ws, ...r });

beforeAll(async () => {
  admin = createClickHouseClient({ url: E.CLICKHOUSE_URL!, user: E.TEST_CH_ADMIN_USER, password: E.TEST_CH_ADMIN_PASSWORD, database: E.CLICKHOUSE_DATABASE! });
  // Keep every inserted version a separate row, as production holds them before a merge.
  await admin.command("SYSTEM STOP MERGES transcript_events");
});

describe("entry and configuration", () => {
  test("health answers without a token; /v1 without one is 401", async () => {
    const h = await get("/health");
    expect(h.status).toBe(200);
    expect(h.headers.get("x-sessions-read-backend")).toBe("96bulls");
    expect(h.headers.get("cache-control")).toBe("private, no-store");
    expect((await get("/v1/events?workspace=w&q=x")).status).toBe(401);
  });

  test("GET only", async () => {
    const t = tenant();
    const r = await get(`/v1/events?workspace=${t.ws}&q=x`, await token(t), { method: "POST", body: "{}" });
    expect(r.status).toBe(405);
    expect(r.headers.get("allow")).toBe("GET");
  });

  test("missing configuration is a 503 that names no value; missing origin refuses every token", async () => {
    const t = tenant();
    const call = async (over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/events?workspace=${t.ws}&q=x`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    for (const over of [{ CLICKHOUSE_PASSWORD: "" }, { CLICKHOUSE_DATABASE: "" }, { READ_API_COMPLETE_SINCE: "" }, { READ_API_COMPLETE_SINCE: "soon" }]) {
      const r = await call(over);
      expect(r.status).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
    // A plain-http ClickHouse anywhere but loopback is refused before any request.
    expect((await call({ CLICKHOUSE_URL: "http://clickhouse.example.com:8123" })).status).toBe(503);
    const noOrigin = await call({ READ_API_ORIGIN: "" });
    expect(noOrigin.status).toBe(401);
    expect((await body(noOrigin)).error.code).toBe("not_configured");
  });

  test("Supabase or ClickHouse down is a 503 the caller falls back from, never a page without erasure checks", async () => {
    const t = tenant();
    await insert([ev(t, { id: "d-1", session: "s-down", ts: "2026-10-23 08:00:00.000" })]);
    await pointer(t, { session: "s-down" });
    const bearer = await token(t);
    const call = async (path: string, over: Partial<Env>, connect?: () => Promise<pg.Client>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${bearer}` } }), { ...E, ...over }, ctx, connect ? { connect } : {});
      await waitOnExecutionContext(ctx);
      return r;
    };
    const down = () => Promise.reject(Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" }));
    for (const path of [`/v1/sessions/s-down/events?workspace=${t.ws}`, `/v1/events?workspace=${t.ws}&q=content&limit=20`]) {
      const r = await call(path, {}, down);
      expect(r.status, path).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
    // workerd also logs the refused loopback connection as "Network connection
    // lost"; the fetch still rejects and the request still answers 503.
    const noCh = await call(`/v1/events?workspace=${t.ws}&q=content&limit=20`, { CLICKHOUSE_URL: "http://127.0.0.1:1" });
    expect(noCh.status).toBe(503);
    const badCreds = await call(`/v1/events?workspace=${t.ws}&q=content&limit=20`, { CLICKHOUSE_PASSWORD: "wrong" });
    expect(badCreds.status).toBe(503);
  });

  test("every ClickHouse read the Worker makes carries the per-query bounds, and its client cannot write", async () => {
    const t = tenant();
    await pointer(t, { session: "s-bounds" });
    const seen: URL[] = [];
    const ctx = createExecutionContext();
    const r = await handle(
      new Request(`${ORIGIN}/v1/sessions/s-bounds/events?workspace=${t.ws}`, { headers: { authorization: `Bearer ${await token(t)}` } }),
      E,
      ctx,
      {
        fetcher: async (input) => {
          seen.push(new URL(String(input)));
          return new Response("", { status: 200 });
        },
      },
    );
    await waitOnExecutionContext(ctx);
    expect(r.status).toBe(200);
    // The partial-session check, then the candidate scan.
    expect(seen.length).toBeGreaterThanOrEqual(2);
    for (const u of seen) {
      expect(u.searchParams.get("cancel_http_readonly_queries_on_client_close")).toBe("1");
      expect(u.searchParams.get("max_threads")).toBe("4");
      expect(u.searchParams.get("max_execution_time")).toBe("2");
      expect(u.searchParams.get("database")).toBe(E.CLICKHOUSE_DATABASE);
    }
    const ch = boundedClickHouse(createClickHouseClient({ url: "https://ch.example.com:8443", user: "u", password: "p", database: "relay_search" }, async () => new Response("")));
    await expect(ch.command("DROP TABLE x")).rejects.toThrow(/does not write/);
    await expect(ch.insert("transcript_events", [{}])).rejects.toThrow(/does not write/);
  });
});

describe("RelayAuth assertions addressed to this Worker", () => {
  test("only an RS256 assertion minted for this origin, with rth:read, is accepted", async () => {
    const t = tenant();
    const path = `/v1/events?workspace=${t.ws}&q=hello&limit=20`;
    const ok = await get(path, await token(t));
    expect(ok.status).toBe(200);
    expect(ok.headers.get("x-relayhistory-workspace-id")).toBe(t.ws);

    const refused: Record<string, Record<string, unknown>> = {
      "another History's origin": { aud: ["https://history.agentrelay.com"], meta: { tokenClass: "relayhistory_assertion", historyOrigin: "https://history.agentrelay.com" } },
      "the legacy shared audience": { aud: ["relayhistory"] },
      "this origin next to another": { aud: [ORIGIN, "relayhistory"] },
      "not the assertion class": { sub: "agent_x", meta: {} },
      "another issuer": { iss: "https://evil.example" },
      expired: { exp: Math.floor(Date.now() / 1000) - 3600 },
      "alternative tenancy claim names only": { org: undefined, wks: undefined, org_id: t.org, workspace_id: t.ws },
      "a migration assertion": { meta: { tokenClass: "relayhistory_migration_assertion" }, sub: "agent_relayhistory_migration" },
    };
    for (const [why, over] of Object.entries(refused)) {
      const r = await get(path, await token(t, over));
      expect(r.status, why).toBe(401);
    }

    // A key the JWKS does not hold, under the same kid.
    const other = await crypto.subtle.generateKey({ name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["sign"]);
    const otherJwk = (await crypto.subtle.exportKey("jwk", (other as CryptoKeyPair).privateKey)) as JsonWebKey;
    const forged = await signWith({ ...otherJwk, kid: E.TEST_SIGNING_JWK.kid }, assertionClaims(t));
    expect((await get(path, forged)).status).toBe(401);

    const noScope = await get(path, await token(t, { scopes: ["rth:sync"] }));
    expect(noScope.status).toBe(403);
    expect((await get(`/v1/events?workspace=ws-other&q=hello&limit=20`, await token(t))).status).toBe(403);
  });
});

describe("reads through ClickHouse and Supabase", () => {
  test("search is tenant-isolated and serves the newest version only (#242 H1)", async () => {
    const a = tenant();
    const b = tenant();
    await insert([
      ev(a, { id: "a-1", ts: "2026-10-20 10:00:00.000", session: "sa", content: "needle in a" }),
      ev(b, { id: "b-1", ts: "2026-10-20 10:00:01.000", session: "sb", content: "needle in b" }),
      // a-2's newer version was redacted: the stale "needle" must never surface.
      ev(a, { id: "a-2", ts: "2026-10-20 10:00:02.000", session: "sa", content: "needle, before redaction", version: 1 }),
      ev(a, { id: "a-2", ts: "2026-10-20 10:00:02.000", session: "sa", content: "[redacted]", version: 2 }),
    ]);
    await pointer(a, { session: "sa" });
    await pointer(b, { session: "sb" });
    const r = await get(`/v1/events?workspace=${a.ws}&q=needle&limit=20`, await token(a));
    expect(r.status).toBe(200);
    const page = await body(r);
    expect(page.events.map((e: { eventId: string }) => e.eventId)).toEqual(["a-1"]);
    expect(page.events[0]).toMatchObject({ sessionId: "sa", source: "codex", content: "needle in a", ts: "2026-10-20T10:00:00.000Z" });
    expect(page.nextCursor).toBeNull();
  });

  test("session pages walk every event exactly once through b96e1 cursors; a foreign cursor is a 501", async () => {
    const t = tenant();
    const rows = Array.from({ length: 23 }, (_, i) =>
      // Pairs share a millisecond: the cursor must split inside one.
      ev(t, { id: `e-${String(i).padStart(2, "0")}`, session: "s-walk", ts: `2026-10-21 09:00:${String(Math.floor(i / 2)).padStart(2, "0")}.000` }),
    );
    await insert(rows);
    await pointer(t, { session: "s-walk" });
    const bearer = await token(t);
    const seen: string[] = [];
    let cursor: string | null = null;
    for (let i = 0; i < 10; i++) {
      const r = await get(`/v1/sessions/s-walk/events?workspace=${t.ws}&limit=5${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`, bearer);
      expect(r.status).toBe(200);
      const page = await body(r);
      seen.push(...page.events.map((e: { eventId: string }) => e.eventId));
      cursor = page.nextCursor;
      if (!cursor) break;
      expect(cursor.startsWith("b96e1:")).toBe(true);
    }
    expect(cursor).toBeNull();
    expect(seen).toEqual(rows.map((r) => r.event_id));

    const foreign = await get(`/v1/sessions/s-walk/events?workspace=${t.ws}&cursor=events-v2:abc`, bearer);
    expect(foreign.status).toBe(501);
    expect((await body(foreign)).error.reason).toBe("foreign_cursor");
  });

  test("an erased session reads empty and drops out of search before the pipeline deletes its rows", async () => {
    const t = tenant();
    await insert([
      ev(t, { id: "x-1", session: "s-gone", ts: "2026-10-22 08:00:00.000", content: "secret plan" }),
      ev(t, { id: "x-2", session: "s-kept", ts: "2026-10-22 08:00:01.000", content: "secret plan, kept" }),
    ]);
    await pointer(t, { session: "s-gone", machine: "m-1", deleted: true });
    await pointer(t, { session: "s-gone", machine: "m-2", deleted: true });
    await pointer(t, { session: "s-kept" });
    const bearer = await token(t);
    const read = await body(await get(`/v1/sessions/s-gone/events?workspace=${t.ws}`, bearer));
    expect(read.events).toEqual([]);
    expect(read.nextCursor).toBeNull();
    const search = await body(await get(`/v1/events?workspace=${t.ws}&q=secret&limit=20`, bearer));
    expect(search.events.map((e: { eventId: string }) => e.eventId)).toEqual(["x-2"]);

    // One live pointer row keeps the session readable.
    await pointer(t, { session: "s-gone", machine: "m-3" });
    expect((await body(await get(`/v1/sessions/s-gone/events?workspace=${t.ws}`, bearer))).events).toHaveLength(1);
  });

  test("another tenant's tombstone never erases this tenant's session", async () => {
    const a = tenant();
    const b = tenant();
    await insert([ev(a, { id: "t-1", session: "s-shared-id", ts: "2026-10-22 09:00:00.000" })]);
    await pointer(a, { session: "s-shared-id" });
    await pointer(b, { session: "s-shared-id", deleted: true });
    expect((await body(await get(`/v1/sessions/s-shared-id/events?workspace=${a.ws}`, await token(a)))).events).toHaveLength(1);
  });

  test("the history floor: older sessions, and sessions with no pointer, are 501 for the caller to fall back", async () => {
    const t = tenant();
    await insert([
      ev(t, { id: "f-1", session: "s-old", ts: "2026-09-01 00:00:00.000" }),
      ev(t, { id: "f-2", session: "s-new", ts: "2026-10-25 00:00:00.000" }),
      ev(t, { id: "f-3", session: "s-nopointer", ts: "2026-10-25 00:00:01.000" }),
    ]);
    await pointer(t, { session: "s-old", created: "2026-09-01T00:00:00Z" });
    await pointer(t, { session: "s-new", created: "2026-10-25T00:00:00Z" });
    const bearer = await token(t);
    const call = async (path: string) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${bearer}` } }), { ...E, READ_API_COMPLETE_SINCE: "2026-10-01T00:00:00Z" }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    const old = await call(`/v1/sessions/s-old/events?workspace=${t.ws}`);
    expect(old.status).toBe(501);
    expect((await body(old)).error.reason).toBe("history_floor");
    const none = await call(`/v1/sessions/s-nopointer/events?workspace=${t.ws}`);
    expect(none.status).toBe(501);
    expect((await body(none)).error.reason).toBe("history_floor_unknown_session");
    expect((await call(`/v1/sessions/s-new/events?workspace=${t.ws}`)).status).toBe(200);
    // A first search page that would need rows below the floor falls back whole.
    const search = await call(`/v1/events?workspace=${t.ws}&q=content&limit=20`);
    expect(search.status).toBe(501);
    expect((await body(search)).error.reason).toBe("history_floor");
  });
});

describe("directory list and activity (P3b)", () => {
  test("off the canary allowlist every workspace falls back; on it, 501 until migration 20261105029000 exists", async () => {
    const t = tenant();
    const bearer = await token(t);
    const call = async (path: string, over: Partial<Env> = {}) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${bearer}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    const list = `/v1/sessions?workspace=${t.ws}`;
    const activity = `/v1/sessions/activity?workspace=${t.ws}&since=2026-10-01T00:00:00Z&until=2026-10-02T00:00:00Z`;
    for (const over of [{}, { READ_API_DIRECTORY_WORKSPACES: "" }, { READ_API_DIRECTORY_WORKSPACES: "ws-someone-else" }]) {
      for (const path of [list, activity]) {
        const r = await call(path, over);
        expect(r.status).toBe(501);
        expect((await body(r)).error.reason).toBe("directory_workspace_not_enabled");
      }
    }
    for (const allow of [t.ws, `ws-other,${t.ws}`, "*"]) {
      for (const path of [list, activity]) {
        const r = await call(path, { READ_API_DIRECTORY_WORKSPACES: allow });
        expect(r.status).toBe(501);
        expect((await body(r)).error.reason).toBe("directory_not_migrated");
      }
    }
    // Timing: the same allowlist, then the canonical S3 source (no raw-store secrets here).
    const timing = `/v1/sessions/s-1/timing?workspace=${t.ws}&source=codex`;
    const off = await call(timing);
    expect(off.status).toBe(501);
    expect((await body(off)).error.reason).toBe("timing_workspace_not_enabled");
    const on = await call(timing, { READ_API_DIRECTORY_WORKSPACES: t.ws });
    expect(on.status).toBe(501);
    expect((await body(on)).error.reason).toBe("timing_not_configured");
  });
});

describe("bulls96_read_api through Hyperdrive", () => {
  async function asRole<T>(work: (c: pg.Client) => Promise<T>): Promise<T> {
    const c = await connectPg(E.SUPABASE.connectionString);
    try {
      return await work(c);
    } finally {
      await c.end();
    }
  }

  test("RLS admits only the tenant this transaction set, and nothing without one", async () => {
    const a = tenant();
    const b = tenant();
    await pointer(a, { session: "rls-a" });
    await pointer(b, { session: "rls-b" });
    await asRole(async (c) => {
      const ids = async () => (await c.query("SELECT session_id FROM sessions.session_index WHERE session_id IN ('rls-a', 'rls-b')")).rows.map((r) => r.session_id);
      expect(await ids()).toEqual([]);
      await c.query("BEGIN READ ONLY");
      await c.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [a.org, a.ws]);
      expect(await ids()).toEqual(["rls-a"]);
      await c.query("COMMIT");
      // Transaction-local: the next transaction on this connection sees nothing again.
      expect(await ids()).toEqual([]);
    });
  });

  test("column SELECT only, and no writes", async () => {
    const a = tenant();
    await pointer(a, { session: "cols" });
    await asRole(async (c) => {
      // The object-pointer columns (timing's canonical read) are granted; content and metadata are not.
      await c.query("SELECT manifest_key, bucket, content_sha256, revision FROM sessions.session_index LIMIT 1");
      await expect(c.query("SELECT title FROM sessions.session_index LIMIT 1")).rejects.toMatchObject({ code: "42501" });
      await expect(c.query("SELECT model FROM sessions.session_index LIMIT 1")).rejects.toMatchObject({ code: "42501" });
      await expect(c.query("SELECT * FROM sessions.session_index LIMIT 1")).rejects.toMatchObject({ code: "42501" });
      await expect(c.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 'cols'")).rejects.toMatchObject({ code: expect.stringMatching(/^(25006|42501)$/) });
    });
  });
});
