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
import { analysisDeps, ANALYSIS_RESULT_REQUEST_BUDGET_BYTES, boundedClickHouse, connectPg, handle, methodAllowed, ownerBufferedDeps, type Env } from "../src/index.ts";
import { BufferedFenced, BufferedIndexVerifier } from "../../../src/session-read-api/owner-buffered.ts";
import { BRIEF_SUMMARY_SIGNATURE } from "../../../src/session-read-api/brief-summary.ts";
import { ANALYSIS_ADMIT_BOUNDS_SQL, ANALYSIS_ADMIT_SQL, ANALYSIS_READ_SQL } from "../../../src/session-read-api/analysis-source.ts";
import { SESSION_HARNESSES_SQL } from "../../../src/session-read-api/session-turns-source.ts";
import { TENANT_SETTINGS_SQL } from "../../../src/session-read-api/liveness.ts";

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

  test("owner-buffered on without the HISTORY_CONTROL_HEAD binding refuses every request with a 503; off or bound, it does not", async () => {
    const t = tenant();
    const calls: string[] = [];
    const binding = { fetch: async (input: RequestInfo | URL) => (calls.push(String(input)), new Response("{}", { status: 503 })) } as unknown as Fetcher;
    const call = async (path: string, over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    const on = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: t.ws, READ_API_DIRECTORY_WORKSPACES: t.ws, INGEST_S3_BUCKET: "ingest-bucket-example" };
    for (const flag of ["true", "1", " true "]) {
      for (const unbound of [{}, { HISTORY_CONTROL_HEAD: undefined }, { HISTORY_CONTROL_HEAD: {} as unknown as Fetcher }]) {
        for (const path of [`/v1/events?workspace=${t.ws}&q=x`, `/v1/sessions?workspace=${t.ws}`, `/v1/sessions/s-x/turns?workspace=${t.ws}&source=claude`]) {
          const r = await call(path, { ...on, RELAYHISTORY_READ_OWNER_BUFFERED: flag, ...unbound });
          expect(r.status, `${flag} ${path}`).toBe(503);
          expect((await body(r)).error.code).toBe("backend_unavailable");
        }
      }
    }
    // Off (the default deploy) never needs the binding and never calls it.
    for (const flag of [undefined, "", "false", "0"]) {
      const r = await call(`/v1/events?workspace=${t.ws}&q=x`, { ...on, RELAYHISTORY_READ_OWNER_BUFFERED: flag });
      expect(r.status, String(flag)).not.toBe(503);
    }
    // On and bound: the config check passes (the request is not refused for configuration).
    const bound = await call(`/v1/events?workspace=${t.ws}&q=x`, { ...on, HISTORY_CONTROL_HEAD: binding });
    expect(bound.status).not.toBe(503);
    expect(calls).toHaveLength(0);
  });

  test("all-workspaces mode (READ_API_OWNER_BUFFERED_WORKSPACES=*): parsed as *, still needs the flag, keys and binding; default stays none", () => {
    const run = (async () => {
      throw new Error("no database in this test");
    }) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_WORKSPACES: "*" } as Env, run)?.workspaces).toBe("*");
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_WORKSPACES: " * " } as Env, run)?.workspaces).toBe("*");
    // Unset (the default deploy) is none, never all; "*" mixed with ids is a literal id no workspace has.
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_WORKSPACES: undefined } as Env, run)?.workspaces).toEqual(new Set());
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_WORKSPACES: "ws-a,*" } as Env, run)?.workspaces).toEqual(new Set(["ws-a", "*"]));
    // "*" alone does not turn the feature on: flag off or S3 keys missing is still undefined.
    for (const over of [{ RELAYHISTORY_READ_OWNER_BUFFERED: undefined }, { RELAYHISTORY_READ_OWNER_BUFFERED: "false" }, { INGEST_S3_BUCKET: "" }, { PIPELINE_RAW_S3_ACCESS_KEY_ID: undefined }]) {
      expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_WORKSPACES: "*", ...over } as Env, run), JSON.stringify(over)).toBeUndefined();
    }
  });

  test("READ_API_OWNER_BUFFERED_EVENTS: only \"on\" sets events; unset/empty leaves the deps unchanged; any other value is a 503 before any read", async () => {
    const run = (async () => {
      throw new Error("no database in this test");
    }) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "ws-a", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    const keysOf = (d: object | undefined) => Object.keys(d ?? {}).sort();
    const base = ownerBufferedDeps({ ...E, ...keys } as Env, run);
    expect(base).toBeDefined();
    expect("events" in base!).toBe(false);
    for (const flag of [undefined, ""]) {
      const d = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_EVENTS: flag } as Env, run);
      expect(keysOf(d), String(flag)).toEqual(keysOf(base));
      expect("events" in d!).toBe(false);
    }
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_EVENTS: "on" } as Env, run)?.events).toBe(true);
    // The events flag alone never enables owner-buffered reads.
    expect(ownerBufferedDeps({ ...E, ...keys, RELAYHISTORY_READ_OWNER_BUFFERED: undefined, READ_API_OWNER_BUFFERED_EVENTS: "on" } as Env, run)).toBeUndefined();

    const t = tenant();
    const call = async (over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/sessions/s-x/events?workspace=${t.ws}&source=claude`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    for (const bad of ["true", "1", "ON", " on", "off", "yes"]) {
      const r = await call({ READ_API_OWNER_BUFFERED_EVENTS: bad });
      expect(r.status, bad).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
  });

  test("047010 exact-session flag selects only the stages RPC, with default off", async () => {
    const seen: string[] = [];
    const run = (async (_org: string, _ws: string, work: any) => work({ query: async (sql: string) => { seen.push(sql); return { rows: [] }; } })) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    const head = { cutoverEpoch: "1".repeat(64), privacyBaselineSha256: "4".repeat(64), head: null } as never;
    const scope = { orgId: "org-a", workspaceId: "ws-a", userId: "u-1" };
    for (const flag of [undefined, "off", "true", "ON", " on ", "on"]) {
      const deps = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_EXACT_SESSION: flag } as Env, run)!;
      seen.length = 0;
      await deps.reads.stages(scope, head, "droid", "fixture-session");
      expect(seen.at(-1)).toContain(flag === "on" ? "sessions.buffered_session_stages_for_target(" : "sessions.buffered_session_stages_for(");
      seen.length = 0;
      await deps.reads.list(scope, head, { limit: 1 });
      expect(seen.at(-1)).toContain("sessions.buffered_sessions_for(");
    }
  });

  test("047020 paged stages flag: default off; the paged RPC only with exact-session on and the exact string on", async () => {
    const seen: string[] = [];
    const envelope = { stages: [], cursor: null, has_more: false, snapshot_at: "2026-11-05T12:30:00.000Z" };
    const run = (async (_org: string, _ws: string, work: any) =>
      work({ query: async (sql: string) => { seen.push(sql); return { rows: sql.includes("buffered_session_stages_for_target_paged(") ? [envelope] : [] }; } })) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    const head = { cutoverEpoch: "1".repeat(64), privacyBaselineSha256: "4".repeat(64), head: null } as never;
    const scope = { orgId: "org-a", workspaceId: "ws-a", userId: "u-1" };
    for (const exact of [undefined, "off", "on"]) {
      for (const paged of [undefined, "", "off", "true", "ON", " on ", "on"]) {
        const deps = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_EXACT_SESSION: exact, READ_API_OWNER_BUFFERED_STAGES_PAGED: paged } as Env, run)!;
        seen.length = 0;
        await deps.reads.stages(scope, head, "droid", "fixture-session");
        const rpc = seen.filter((sql) => sql.includes("sessions.buffered_session_stages_for"));
        expect(rpc, `${exact}/${paged}`).toHaveLength(1);
        const want = exact !== "on" ? "sessions.buffered_session_stages_for(" : paged === "on" ? "sessions.buffered_session_stages_for_target_paged(" : "sessions.buffered_session_stages_for_target(";
        expect(rpc[0], `${exact}/${paged}`).toContain(want);
        if (!(exact === "on" && paged === "on")) expect(seen.some((sql) => sql.includes("_paged(")), `${exact}/${paged}`).toBe(false);
      }
    }
    // The paged flag alone never enables owner-buffered reads.
    expect(ownerBufferedDeps({ ...E, ...keys, RELAYHISTORY_READ_OWNER_BUFFERED: undefined, READ_API_OWNER_BUFFERED_EXACT_SESSION: "on", READ_API_OWNER_BUFFERED_STAGES_PAGED: "on" } as Env, run)).toBeUndefined();
  });

  test("READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: unset keeps 2000; 2000..6000 is passed to the buffered RPCs; a bad value is a 503", async () => {
    const seen: string[] = [];
    const run = (async (_org: string, _ws: string, work: (q: { query(sql: string, params?: unknown[]): Promise<{ rows: unknown[] }> }) => Promise<unknown>) =>
      work({
        async query(sql: string) {
          seen.push(sql);
          return { rows: [] };
        },
      })) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "ws-a", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    const head = { cutoverEpoch: "1".repeat(64), privacyBaselineSha256: "4".repeat(64), head: { epoch: "1".repeat(64), version: "7", digest: "3".repeat(64), kind: "selection", pending: false } } as never;
    const scope = { orgId: "org-a", workspaceId: "ws-a", userId: "u-1" };
    for (const [flag, want] of [[undefined, 2000], ["", 2000], ["2000", 2000], ["5000", 5000], ["6000", 6000]] as const) {
      seen.length = 0;
      const d = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: flag } as Env, run);
      await d!.reads.list(scope, head, { limit: 1 });
      expect(seen[0], String(flag)).toBe(`SET LOCAL statement_timeout = ${want}`);
      seen.length = 0;
      await d!.reads.stages(scope, head, "claude", "s-x");
      expect(seen[0], `stages ${String(flag)}`).toBe(`SET LOCAL statement_timeout = ${want}`);
    }

    const t = tenant();
    const call = async (over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/sessions/s-x/events?workspace=${t.ws}&source=claude`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    for (const bad of ["1999", "6001", "10000", "02000", " 3000", "on"]) {
      const r = await call({ READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: bad });
      expect(r.status, bad).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
  });

  test("READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY and _PHASE_LOG: default off leaves the deps unchanged; 2..32 and exactly \"1\" set them; a bad concurrency is a 503", async () => {
    const run = (async () => {
      throw new Error("no database in this test");
    }) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "ws-a", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    const keysOf = (d: object | undefined) => Object.keys(d ?? {}).sort();
    const base = ownerBufferedDeps({ ...E, ...keys } as Env, run);
    expect("stageConcurrency" in base!).toBe(false);
    expect("phaseLog" in base!).toBe(false);
    for (const flag of [undefined, "", "0", "1"]) {
      const d = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY: flag } as Env, run);
      expect(keysOf(d), String(flag)).toEqual(keysOf(base));
    }
    for (const flag of [undefined, "", "0", "true", "on", " 1"]) {
      const d = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_PHASE_LOG: flag } as Env, run);
      expect(keysOf(d), String(flag)).toEqual(keysOf(base));
    }
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY: "2" } as Env, run)?.stageConcurrency).toBe(2);
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY: "32" } as Env, run)?.stageConcurrency).toBe(32);
    expect(ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_PHASE_LOG: "1" } as Env, run)?.phaseLog).toBe(true);
    // Neither flag enables owner-buffered reads on its own.
    expect(ownerBufferedDeps({ ...E, ...keys, RELAYHISTORY_READ_OWNER_BUFFERED: undefined, READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY: "8", READ_API_OWNER_BUFFERED_PHASE_LOG: "1" } as Env, run)).toBeUndefined();

    const t = tenant();
    const call = async (over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/sessions/s-x/events?workspace=${t.ws}&source=claude`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    for (const bad of ["33", "02", "-2", "2.0", " 4", "on", "true"]) {
      const r = await call({ READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY: bad });
      expect(r.status, bad).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
  });

  test("READ_API_OWNER_BUFFERED_INDEX_VERIFY: only \"on\" adds the index check; unset/empty leaves the deps unchanged; any other value is a 503 before any read", async () => {
    let probes = 0;
    const run = (async () => {
      probes += 1;
      throw new Error("no database in this test");
    }) as never;
    const keys = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "ws-a", INGEST_S3_BUCKET: "ingest-bucket-example", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
    const keysOf = (d: object | undefined) => Object.keys(d ?? {}).sort();
    const base = ownerBufferedDeps({ ...E, ...keys } as Env, run);
    expect("indexCheck" in base!).toBe(false);
    for (const flag of [undefined, ""]) {
      const d = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_INDEX_VERIFY: flag } as Env, run);
      expect(keysOf(d), String(flag)).toEqual(keysOf(base));
    }
    expect(probes).toBe(0);
    // On: a fresh verifier (not the isolate one) proves the check is wired to this request's runner and
    // that a probe error fences as index_unverified rather than throwing anything else.
    const verifier = new BufferedIndexVerifier();
    const on = ownerBufferedDeps({ ...E, ...keys, READ_API_OWNER_BUFFERED_INDEX_VERIFY: "on" } as Env, run, undefined, verifier);
    expect(on?.indexCheck).toBeDefined();
    const err = await on!.indexCheck!.ensure({ orgId: "o", workspaceId: "ws-a", userId: "u" } as never).then(() => undefined, (e: unknown) => e);
    expect(err).toBeInstanceOf(BufferedFenced);
    expect((err as BufferedFenced).reason).toBe("index_unverified");
    expect(probes).toBe(1);
    // The flag alone never enables owner-buffered reads.
    expect(ownerBufferedDeps({ ...E, ...keys, RELAYHISTORY_READ_OWNER_BUFFERED: undefined, READ_API_OWNER_BUFFERED_INDEX_VERIFY: "on" } as Env, run)).toBeUndefined();

    const t = tenant();
    const call = async (over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/sessions/s-x/events?workspace=${t.ws}&source=claude`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    for (const bad of ["true", "1", "ON", " on", "off", "yes"]) {
      const r = await call({ READ_API_OWNER_BUFFERED_INDEX_VERIFY: bad });
      expect(r.status, bad).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
  });

  test("all-workspaces mode without the HISTORY_CONTROL_HEAD binding is the same 503; bound, it is not refused", async () => {
    const t = tenant();
    const call = async (over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/sessions?workspace=${t.ws}`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    const all = { RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "*", READ_API_DIRECTORY_WORKSPACES: "*", INGEST_S3_BUCKET: "ingest-bucket-example" };
    const unbound = await call(all);
    expect(unbound.status).toBe(503);
    expect((await body(unbound)).error.code).toBe("backend_unavailable");
    const binding = { fetch: async () => new Response("{}", { status: 503 }) } as unknown as Fetcher;
    expect((await call({ ...all, HISTORY_CONTROL_HEAD: binding })).status).not.toBe(503);
  });

  test("brief summary and list short summary: default off (501), on only with the exact string \"true\", as the hosted Lambda", async () => {
    const t = tenant();
    const seen: Array<{ text: string; params?: unknown[] }> = [];
    // A fake pg client: records every statement; the brief-summary function reads as not installed.
    const connect = async () => ({
      async query(text: string, params?: unknown[]) {
        seen.push({ text, params });
        return { rows: text.includes("to_regprocedure") ? [{ installed: false }] : [] };
      },
      async end() {},
    }) as unknown as pg.Client;
    const call = async (path: string, over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, READ_API_DIRECTORY_WORKSPACES: t.ws, ...over }, ctx, { connect });
      await waitOnExecutionContext(ctx);
      return r;
    };
    const summary = `/v1/sessions/s-x/summary?workspace=${t.ws}&source=claude`;
    const list = `/v1/sessions?workspace=${t.ws}&includeShortSummary=recorded`;
    const summarySql = () => seen.filter((q) => q.params?.includes(BRIEF_SUMMARY_SIGNATURE));
    // Unset (the default deploy) and every value but exactly "true": unchanged 501s, no summary SQL.
    // As on the hosted Lambda (the shared app), the 501 code is unsupported_by_backend; the cause is error.reason.
    for (const flag of [undefined, "", "1", "TRUE", "True", " true", "true ", "on", "yes"]) {
      seen.length = 0;
      const s = await call(summary, { READ_API_BRIEF_SUMMARY_ENABLED: flag, READ_API_LIST_SHORT_SUMMARY_ENABLED: flag });
      expect(s.status, `summary ${String(flag)}`).toBe(501);
      expect((await body(s)).error, `summary ${String(flag)}`).toMatchObject({ code: "unsupported_by_backend", reason: "summary_not_configured" });
      expect(summarySql(), `summary ${String(flag)}`).toHaveLength(0);
      const l = await call(list, { READ_API_BRIEF_SUMMARY_ENABLED: flag, READ_API_LIST_SHORT_SUMMARY_ENABLED: flag });
      expect(l.status, `list ${String(flag)}`).toBe(501);
      expect((await body(l)).error, `list ${String(flag)}`).toMatchObject({ code: "unsupported_by_backend", reason: "param_includeShortSummary" });
    }
    // Exactly "true": the summary route reaches the brief-summary SQL through the request's tenant runner.
    seen.length = 0;
    const on = await call(summary, { READ_API_BRIEF_SUMMARY_ENABLED: "true" });
    expect(summarySql()).toHaveLength(1);
    expect(on.status).toBe(501);
    expect((await body(on)).error).toMatchObject({ code: "unsupported_by_backend", reason: "summary_not_migrated" });
    // Each flag is independent: the summary flag alone leaves the list extension off, and vice versa.
    const listOff = await call(list, { READ_API_BRIEF_SUMMARY_ENABLED: "true" });
    expect((await body(listOff)).error).toMatchObject({ code: "unsupported_by_backend", reason: "param_includeShortSummary" });
    const listOn = await call(list, { READ_API_LIST_SHORT_SUMMARY_ENABLED: "true" });
    expect((await body(listOn)).error?.reason).not.toBe("param_includeShortSummary");
    const summaryOff = await call(summary, { READ_API_LIST_SHORT_SUMMARY_ENABLED: "true" });
    expect((await body(summaryOff)).error).toMatchObject({ code: "unsupported_by_backend", reason: "summary_not_configured" });
  });

  test("summary fallback: off unless exactly \"true\" with the brief flag; on without key material is 501 summary_fallback_not_configured", async () => {
    const t = tenant();
    // A fake pg client: the brief-summary function is installed and returns no brief.
    const connect = async () => ({
      async query(text: string) {
        return { rows: text.includes("to_regprocedure") ? [{ installed: true }] : [] };
      },
      async end() {},
    }) as unknown as pg.Client;
    const call = async (path: string, over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, READ_API_DIRECTORY_WORKSPACES: t.ws, READ_API_BRIEF_SUMMARY_ENABLED: "true", ...over }, ctx, { connect });
      await waitOnExecutionContext(ctx);
      return r;
    };
    const long = `/v1/sessions/s-x/summary?workspace=${t.ws}&source=claude&kind=long`;
    const short = `/v1/sessions/s-x/summary?workspace=${t.ws}&source=claude`;
    for (const flag of [undefined, "", "1", "TRUE", " true", "on"]) {
      const r = await call(long, { READ_API_SUMMARY_FALLBACK_ENABLED: flag, SUMMARY_MASTER_KEYS: `k1:${"a".repeat(64)}` });
      expect(r.status, String(flag)).toBe(501);
      expect((await body(r)).error, String(flag)).toMatchObject({ reason: "summary_kind" });
      // Off: the short read is exactly today's empty body.
      expect(await (await call(short, { READ_API_SUMMARY_FALLBACK_ENABLED: flag })).text(), String(flag)).toBe(JSON.stringify({ summary: { status: "empty", kind: "short", state: null } }));
    }
    // The fallback flag alone never enables the route.
    const alone = await call(long, { READ_API_BRIEF_SUMMARY_ENABLED: undefined, READ_API_SUMMARY_FALLBACK_ENABLED: "true" });
    expect((await body(alone)).error).toMatchObject({ reason: "summary_not_configured" });
    // On, no (or a malformed) SUMMARY_MASTER_KEYS: a loud 501, never an empty 200; the value is never echoed.
    for (const keys of [undefined, "", "not-a-key"]) {
      const r = await call(long, { READ_API_SUMMARY_FALLBACK_ENABLED: "true", SUMMARY_MASTER_KEYS: keys });
      expect(r.status, String(keys)).toBe(501);
      const text = JSON.stringify(await body(r));
      expect(text).toContain("summary_fallback_not_configured");
      if (keys) expect(text).not.toContain(keys);
    }
  });

  test("memory documents: default off (501 memory_documents_disabled, no SQL), on only with the exact string \"true\"", async () => {
    const t = tenant();
    const seen: Array<{ text: string; params?: unknown[] }> = [];
    // A fake pg client: records every statement; the memory documents functions read as not installed.
    const connect = async () => ({
      async query(text: string, params?: unknown[]) {
        seen.push({ text, params });
        return { rows: text.includes("to_regprocedure") ? [{ installed: false }] : [] };
      },
      async end() {},
    }) as unknown as pg.Client;
    const call = async (path: string, over: Partial<Env>) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}${path}`, { headers: { authorization: `Bearer ${await token(t)}` } }), { ...E, READ_API_DIRECTORY_WORKSPACES: t.ws, ...over }, ctx, { connect });
      await waitOnExecutionContext(ctx);
      return r;
    };
    const paths = [`/v1/memory/documents?workspace=${t.ws}`, `/v1/memory/documents/src-session-read-api?workspace=${t.ws}`];
    const memorySql = () => seen.filter((q) => q.params?.some((p) => typeof p === "string" && p.startsWith("sessions.decision_records_")));
    for (const flag of [undefined, "", "1", "TRUE", "True", " true", "true ", "on", "yes"]) {
      for (const path of paths) {
        seen.length = 0;
        const r = await call(path, { READ_API_MEMORY_DOCUMENTS_ENABLED: flag });
        expect(r.status, `${path} ${String(flag)}`).toBe(501);
        expect(await r.json(), `${path} ${String(flag)}`).toEqual({ error: "memory_documents_disabled" });
        expect(memorySql(), `${path} ${String(flag)}`).toHaveLength(0);
      }
    }
    // Exactly "true": each route reaches its function check through the request's tenant runner.
    for (const path of paths) {
      seen.length = 0;
      const on = await call(path, { READ_API_MEMORY_DOCUMENTS_ENABLED: "true" });
      expect(memorySql(), path).toHaveLength(1);
      expect(on.status, path).toBe(501);
      expect((await body(on)).error, path).toMatchObject({ code: "unsupported_by_backend", reason: "memory_documents_not_migrated" });
    }
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
  test("brief titles: off unless READ_API_BRIEF_TITLES_WORKSPACES names the workspace; on, the tenant's titles only", async () => {
    const t = tenant();
    const other = tenant();
    const bearer = await token(t);
    // The same (codex, t-1) key holds a brief in this tenant and in a foreign one.
    for (const [org, ws, title] of [[t.org, t.ws, "Tenant title"], [other.org, other.ws, "Foreign title"]] as const)
      await owner(async (c) => {
        await c.query(
          `INSERT INTO sessions.session_index
             (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
              storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title)
           VALUES ($1, $2, 'codex', 't-1', 'm-1', 'u-1', now(), now(), 'fs', 'bucket', $3,
                   decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 'pointer title')`,
          [org, ws, `manifest/${org}/${ws}/t-1`],
        );
        await c.query(
          `INSERT INTO sessions.session_briefs (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
             title, description, state, brief_revision, brief_source_revisions, brief_fingerprint, scrub_ruleset, scrub_version,
             prompt_version, engine_version, model, generated_at)
           VALUES ($1, $2, 'codex', 't-1', 'ready', 1, '{"m-1":1}', $3, 'A description.', 'completed', 1, '{"m-1":1}',
                   repeat('e', 64), 'relayhistory-scrub', 1, 'session-summary-v3', 'session-brief-v3', 'gpt-5.6-luna', now())`,
          [org, ws, title],
        );
      });
    const call = async (over: Partial<Env> = {}) => {
      const ctx = createExecutionContext();
      const r = await handle(new Request(`${ORIGIN}/v1/sessions/titles?workspace=${t.ws}&key=codex:t-1`, { headers: { authorization: `Bearer ${bearer}` } }), { ...E, ...over }, ctx);
      await waitOnExecutionContext(ctx);
      return r;
    };
    for (const over of [{}, { READ_API_BRIEF_TITLES_WORKSPACES: "" }, { READ_API_BRIEF_TITLES_WORKSPACES: "ws-someone-else" }, { READ_API_DIRECTORY_WORKSPACES: "*" }]) {
      const r = await call(over);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe("brief_titles_workspace_not_enabled");
    }
    // With migration 20261105043000 applied (global-setup runs every migration): only this
    // tenant's brief title, never the foreign tenant's for the same key.
    for (const allow of [t.ws, `ws-other,${t.ws}`, "*"]) {
      const r = await call({ READ_API_BRIEF_TITLES_WORKSPACES: allow });
      expect(r.status).toBe(200);
      expect(r.headers.get("x-relayhistory-workspace-id")).toBe(t.ws);
      expect((await body(r)).titles).toEqual([{ source: "codex", sessionId: "t-1", title: "Tenant title" }]);
    }
  });

  test("off the canary allowlist every workspace falls back; on it, the directory serves the tenant only", async () => {
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
    // With migration 20261105034000 applied (global-setup runs every migration).
    await owner((c) =>
      c.query(
        `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, task_title)
         VALUES ($1, $2, 'codex', 'd-1', '2026-10-01T05:00:00Z', '2026-10-01T06:00:00Z', 'Directory session')`,
        [t.org, t.ws],
      ),
    );
    for (const allow of [t.ws, `ws-other,${t.ws}`, "*"]) {
      const r = await call(list, { READ_API_DIRECTORY_WORKSPACES: allow });
      expect(r.status).toBe(200);
      expect(r.headers.get("x-relayhistory-workspace-id")).toBe(t.ws);
      const b = await body(r);
      expect(b.sessions.map((s: { sessionId: string }) => s.sessionId)).toEqual(["d-1"]);
      expect(b.sessions[0]).toMatchObject({ source: "codex", taskTitle: "Directory session", summarySource: "task_title" });
      const a = await call(activity, { READ_API_DIRECTORY_WORKSPACES: allow });
      expect(a.status).toBe(200);
      expect((await body(a)).days).toEqual([
        { date: "2026-10-01", total: 1, bySource: { codex: 1 } },
        { date: "2026-10-02", total: 0, bySource: {} },
      ]);
    }
    // Timing: the same allowlist, then the canonical S3 source (no raw-store secrets here).
    const timing = `/v1/sessions/s-1/timing?workspace=${t.ws}&source=codex`;
    const off = await call(timing);
    expect(off.status).toBe(501);
    expect((await body(off)).error.reason).toBe("timing_workspace_not_enabled");
    const on = await call(timing, { READ_API_DIRECTORY_WORKSPACES: t.ws });
    expect(on.status).toBe(501);
    expect((await body(on)).error.reason).toBe("timing_not_configured");
    // Another tenant's token never sees it.
    const other = tenant();
    const otherBearer = await token(other);
    const ctx = createExecutionContext();
    const r = await handle(new Request(`${ORIGIN}/v1/sessions?workspace=${other.ws}`, { headers: { authorization: `Bearer ${otherBearer}` } }), { ...E, READ_API_DIRECTORY_WORKSPACES: "*" }, ctx);
    await waitOnExecutionContext(ctx);
    expect(r.status).toBe(200);
    expect((await body(r)).sessions).toEqual([]);
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

describe("session analysis (READ_API_ANALYSIS_ENABLED; lane F1)", () => {
  const RAW = { PIPELINE_RAW_S3_BUCKET: "raw-bucket-example", PIPELINE_RAW_S3_REGION: "us-east-1", PIPELINE_RAW_S3_ACCESS_KEY_ID: "k", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "s" };
  const JOB = "0b9f6a3e-5c1d-4e2f-8a7b-6c5d4e3f2a1b";
  const result = { schemaVersion: 2, summary: "fixture analysis" };
  const resultBytes = new TextEncoder().encode(JSON.stringify(result));
  const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");

  interface Fake { seen: Array<{ text: string; params?: unknown[] }>; ended: number; connects: number }
  /** A fake pg client factory: records every statement; answers the harness and analysis SQL. */
  const fakePg = (answer: (text: string, params?: unknown[]) => unknown[] | Error) => {
    const f: Fake = { seen: [], ended: 0, connects: 0 };
    const connect = async () => {
      f.connects++;
      return {
        async query(text: string, params?: unknown[]) {
          f.seen.push({ text, params });
          const rows = answer(text, params);
          if (rows instanceof Error) throw rows;
          return { rows };
        },
        async end() { f.ended++; },
      } as unknown as pg.Client;
    };
    return { f, connect };
  };

  const setup = async (t: Tenant) => {
    const sha = hex(await crypto.subtle.digest("SHA-256", resultBytes));
    const key = `${t.org}/${t.ws}/m-1/claude/2026-10-29/s-1/normalized/session-analysis-v2/${JOB}/${sha}.json`;
    const rpc = {
      outcome: "ok",
      sessionId: "s-1",
      requests: [{ id: "r-1", sessionId: "s-1", status: "completed", analysisId: JOB }],
      analyses: [{ id: JOB, sessionId: "s-1", status: "completed", resultRef: { key, sha256: sha, bytes: resultBytes.length }, origin: "native", harness: "claude" }],
    };
    const read = fakePg((text) => (text === SESSION_HARNESSES_SQL ? [{ harness: "claude", pointers: 1, live: 1 }] : text === ANALYSIS_READ_SQL ? [{ r: rpc }] : []));
    const fetched: string[] = [];
    // The raw store: answers only the key the job row names; any other object is a 404.
    const fetcher = (async (input: RequestInfo | URL) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      fetched.push(decodeURIComponent(url.pathname));
      if (decodeURIComponent(url.pathname).endsWith(key)) return new Response(resultBytes, { status: 200, headers: { "content-length": String(resultBytes.length), etag: '"e"' } });
      return new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
    }) as typeof fetch;
    return { key, rpc, read, fetched, fetcher };
  };

  const call = async (t: Tenant, method: string, path: string, over: Partial<Env>, overrides: Parameters<typeof handle>[3], scopes = ["rth:read", "rth:sync"]) => {
    const ctx = createExecutionContext();
    const r = await handle(new Request(`${ORIGIN}${path}`, { method, headers: { authorization: `Bearer ${await token(t, { scopes })}` } }), { ...E, READ_API_DIRECTORY_WORKSPACES: t.ws, ...over }, ctx, overrides);
    await waitOnExecutionContext(ctx);
    return r;
  };
  const analysisSql = (f: Fake) => f.seen.filter((q) => q.text === ANALYSIS_READ_SQL || q.text === ANALYSIS_ADMIT_SQL);

  test("methodAllowed: GET everywhere; POST only on exactly /v1/sessions/:id/analysis with the exact flag", () => {
    const on = { READ_API_ANALYSIS_ENABLED: "true" };
    expect(methodAllowed("GET", "/v1/events", {})).toBe(true);
    expect(methodAllowed("POST", "/v1/sessions/s-1/analysis", on)).toBe(true);
    for (const flag of [undefined, "", "1", "TRUE", " true", "true ", "on"]) expect(methodAllowed("POST", "/v1/sessions/s-1/analysis", { READ_API_ANALYSIS_ENABLED: flag }), String(flag)).toBe(false);
    for (const path of ["/v1/events", "/v1/sessions/s-1/summary", "/v1/sessions/s-1/analysis/", "/v1/sessions/s-1/analysis/x", "/v1/sessions//analysis", "/v1/sessions/a/b/analysis", "/v2/sessions/s-1/analysis", "/v1/sessions/s-1/analysisx", "/v1/coordination/advisories/a-1/ack"]) {
      expect(methodAllowed("POST", path, on), path).toBe(false);
    }
    for (const method of ["PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]) expect(methodAllowed(method, "/v1/sessions/s-1/analysis", on), method).toBe(false);
  });

  test("flag off (every value but exactly \"true\"): GET 501 analysis_not_configured, POST 405, no analysis SQL, no admission connection, no object read", async () => {
    const t = tenant();
    const { read, fetched, fetcher } = await setup(t);
    const admit = fakePg(() => []);
    for (const flag of [undefined, "", "1", "TRUE", "True", " true", "true ", "on", "yes"]) {
      const g = await call(t, "GET", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: flag }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
      expect(g.status, String(flag)).toBe(501);
      expect((await body(g)).error, String(flag)).toMatchObject({ code: "unsupported_by_backend", reason: "analysis_not_configured" });
      const p = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: flag }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
      expect(p.status, String(flag)).toBe(405);
      expect(p.headers.get("allow")).toBe("GET");
      expect(await p.json()).toEqual({ error: { code: "method_not_allowed", message: "GET only" } });
    }
    expect(analysisSql(read.f)).toHaveLength(0);
    expect(admit.f.connects).toBe(0);
    expect(fetched).toHaveLength(0);
  });

  test("flag on: every other non-GET stays 405", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    const admit = fakePg(() => []);
    for (const [method, path] of [["POST", `/v1/events?workspace=${t.ws}&q=x`], ["POST", `/v1/sessions/s-1/summary?workspace=${t.ws}`], ["POST", `/v1/sessions/s-1/analysis/x?workspace=${t.ws}`], ["PUT", `/v1/sessions/s-1/analysis?workspace=${t.ws}`], ["DELETE", `/v1/sessions/s-1/analysis?workspace=${t.ws}`], ["PATCH", `/v1/sessions/s-1/analysis?workspace=${t.ws}`]] as const) {
      const r = await call(t, method, path, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
      expect(r.status, `${method} ${path}`).toBe(405);
      expect(r.headers.get("allow")).toBe("GET");
    }
    expect(admit.f.connects).toBe(0);
  });

  test("flag on without the raw store: GET and POST 501 analysis_not_configured", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    for (const method of ["GET", "POST"]) {
      const r = await call(t, method, `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, fetcher });
      expect(r.status, method).toBe(501);
      expect((await body(r)).error, method).toMatchObject({ reason: "analysis_not_configured" });
    }
  });

  test("flag on: GET serves the contract, the read as bulls96_read_api in a READ ONLY tenant transaction and the object at the job row's key", async () => {
    const t = tenant();
    const { key, read, fetched, fetcher } = await setup(t);
    const r = await call(t, "GET", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, fetcher });
    expect(r.status).toBe(200);
    expect(r.headers.get("cache-control")).toContain("no-store");
    const b = await body(r);
    expect(b.sessionId).toBe("s-1");
    expect(b.requests).toEqual([{ id: "r-1", sessionId: "s-1", status: "completed", analysisId: JOB }]);
    expect(b.analyses).toEqual([{ id: JOB, sessionId: "s-1", status: "completed", result }]);
    expect(JSON.stringify(b)).not.toContain("resultRef");
    // The read: the workspace from the verified token and the path's session, in a tenant-bound READ ONLY transaction.
    const q = read.f.seen.find((x) => x.text === ANALYSIS_READ_SQL);
    expect(q?.params).toEqual([t.ws, "s-1"]);
    const i = read.f.seen.indexOf(q!);
    expect(read.f.seen.slice(0, i).map((x) => x.text)).toContain(TENANT_SETTINGS_SQL);
    expect(read.f.seen.find((x) => x.text.startsWith("BEGIN"))?.text).toMatch(/READ ONLY/);
    expect(read.f.seen.some((x) => x.text === ANALYSIS_ADMIT_SQL)).toBe(false);
    // The object: exactly the key the row recorded, in the raw bucket.
    expect(fetched).toHaveLength(1);
    expect(fetched[0]!.endsWith(key)).toBe(true);
    expect(fetched[0]).toContain("raw-bucket-example");
  });

  test("flag on: an altered result object is a 503, never the bytes", async () => {
    const t = tenant();
    const { key, read } = await setup(t);
    const tampered = (async (input: RequestInfo | URL) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (decodeURIComponent(url.pathname).endsWith(key)) return new Response(new TextEncoder().encode(JSON.stringify({ ...result, summary: "other text!" })), { status: 200 });
      return new Response("", { status: 404 });
    }) as typeof fetch;
    const r = await call(t, "GET", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, fetcher: tampered });
    expect(r.status).toBe(503);
    expect(JSON.stringify(await body(r))).not.toContain("other text");
  });

  test("result reads: an object larger than recorded is refused by content-length before its body is read; one request reads at most the byte budget", async () => {
    const t = tenant();
    const { key } = await setup(t);
    const sha = key.slice(-69, -5);
    const run = (async () => { throw new Error("no database in this test"); }) as never;
    let pulled = 0;
    let fetches = 0;
    const big = (async () => {
      fetches++;
      const stream = new ReadableStream<Uint8Array>({ pull(c) { pulled++; c.enqueue(new Uint8Array(1024)); } });
      return new Response(stream, { status: 200, headers: { "content-length": String(resultBytes.length + 1) } });
    }) as typeof fetch;
    const deps = analysisDeps({ ...E, ...RAW } as Env, run, undefined, big)!;
    await expect(deps.result({ key, sha256: sha, bytes: resultBytes.length })).rejects.toMatchObject({ name: "AnalysisResultError", message: "analysis_result_mismatch" });
    expect(fetches).toBe(1);
    expect(pulled).toBeLessThanOrEqual(1);
    // No size on the response (content-length absent = 0) while the row recorded one: refused before the body too.
    pulled = 0;
    const unsized = (async () => new Response(new ReadableStream<Uint8Array>({ pull(c) { pulled++; c.enqueue(new Uint8Array(1024)); } }), { status: 200 })) as typeof fetch;
    await expect(analysisDeps({ ...E, ...RAW } as Env, run, undefined, unsized)!.result({ key, sha256: sha, bytes: resultBytes.length })).rejects.toMatchObject({ name: "AnalysisResultError", message: "analysis_result_mismatch" });
    expect(pulled).toBeLessThanOrEqual(1);
    // The budget: refs whose recorded sizes sum past it are refused before any fetch of the overflow.
    const counted = (async () => { fetches++; return new Response(resultBytes, { status: 200, headers: { "content-length": String(resultBytes.length) } }); }) as typeof fetch;
    fetches = 0;
    const budget = analysisDeps({ ...E, ...RAW } as Env, run, undefined, counted)!;
    const chunk = 8 * 1024 * 1024;
    const n = Math.floor(ANALYSIS_RESULT_REQUEST_BUDGET_BYTES / chunk);
    // Each fits the per-object bound but mismatches the tiny object: still spends budget, one fetch each.
    for (let i = 0; i < n; i++) await expect(budget.result({ key, sha256: sha, bytes: chunk })).rejects.toMatchObject({ name: "AnalysisResultError" });
    expect(fetches).toBe(n);
    await expect(budget.result({ key, sha256: sha, bytes: 1 })).rejects.toMatchObject({ message: "analysis_result_budget" });
    expect(fetches).toBe(n);
    // A fresh request (deps are per request) has its own budget and serves the real object.
    expect(await analysisDeps({ ...E, ...RAW } as Env, run, undefined, counted)!.result({ key, sha256: sha, bytes: resultBytes.length })).toEqual(result);
  });

  test("flag on without ANALYSIS_ADMIT: POST 501 analysis_admission_not_configured; GET still serves", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    const over = { ...RAW, READ_API_ANALYSIS_ENABLED: "true", ANALYSIS_ADMIT: undefined };
    const p = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, over, { connect: read.connect, fetcher });
    expect(p.status).toBe(501);
    expect((await body(p)).error).toMatchObject({ reason: "analysis_admission_not_configured" });
    expect((await call(t, "GET", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, over, { connect: read.connect, fetcher })).status).toBe(200);
    expect(read.f.seen.some((x) => x.text === ANALYSIS_ADMIT_SQL)).toBe(false);
  });

  test("flag on with the admission principal: POST 202 through its own client, BEGIN / tenant / admit / COMMIT, closed after the response", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    const request = { id: "r-2", sessionId: "s-1", status: "queued", analysisId: JOB };
    const admit = fakePg((text) => (text === ANALYSIS_ADMIT_SQL ? [{ r: { outcome: "accepted", request } }] : []));
    const r = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
    expect(r.status).toBe(202);
    expect((await body(r)).request).toEqual(request);
    expect(admit.f.seen.map((x) => x.text)).toEqual(["BEGIN", ANALYSIS_ADMIT_BOUNDS_SQL, TENANT_SETTINGS_SQL, ANALYSIS_ADMIT_SQL, "COMMIT"]);
    expect(admit.f.seen[2]!.params).toEqual([t.org, t.ws]);
    expect(admit.f.seen[3]!.params).toEqual([t.org, t.ws, t.user, "s-1"]);
    expect(admit.f.connects).toBe(1);
    expect(admit.f.ended).toBe(1);
    // The read role's client never runs the admission; the admission's never runs a read.
    expect(read.f.seen.some((x) => x.text === ANALYSIS_ADMIT_SQL)).toBe(false);
    expect(admit.f.seen.some((x) => x.text === ANALYSIS_READ_SQL || x.text === SESSION_HARNESSES_SQL)).toBe(false);
  });

  test("flag on: POST keeps relayhistory's outcome map through the Worker (404, 429 with Retry-After: 86400, 400 invalid_request)", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    for (const [outcome, status, code] of [["not_found", 404, "not_found"], ["rate_limited", 429, "rate_limited"], ["invalid_request", 400, "invalid_request"]] as const) {
      const a = fakePg((text) => (text === ANALYSIS_ADMIT_SQL ? [{ r: { outcome } }] : []));
      const x = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, connectAnalysisAdmit: a.connect, fetcher });
      expect(x.status, outcome).toBe(status);
      expect(x.headers.get("retry-after"), outcome).toBe(outcome === "rate_limited" ? "86400" : null);
      expect((await body(x)).error?.code, outcome).toBe(code);
      expect(a.f.seen.map((q) => q.text), outcome).toEqual(["BEGIN", ANALYSIS_ADMIT_BOUNDS_SQL, TENANT_SETTINGS_SQL, ANALYSIS_ADMIT_SQL, "COMMIT"]);
      expect(a.f.ended, outcome).toBe(1);
    }
  });

  test("flag on: a cross-workspace POST is exactly 403 and runs no SQL on either principal", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    const admit = fakePg(() => []);
    const r = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=ws-not-mine`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
    expect(r.status).toBe(403);
    expect((await body(r)).error.code).toBe("forbidden");
    expect(read.f.seen).toEqual([]);
    expect(admit.f.connects).toBe(0);
  });

  test("flag on: a failing admission rolls back and is a 5xx; POST without rth:sync is 403 and opens no admission connection", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    const failing = fakePg((text) => (text === ANALYSIS_ADMIT_SQL ? Object.assign(new Error("tenant binding missing or not this tenant"), { code: "42501" }) : []));
    const r = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, connectAnalysisAdmit: failing.connect, fetcher });
    expect(r.status).toBeGreaterThanOrEqual(500);
    expect(JSON.stringify(await body(r))).not.toContain("tenant binding");
    expect(failing.f.seen.map((x) => x.text)).toEqual(["BEGIN", ANALYSIS_ADMIT_BOUNDS_SQL, TENANT_SETTINGS_SQL, ANALYSIS_ADMIT_SQL, "ROLLBACK"]);
    expect(failing.f.ended).toBe(1);
    const unscoped = fakePg(() => []);
    const fresh = await setup(t);
    const f = await call(t, "POST", `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: fresh.read.connect, connectAnalysisAdmit: unscoped.connect, fetcher }, ["rth:read"]);
    expect(f.status).toBe(403);
    expect(unscoped.f.connects).toBe(0);
    // rth:sync is checked before the read role's harness query: no SQL at all.
    expect(fresh.read.f.seen).toEqual([]);
  });

  test("flag on: the canary allowlist and the token's workspace still gate both methods", async () => {
    const t = tenant();
    const { read, fetcher } = await setup(t);
    const admit = fakePg(() => []);
    for (const method of ["GET", "POST"]) {
      const off = await call(t, method, `/v1/sessions/s-1/analysis?workspace=${t.ws}`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true", READ_API_DIRECTORY_WORKSPACES: "" }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
      expect(off.status, method).toBe(501);
      const other = await call(t, method, `/v1/sessions/s-1/analysis?workspace=ws-not-mine`, { ...RAW, READ_API_ANALYSIS_ENABLED: "true" }, { connect: read.connect, connectAnalysisAdmit: admit.connect, fetcher });
      expect(other.status, method).toBeGreaterThanOrEqual(400);
      expect(other.status, method).not.toBe(200);
      expect(other.status, method).not.toBe(202);
    }
    expect(admit.f.connects).toBe(0);
    expect(analysisSql(read.f)).toHaveLength(0);
  });
});
