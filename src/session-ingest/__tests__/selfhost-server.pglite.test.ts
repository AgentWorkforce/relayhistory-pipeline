/**
 * The self-host ingest edge (SERVICE=session-ingest, selfhost-server.ts) end to
 * end over real loopback node:http: a stub relaycontrol introspection server
 * (also node:http) answers the real introspector, the ledger is PGlite with
 * every Supabase migration and the ingest role (SET ROLE bulls96_session_ingest for
 * the role's grants; PGlite has no real login, so startup admission, which refuses
 * SET ROLE, is tested over real-login rows below), and the store is a scratch fs store.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { IDENTITY_HEADER, encodeIdentity } from "../app.ts";
import type { SqlRun } from "../ledger-sql.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector } from "../selfhost-auth.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import {
  INGEST_LOGIN_SQL, INGEST_OPTIONS, INGEST_PG16_SQL, INGEST_TIMEOUTS, IngestLoginError, assertIngestRole, ingestLoginRefusal,
  selfhostIngestRuntime, startSelfhostIngestService, type RuntimeBuilder, type SelfhostIngestService,
} from "../selfhost-server.ts";
import { SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";

setDefaultTimeout(60_000);

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
function sql(path: string): string {
  return read(path).split("\n").map((line) => (line.startsWith("\\ir ") ? sql(join(dirname(path), line.slice(4).trim())) : line.startsWith("\\") ? "" : line)).join("\n");
}

const ORIGIN = "https://history.selfhost.test";
const SECRET = "introspect-secret-test";
const TOKEN = "sh_at_desktop-token-1";

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

async function database(): Promise<{ db: PGlite; run: SqlRun; runAs: (role: string) => SqlRun }> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  cleanups.push(() => db.close());
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await db.exec(read(`supabase/bootstrap/${f}.sql`));
  for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(sql("supabase/bootstrap/session_ingest_role.sql"));
  // PGlite is one connection: serialize so SET ROLE / RESET ROLE never interleave.
  let chain: Promise<unknown> = Promise.resolve();
  const runAs = (role: string): SqlRun => (text, params) => {
    const next = chain.then(async () => {
      await db.exec(`SET ROLE ${role}`);
      try {
        return (await db.query(text, params as never)) as { rows: Record<string, unknown>[] };
      } finally {
        await db.exec("RESET ROLE");
      }
    });
    chain = next.catch(() => {});
    return next;
  };
  return { db, run: runAs("bulls96_session_ingest"), runAs };
}

type Answer = { status: number; body?: unknown } | "hang";
/** relaycontrol's introspection endpoint, scripted per call. Records each call's body and Authorization. */
async function relaycontrol(answer: (body: Record<string, unknown>) => Answer) {
  const calls: { body: Record<string, unknown>; authorization: string | undefined; path: string }[] = [];
  const server: Server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
      calls.push({ body, authorization: req.headers.authorization, path: req.url ?? "" });
      const a = answer(body);
      if (a === "hang") return;
      res.writeHead(a.status, { "content-type": "application/json" });
      res.end(a.body === undefined ? "" : JSON.stringify(a.body));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  let closed = false;
  const close = () => (closed ? undefined : ((closed = true), new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); })));
  cleanups.push(close);
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, calls, close };
}

const now = () => Math.floor(Date.now() / 1000);
const active = (scopes: string[], over: Record<string, unknown> = {}) => ({
  status: 200,
  body: { active: true, sub: TENANT.userId, email: null, principal_type: "desktop-user", machine_id: null, org: TENANT.orgId, workspace: TENANT.workspaceId, scopes, aud: ORIGIN, iat: now() - 10, exp: now() + 600, token_id: "tok-1", ...over },
});

function env(introspectUrl: string, over: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  return {
    RELAYHISTORY_SELFHOST_INGEST: "true",
    RELAYHISTORY_INGEST_ORIGIN: ORIGIN,
    RELAYCONTROL_INTROSPECT_URL: introspectUrl,
    RELAYCONTROL_INTROSPECT_SECRET: SECRET,
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres?sslmode=disable",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    LOCKER_BUCKET: "transcripts",
    ...over,
  };
}

async function chunkKeys(store: ObjectStore): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey("96bulls"), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.cursor;
  } while (cursor);
  return out;
}

async function start(rc: { url: string }, over: NodeJS.ProcessEnv = {}) {
  const { run } = await database();
  const fs = scratchStore();
  cleanups.push(fs.done);
  // No assertIngestRole here: PGlite has no real login, and the SET ROLE harness is exactly what
  // admission refuses (see "startup admission" below).
  const build: RuntimeBuilder = async (settings, e) => {
    return selfhostIngestRuntime(settings, {
      run,
      store: fs.store,
      verify: relaycontrolIngestIdentity(settings, selfhostIntrospector(settings, e)),
      ready: async () => true,
      close: async () => {},
    });
  };
  const service: SelfhostIngestService = await startSelfhostIngestService(env(rc.url, over), { host: "127.0.0.1", port: 0, build });
  cleanups.push(() => service.close());
  const url = (path: string) => `http://127.0.0.1:${service.port}${path}`;
  return { service, url, store: fs.store };
}

const deliveryBody = async (id = "e1", over: Parameters<typeof batch>[1] = {}) =>
  JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: id })], over) });
const post = (url: string, body: string, headers: Record<string, string> = {}) =>
  fetch(url, { method: "POST", body, headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}`, ...headers } });
const code = async (res: Response) => ((await res.json()) as { error: { code: string } }).error.code;

describe("self-host ingest edge over node:http", () => {
  test("accepts a Desktop batch whose relaycontrol token grants rth:sync; re-introspects every request", async () => {
    const rc = await relaycontrol(() => active(["rth:read", "rth:sync"]));
    const { url, store } = await start(rc);
    const res = await post(url("/v1/delivery/batches"), await deliveryBody(), { "x-correlation-id": "corr-1" });
    expect(res.status).toBe(200);
    expect(res.headers.get("x-correlation-id")).toBe("corr-1");
    const receipt = (await res.json()) as { receiptId: string; limits: { contentEncodings: string[] } };
    expect(receipt.receiptId).toMatch(/^rhr_/);
    expect(receipt.limits.contentEncodings).toContain("gzip");
    expect((await chunkKeys(store)).length).toBeGreaterThan(0);
    // The introspection carried our service secret and this edge's origin, never anything from the request.
    expect(rc.calls[0]!.path).toBe("/internal/v1/auth/introspect");
    expect(rc.calls[0]!.authorization).toBe(`Bearer ${SECRET}`);
    expect(rc.calls[0]!.body).toEqual({ token: TOKEN, aud: ORIGIN });
    // Same token again (an idempotent replay): introspected again, not served from a cache.
    expect((await post(url("/v1/delivery/batches"), await deliveryBody())).status).toBe(200);
    const limits = await fetch(url("/v1/delivery/limits"), { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(limits.status).toBe(200);
    expect(((await limits.json()) as { contentEncodings: string[] }).contentEncodings).toContain("gzip");
    expect(rc.calls).toHaveLength(3);
  });

  test("a revoked token is refused on the very next request (no positive cache)", async () => {
    let revoked = false;
    const rc = await relaycontrol(() => (revoked ? { status: 200, body: { active: false, reason: "revoked", token_id: "tok-1" } } : active(["rth:sync"])));
    const { url } = await start(rc);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody())).status).toBe(200);
    revoked = true;
    const res = await post(url("/v1/delivery/batches"), await deliveryBody("e2"));
    expect([res.status, await code(res)]).toEqual([401, "invalid_token"]);
    expect(rc.calls).toHaveLength(2);
  });

  test("refuses a token without rth:sync with 403, before reading or writing anything", async () => {
    const rc = await relaycontrol(() => active(["rth:read"]));
    const { url, store } = await start(rc);
    const res = await post(url("/v1/delivery/batches"), await deliveryBody());
    expect([res.status, await code(res)]).toEqual([403, "forbidden"]);
    const turns = await post(url("/v1/turns/batches"), "{}");
    expect([turns.status, await code(turns)]).toEqual([403, "forbidden"]);
    const limits = await fetch(url("/v1/delivery/limits"), { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(limits.status).toBe(403);
    expect(await chunkKeys(store)).toEqual([]);
  });

  test("an unknown scope (not on the allowlist) never grants sync", async () => {
    const rc = await relaycontrol(() => active(["rth:admin", "rth:sync:all", "RTH:SYNC"]));
    const { url } = await start(rc);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody())).status).toBe(403);
  });

  test("introspection outage fails closed with a retryable 503, never 401 and never a write", async () => {
    let mode: "500" | "401" | "malformed" | "bad-json" = "500";
    const rc = await relaycontrol(() =>
      mode === "500" ? { status: 500, body: {} } : mode === "401" ? { status: 401, body: {} } : mode === "malformed" ? { status: 200, body: { active: true, sub: "u" } } : { status: 200, body: "not json" as unknown },
    );
    const { url, store } = await start(rc);
    for (const m of ["500", "401", "malformed"] as const) {
      mode = m;
      const res = await post(url("/v1/delivery/batches"), await deliveryBody());
      expect([m, res.status, res.headers.get("retry-after"), await code(res)]).toEqual([m, 503, "1", "relaycontrol_unavailable"]);
    }
    // relaycontrol unreachable entirely.
    await rc.close();
    const down = await post(url("/v1/delivery/batches"), await deliveryBody());
    expect([down.status, await code(down)]).toEqual([503, "relaycontrol_unavailable"]);
    expect(await chunkKeys(store)).toEqual([]);
  });

  test("missing, non-relaycontrol and wrong-audience bearers are 401; a forged identity header grants nothing", async () => {
    const rc = await relaycontrol((b) => (b.token === "sh_at_other-aud" ? active(["rth:sync"], { aud: "https://evil.example" }) : active(["rth:sync"])));
    const { url, store } = await start(rc);
    const forged = encodeIdentity({ ...TENANT, scopes: ["rth:sync"] });
    const noBearer = await fetch(url("/v1/delivery/batches"), { method: "POST", body: await deliveryBody(), headers: { [IDENTITY_HEADER]: forged } });
    expect([noBearer.status, await code(noBearer)]).toEqual([401, "missing_authorization"]);
    const jwt = await post(url("/v1/delivery/batches"), await deliveryBody(), { authorization: "Bearer eyJhbGciOiJSUzI1NiJ9.e30.sig" });
    expect([jwt.status, await code(jwt)]).toEqual([401, "invalid_token"]);
    const aud = await post(url("/v1/delivery/batches"), await deliveryBody(), { authorization: "Bearer sh_at_other-aud" });
    expect([aud.status, await code(aud)]).toEqual([401, "invalid_token"]);
    expect(rc.calls.map((c) => c.body.token)).toEqual(["sh_at_other-aud"]); // the JWT never left the edge
    expect(await chunkKeys(store)).toEqual([]);
  });

  test("tenancy comes only from relaycontrol: a batch for another account is refused", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"], { org: "org-b" }));
    const { url, store } = await start(rc);
    // The batch's account is org-a's (support.ts TENANT); a forged identity header naming org-a changes nothing.
    const res = await post(url("/v1/delivery/batches"), await deliveryBody(), { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: ["rth:sync"] }) });
    expect(res.status).toBe(403);
    expect(await code(res)).toBe("delivery_account_mismatch");
    expect(await chunkKeys(store)).toEqual([]);
  });

  test("causal: a spoofed, validly signed x-relay-ingest-identity for tenant B plus a bearer for tenant A is admitted only as A, never as B", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"])); // tenant A (org-a / ws-a / user-1)
    const { url, store } = await start(rc);
    const B = { orgId: "org-b", workspaceId: "ws-b", userId: "user-b" };
    const spoof = (body: string, path = "/v1/delivery/batches") =>
      signedHeaders({ ...B, scopes: ["rth:sync", "rth:read"] }, "POST", path, new TextEncoder().encode(body), SIGNING_KEY);
    // (1) A's batch with B's spoofed identity + signature: accepted, and written only under A.
    const bodyA = await deliveryBody("spoof-a");
    const res = await post(url("/v1/delivery/batches"), bodyA, spoof(bodyA));
    expect(res.status).toBe(200);
    const keys = await chunkKeys(store);
    expect(keys.length).toBeGreaterThan(0);
    expect(keys.every((k) => k.startsWith("96bulls/org-a/"))).toBe(true);
    expect(keys.some((k) => k.includes("org-b") || k.includes("ws-b"))).toBe(false);
    // (2) B's batch (B's account) with B's spoofed identity + signature, bearer still A: refused, nothing written for B.
    const bodyB = await deliveryBody("spoof-b", { account_id: await deliveryAccount(B) });
    const refused = await post(url("/v1/delivery/batches"), bodyB, spoof(bodyB));
    expect([refused.status, await code(refused)]).toEqual([403, "delivery_account_mismatch"]);
    expect(await chunkKeys(store)).toEqual(keys);
    // (3) The spoofed headers alone (no bearer), even validly signed, are not an identity at all.
    const alone = await fetch(url("/v1/delivery/batches"), { method: "POST", body: bodyB, headers: { "content-type": "application/json", ...spoof(bodyB) } });
    expect([alone.status, await code(alone)]).toEqual([401, "missing_authorization"]);
    // relaycontrol saw only the bearer, never the spoofed headers' tenant.
    expect(rc.calls.map((c) => c.body)).toEqual([{ token: TOKEN, aud: ORIGIN }, { token: TOKEN, aud: ORIGIN }]);
  });

  test("only the three ingest routes are served; everything else is refused before auth", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url } = await start(rc);
    for (const path of ["/v1/delivery/sessions/selection", "/v1/delivery/sessions/reset", "/v1/delivery/recovery", "/v1/custody/pending", "/v1/onboarding/heartbeat"]) {
      const res = await post(url(path), "{}");
      expect([path, res.status, await code(res)]).toEqual([path, 501, "unsupported_by_backend"]);
    }
    const encoded = await post(url("/v1/delivery%2Fbatches"), "{}");
    expect(encoded.status).toBe(501);
    const wrong = await fetch(url("/v1/delivery/batches"), { headers: { authorization: `Bearer ${TOKEN}` } });
    expect([wrong.status, wrong.headers.get("allow")]).toEqual([405, "POST"]);
    expect((await fetch(url("/nope"))).status).toBe(404);
    expect(rc.calls).toHaveLength(0);
    const health = await fetch(url("/healthz"));
    expect(await health.json()).toEqual({ ok: true, enabled: true });
    expect((await fetch(url("/readyz"))).status).toBe(200);
  });

  test("an oversized body is 413 after auth, and nothing is written", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, store } = await start(rc);
    const res = await post(url("/v1/delivery/batches"), "x".repeat(2_097_152 + 4096)).catch((e: unknown) => e);
    // The edge closes the connection once the 413 is out; a client still uploading may see a reset instead.
    if (res instanceof Response) expect([res.status, await code(res)]).toEqual([413, "delivery_too_large"]);
    expect(await chunkKeys(store)).toEqual([]);
  });
});

describe("dormant and configuration", () => {
  test("dormant (default): /healthz only, every route 503 not_configured, nothing assembled", async () => {
    let built = false;
    const service = await startSelfhostIngestService({}, { host: "127.0.0.1", port: 0, build: async () => ((built = true), null as never) });
    cleanups.push(() => service.close());
    const url = (p: string) => `http://127.0.0.1:${service.port}${p}`;
    expect(await (await fetch(url("/healthz"))).json()).toEqual({ ok: true, enabled: false });
    expect((await fetch(url("/readyz"))).status).toBe(503);
    const res = await fetch(url("/v1/delivery/batches"), { method: "POST", body: "{}" });
    expect([res.status, await code(res)]).toEqual([503, "not_configured"]);
    expect(built).toBe(false);
  });

  test("strict flag, required settings, hosted-only settings refused; errors never echo values", () => {
    expect(() => selfhostIngestConfig({ RELAYHISTORY_SELFHOST_INGEST: "TRUE" })).toThrow("must be true or false");
    const good = env("http://127.0.0.1:9");
    const parsed = selfhostIngestConfig(good);
    expect(parsed).toMatchObject({ enabled: true, port: 8081, origin: ORIGIN, keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 32 });
    if (!parsed.enabled) throw new Error("expected enabled");
    // Only discrete pg fields; the URL's sslmode never reaches pg.
    expect(parsed.pool).toMatchObject({ host: "db.internal", port: 5432, user: "bulls96_session_ingest", database: "postgres", max: 4 });
    expect(JSON.stringify(parsed.pool)).not.toContain("sslmode");
    for (const name of ["RELAYHISTORY_INGEST_ORIGIN", "RELAYCONTROL_INTROSPECT_URL", "RELAYCONTROL_INTROSPECT_SECRET", "RELAYHISTORY_INGEST_DATABASE_URL", "SESSION_POSTGRES_TLS_PROFILE", "LOCKER_BUCKET"])
      expect(() => selfhostIngestConfig({ ...good, [name]: "" })).toThrow(name);
    for (const [name, value] of [["INGEST_SIGNING_KEY", "k-secret-value"], ["INGEST_ACCEPTANCE_MODE", "buffered"], ["INGEST_INLINE_MAX_SESSIONS", "40"], ["INGEST_REPLICA_MODE", "required"], ["INGEST_CUSTODY_SOURCE", "frozen-supabase"], ["INGEST_PRIVACY_AUTHORITY", "supabase"]]) {
      let message = "";
      try {
        selfhostIngestConfig({ ...good, [name!]: value });
      } catch (e) {
        message = (e as Error).message;
      }
      expect(message).toContain(name!);
      expect(message).not.toContain(value!);
    }
    // Plain-http introspection to a non-loopback host needs the in-cluster policy.
    expect(() => selfhostIngestConfig({ ...good, RELAYCONTROL_INTROSPECT_URL: "http://relaycontrol.example.com" })).toThrow("RELAYCONTROL_INTROSPECT_URL");
    expect(() => selfhostIngestConfig({ ...good, RELAYCONTROL_INTROSPECT_URL: "http://relaycontrol.rc.svc", ALLOW_IN_CLUSTER_HTTP: "true" })).not.toThrow();
    expect(() => selfhostIngestConfig({ ...good, RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://db.internal/postgres" })).toThrow("needs a user, password and database");
    let leaked = "";
    try {
      selfhostIngestConfig({ ...good, RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://u:hunter2@h/db?host=evil" });
    } catch (e) {
      leaked = (e as Error).message;
    }
    expect(leaked).toContain("RELAYHISTORY_INGEST_DATABASE_URL");
    expect(leaked).not.toContain("hunter2");
  });

});

// ---------------------------------------------------------------- startup admission (P1 identity, P2 role shape)

/**
 * Rows captured verbatim from real password (scram) logins as bulls96_session_ingest, with the pool's
 * INGEST_OPTIONS, after session_ingest_roles.sql ran as a non-superuser CREATEROLE `postgres` under the
 * bootstrap superuser `supabase_admin` (the Supabase shape) and session_ingest_role.sql pinned LOGIN
 * NOINHERIT and the timeouts: postgres:16 (160015) and postgres:15 (150019). PGlite has no real login
 * (session_user is always its superuser), so the decision is tested over these rows.
 */
const REAL_PG16_LOGIN = {
  session_user: "bulls96_session_ingest", current_user: "bulls96_session_ingest", role_setting: "none", server_version: 160015,
  statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s",
  rolsuper: false, rolbypassrls: false, rolcreaterole: false, rolcreatedb: false, rolreplication: false, rolinherit: false,
  rolconfig: ["statement_timeout=5s", "lock_timeout=2s", "idle_in_transaction_session_timeout=10s"],
  memberships: 0, members: 1, database_settings: 0,
};
const CREATOR_EDGE = { member: "postgres", grantor: "supabase_admin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false };
const REAL_PG16_EXTRA = { system_user: "scram-sha-256:bulls96_session_ingest", member_edges: [CREATOR_EDGE] };
const REAL_PG15_LOGIN = { ...REAL_PG16_LOGIN, server_version: 150019, members: 0 };

/** A SqlRun that answers the two admission statements with the given rows, and records what ran. */
function injected(login: Record<string, unknown> | null, extra: Record<string, unknown> | null = REAL_PG16_EXTRA) {
  const seen: string[] = [];
  const run: SqlRun = async (text) => {
    seen.push(text);
    if (text === INGEST_LOGIN_SQL) return { rows: login ? [login] : [] };
    if (text === INGEST_PG16_SQL) return { rows: extra ? [extra] : [] };
    throw new Error("unexpected statement");
  };
  return { run, seen };
}
const SUPABASE = { supabase: true };
const NOT_SUPABASE = { supabase: false };
async function admission(run: SqlRun, provider = SUPABASE): Promise<string> {
  try {
    await assertIngestRole(run, provider);
  } catch (e) {
    return e instanceof IngestLoginError ? e.code : `other: ${String(e)}`;
  }
  return "admitted";
}

describe("startup admission", () => {
  test("a real login as the ingest role is admitted (PG16 with the Supabase creator edge, and PG15)", async () => {
    const pg16 = injected(REAL_PG16_LOGIN);
    expect(await admission(pg16.run)).toBe("admitted");
    expect(pg16.seen).toEqual([INGEST_LOGIN_SQL, INGEST_PG16_SQL]);
    const pg15 = injected(REAL_PG15_LOGIN);
    expect(await admission(pg15.run, NOT_SUPABASE)).toBe("admitted");
    expect(pg15.seen).toEqual([INGEST_LOGIN_SQL]); // system_user and set_option do not exist before 16
    // A PG16 role with no incoming edge at all (a superuser created it: CNPG, Cloud SQL) is admitted on any provider.
    for (const provider of [SUPABASE, NOT_SUPABASE]) expect(ingestLoginRefusal({ ...REAL_PG16_LOGIN, members: 0 }, { ...REAL_PG16_EXTRA, member_edges: [] }, provider)).toBeNull();
  });

  test("causal: the non-assumable Supabase creator edge is the only incoming edge admitted, and only on a declared Supabase", async () => {
    // Positive: postgres -> ingest, granted by superuser supabase_admin, ADMIN, INHERIT FALSE, SET FALSE.
    expect(ingestLoginRefusal(REAL_PG16_LOGIN, REAL_PG16_EXTRA, SUPABASE)).toBeNull();
    // The same edge off Supabase (the verifier's db_mode gate) is refused.
    expect(ingestLoginRefusal(REAL_PG16_LOGIN, REAL_PG16_EXTRA, NOT_SUPABASE)).toBe("role_members");
    // The one bit that makes it assumable: postgres could now SET ROLE bulls96_session_ingest. Refused.
    expect(ingestLoginRefusal(REAL_PG16_LOGIN, { ...REAL_PG16_EXTRA, member_edges: [{ ...CREATOR_EDGE, set_option: true }] }, SUPABASE)).toBe("role_members");
    // An incoming member that can SET ROLE (a plain GRANT bulls96_session_ingest TO app_member: SET TRUE, the
    // PG16 default), alongside the permitted edge. Startup refuses.
    const assumable = { member: "app_member", grantor: "supabase_admin", grantor_super: true, admin_option: false, inherit_option: false, set_option: true };
    expect(await admission(injected({ ...REAL_PG16_LOGIN, members: 2 }, { ...REAL_PG16_EXTRA, member_edges: [CREATOR_EDGE, assumable] }).run)).toBe("role_members");
  });

  test("PGlite, live: a privileged login that SET ROLE bulls96_session_ingest is refused, as is the plain privileged login", async () => {
    const { run, runAs } = await database();
    // The old check (current_user only) admitted exactly this session.
    const { rows } = await run("SELECT session_user::text AS s, current_user::text AS c, current_setting('role') AS r", []);
    expect(rows[0]).toEqual({ s: "postgres", c: "bulls96_session_ingest", r: "bulls96_session_ingest" });
    expect(await admission(run)).toBe("session_user");
    expect(await admission(runAs("postgres"))).toBe("session_user");
    // The admission SQL itself parses and runs on the PGlite server under the ingest role.
    expect((await run(INGEST_LOGIN_SQL, [])).rows).toHaveLength(1);
    expect((await run(INGEST_PG16_SQL, [])).rows).toHaveLength(1);
  });

  test("causal: one mutation per forbidden class refuses startup, naming only the property", async () => {
    const L = REAL_PG16_LOGIN;
    const X = REAL_PG16_EXTRA;
    const cases: Array<[string, Record<string, unknown> | null, Record<string, unknown> | null, string]> = [
      // P1: identity
      ["privileged login + SET ROLE (session_user differs, role set)", { ...L, session_user: "postgres", role_setting: "bulls96_session_ingest" }, X, "session_user"],
      ["member login + SET ROLE", { ...L, session_user: "app_member", role_setting: "bulls96_session_ingest" }, X, "session_user"],
      ["ingest login switched away (current_user)", { ...L, current_user: "postgres", role_setting: "postgres" }, X, "current_user"],
      ["SET ROLE to itself (role setting not none)", { ...L, role_setting: "bulls96_session_ingest" }, X, "set_role"],
      ["PG16 system_user NULL (trust auth)", L, { ...X, system_user: null }, "system_user"],
      ["PG16 system_user another identity", L, { ...X, system_user: "scram-sha-256:postgres" }, "system_user"],
      ["PG16 system_user suffix only", L, { ...X, system_user: "scram-sha-256:x:bulls96_session_ingest" }, "system_user"],
      ["PG16 system_user without method", L, { ...X, system_user: "bulls96_session_ingest" }, "system_user"],
      ["PG16 extra row missing", L, null, "login_unreadable"],
      ["no login row", null, X, "login_unreadable"],
      ["unreadable version", { ...L, server_version: "160015" }, X, "login_unreadable"],
      // P2: attributes
      ["SUPERUSER", { ...L, rolsuper: true }, X, "superuser"],
      ["BYPASSRLS", { ...L, rolbypassrls: true }, X, "bypassrls"],
      ["CREATEROLE", { ...L, rolcreaterole: true }, X, "role_attributes"],
      ["CREATEDB", { ...L, rolcreatedb: true }, X, "role_attributes"],
      ["REPLICATION", { ...L, rolreplication: true }, X, "role_attributes"],
      ["INHERIT", { ...L, rolinherit: true }, X, "role_attributes"],
      ["attribute unreadable (null)", { ...L, rolcreatedb: null }, X, "role_attributes"],
      // P2: membership, the role is a member of something
      ["member of another role", { ...L, memberships: 1 }, X, "role_membership"],
      // P2: membership, something is a member of the role
      ["another role is a member", { ...L, members: 2 }, { ...X, member_edges: [CREATOR_EDGE, { ...CREATOR_EDGE, member: "attacker" }] }, "role_members"],
      ["creator edge with SET", { ...L }, { ...X, member_edges: [{ ...CREATOR_EDGE, set_option: true }] }, "role_members"],
      ["creator edge with INHERIT", { ...L }, { ...X, member_edges: [{ ...CREATOR_EDGE, inherit_option: true }] }, "role_members"],
      ["creator edge from a non-superuser grantor", { ...L }, { ...X, member_edges: [{ ...CREATOR_EDGE, grantor_super: false }] }, "role_members"],
      ["creator edge granted by another superuser", { ...L }, { ...X, member_edges: [{ ...CREATOR_EDGE, grantor: "root" }] }, "role_members"],
      ["edge to a different member name", { ...L }, { ...X, member_edges: [{ ...CREATOR_EDGE, member: "authenticator" }] }, "role_members"],
      ["unnamed member (dangling)", { ...L }, { ...X, member_edges: [{ ...CREATOR_EDGE, member: null }] }, "role_members"],
      ["edge count disagrees with the list", { ...L, members: 2 }, X, "role_members"],
      ["PG15: any member edge", { ...REAL_PG15_LOGIN, members: 1 }, X, "role_members"],
      // P2: settings and timeouts
      ["per-database role setting", { ...L, database_settings: 1 }, X, "role_settings"],
      ["role profile: statement_timeout off", { ...L, rolconfig: ["statement_timeout=0", "lock_timeout=2s", "idle_in_transaction_session_timeout=10s"] }, X, "timeouts"],
      ["role profile: extra setting", { ...L, rolconfig: [...L.rolconfig, "search_path=public"] }, X, "timeouts"],
      ["role profile: none", { ...L, rolconfig: null }, X, "timeouts"],
      ["live statement_timeout differs", { ...L, statement_timeout: "0" }, X, "timeouts"],
      ["live lock_timeout differs", { ...L, lock_timeout: "0" }, X, "timeouts"],
      ["live idle timeout differs", { ...L, idle_in_transaction_session_timeout: "0" }, X, "timeouts"],
    ];
    for (const [label, login, extra, code] of cases) expect([label, await admission(injected(login, extra).run)]).toEqual([label, code]);
    // A failing query refuses too; the message names the expected role and the property, never an observed value.
    const broken: SqlRun = async () => {
      throw new Error("connect ECONNREFUSED 10.0.0.5");
    };
    let err: unknown;
    try {
      await assertIngestRole(broken, SUPABASE);
    } catch (e) {
      err = e;
    }
    expect(String(err)).toBe("IngestLoginError: 96bulls: the session ingest login (bulls96_session_ingest) refused: login_unreadable");
    expect(String(new IngestLoginError("session_user"))).not.toContain("postgres");
  });

  test("the pool's startup options are exactly the role's pinned timeout profile (session_ingest_role.sql)", () => {
    expect(INGEST_OPTIONS).toBe("-c statement_timeout=5s -c lock_timeout=2s -c idle_in_transaction_session_timeout=10s");
    const pinned = [...read("supabase/bootstrap/session_ingest_role.sql").matchAll(/ALTER ROLE bulls96_session_ingest SET (\w+) = '([^']+)'/g)].map((m) => [m[1], m[2]]);
    expect(Object.fromEntries(pinned)).toEqual({ ...INGEST_TIMEOUTS });
  });
});
