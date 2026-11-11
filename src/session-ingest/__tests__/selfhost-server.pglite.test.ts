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
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { SESSION_DIRECTORY_SEARCH_RPC } from "../../hoopdb/search-index-contract.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { IDENTITY_HEADER, contentEncodings, encodeIdentity } from "../app.ts";
import { DELIVERY_LIMITS } from "../vendor/delivery/delivery-contracts.ts";
import type { SqlRun } from "../ledger-sql.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector } from "../selfhost-auth.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import {
  INGEST_LOGIN_SQL, INGEST_OPTIONS, INGEST_PG16_SQL, INGEST_TIMEOUTS, IngestLoginError, assertIngestRole, ingestLoginRefusal,
  selfhostIngestRuntime, startSelfhostIngestService, type RuntimeBuilder, type SelfhostIngestService,
} from "../selfhost-server.ts";
import { SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { liveMachineId } from "../writer.ts";
import { TURN_ORIGIN_ID } from "../turns.ts";

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
  // Single-sourced bootstrap chain: the grant migration FAILs closed without the bare consumer grantee.
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
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
  const { run, db } = await database();
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
  return { service, url, store: fs.store, db };
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

  test("the limits answer says sessionDeletion:false and sessionReset:false at the top level, after auth", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url } = await start(rc);
    const limits = await fetch(url("/v1/delivery/limits"), { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(limits.status).toBe(200);
    const text = await limits.text();
    const body = JSON.parse(text) as Record<string, unknown>;
    expect(body).toEqual({ ...DELIVERY_LIMITS, contentEncodings: contentEncodings(), sessionDeletion: false, sessionReset: false });
    expect(limits.headers.get("content-type")).toContain("application/json");
    expect(rc.calls).toHaveLength(1);
    // Unauthenticated: the same refusal as before, with no capabilities leaked.
    const anon = await fetch(url("/v1/delivery/limits"));
    expect([anon.status, await code(anon)]).toEqual([401, "missing_authorization"]);
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
    expect(await health.json()).toEqual({ ok: true, enabled: true, admission: "sync" });
    expect((await fetch(url("/readyz"))).status).toBe(200);
  });

  test("token refresh is a 401 the Desktop reads as a refused refresh, with no introspection", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, store } = await start(rc);
    // The Desktop's own call: no bearer, a small JSON body holding the refresh token.
    const res = await fetch(url("/v1/auth/token/refresh"), { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ refreshToken: TOKEN }) });
    const text = await res.text();
    expect([res.status, res.headers.get("www-authenticate")]).toEqual([401, "Bearer"]);
    expect((JSON.parse(text) as { error: { code: string } }).error.code).toBe("refresh_not_supported");
    expect(text).not.toContain(TOKEN);
    const wrong = await fetch(url("/v1/auth/token/refresh"));
    expect([wrong.status, wrong.headers.get("allow")]).toEqual([405, "POST"]);
    expect(rc.calls).toHaveLength(0);
    expect(await chunkKeys(store)).toEqual([]);
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

describe("onboarding heartbeat (RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true)", () => {
  const HEARTBEAT = "/v1/onboarding/heartbeat";
  const on = { RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT: "true" };
  const progress = JSON.stringify({ phase: "uploading", uploaded: 3, total: 10 });

  test("off by default: 501 unsupported_by_backend, no introspection", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url } = await start(rc);
    const res = await post(url(HEARTBEAT), progress);
    expect([res.status, await code(res)]).toEqual([501, "unsupported_by_backend"]);
    expect(rc.calls).toHaveLength(0);
  });

  test("an unexpected flag value refuses startup", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    await expect(start(rc, { RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT: "1" })).rejects.toThrow("RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT");
  });

  test("on: an authenticated heartbeat is 204 with no body, introspected, nothing written", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, store } = await start(rc, on);
    const res = await post(url(HEARTBEAT), progress, { "x-correlation-id": "hb-1" });
    expect(res.status).toBe(204);
    expect(await res.text()).toBe("");
    expect(res.headers.get("x-correlation-id")).toBe("hb-1");
    expect(rc.calls.map((c) => c.body)).toEqual([{ token: TOKEN, aud: ORIGIN }]);
    // Keep-alive survives a drained small body: a second heartbeat on the same client works.
    expect((await post(url(HEARTBEAT), progress)).status).toBe(204);
    expect(await chunkKeys(store)).toEqual([]);
  });

  test("on: unauthenticated is 401, without rth:sync is 403, GET is 405; none introspects past the gate", async () => {
    const rc = await relaycontrol(() => active(["rth:read"]));
    const { url } = await start(rc, on);
    const anon = await fetch(url(HEARTBEAT), { method: "POST", headers: { "content-type": "application/json" }, body: progress });
    expect([anon.status, anon.headers.get("www-authenticate"), await code(anon)]).toEqual([401, "Bearer", "missing_authorization"]);
    expect(rc.calls).toHaveLength(0);
    const unscoped = await post(url(HEARTBEAT), progress);
    expect([unscoped.status, await code(unscoped)]).toEqual([403, "forbidden"]);
    const wrong = await fetch(url(HEARTBEAT), { headers: { authorization: `Bearer ${TOKEN}` } });
    expect([wrong.status, wrong.headers.get("allow"), await code(wrong)]).toEqual([405, "POST", "method_not_allowed"]);
    expect(rc.calls).toHaveLength(1);
    // The rest of /v1/onboarding stays unserved.
    const connection = await fetch(url("/v1/onboarding/connection"), { headers: { authorization: `Bearer ${TOKEN}` } });
    expect([connection.status, await code(connection)]).toEqual([501, "unsupported_by_backend"]);
  });

  test("on: a body past the drain cap is not read; still 204 and the connection closes", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url } = await start(rc, on);
    const res = await post(url(HEARTBEAT), "x".repeat(4 * 1024 * 1024)).catch((e: unknown) => e);
    // The edge closes the connection once the 204 is out; a client still uploading may see a reset instead.
    if (res instanceof Response) {
      expect(res.status).toBe(204);
      expect(res.headers.get("connection")).toBe("close");
    }
  });
});

describe("capture policy upload check (RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS)", () => {
  const HEADER = "x-agent-relay-capture-policy-revision";
  const SINCE = "2026-11-01T00:00:00.000Z";
  const policy = (settings: Record<string, unknown>, extra: Record<string, unknown> = {}) =>
    active(["rth:sync"], { role: "member", capture_policy: { version: 1, revision: 7, settings, ...extra } });
  const oldSession = async (id: string) => JSON.stringify({ protocolVersion: 1, batch: await batch([
    record({ record_id: id, kind: "session", payload: { first_activity_ms: Date.parse(SINCE) - 60_000 } }),
  ], { batch_id: `batch-${id}` }) });

  test("report (default): a missing or older revision, or an older session, is logged and accepted", async () => {
    const rc = await relaycontrol(() => policy({ sharing_mode: "new" }, { sharing_mode_since: SINCE }));
    const { url } = await start(rc);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody("e1"))).status).toBe(200);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody("e2", { batch_id: "batch-2" }), { [HEADER]: "6" })).status).toBe(200);
    expect((await post(url("/v1/delivery/batches"), await oldSession("s1"), { [HEADER]: "7" })).status).toBe(200);
  });

  test("enforce: 409 capture_policy_stale for a missing or older revision; the current one is accepted", async () => {
    const rc = await relaycontrol(() => policy({ upload_control: "operator" }));
    const { url, store } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    for (const revision of [undefined, "6", "seven"]) {
      const res = await post(url("/v1/delivery/batches"), await deliveryBody(), revision ? { [HEADER]: revision } : {});
      expect([revision, res.status, await code(res)]).toEqual([revision, 409, "capture_policy_stale"]);
    }
    expect(await chunkKeys(store)).toEqual([]);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody(), { [HEADER]: "7" })).status).toBe(200);
    expect((await chunkKeys(store)).length).toBeGreaterThan(0);
  });

  test("enforce: under sharing_mode new, a session older than the policy is 422; a newer one is accepted", async () => {
    const rc = await relaycontrol(() => policy({ sharing_mode: "new" }, { sharing_mode_since: SINCE }));
    const { url } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    const refused = await post(url("/v1/delivery/batches"), await oldSession("s1"), { [HEADER]: "7" });
    expect([refused.status, await code(refused)]).toEqual([422, "capture_policy_session_predates_policy"]);
    const fresh = JSON.stringify({ protocolVersion: 1, batch: await batch([
      record({ record_id: "s2", kind: "session", payload: { first_activity_ms: Date.parse(SINCE) + 60_000 } }),
    ], { batch_id: "batch-s2" }) });
    expect((await post(url("/v1/delivery/batches"), fresh, { [HEADER]: "7" })).status).toBe(200);
  });

  test("enforce: a legacy All client stores its payload without a policy revision header", async () => {
    const rc = await relaycontrol(() => policy({ sharing_mode: "all", user_retroactive_delete: false }));
    const { url, store, db } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    const marker = "synthetic legacy All upload compatibility marker";
    const input = await batch([record({ record_id: "legacy-all-event", payload: { text: marker, ts_ms: Date.parse(SINCE) - 60_000 } })], { batch_id: "legacy-all-batch" });
    const response = await post(url("/v1/delivery/batches"), JSON.stringify({ protocolVersion: 1, batch: input }));
    expect(response.status).toBe(200);
    expect((await response.json() as { acceptedRevisionIds: string[] }).acceptedRevisionIds).toHaveLength(1);
    const keys = await chunkKeys(store);
    expect(keys.some(key => key.endsWith("manifest.json"))).toBe(true);
    let payloadFound = false;
    for (const key of keys.filter(key => key.endsWith(".zst"))) {
      const object = await store.getObject(objectKey(key));
      const bytes = new Uint8Array(await new Response(object.body).arrayBuffer());
      payloadFound ||= new TextDecoder().decode(zstdDecompressSync(bytes)).includes(marker);
    }
    expect(payloadFound).toBe(true);
    expect((await db.query("SELECT state FROM sessions.ingest_batches WHERE batch_id='legacy-all-batch'")).rows).toEqual([{ state: "committed" }]);
    expect((await db.query("SELECT record_id FROM sessions.ingest_record_revisions WHERE record_id='legacy-all-event'")).rows).toEqual([{ record_id: "legacy-all-event" }]);
  });

  test("enforce: legacy All uploads cannot lift an existing session exclusion", async () => {
    const rc = await relaycontrol(() => policy({ sharing_mode: "all", user_retroactive_delete: false }));
    const { url, store, db } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    await db.exec("SET ROLE bulls96_session_ingest");
    try {
      await db.query("SELECT sessions.ingest_set_session_uploads($1::jsonb)", [JSON.stringify({
        tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: false,
      })]);
    } finally {
      await db.exec("RESET ROLE");
    }
    const response = await post(url("/v1/delivery/batches"), await deliveryBody("excluded-legacy-all"));
    // Existing exclusion acknowledgments suppress content; this does not grant consent.
    expect(response.status).toBe(200);
    expect(await chunkKeys(store)).toEqual([]);
    expect((await db.query("SELECT record_id FROM sessions.ingest_record_revisions")).rows).toEqual([]);
  });

  test("enforce: All with an upload window still refuses a legacy header before storage", async () => {
    const rc = await relaycontrol(() => policy({ sharing_mode: "all", oops_window_seconds: 60 }));
    const { url, store, db } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    const response = await post(url("/v1/delivery/batches"), await deliveryBody("legacy-window"));
    expect([response.status, await code(response)]).toEqual([409, "capture_policy_stale"]);
    expect(await chunkKeys(store)).toEqual([]);
    expect((await db.query("SELECT state FROM sessions.ingest_batches")).rows).toEqual([]);
  });

  test("enforce: a policy that sets no key, or no policy at all (older relaycontrol), needs no header", async () => {
    let answer = policy({});
    const rc = await relaycontrol(() => answer);
    const { url } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    expect((await post(url("/v1/delivery/batches"), await deliveryBody("e1"))).status).toBe(200);
    answer = active(["rth:sync"]);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody("e2", { batch_id: "batch-2" }))).status).toBe(200);
  });

  test("an unexpected value refuses startup", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    await expect(start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "on" })).rejects.toThrow("RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS");
  });
});

describe("per-session erase (RELAYHISTORY_SELFHOST_SESSION_SELECTION=true)", () => {
  const ORIGIN_ID = "origin-1";
  const SELECTION = "/v1/delivery/sessions/selection";
  const machine = liveMachineId(TENANT.workspaceId, ORIGIN_ID, TENANT.userId);
  const on = { RELAYHISTORY_SELFHOST_SESSION_SELECTION: "true" };
  const pointer = (db: PGlite, session: string, machineId: string, ws = TENANT.workspaceId) =>
    db.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision)
       VALUES ($1, $2, 'claude', $3, $4, $5, now(), now(), 's3', 'bucket', 'k/' || $3 || $4,
               decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2)`,
      [TENANT.orgId, ws, session, machineId, TENANT.userId],
    );
  const directory = (db: PGlite, session: string) =>
    db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at)
       VALUES ($1, $2, 'claude', $3, now(), now())`,
      [TENANT.orgId, TENANT.workspaceId, session],
    );
  const row = async (db: PGlite, session: string, machineId: string) =>
    (await db.query<{ deleted_at: unknown; revision: string }>(
      "SELECT deleted_at, revision::text FROM sessions.session_index WHERE session_id = $1 AND machine_id = $2 AND workspace_id = $3", [session, machineId, TENANT.workspaceId])).rows[0]!;
  const selection = async (over: Record<string, unknown> = {}) =>
    JSON.stringify({ account_id: await deliveryAccount(TENANT), origin_id: ORIGIN_ID, sessions: [{ source: "claude", session_id: "sess-1" }], enabled: false, ...over });

  test("off by default: selection stays 501 and the fence is never reached", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc);
    await pointer(db, "sess-1", machine);
    const res = await post(url(SELECTION), await selection());
    expect([res.status, await code(res)]).toEqual([501, "unsupported_by_backend"]);
    expect((await row(db, "sess-1", machine)).deleted_at).toBeNull();
    expect(rc.calls).toHaveLength(0);
  });

  test("an unexpected flag value refuses startup", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    await expect(start(rc, { RELAYHISTORY_SELFHOST_SESSION_SELECTION: "1" })).rejects.toThrow("RELAYHISTORY_SELFHOST_SESSION_SELECTION");
  });

  test("erases the caller's copy: pointer and directory tombstoned, other machine, session and workspace untouched", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    expect((await post(url("/v1/delivery/batches"), await deliveryBody())).status).toBe(200);
    await pointer(db, "sess-1", machine);
    await pointer(db, "sess-1", "srv_other-machine");
    await pointer(db, "sess-2", machine);
    await pointer(db, "sess-1", machine, "ws-b");
    await directory(db, "sess-1");
    const res = await post(url(SELECTION), await selection());
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ updated: true });
    const erased = await row(db, "sess-1", machine);
    expect(erased.deleted_at).not.toBeNull();
    // Past any manifest revision a queued event could carry: the revision fence can never undelete it.
    expect(BigInt(erased.revision)).toBeGreaterThanOrEqual(9007199254740991n);
    expect((await row(db, "sess-1", "srv_other-machine")).deleted_at).toBeNull();
    expect((await row(db, "sess-2", machine)).deleted_at).toBeNull();
    const other = await db.query<{ deleted_at: unknown }>("SELECT deleted_at FROM sessions.session_index WHERE workspace_id = 'ws-b'");
    expect(other.rows[0]!.deleted_at).toBeNull();
    // Another machine's copy is still live, so the directory row stays; its derived text is cleared.
    const live = await db.query<{ deleted_at: unknown; short_summary: unknown }>("SELECT deleted_at, short_summary FROM sessions.session_directory WHERE session_id = 'sess-1'");
    expect(live.rows[0]).toEqual({ deleted_at: null, short_summary: null });
    const fence = await db.query("SELECT 1 FROM sessions.upload_deletions WHERE session_id = 'sess-1' AND origin_id = $1", [ORIGIN_ID]);
    expect(fence.rows).toHaveLength(1);
    // The last live copy gone, the directory row goes too (erased a different machine's copy with the same selection).
    await db.query("DELETE FROM sessions.session_index WHERE machine_id = 'srv_other-machine'");
    await post(url(SELECTION), await selection());
    const gone = await db.query<{ deleted_at: unknown }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess-1'");
    expect(gone.rows[0]!.deleted_at).not.toBeNull();
  });

  test("a directory row carrying a short summary and its provenance: all five cleared together, never 23514 (044050)", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", machine);
    await pointer(db, "sess-1", "srv_other-machine");
    await directory(db, "sess-1");
    await db.query(
      `UPDATE sessions.session_directory SET short_summary = 'a scrubbed summary', short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 1, short_summary_source = 'bob-costas/extractive/v1', short_summary_generated_at = now()
       WHERE session_id = 'sess-1'`);
    const summary = async () =>
      (await db.query("SELECT deleted_at, short_summary, short_summary_scrub_ruleset, short_summary_scrub_version, short_summary_source, short_summary_generated_at FROM sessions.session_directory WHERE session_id = 'sess-1'")).rows[0];
    const cleared = { short_summary: null, short_summary_scrub_ruleset: null, short_summary_scrub_version: null, short_summary_source: null, short_summary_generated_at: null };
    // Another machine's copy is still live, so the row is not tombstoned and the tombstone trigger does not clear
    // the provenance: before 044050 this UPDATE broke session_directory_short_summary_provenance (23514, a 503).
    const res = await post(url(SELECTION), await selection());
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ updated: true });
    expect((await row(db, "sess-1", machine)).deleted_at).not.toBeNull();
    expect((await row(db, "sess-1", "srv_other-machine")).deleted_at).toBeNull();
    expect(await summary()).toEqual({ deleted_at: null, ...cleared });
    // The last live copy gone, a second erase tombstones the row; still all five NULL.
    await db.query("DELETE FROM sessions.session_index WHERE machine_id = 'srv_other-machine'");
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    const gone = await summary();
    expect(gone!.deleted_at).not.toBeNull();
    expect({ ...gone, deleted_at: null }).toEqual({ deleted_at: null, ...cleared });
  });

  test("the managed-head retire trigger clears the short summary with its provenance, never 23514 (044050)", async () => {
    const { db } = await database();
    await pointer(db, "sess-1", machine);
    await pointer(db, "sess-1", "srv_other-machine");
    await directory(db, "sess-1");
    await db.query(
      `UPDATE sessions.session_directory SET short_summary = 'a scrubbed summary', short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 1, short_summary_source = 'bob-costas/extractive/v1', short_summary_generated_at = now()
       WHERE session_id = 'sess-1'`);
    // A live publication head on the caller's machine (the generation row references the head).
    const generation = "6f1c2a4e-3b5d-4c7e-8f90-a1b2c3d4e5f6";
    await db.query(
      `INSERT INTO sessions.ingest_publication_heads (org_id, workspace_id, user_id, origin_id, source, session_id, physical_workspace,
         physical_source, physical_session, machine_id, generation, fence_epoch, reset_epoch, pointer_revision, current_key, state)
       VALUES ($1, $2, $3, $4, 'claude', 'sess-1', $2, 'claude', 'sess-1', $5, $6, 1, 0, 1, 'k/sess-1' || $5, 'live')`,
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN_ID, machine, generation]);
    await db.query(
      `INSERT INTO sessions.ingest_publication_generations (generation, prefix, org_id, workspace_id, user_id, origin_id, source, session_id,
         fence_epoch, reset_epoch, state, target_bucket, target_region, key_root)
       VALUES ($5::uuid, 'p/' || $5, $1, $2, $3, $4, 'claude', 'sess-1', 1, 0, 'live', 'bucket', 'us-east-1', '96bulls')`,
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN_ID, generation]);
    // A user reset moves the reset epoch past the head's, so ingest_publication_retire_reset retires it. The other
    // machine's copy keeps the directory row live (no tombstone trigger): before 044050 the trigger's directory
    // UPDATE cleared short_summary alone and broke session_directory_short_summary_provenance (23514).
    await db.query("INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id) VALUES ($1, $2, $3, 'r-1')",
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    expect((await db.query("SELECT state FROM sessions.ingest_publication_heads")).rows).toEqual([{ state: "retired" }]);
    expect((await db.query("SELECT state FROM sessions.ingest_publication_generations")).rows).toEqual([{ state: "retired" }]);
    expect((await row(db, "sess-1", "srv_other-machine")).deleted_at).toBeNull();
    expect((await db.query(
      "SELECT deleted_at, short_summary, short_summary_scrub_ruleset, short_summary_scrub_version, short_summary_source, short_summary_generated_at FROM sessions.session_directory WHERE session_id = 'sess-1'",
    )).rows).toEqual([{ deleted_at: null, short_summary: null, short_summary_scrub_ruleset: null, short_summary_scrub_version: null, short_summary_source: null, short_summary_generated_at: null }]);
  });

  // 044070: the erased summary must also leave search_text, which the tenant search path matches (ILIKE). A row that
  // keeps another live pointer is not tombstoned, so the tombstone trigger does not empty search_text for it.
  const PHRASE = "zephyrquokka ledger";
  const TITLE = "Refactor billing gateway";
  const searchable = async (db: PGlite) => {
    await db.query(
      `UPDATE sessions.session_directory SET title = $1, agent_name = 'agent-a', project_id = 'proj-a', git_repo = 'org/repo',
         git_branch = 'main', model = 'model-a', short_summary = 'notes about the ' || $2 || ' cleanup', short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 1, short_summary_source = 'bob-costas/extractive/v1', short_summary_generated_at = now()
       WHERE session_id = 'sess-1'`, [TITLE, PHRASE]);
    await db.query(
      `UPDATE sessions.session_directory SET search_text = sessions.session_directory_search_text(title, task_title, agent_name, project_id,
         git_repo, git_branch, model, short_summary) WHERE session_id = 'sess-1'`);
    await db.query("INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2) ON CONFLICT DO NOTHING",
      ["https://issuer.test/session-directory", SESSION_DIRECTORY_SEARCH_RPC.audience]);
  };
  /** The tenant search path (sessions.search_session_directory with p_query) as the Worker role sees it. */
  const search = async (db: PGlite, query: string) => {
    const iat = Math.floor(Date.now() / 1000);
    await db.query("SELECT set_config('request.jwt.claims', $1, false)", [JSON.stringify({
      role: SESSION_DIRECTORY_SEARCH_RPC.workerRole, iss: "https://issuer.test/session-directory", aud: SESSION_DIRECTORY_SEARCH_RPC.audience,
      iat, exp: iat + 60, app_metadata: { org_id: TENANT.orgId, workspace_ids: [TENANT.workspaceId] } })]);
    await db.exec(`SET ROLE ${SESSION_DIRECTORY_SEARCH_RPC.workerRole}`);
    try {
      return (await db.query<{ session_id: string }>("SELECT session_id FROM sessions.search_session_directory(p_workspace_id => $1, p_query => $2)",
        [TENANT.workspaceId, query])).rows.map((r) => r.session_id);
    } finally {
      await db.exec("RESET ROLE");
      await db.query("SELECT set_config('request.jwt.claims', '', false)");
    }
  };
  /** After the clear: no summary, no provenance, not tombstoned, search_text is the metadata-only document. */
  const expectMetadataOnly = async (db: PGlite) => {
    expect((await db.query(
      `SELECT deleted_at, short_summary, short_summary_scrub_ruleset, short_summary_scrub_version, short_summary_source, short_summary_generated_at,
         search_text = sessions.session_directory_search_text(title, task_title, agent_name, project_id, git_repo, git_branch, model, NULL) AS rebuilt,
         strpos(search_text, $1) = 0 AS phrase_gone, strpos(search_text, $2) > 0 AS title_kept
       FROM sessions.session_directory WHERE session_id = 'sess-1'`, [PHRASE, TITLE])).rows).toEqual([{
      deleted_at: null, short_summary: null, short_summary_scrub_ruleset: null, short_summary_scrub_version: null, short_summary_source: null,
      short_summary_generated_at: null, rebuilt: true, phrase_gone: true, title_kept: true }]);
    expect(await search(db, PHRASE)).toEqual([]);
    expect(await search(db, "zephyrquokka")).toEqual([]);
    expect(await search(db, TITLE)).toEqual(["sess-1"]);
  };

  test("the erase rebuilds search_text without the summary: an erased summary is no longer searchable on a live row (044070)", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", machine);
    await pointer(db, "sess-1", "srv_other-machine");
    await directory(db, "sess-1");
    await searchable(db);
    expect(await search(db, PHRASE)).toEqual(["sess-1"]);
    expect(await search(db, TITLE)).toEqual(["sess-1"]);
    // Another machine's copy is still live, so the row stays untombstoned: before 044070 the summary stayed in
    // search_text and the phrase still matched.
    const res = await post(url(SELECTION), await selection());
    expect(res.status).toBe(200);
    expect((await row(db, "sess-1", machine)).deleted_at).not.toBeNull();
    expect((await row(db, "sess-1", "srv_other-machine")).deleted_at).toBeNull();
    await expectMetadataOnly(db);
  });

  test("the managed-head retire trigger rebuilds search_text without the summary: no longer searchable on a live row (044070)", async () => {
    const { db } = await database();
    await pointer(db, "sess-1", machine);
    await pointer(db, "sess-1", "srv_other-machine");
    await directory(db, "sess-1");
    await searchable(db);
    expect(await search(db, PHRASE)).toEqual(["sess-1"]);
    const generation = "7a2d3b5f-4c6e-4d8f-9a01-b2c3d4e5f607";
    await db.query(
      `INSERT INTO sessions.ingest_publication_heads (org_id, workspace_id, user_id, origin_id, source, session_id, physical_workspace,
         physical_source, physical_session, machine_id, generation, fence_epoch, reset_epoch, pointer_revision, current_key, state)
       VALUES ($1, $2, $3, $4, 'claude', 'sess-1', $2, 'claude', 'sess-1', $5, $6, 1, 0, 1, 'k/sess-1' || $5, 'live')`,
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN_ID, machine, generation]);
    await db.query(
      `INSERT INTO sessions.ingest_publication_generations (generation, prefix, org_id, workspace_id, user_id, origin_id, source, session_id,
         fence_epoch, reset_epoch, state, target_bucket, target_region, key_root)
       VALUES ($5::uuid, 'p/' || $5, $1, $2, $3, $4, 'claude', 'sess-1', 1, 0, 'live', 'bucket', 'us-east-1', '96bulls')`,
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN_ID, generation]);
    // A user reset retires the head (ingest_publication_retire_reset); the other machine's copy keeps the row live.
    await db.query("INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id) VALUES ($1, $2, $3, 'r-1')",
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    expect((await db.query("SELECT state FROM sessions.ingest_publication_heads")).rows).toEqual([{ state: "retired" }]);
    expect((await row(db, "sess-1", machine)).deleted_at).not.toBeNull();
    expect((await row(db, "sess-1", "srv_other-machine")).deleted_at).toBeNull();
    await expectMetadataOnly(db);
  });

  test("one selection erases every origin of the caller's session, relay turns included; another user's copy stays", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    const secondOrigin = liveMachineId(TENANT.workspaceId, "origin-2", TENANT.userId);
    const turns = liveMachineId(TENANT.workspaceId, TURN_ORIGIN_ID, TENANT.userId);
    const otherUser = liveMachineId(TENANT.workspaceId, "origin-2", "user-2");
    for (const m of [machine, secondOrigin, turns, otherUser]) await pointer(db, "sess-1", m);
    await db.query(
      `INSERT INTO sessions.ingest_session_ownership (org_id, workspace_id, user_id, origin_id, source, session_id)
       VALUES ($1, $2, $3, 'origin-2', 'claude', 'sess-1'), ($1, $2, 'user-2', 'origin-2', 'claude', 'sess-1')`,
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId],
    );
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    for (const m of [machine, secondOrigin, turns]) expect((await row(db, "sess-1", m)).deleted_at).not.toBeNull();
    expect((await row(db, "sess-1", otherUser)).deleted_at).toBeNull();
    const fences = await db.query<{ user_id: string; origin_id: string }>("SELECT user_id, origin_id FROM sessions.upload_deletions WHERE session_id = 'sess-1' ORDER BY origin_id COLLATE \"C\"");
    expect(fences.rows).toEqual(["origin-1", "origin-2", TURN_ORIGIN_ID].sort().map((origin_id) => ({ user_id: TENANT.userId, origin_id })));
  });

  test("a selection naming a session the caller has no pointer for changes no directory row", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", "srv_someone-else");
    await directory(db, "sess-1");
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    expect((await row(db, "sess-1", "srv_someone-else")).deleted_at).toBeNull();
    const d = await db.query<{ deleted_at: unknown }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess-1'");
    expect(d.rows[0]!.deleted_at).toBeNull();
  });

  test("a stale retry of the original batch replays its receipt and writes nothing new", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db, store } = await start(rc, on);
    const first = await post(url("/v1/delivery/batches"), await deliveryBody());
    const receipt = ((await first.json()) as { receiptId: string }).receiptId;
    await pointer(db, "sess-1", machine);
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    const keys = await chunkKeys(store);
    const retry = await post(url("/v1/delivery/batches"), await deliveryBody());
    expect([retry.status, ((await retry.json()) as { receiptId: string }).receiptId]).toEqual([200, receipt]);
    expect(await chunkKeys(store)).toEqual(keys);
    expect((await row(db, "sess-1", machine)).deleted_at).not.toBeNull();
  });

  test("capture policy user_retroactive_delete:false refuses a member's own delete 403; owners and admins still delete", async () => {
    const policy = (settings: Record<string, unknown>) => ({ version: 1, revision: 3, settings });
    let answer = active(["rth:sync"], { role: "member", capture_policy: policy({ user_retroactive_delete: false }) });
    const rc = await relaycontrol(() => answer);
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", machine);
    const refused = await post(url(SELECTION), await selection());
    expect([refused.status, await code(refused)]).toEqual([403, "policy_forbids_user_delete"]);
    expect((await row(db, "sess-1", machine)).deleted_at).toBeNull();
    expect((await db.query("SELECT 1 FROM sessions.upload_deletions WHERE session_id = 'sess-1'")).rows).toHaveLength(0);
    // The policy is read live from each introspection: the key absent leaves the member's delete.
    answer = active(["rth:sync"], { role: "member", capture_policy: policy({ sharing_mode: "all" }) });
    await pointer(db, "sess-unset", machine);
    expect((await post(url(SELECTION), await selection({ sessions: [{ source: "claude", session_id: "sess-unset" }] }))).status).toBe(200);
    expect((await row(db, "sess-unset", machine)).deleted_at).not.toBeNull();
    for (const role of ["owner", "admin"]) {
      answer = active(["rth:sync"], { role, capture_policy: policy({ user_retroactive_delete: false }) });
      await pointer(db, `sess-${role}`, machine);
      const res = await post(url(SELECTION), await selection({ sessions: [{ source: "claude", session_id: `sess-${role}` }] }));
      expect([role, res.status]).toEqual([role, 200]);
      expect((await row(db, `sess-${role}`, machine)).deleted_at).not.toBeNull();
    }
    answer = active(["rth:sync"], { role: "member", capture_policy: policy({ user_retroactive_delete: true }) });
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    expect((await row(db, "sess-1", machine)).deleted_at).not.toBeNull();
  });

  test("no capture policy in the introspection answer (an older relaycontrol): a member's delete works as before", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", machine);
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    expect((await row(db, "sess-1", machine)).deleted_at).not.toBeNull();
  });

  test("enabled:true is refused 400 and the fence stays; bad account and scope are refused", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"]));
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", machine);
    expect((await post(url(SELECTION), await selection())).status).toBe(200);
    const again = await post(url(SELECTION), await selection({ enabled: true }));
    expect([again.status, await code(again)]).toEqual([400, "unsupported_by_backend"]);
    expect((await db.query("SELECT 1 FROM sessions.upload_deletions WHERE session_id = 'sess-1' AND origin_id = $1", [ORIGIN_ID])).rows).toHaveLength(1);
    const wrongAccount = await post(url(SELECTION), await selection({ account_id: "acct_someone_else" }));
    expect(wrongAccount.status).toBe(403);
    const unsigned = await fetch(url(SELECTION), { method: "POST", body: await selection(), headers: { "content-type": "application/json" } });
    expect([unsigned.status, await code(unsigned)]).toEqual([401, "missing_authorization"]);
  });

  test("a token without rth:sync is refused before any fence is written", async () => {
    const rc = await relaycontrol(() => active(["rth:read"]));
    const { url, db } = await start(rc, on);
    await pointer(db, "sess-1", machine);
    const res = await post(url(SELECTION), await selection());
    expect([res.status, await code(res)]).toEqual([403, "forbidden"]);
    expect((await db.query("SELECT 1 FROM sessions.upload_deletions")).rows).toHaveLength(0);
    expect((await row(db, "sess-1", machine)).deleted_at).toBeNull();
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

  test("dormant: token refresh is 503 not_configured, so a Desktop never reads it as a refused sign-in", async () => {
    const service = await startSelfhostIngestService({}, { host: "127.0.0.1", port: 0 });
    cleanups.push(() => service.close());
    const res = await fetch(`http://127.0.0.1:${service.port}/v1/auth/token/refresh`, { method: "POST", body: "{}" });
    expect([res.status, await code(res)]).toEqual([503, "not_configured"]);
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
 * NOINHERIT and the timeouts: postgres:16 (160015), postgres:15 (150019) and postgres:17 (170011). PGlite
 * has no real login (session_user is always its superuser), so the decision is tested over these rows.
 */
const R = "bulls96_session_ingest";
const REAL_PG16_LOGIN = {
  session_user: R, current_user: R, role_setting: "none", server_version: 160015, backend_user: R,
  statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s",
  rolsuper: false, rolbypassrls: false, rolcreaterole: false, rolcreatedb: false, rolreplication: false, rolinherit: false,
  rolconfig: ["statement_timeout=5s", "lock_timeout=2s", "idle_in_transaction_session_timeout=10s"],
  memberships: 0, members: 1, database_settings: 0,
};
const CREATOR_EDGE = { member: "postgres", grantor: "supabase_admin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false };
const REAL_PG16_EXTRA = { system_user: "scram-sha-256:bulls96_session_ingest", member_edges: [CREATOR_EDGE] };
const REAL_PG15_LOGIN = { ...REAL_PG16_LOGIN, server_version: 150019, members: 0 };
const REAL_PG17_LOGIN = { ...REAL_PG16_LOGIN, server_version: 170011 };

/**
 * Identity columns recorded on the same servers (2026-11-08) for the three sessions that must be told apart.
 * `ssa` is a superuser (supabase_admin) login that ran SET SESSION AUTHORIZATION bulls96_session_ingest:
 * session_user and current_user become the role and role stays 'none'; only this backend's
 * pg_stat_activity usename (and, on 16+, system_user) still names the authenticated login. These match
 * #366's independent recordings (there with `postgres` as the superuser).
 */
const RECORDED_IDENTITY: Record<string, { base: Record<string, unknown>; sessions: Record<string, [Record<string, unknown>, string | undefined]> }> = {
  "15": {
    base: REAL_PG15_LOGIN,
    sessions: {
      genuine: [{ session_user: R, current_user: R, role_setting: "none", backend_user: R }, undefined],
      ssa: [{ session_user: R, current_user: R, role_setting: "none", backend_user: "supabase_admin" }, undefined],
      setrole: [{ session_user: "supabase_admin", current_user: R, role_setting: R, backend_user: "supabase_admin" }, undefined],
    },
  },
  "16": {
    base: REAL_PG16_LOGIN,
    sessions: {
      genuine: [{ session_user: R, current_user: R, role_setting: "none", backend_user: R }, `scram-sha-256:${R}`],
      ssa: [{ session_user: R, current_user: R, role_setting: "none", backend_user: "supabase_admin" }, "scram-sha-256:supabase_admin"],
      setrole: [{ session_user: "supabase_admin", current_user: R, role_setting: R, backend_user: "supabase_admin" }, "scram-sha-256:supabase_admin"],
    },
  },
  "17": {
    base: REAL_PG17_LOGIN,
    sessions: {
      genuine: [{ session_user: R, current_user: R, role_setting: "none", backend_user: R }, `scram-sha-256:${R}`],
      ssa: [{ session_user: R, current_user: R, role_setting: "none", backend_user: "supabase_admin" }, "scram-sha-256:supabase_admin"],
      setrole: [{ session_user: "supabase_admin", current_user: R, role_setting: R, backend_user: "supabase_admin" }, "scram-sha-256:supabase_admin"],
    },
  },
};

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

  test("recorded real-server rows: SET SESSION AUTHORIZATION and SET ROLE by a superuser refuse on PG15, 16 and 17", async () => {
    const want = { genuine: "admitted", ssa: "backend_user", setrole: "session_user" };
    for (const [v, { base, sessions }] of Object.entries(RECORDED_IDENTITY)) {
      for (const [name, [identity, systemUser]] of Object.entries(sessions)) {
        const login = { ...base, ...identity };
        const extra = systemUser === undefined ? null : { ...REAL_PG16_EXTRA, system_user: systemUser };
        expect([v, name, await admission(injected(login, extra).run)]).toEqual([v, name, want[name as keyof typeof want]]);
      }
    }
    // PG15 has no system_user: the backend usename is the only signal there. On 16+ system_user also refuses
    // SET SESSION AUTHORIZATION on its own, should the usename ever agree.
    const [ssa16, su16] = RECORDED_IDENTITY["16"]!.sessions.ssa!;
    expect(ingestLoginRefusal({ ...REAL_PG16_LOGIN, ...ssa16, backend_user: R }, { ...REAL_PG16_EXTRA, system_user: su16 }, SUPABASE)).toBe("system_user");
    expect(ingestLoginRefusal({ ...REAL_PG16_LOGIN, backend_user: null }, REAL_PG16_EXTRA, SUPABASE)).toBe("backend_user");
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
      ["SET SESSION AUTHORIZATION (backend usename differs)", { ...L, backend_user: "postgres" }, X, "backend_user"],
      ["backend row unreadable", { ...L, backend_user: null }, X, "backend_user"],
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
