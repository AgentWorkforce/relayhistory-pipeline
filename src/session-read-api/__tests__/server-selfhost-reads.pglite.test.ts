/**
 * The self-host SERVICE (server.ts) with its real assembly (lambda.ts
 * readAppFromSecret, entry "service"), flags on, over real loopback HTTP:
 *
 *   - GET /v1/sessions/:id/timing, /turns, /metadata and /catalog served from
 *     canonical S3 (an S3-API stub on loopback standing in for MinIO, LOCKER_*)
 *     through pointer rows read AS bulls96_read_api (every Supabase migration +
 *     the role bootstrap in PGlite, tenant-bound RLS);
 *   - READ_API_AUTH_MODE=relaycontrol_only against a real relaycontrol
 *     introspection stub (node:http): valid token, wrong audience, wrong
 *     workspace, outage (5xx, dropped connection, timeout) fail closed,
 *     refusal, and a non-`sh_at_` bearer refused with zero outbound fetches.
 *
 * pg.Pool is replaced by a PGlite-backed pool (one client at a time). Every id,
 * token and secret here is synthetic.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { readdirSync, readFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import pg from "pg";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { resetSharedRelaycontrolIntrospectors } from "@rhc/auth/relaycontrol.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { computeContentSha256, manifestKey, objectKey, rawChunkKey, type ChunkEntry, type ManifestV1 } from "../../../packages/object-store/src/index.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../../session-ingest/turns.ts";
import { AuthError } from "../app.ts";
import { readApiVerifier } from "../auth.ts";
import { readApiDepsFromSecret, readAppFromSecret } from "../lambda.ts";
import { startReadApiService, type ReadApiService } from "../server.ts";
import { readerLoginAnswer } from "./reader-login-support.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const psql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");

const ORIGIN = "https://read.selfhost.test";
const INTROSPECT_SECRET = "synthetic-introspect-secret";
const AT = Date.parse("2026-11-01T10:00:00.000Z");
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const ORG = "org_1", WS = "ws_1", SOURCE = "codex", SESSION = "sess_1";
const locationFor = (org: string) => ({ prefix: `96bulls/${org}`, workspaceId: WS, machineId: "srv_bb", harness: SOURCE, sessionId: SESSION, startDate: "2026-11-01" });

// ---------------------------------------------------------------------------
// Loopback stubs: S3 (MinIO stand-in), relaycontrol introspection, a JWKS.

interface Stub { server: Server; port: number; hits: string[] }
async function listen(handler: (req: IncomingMessage, res: ServerResponse, body: string) => void): Promise<Stub> {
  const hits: string[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      hits.push(`${req.method} ${req.url}`);
      handler(req, res, Buffer.concat(chunks).toString("utf8"));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { server, port: (server.address() as AddressInfo).port, hits };
}

const objects = new Map<string, Uint8Array>();
let s3: Stub;
let introspect: Stub;
let jwks: Stub;
let gcsToken: Stub;
/** EKS Pod Identity agent stand-in: answers container credentials for POD_TOKEN only, with `podExpiresInMs`. */
let podCreds: Stub;
const POD_TOKEN = "synthetic-pod-identity-token";
let podExpiresInMs = 6 * 60 * 60 * 1000;
let podIssued = 0;
let podTokenDir = "";
const GCS_ACCESS_TOKEN = "synthetic-gcs-access-token";
/** The Authorization header of every object request (S3 SigV4 or the GCS bearer). */
const objectAuth: string[] = [];
/** What the introspection stub answers, by token. */
const answers = new Map<string, (body: { token: string; aud: string; workspace?: string }, res: ServerResponse) => void>();
const introspectBodies: Array<{ token: string; aud: string; workspace?: string; authorization?: string }> = [];

function active(over: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return { active: true, sub: "user_1", email: "a@example.test", principal_type: "desktop-user", org: ORG, workspace: WS, scopes: ["rth:read"], machine_id: null, aud: ORIGIN, iat: now, exp: now + 600, token_id: "tokid_1", ...over };
}
const json = (res: ServerResponse, status: number, value: unknown) => {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(value));
};

// ---------------------------------------------------------------------------
// Supabase side in PGlite: every migration, the read role, pointer rows.

let supa: PGlite;
let lock: Promise<void> = Promise.resolve();
let connects = 0;
/** One PGlite session, so one client at a time: a pool whose clients run AS bulls96_read_api. */
async function exclusive(): Promise<() => void> {
  let unlock!: () => void;
  const prev = lock;
  lock = new Promise<void>((r) => (unlock = r));
  await prev;
  return unlock;
}
class PglitePool {
  static created = 0;
  constructor() { PglitePool.created++; }
  on() { return this; }
  async query(text: string, params?: unknown[]) {
    // Reader admission (lambda.ts assertReaderRole) needs a real login, which PGlite cannot make:
    // answer it with the production login rows (the decision is tested in reader-login.test.ts).
    const admission = readerLoginAnswer(text);
    if (admission) return { rows: admission };
    const unlock = await exclusive();
    try {
      await supa.exec("SET ROLE bulls96_read_api");
      return await supa.query(text, params as unknown[]);
    } finally {
      await supa.exec("RESET ROLE");
      unlock();
    }
  }
  async connect() {
    const unlock = await exclusive();
    connects++;
    await supa.exec("SET ROLE bulls96_read_api");
    return {
      query: (text: string, params?: unknown[]) => supa.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>,
      release: () => void supa.exec("RESET ROLE").finally(unlock),
    };
  }
  async end() {}
}

/** A pointer row of `org` naming the objects written under `objectsOrg`'s key prefix (default org_1). */
async function pointerRow(manifest: ManifestV1, over: { org?: string; ws?: string; deleted?: boolean; provider?: string; objectsOrg?: string } = {}) {
  await supa.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, canonical_session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, deleted_at)
     VALUES ($1, $2, 'codex', 'sess_1', NULL, 'srv_bb', 'user_1', $3::timestamptz, $3::timestamptz, $8, 'bucket', $4, decode($5, 'hex'), 'ndjson', $6, 1,
             CASE WHEN $7 THEN now() END)`,
    [over.org ?? ORG, over.ws ?? WS, new Date(AT).toISOString(), manifestKey(locationFor(over.objectsOrg ?? ORG)), manifest.raw.content_sha256, TRACE, over.deleted ?? false, over.provider ?? "s3"],
  );
}

/** A delivery stream with events, markers, a catalog record, the legacy coverage marker and one live turn. */
function deliveryLines(org = ORG) {
  const line = (record_id: string, kind: string, origin_id: string, payload: Record<string, unknown>, at = AT) => ({
    kind, origin_id, record_id, revision: "1", operation: "upsert", received_at: new Date(at).toISOString(), payload,
  });
  return [
    line("u1", "session_event", "origin-1", { ts_ms: AT + 1_000, role: "user", text: "please fix it", message_id: "m-u1" }),
    line("a1", "session_event", "origin-1", { ts_ms: AT + 9_000, role: "assistant", text: "done", message_id: "m-a1" }),
    line("k1", "session_marker", "origin-1", { marker_uid: "mk1", kind: "task_started", turn_id: "turn-1", ts_ms: AT + 900 }),
    line("k2", "session_marker", "origin-1", { marker_uid: "mk2", kind: "task_complete", turn_id: "turn-1", ts_ms: AT + 9_500 }),
    line("c1", "session", "origin-1", { title: "Fix the flaky test", cwd: "/repo", git_branch: "main", agent_name: "Ada" }),
    line("legacy:scope:coverage", "legacy_scope", "legacy:scope", { version: 1, org_id: org, workspace_id: WS, source: SOURCE, session_id: SESSION, tables: ["conversation_turns", "session_links", "session_outcomes"] }),
    {
      kind: TURN_KIND, origin_id: TURN_ORIGIN_ID, record_id: turnRecordId(WS, SOURCE, SESSION, 0), revision: "1793000000000000", operation: "upsert", received_at: "2026-11-01T10:00:10.000Z",
      payload: { id: "7f1c6a52-6a43-4c2e-9b44-1d3c2a6b9e01", org_id: org, workspace_id: WS, source: SOURCE, session_id: SESSION, turn_index: 0, session_owner: "user_1", role: "assistant",
        content: "live turn 0", actor_name: "Ada", actor_role: "owner", metadata_json: JSON.stringify({ nativeCli: "codex" }), ts: "2026-11-01T10:00:09.000Z", tagged: true },
    },
  ];
}

async function writeSession(location = locationFor(ORG)): Promise<ManifestV1> {
  const lines = deliveryLines(location.prefix.split("/")[1]);
  const raw = new TextEncoder().encode(lines.map((r) => JSON.stringify(r)).join("\n") + "\n");
  const stored = new Uint8Array(zstdCompressSync(raw));
  const sha = await sha256Hex(raw);
  const chunk: ChunkEntry = {
    seq: 0, epoch: 0, start_offset: 0, bytes: raw.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored), stored_raw_bytes: raw.byteLength, raw_sha256: sha, compression: "zstd", records: lines.length,
  };
  const iso = new Date(AT).toISOString();
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: TRACE, revision: 1, created_at: iso, updated_at: iso,
    session: { workspace_id: WS, machine_id: "srv_bb", harness: SOURCE, session_id: SESSION, started_at: iso },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
    raw: {
      owner: "srv_bb", state: "open", updated_at: iso, format: "ndjson", compression: "zstd", chunk_count: 1,
      raw_bytes: raw.byteLength, stored_bytes: stored.byteLength, content_sha256: await computeContentSha256([chunk]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: iso, reason: "initial", first_seq: 0, opened_by: "srv_bb" }],
      next_offset: raw.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1", canonical_session_id: SESSION } },
  } as ManifestV1;
  objects.set(String(rawChunkKey(location, 0, 0, sha, "ndjson")), stored);
  objects.set(String(objectKey(manifestKey(location))), new TextEncoder().encode(JSON.stringify(manifest)));
  return manifest;
}

// ---------------------------------------------------------------------------
// Outbound fetch accounting: every fetch this process makes, bar the test's own calls to the service.

const realFetch = globalThis.fetch;
let outbound: string[] = [];
let servicePorts = new Set<number>();
const RealPool = pg.Pool;

beforeAll(async () => {
  supa = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await supa.exec(read("supabase/tests/supabase_shim.sql"));
  await supa.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await supa.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await supa.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  await supa.exec(read("supabase/bootstrap/session_read_api_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await supa.exec(read(`supabase/migrations/${file}`));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role_verify.sql"));
  const manifest = await writeSession();
  await pointerRow(manifest);
  // Another org's and another workspace's pointer to the same objects: RLS must keep them out.
  await pointerRow(manifest, { org: "org_2" });
  // org_3 keeps its session in a GCS Locker (served by the same loopback stub as a GCS emulator).
  const gcsManifest = await writeSession(locationFor("org_3"));
  await pointerRow(gcsManifest, { org: "org_3", objectsOrg: "org_3", provider: "gcs" });

  s3 = await listen((req, res) => {
    objectAuth.push(req.headers.authorization ?? "");
    const url = new URL(req.url ?? "/", "http://s3.test");
    const key = decodeURIComponent(url.pathname).replace(/^\/bucket\//u, "");
    const body = url.pathname.startsWith("/bucket/") ? objects.get(key) : undefined;
    if (!body) return json(res, 404, { error: "NoSuchKey" });
    res.writeHead(200, { "content-type": "application/octet-stream", "content-length": String(body.byteLength), etag: `"${key.length}"`, "last-modified": new Date(AT).toUTCString() });
    res.end(req.method === "HEAD" ? undefined : Buffer.from(body));
  });
  introspect = await listen((req, res, text) => {
    if (req.method !== "POST" || req.url !== "/internal/v1/auth/introspect") return json(res, 404, {});
    const body = JSON.parse(text) as { token: string; aud: string; workspace?: string };
    introspectBodies.push({ ...body, authorization: req.headers.authorization });
    const answer = answers.get(body.token);
    if (!answer) return json(res, 200, { active: false, reason: "unknown" });
    answer(body, res);
  });
  jwks = await listen((_req, res) => json(res, 200, { keys: [] }));
  podCreds = await listen((req, res) => {
    if (req.headers.authorization !== POD_TOKEN) return json(res, 403, {});
    podIssued++;
    json(res, 200, { AccessKeyId: `ASIAPOD${podIssued}`, SecretAccessKey: `synthetic-secret-${podIssued}`, Token: `synthetic-session-${podIssued}`, Expiration: new Date(Date.now() + podExpiresInMs).toISOString() });
  });
  podTokenDir = await mkdtemp(join(tmpdir(), "pod-identity-"));
  await writeFile(join(podTokenDir, "token"), `${POD_TOKEN}\n`);
  gcsToken = await listen((req, res) => {
    if (req.headers["metadata-flavor"] !== "Google") return json(res, 403, {});
    json(res, 200, { access_token: GCS_ACCESS_TOKEN, expires_in: 3600, token_type: "Bearer" });
  });

  pg.Pool = PglitePool as unknown as typeof pg.Pool;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    if (!(url.hostname === "127.0.0.1" && servicePorts.has(Number(url.port)))) outbound.push(`${url.protocol}//${url.host}${url.pathname}`);
    return realFetch(input, init);
  }) as typeof fetch;
}, 180_000);

afterAll(async () => {
  pg.Pool = RealPool;
  globalThis.fetch = realFetch;
  for (const stub of [s3, introspect, jwks, gcsToken, podCreds]) stub?.server.closeAllConnections?.();
  await Promise.all([s3, introspect, jwks, gcsToken, podCreds].map((stub) => stub && new Promise<void>((r) => stub.server.close(() => r()))));
  if (podTokenDir) await rm(podTokenDir, { recursive: true, force: true });
  await supa?.close();
});

const running: ReadApiService[] = [];
afterEach(async () => {
  for (const s of running.splice(0)) await s.close();
  servicePorts = new Set();
  resetSharedRelaycontrolIntrospectors();
  answers.clear();
  for (const stub of [s3, introspect, jwks, gcsToken]) stub.hits.length = 0;
  objectAuth.length = 0;
});

function baseEnv(over: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  return {
    RELAYHISTORY_SELFHOST_READ_API: "true",
    RELAYHISTORY_READ_API_SECRET: JSON.stringify({
      supabase_url: "postgresql://bulls96_read_api:synthetic@db.selfhost.invalid:5432/postgres",
      clickhouse: { url: "https://ch.selfhost.invalid:8443", user: "reader", password: "synthetic", database: "relay_search" },
    }),
    SEARCH_INDEX_BACKEND: "clickhouse",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    READ_API_COMPLETE_SINCE: "epoch",
    READ_API_ORIGIN: ORIGIN,
    READ_API_DIRECTORY_WORKSPACES: "ws_1,ws_2",
    READ_API_TIMING_ASSUME_NO_TURNS: "true",
    READ_API_AUTH_MODE: "relaycontrol_only",
    RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: "true",
    RELAYCONTROL_INTROSPECT_URL: `http://127.0.0.1:${introspect.port}`,
    RELAYCONTROL_INTROSPECT_SECRET: INTROSPECT_SECRET,
    LOCKER_BUCKET: "bucket",
    LOCKER_ENDPOINT: `http://127.0.0.1:${s3.port}`,
    LOCKER_REGION: "us-east-1",
    LOCKER_ACCESS_KEY_ID: "synthetic-access-key",
    LOCKER_SECRET_ACCESS_KEY: "synthetic-secret-key",
    PIPELINE_KEY_PREFIX: "96bulls",
    PIPELINE_ORG_FROM_PREFIX: "true",
    ...over,
  };
}

async function start(env: NodeJS.ProcessEnv) {
  const service = await startReadApiService(env, { host: "127.0.0.1", port: 0 });
  running.push(service);
  servicePorts.add(service.port);
  // Startup traffic (none expected) is not a request's.
  outbound = [];
  introspectBodies.length = 0;
  for (const stub of [s3, introspect, jwks, gcsToken]) stub.hits.length = 0;
  objectAuth.length = 0;
  connects = 0;
  return (path: string, token?: string) =>
    fetch(`http://127.0.0.1:${service.port}${path}`, token === undefined ? {} : { headers: { authorization: `Bearer ${token}` } });
}

const code = async (r: Response) => ((await r.json()) as { error?: { code: string; reason?: string } }).error;
const SESSION_PATH = `/v1/sessions/${SESSION}`;
const ROUTES = [`${SESSION_PATH}/timing?workspace=ws_1&source=codex`, `${SESSION_PATH}/turns?workspace=ws_1`, `${SESSION_PATH}/metadata?workspace=ws_1`, `${SESSION_PATH}/catalog?workspace=ws_1&source=codex`];

describe("relaycontrol_only, canonical reads wired (server.ts, real assembly)", () => {
  test("a valid sh_at_ token reads timing, turns, metadata and catalog from canonical S3; nothing reaches RelayAuth", async () => {
    const token = "sh_at_synthetic-valid";
    answers.set(token, (_b, res) => json(res, 200, active()));
    const get = await start(baseEnv());

    const health = await get("/healthz");
    expect(await health.json()).toEqual({ ok: true, enabled: true });

    const timing = await get(ROUTES[0]!, token);
    expect(timing.status).toBe(200);
    expect(timing.headers.get("x-relayhistory-workspace-id")).toBe(WS);
    const t = (await timing.json()) as { coverage: { eventCount: number; markerCount: number } };
    expect(t.coverage.eventCount).toBeGreaterThan(0);
    expect(t.coverage.markerCount).toBe(2);

    const turns = await get(ROUTES[1]!, token);
    expect(turns.status).toBe(200);
    const body = (await turns.json()) as { sessionId: string; turns: Array<{ turnIndex: number; content: string }> };
    expect(body.sessionId).toBe(SESSION);
    expect(body.turns.map((x) => [x.turnIndex, x.content])).toEqual([[0, "live turn 0"]]);

    const metadata = await get(ROUTES[2]!, token);
    expect(metadata.status).toBe(200);
    expect(await metadata.json()).toMatchObject({ nativeCli: "codex", sessionOwner: "user_1" });

    const catalog = await get(ROUTES[3]!, token);
    expect(catalog.status).toBe(200);
    const c = (await catalog.json()) as { workspaceId: string; catalog: { title: string; agentName: string } | null; parents: unknown[] };
    expect(c.workspaceId).toBe(WS);
    expect(c.catalog).toMatchObject({ title: "Fix the flaky test", agentName: "Ada" });
    expect(c.parents).toEqual([]);

    // Introspection: aud = READ_API_ORIGIN, the request's workspace, the service secret.
    expect(introspectBodies.length).toBeGreaterThan(0);
    for (const b of introspectBodies) expect(b).toEqual({ token, aud: ORIGIN, workspace: WS, authorization: `Bearer ${INTROSPECT_SECRET}` });
    // Objects came from the Locker stub; the only other egress is the introspection stub.
    expect(s3.hits.some((h) => h.startsWith("GET /bucket/96bulls/org_1/"))).toBe(true);
    const allowed = new Set([`http://127.0.0.1:${introspect.port}`, `http://127.0.0.1:${s3.port}`]);
    for (const url of outbound) expect([url, allowed.has(new URL(url).origin)]).toEqual([url, true]);
    expect(jwks.hits).toEqual([]);
  }, 60_000);

  test("wrong audience is 401 before any SQL or object read", async () => {
    const token = "sh_at_synthetic-wrong-aud";
    answers.set(token, (_b, res) => json(res, 200, active({ aud: "https://history.other.test" })));
    const get = await start(baseEnv());
    for (const path of ROUTES) {
      const r = await get(path, token);
      expect([path, r.status]).toEqual([path, 401]);
    }
    expect(connects).toBe(0);
    expect(s3.hits).toEqual([]);
  });

  test("wrong workspace: a token of ws_1 asking for ws_2 is 401; a ws_2 token never sees ws_1's session", async () => {
    const mine = "sh_at_synthetic-ws1";
    // Relaycontrol answers with the token's real workspace, whatever was asked.
    answers.set(mine, (_b, res) => json(res, 200, active()));
    const other = "sh_at_synthetic-ws2";
    answers.set(other, (_b, res) => json(res, 200, active({ workspace: "ws_2", token_id: "tokid_2" })));
    const get = await start(baseEnv());
    for (const path of ROUTES) {
      const r = await get(path.replace("workspace=ws_1", "workspace=ws_2"), mine);
      expect([path, r.status]).toEqual([path, 401]);
    }
    expect(connects).toBe(0);
    // A legitimately authenticated other workspace (on the allowlist): no pointer under RLS, no object read.
    const ws2 = (path: string) => path.replace("workspace=ws_1", "workspace=ws_2");
    const timing = await get(ws2(ROUTES[0]!), other);
    expect(timing.status).toBe(501);
    expect((await code(timing))?.reason).toBe("timing_no_pointers");
    const turns = await get(ws2(ROUTES[1]!), other);
    expect(turns.status).toBe(501);
    expect((await code(turns))?.reason).toBe("turns_no_pointers");
    const catalog = await get(ws2(ROUTES[3]!), other);
    expect(catalog.status).toBe(404);
    // Workspace attestation: a ws_2 token without workspace= cannot fall into ws_1 either.
    const noWs = await get(`${SESSION_PATH}/turns`, other);
    expect(noWs.status).toBe(501);
    expect((await code(noWs))?.reason).toBe("org_scope");
    expect(s3.hits).toEqual([]);
  });

  test("another org's pointer row naming this org's objects is refused, never served", async () => {
    // org_2 holds a (forged) pointer in ws_1 to org_1's manifest: the key prefix binds the org.
    const token = "sh_at_synthetic-org2";
    answers.set(token, (_b, res) => json(res, 200, active({ org: "org_2", token_id: "tokid_3" })));
    const get = await start(baseEnv());
    const reasons = [];
    for (const path of ROUTES) {
      const r = await get(path, token);
      expect([path, r.status]).toEqual([path, 501]);
      reasons.push((await code(r))?.reason);
    }
    expect(reasons).toEqual(["timing_canonical_prefix_mismatch", "turns_canonical_prefix_mismatch", "turns_canonical_prefix_mismatch", "catalog_canonical_prefix_mismatch"]);
  });

  test.each([
    ["5xx", (res: ServerResponse) => json(res, 500, { error: "boom" })],
    ["dropped connection", (res: ServerResponse) => res.socket?.destroy()],
    ["timeout (never answers)", (_res: ServerResponse) => {}],
  ])("introspection outage (%s) fails closed: 503 relaycontrol_unavailable, nothing read", async (_name, answer) => {
    const token = `sh_at_synthetic-outage-${_name.replace(/\W+/gu, "-")}`;
    answers.set(token, (_b, res) => answer(res));
    const get = await start(baseEnv());
    const r = await get(ROUTES[1]!, token);
    expect(r.status).toBe(503);
    expect(r.headers.get("retry-after")).toBe("1");
    expect((await code(r))?.code).toBe("relaycontrol_unavailable");
    expect(connects).toBe(0);
    expect(s3.hits).toEqual([]);
  }, 20_000);

  test("introspection refusal (inactive, wrong principal, no rth:read) is refused, nothing read", async () => {
    const cases: Array<[string, Record<string, unknown>, number]> = [
      ["sh_at_synthetic-revoked", { active: false, reason: "revoked" }, 401],
      ["sh_at_synthetic-agent", active({ principal_type: "agent" }), 401],
      ["sh_at_synthetic-noscope", active({ scopes: ["cli:auth"] }), 403],
    ];
    for (const [token, answer] of cases) answers.set(token, (_b, res) => json(res, 200, answer));
    const get = await start(baseEnv());
    for (const [token, , status] of cases) {
      const r = await get(ROUTES[3]!, token);
      expect([token, r.status]).toEqual([token, status]);
    }
    expect(connects).toBe(0);
    expect(s3.hits).toEqual([]);
  });

  test("a non-sh_at_ bearer is 401 invalid_token with zero outbound fetches (no JWKS, no introspection)", async () => {
    const get = await start(baseEnv());
    const jwt = [{ alg: "RS256", kid: "k1", typ: "JWT" }, { iss: "https://relayauth.test", aud: ORIGIN, sub: "u" }]
      .map((p) => Buffer.from(JSON.stringify(p)).toString("base64url")).join(".") + ".c2ln";
    for (const token of [jwt, "opaque-token", "sh_at", "SH_AT_upper", "relay_sh_at_x"]) {
      for (const path of ROUTES) {
        const r = await get(path, token);
        expect([token, path, r.status]).toEqual([token, path, 401]);
        expect((await code(r))?.code).toBe("invalid_token");
      }
    }
    expect(outbound).toEqual([]);
    expect(introspect.hits).toEqual([]);
    expect(jwks.hits).toEqual([]);
    expect(s3.hits).toEqual([]);
    expect(connects).toBe(0);
  });

  test("contrast: the default mode sends the same JWT-shaped bearer to the RelayAuth JWKS (what relaycontrol_only removes)", async () => {
    const get = await start(baseEnv({
      READ_API_AUTH_MODE: undefined,
      RELAYAUTH_JWKS_URL: `http://127.0.0.1:${jwks.port}/.well-known/jwks.json`,
      RELAYAUTH_ISSUER: "https://relayauth.test",
    }));
    const jwt = [{ alg: "RS256", kid: "contrast-kid", typ: "JWT" }, { iss: "https://relayauth.test", aud: ORIGIN }]
      .map((p) => Buffer.from(JSON.stringify(p)).toString("base64url")).join(".") + ".c2ln";
    const r = await get(ROUTES[1]!, jwt);
    expect(r.status).toBe(401);
    expect(jwks.hits.length).toBeGreaterThan(0);
    expect(outbound).toContain(`http://127.0.0.1:${jwks.port}/.well-known/jwks.json`);
    jwks.hits.length = 0;
  });
});

describe("startup and wiring rules", () => {
  test.each([
    [{ RELAYAUTH_JWKS_URL: "https://relayauth.test/.well-known/jwks.json" }, "refuses RELAYAUTH_JWKS_URL"],
    [{ RELAYAUTH_ISSUER: "https://relayauth.test" }, "refuses RELAYAUTH_ISSUER"],
    [{ READ_API_AUTH_MODE: "relaycontrol-only" }, "READ_API_AUTH_MODE must be"],
    [{ READ_API_AUTH_MODE: "RELAYCONTROL_ONLY" }, "READ_API_AUTH_MODE must be"],
    [{ RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: undefined }, "needs RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true"],
    [{ RELAYCONTROL_INTROSPECT_URL: undefined }, "needs RELAYCONTROL_INTROSPECT_URL"],
    [{ RELAYCONTROL_INTROSPECT_SECRET: " " }, "needs RELAYCONTROL_INTROSPECT_SECRET"],
    [{ READ_API_ORIGIN: undefined }, "needs READ_API_ORIGIN"],
    [{ RELAYCONTROL_INTROSPECT_URL: "http://relaycontrol.example.com" }, "RELAYCONTROL_INTROSPECT_URL"],
    [{ LOCKER_ACCESS_KEY_ID: undefined }, "no Locker credentials"],
    [{ LOCKER_ACCESS_KEY_ID: undefined, LOCKER_SECRET_ACCESS_KEY: undefined, AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.169.254/latest/creds" }, "not a trusted credentials endpoint"],
    [{ LOCKER_ACCESS_KEY_ID: undefined, LOCKER_SECRET_ACCESS_KEY: undefined, AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://creds.example.com/v1" }, "not a trusted credentials endpoint"],
    [{ LOCKER_PROVIDER: "minio" }, "LOCKER_PROVIDER must be s3 or gcs"],
  ] as Array<[Record<string, string | undefined>, string]>)("%p refuses startup", async (over, message) => {
    const before = PglitePool.created;
    await expect(startReadApiService(baseEnv(over), { host: "127.0.0.1", port: 0 })).rejects.toThrow(message);
    // Auth settings refuse before the read pool is opened.
    if (!Object.keys(over).some((k) => k.startsWith("LOCKER_"))) expect(PglitePool.created).toBe(before);
    expect(podCreds.hits).toEqual([]);
  });

  test("an empty RELAYAUTH_* value is not a setting (relaycontrol_only still starts)", async () => {
    const get = await start(baseEnv({ RELAYAUTH_JWKS_URL: "", RELAYAUTH_ISSUER: "  " }));
    expect((await get("/healthz")).status).toBe(200);
  });

  test("LOCKER_BUCKET unset: the routes keep their 501 *_not_configured, exactly as before", async () => {
    const token = "sh_at_synthetic-nolocker";
    answers.set(token, (_b, res) => json(res, 200, active()));
    const get = await start(baseEnv({ LOCKER_BUCKET: undefined }));
    const reasons = [];
    for (const path of ROUTES) reasons.push((await code(await get(path, token)))?.reason);
    expect(reasons).toEqual(["timing_not_configured", "turns_not_configured", "turns_not_configured", "catalog_not_configured"]);
    expect(s3.hits).toEqual([]);
  });

  test.each([[undefined], [""], ["false"], ["TRUE"]])(
    "static key layout (PIPELINE_ORG_FROM_PREFIX=%p) refuses startup: the forged org_2 pointer to org_1's objects is never served",
    async (flag) => {
      // org_2's session_index row names org_1's manifest (beforeAll). With a static layout only the
      // shared base prefix would be checked, so the service must not start at all.
      const before = PglitePool.created;
      await expect(startReadApiService(baseEnv({ PIPELINE_ORG_FROM_PREFIX: flag }), { host: "127.0.0.1", port: 0 })).rejects.toThrow(
        "LOCKER_BUCKET needs PIPELINE_ORG_FROM_PREFIX=true",
      );
      expect(PglitePool.created).toBe(before + 1); // the pool opened for reader admission, then closed
      expect(s3.hits).toEqual([]);
      expect(introspect.hits).toEqual([]);
    },
  );

  /** EKS Pod Identity: no static keys, the agent's URI and token file only. */
  const podEnv = () => baseEnv({
    LOCKER_ACCESS_KEY_ID: undefined,
    LOCKER_SECRET_ACCESS_KEY: undefined,
    AWS_CONTAINER_CREDENTIALS_FULL_URI: `http://127.0.0.1:${podCreds.port}/v1/credentials`,
    AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: join(podTokenDir, "token"),
  });
  const accessKeys = () => objectAuth.map((a) => /Credential=([^/]+)\//u.exec(a)?.[1]);

  test("EKS Pod Identity (no static keys) starts and serves all four routes; credentials are fetched once and cached", async () => {
    podIssued = 0;
    podExpiresInMs = 6 * 60 * 60 * 1000;
    const token = "sh_at_synthetic-pod";
    answers.set(token, (_b, res) => json(res, 200, active()));
    const get = await start(podEnv());
    // Startup validates the source but fetches nothing.
    expect(podIssued).toBe(0);
    for (const path of ROUTES) {
      const r = await get(path, token);
      expect([path, r.status]).toEqual([path, 200]);
    }
    // One credentials fetch for the whole process, sent the token file's token (the stub refuses otherwise).
    expect(podCreds.hits).toEqual(["GET /v1/credentials"]);
    expect(accessKeys().length).toBeGreaterThan(0);
    for (const key of accessKeys()) expect(key).toBe("ASIAPOD1");
    const allowed = new Set([`http://127.0.0.1:${introspect.port}`, `http://127.0.0.1:${s3.port}`, `http://127.0.0.1:${podCreds.port}`]);
    for (const url of outbound) expect([url, allowed.has(new URL(url).origin)]).toEqual([url, true]);
  }, 60_000);

  test("EKS Pod Identity credentials near expiry are refreshed before the next object read", async () => {
    podIssued = 0;
    // Inside the 5-minute refresh window: every read must fetch fresh credentials.
    podExpiresInMs = 60_000;
    const token = "sh_at_synthetic-pod-refresh";
    answers.set(token, (_b, res) => json(res, 200, active()));
    const get = await start(podEnv());
    expect((await get(ROUTES[0]!, token)).status).toBe(200);
    const first = new Set(accessKeys());
    objectAuth.length = 0;
    expect((await get(ROUTES[3]!, token)).status).toBe(200);
    const second = new Set(accessKeys());
    expect(podIssued).toBeGreaterThanOrEqual(2);
    expect([...first].some((k) => second.has(k))).toBe(false); // the later read signed with the refreshed key
    podExpiresInMs = 6 * 60 * 60 * 1000;
  }, 60_000);

  test("GCS Locker: every object read and token fetch goes only to the configured loopback endpoints", async () => {
    const token = "sh_at_synthetic-gcs";
    answers.set(token, (_b, res) => json(res, 200, active({ org: "org_3", token_id: "tokid_gcs" })));
    const get = await start(baseEnv({
      LOCKER_PROVIDER: "gcs",
      LOCKER_ENDPOINT: `http://127.0.0.1:${s3.port}`,
      LOCKER_GCS_TOKEN_URI: `http://127.0.0.1:${gcsToken.port}/computeMetadata/v1/instance/service-accounts/default/token`,
      LOCKER_ACCESS_KEY_ID: undefined,
      LOCKER_SECRET_ACCESS_KEY: undefined,
      LOCKER_REGION: undefined,
    }));
    for (const path of ROUTES) {
      const r = await get(path, token);
      expect([path, r.status]).toEqual([path, 200]);
    }
    expect(s3.hits.some((h) => h.startsWith("GET /bucket/96bulls/org_3/"))).toBe(true);
    expect(gcsToken.hits.length).toBeGreaterThan(0);
    // Objects were fetched with the token the configured token URI issued.
    expect(objectAuth.length).toBeGreaterThan(0);
    for (const auth of objectAuth) expect(auth).toBe(`Bearer ${GCS_ACCESS_TOKEN}`);
    // Zero calls anywhere else: no storage.googleapis.com, no 169.254.169.254, no JWKS.
    const allowed = new Set([`http://127.0.0.1:${introspect.port}`, `http://127.0.0.1:${s3.port}`, `http://127.0.0.1:${gcsToken.port}`]);
    expect(outbound.length).toBeGreaterThan(0);
    for (const url of outbound) expect([url, allowed.has(new URL(url).origin)]).toEqual([url, true]);
  }, 60_000);

  test.each([
    [{ LOCKER_ENDPOINT: "https://storage.example.com" }, "GCS LOCKER_ENDPOINT must be the official HTTPS API or loopback emulator"],
    [{ LOCKER_GCS_TOKEN_URI: "http://metadata.example.com/token" }, "LOCKER_GCS_TOKEN_URI is not the GKE metadata server or loopback"],
    [{ LOCKER_ACCESS_KEY_ID: "synthetic" }, "GCS Locker uses Workload Identity"],
  ] as Array<[Record<string, string | undefined>, string]>)("GCS Locker with %p refuses startup", async (over, message) => {
    const env = baseEnv({ LOCKER_PROVIDER: "gcs", LOCKER_ENDPOINT: `http://127.0.0.1:${s3.port}`, LOCKER_ACCESS_KEY_ID: undefined, LOCKER_SECRET_ACCESS_KEY: undefined, ...over });
    await expect(startReadApiService(env, { host: "127.0.0.1", port: 0 })).rejects.toThrow(message);
    expect(s3.hits).toEqual([]);
  });

  test("the hosted Lambda assembly never wires canonical S3, even with LOCKER_* present", async () => {
    const env = baseEnv({ READ_API_AUTH_MODE: undefined, RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS: undefined, RELAYHISTORY_SELFHOST_READ_API: undefined });
    const { deps, close } = await readApiDepsFromSecret(env.RELAYHISTORY_READ_API_SECRET!, env, undefined, { entry: "lambda" });
    try {
      expect(deps.turns).toBeUndefined();
      expect(deps.timing).toBeUndefined();
      expect(deps.selfhost).toBeUndefined();
      // The same assembly as a full app: the route exists and authentication runs first.
      const app = (await readAppFromSecret(env.RELAYHISTORY_READ_API_SECRET!, env, undefined, { entry: "lambda" }));
      expect((await app.app.fetch(new Request(`https://read.internal${ROUTES[1]}`))).status).toBe(401);
      await app.close();
    } finally {
      await close();
    }
  });

  test("readApiVerifier relaycontrol_only never calls the RelayAuth verifier", async () => {
    const calls: string[] = [];
    const verify = (async (t: string) => {
      calls.push(t);
      throw new Error("must not be called");
    }) as never;
    const verifier = readApiVerifier({ origin: ORIGIN, mode: "relaycontrol_only", relaycontrol: { url: "http://127.0.0.1:1", secret: "s" } }, { verify });
    for (const token of ["eyJ.a.b", "anything"]) {
      const error = await verifier(token, { workspace: WS }).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(AuthError);
      expect([(error as AuthError).status, (error as AuthError).code]).toEqual([401, "invalid_token"]);
    }
    expect(calls).toEqual([]);
    expect(() => readApiVerifier({ origin: ORIGIN, mode: "relaycontrol_only", jwksUrl: "https://x.test/jwks", relaycontrol: { url: "http://127.0.0.1:1", secret: "s" } })).toThrow();
    expect(() => readApiVerifier({ origin: ORIGIN, mode: "relaycontrol_only" })).toThrow();
  });
});
