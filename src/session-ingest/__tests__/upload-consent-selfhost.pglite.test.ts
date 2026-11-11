/** Regression for the upload-consent baseline reproduction (infra-kind-stack, pipeline 7238b562), INVERTED: the same synthetic uploads must now store nothing. Harness copied from selfhost-server.pglite.test.ts. Synthetic, local only. */
/**
 * The self-host ingest edge (SERVICE=session-ingest, selfhost-server.ts) end to
 * end over real loopback node:http: a stub relaycontrol introspection server
 * (also node:http) answers the real introspector, the ledger is PGlite with
 * every Supabase migration and the ingest role (SET ROLE bulls96_session_ingest for
 * the role's grants; PGlite has no real login, so startup admission, which refuses
 * SET ROLE, is tested over real-login rows below), and the store is a scratch fs store.
 */
import { zstdDecompressSync } from "node:zlib";
import { sqlUploadConsentStore } from "../upload-consent-sql.ts";
import { UPLOAD_CONSENT_REQUIRED } from "../upload-consent.ts";
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
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
  return { service, url, store: fs.store, db, run };
}

const deliveryBody = async (id = "e1", over: Parameters<typeof batch>[1] = {}) =>
  JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: id })], over) });
const post = (url: string, body: string, headers: Record<string, string> = {}) =>
  fetch(url, { method: "POST", body, headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}`, ...headers } });
const code = async (res: Response) => ((await res.json()) as { error: { code: string } }).error.code;




/** What one synthetic upload left behind: objects, chunks, manifests, the marker, ledger rows. */
async function persistedSynthetic(store: ObjectStore, db: PGlite, batchId: string, recordId: string, marker: string) {
  const keys = await chunkKeys(store);
  const raw = keys.filter((key) => key.endsWith(".zst"));
  let payloadFound = false;
  for (const key of raw) {
    const object = await store.getObject(objectKey(key));
    const bytes = new Uint8Array(await new Response(object.body).arrayBuffer());
    const text = new TextDecoder().decode(zstdDecompressSync(bytes));
    if (text.includes(marker)) payloadFound = true;
  }
  const batches = (await db.query<{state:string}>(
    "SELECT state FROM sessions.ingest_batches WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 AND origin_id='origin-1' AND batch_id=$4",
    [TENANT.orgId,TENANT.workspaceId,TENANT.userId,batchId],
  )).rows;
  const revisions = (await db.query<{record_id:string}>(
    "SELECT record_id FROM sessions.ingest_record_revisions WHERE org_id=$1 AND workspace_id=$2 AND origin_id='origin-1' AND record_id=$3",
    [TENANT.orgId,TENANT.workspaceId,recordId],
  )).rows;
  return { objects: keys.length, rawChunks: raw.length, manifests: keys.filter(key=>key.endsWith("manifest.json")).length,
    payloadFound, committedBatches: batches.filter(row=>row.state==="committed").length, storedRevisions: revisions.length };
}

const HEADER = "x-agent-relay-capture-policy-revision";
const SINCE = "2026-11-01T00:00:00.000Z";

/** The reproduction's synthetic batch: one session record and one event for an UNSELECTED session. */
async function syntheticUpload(mode: string, startedMs: number) {
  const recordId = `synthetic-${mode}-event`, sessionId = `synthetic-${mode}-unselected-session`, batchId = `synthetic-${mode}-batch`;
  const marker = `synthetic ${mode} history payload marker`;
  const body = JSON.stringify({ protocolVersion: 1, batch: await batch([
    record({ record_id: `synthetic-${mode}-session`, kind: "session", session_id: sessionId,
      payload: { source: "claude", session_id: sessionId, title: `Synthetic ${mode} session`, first_activity_ms: startedMs } }),
    record({ record_id: recordId, session_id: sessionId, payload: { text: marker, ts_ms: startedMs + 1000 } }),
  ], { batch_id: batchId }) });
  return { recordId, sessionId, batchId, marker, body };
}
const policy = (mode: string) => active(["rth:sync"], { role: "member", capture_policy: { version: 1, revision: 7, settings: { sharing_mode: mode }, sharing_mode_since: SINCE } });
const nothingStored = (stored: Awaited<ReturnType<typeof persistedSynthetic>>) => {
  expect(stored).toEqual({ objects: 0, rawChunks: 0, manifests: 0, payloadFound: false, committedBatches: 0, storedRevisions: 0 });
};

describe("upload consent: the baseline reproduction now stores nothing", () => {
  for (const mode of ["selected", "new"] as const) {
    // INGEST_UPLOAD_CONSENT is the opt-in (default off: capture enforce behaves exactly as on main). Even in
    // report mode, the operator's ENFORCED sharing mode caps the person's choice and is enforced.
    test(`capture policy enforce, sharing_mode ${mode}, consent report, no confirmed choice: held, zero stored`, async () => {
      const rc = await relaycontrol(() => policy(mode));
      const { url, store, db } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce", INGEST_UPLOAD_CONSENT: "report" });
      const u = await syntheticUpload(mode, Date.parse(SINCE) + (mode === "new" ? 60_000 : -60_000));
      const response = await post(url("/v1/delivery/batches"), u.body, { [HEADER]: "7" });
      expect(response.status).toBe(503);
      expect(response.headers.get("retry-after")).toBe("3600");
      expect(await code(response)).toBe(UPLOAD_CONSENT_REQUIRED);
      nothingStored(await persistedSynthetic(store, db, u.batchId, u.recordId, u.marker));
    });
  }

  for (const mode of ["selected", "new", "all"] as const) {
    test(`INGEST_UPLOAD_CONSENT=enforce, sharing_mode ${mode}, no confirmed choice: held, zero stored`, async () => {
      const rc = await relaycontrol(() => policy(mode));
      const { url, store, db } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce", INGEST_UPLOAD_CONSENT: "enforce" });
      const u = await syntheticUpload(mode, Date.parse(SINCE) + 60_000);
      const response = await post(url("/v1/delivery/batches"), u.body, { [HEADER]: "7" });
      expect(response.status).toBe(503);
      nothingStored(await persistedSynthetic(store, db, u.batchId, u.recordId, u.marker));
    });
  }

  test("operator 'selected' caps a person's 'all': only an explicitly allowed session lands", async () => {
    const rc = await relaycontrol(() => policy("selected"));
    const { url, store, db, run } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce", INGEST_UPLOAD_CONSENT: "enforce" });
    const consent = sqlUploadConsentStore(run);
    await consent.apply(TENANT, { mode: "all", confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: Date.parse(SINCE) });
    const u = await syntheticUpload("capped", Date.parse(SINCE) + 60_000);
    expect((await post(url("/v1/delivery/batches"), u.body, { [HEADER]: "7" })).status).toBe(503);
    nothingStored(await persistedSynthetic(store, db, u.batchId, u.recordId, u.marker));
    await consent.apply(TENANT, { allow: [{ source: "claude", sessionId: u.sessionId }] }, { trusted: true, nowMs: Date.parse(SINCE) + 1 });
    const ok = await post(url("/v1/delivery/batches"), u.body, { [HEADER]: "7" });
    expect(ok.status).toBe(200);
    const stored = await persistedSynthetic(store, db, u.batchId, u.recordId, u.marker);
    expect([stored.payloadFound, stored.committedBatches]).toEqual([true, 1]);
  });

  test("a missing policy revision is still refused before storage (unchanged)", async () => {
    const rc = await relaycontrol(() => active(["rth:sync"], { role: "member", capture_policy: { version: 1, revision: 7, settings: { sharing_mode: "all" } } }));
    const { url, store, db } = await start(rc, { RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS: "enforce" });
    const response = await post(url("/v1/delivery/batches"), await deliveryBody("synthetic-legacy-all"));
    expect([response.status, await code(response)]).toEqual([409, "capture_policy_stale"]);
    expect(await chunkKeys(store)).toHaveLength(0);
    expect((await db.query("SELECT state FROM sessions.ingest_batches")).rows).toHaveLength(0);
  });

  test("self-host introspection: rth:consent reaches the trusted consent write; a sync-only token cannot widen", async () => {
    let scopes = ["rth:sync"];
    const rc = await relaycontrol(() => active(scopes, { role: "member" }));
    const { url } = await start(rc, { INGEST_UPLOAD_CONSENT: "enforce" });
    const write = (body: unknown) => post(url("/v1/upload-consent"), JSON.stringify(body));
    const untrusted = await write({ mode: "all", confirm_origins: ["origin-1"] });
    expect([untrusted.status, await code(untrusted)]).toEqual([403, "upload_consent_untrusted"]);
    scopes = ["rth:sync", "rth:consent"];
    const trusted = await post(url("/v1/upload-consent"), JSON.stringify({ mode: "all", confirm_origins: ["origin-1"] }), { authorization: "Bearer sh_at_desktop-token-2" });
    expect(trusted.status).toBe(200);
    // ... and the confirmed choice now admits an upload from that installation.
    expect((await post(url("/v1/delivery/batches"), await deliveryBody("after-confirm"), { authorization: "Bearer sh_at_desktop-token-2" })).status).toBe(200);
  });

  test("self-host: a consent-only token (no rth:sync) reaches the consent write and nothing else", async () => {
    const rc = await relaycontrol(() => active(["rth:consent"], { role: "member" }));
    const { url } = await start(rc, { INGEST_UPLOAD_CONSENT: "enforce" });
    expect((await post(url("/v1/upload-consent"), JSON.stringify({ mode: "all", confirm_origins: ["origin-1"] }))).status).toBe(200);
    const batches = await post(url("/v1/delivery/batches"), await deliveryBody("consent-only"));
    expect([batches.status, await code(batches)]).toEqual([403, "forbidden"]);
  });
});

