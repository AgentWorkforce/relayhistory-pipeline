/** Run only against the owned, isolated Docker PostgreSQL17 TLS fixture.
 * Never a cloud/prod proof. Imports the installed ZIPs, not source modules. */
import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, sign } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

assert.equal(process.env.PROOF_DISPOSABLE, "1");
assert.equal(process.platform, "linux");
assert.equal(process.arch, "arm64");
assert.match(process.version, /^v24\./);
const url = new URL(process.env.PORTABLE_LAMBDA_PG_URL ?? "");
assert.equal(url.hostname, "postgres"); assert.equal(url.pathname, "/portable_lambda_fixture");
assert.equal(url.password, ""); // isolated network trust, no credential material
const base = process.argv[2];
const pipelineManifest = JSON.parse(await readFile(join(base, "pipeline", "artifact-manifest.json"), "utf8"));
const readerManifest = JSON.parse(await readFile(join(base, "read-api", "artifact-manifest.json"), "utf8"));
assert.equal(pipelineManifest.sourceSha, readerManifest.sourceSha);
for (const manifest of [pipelineManifest, readerManifest]) {
  assert.equal(manifest.architecture, "arm64"); assert.equal(manifest.runtime, "nodejs24.x"); assert.equal(manifest.handler, "index.handler");
}
const pipeline = await import(pathToFileURL(join(base, "pipeline", "index.mjs")).href);
const reader = await import(pathToFileURL(join(base, "read-api", "index.mjs")).href);
const assets = await pipeline.derivedSchemaAssets();
assert.equal(assets.expected.generation, pipelineManifest.schema.generation);
assert.equal((await reader.derivedSchemaAssets()).expected.generation, assets.expected.generation);
const ca = "/fixture/ca.crt";
const caHash = createHash("sha256").update(await readFile(ca)).digest("hex");
const env = { AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "synthetic", PIPELINE_KEY_PREFIX: "96bulls", SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "synthetic", PIPELINE_ORG_FROM_PREFIX: "true", SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_BACKEND: "postgres", SESSION_POSTGRES_TLS_PROFILE: "custom-ca", SESSION_POSTGRES_CA_FILE: ca, SESSION_POSTGRES_CA_SHA256: caHash };
const config = { backendId: "portable-proof-v1", fromMs: 0 };
const composition = pipeline.createPostgresSearchComposition(config);
const rt = pipeline.runtimeFromSecret(pipeline.lambdaEnv(env, composition), JSON.stringify({ hoopdb_url: url.href + "?sslmode=disable" }), assets.expected.generation, composition);
const db = rt.deps.pointer.db;
let service;
let jwks;
const checks = [];
try {
  const tls = (await db.query("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")).rows[0];
  assert.equal(tls.ssl, true); checks.push("actual-pg17-tls-url-disable-cannot-bypass");
  assert.match((await db.query("SELECT version() AS version")).rows[0].version, /^PostgreSQL 17\./);
  for (const mode of ["wrong-ca", "wrong-host"]) {
    const badEnv = { ...env };
    const badUrl = new URL(url);
    if (mode === "wrong-ca") {
      badEnv.SESSION_POSTGRES_CA_FILE = "/fixture/other.crt";
      badEnv.SESSION_POSTGRES_CA_SHA256 = createHash("sha256").update(await readFile(badEnv.SESSION_POSTGRES_CA_FILE)).digest("hex");
    } else badUrl.hostname = "bad-postgres";
    const bad = pipeline.runtimeFromSecret(pipeline.lambdaEnv(badEnv, composition), JSON.stringify({ hoopdb_url: badUrl.href + "?sslmode=disable" }), assets.expected.generation, composition);
    try { await assert.rejects(bad.deps.pointer.db.query("SELECT 1")); }
    finally { await bad.deps.pointer.db.end(); }
    checks.push(mode + "-refused");
  }
  assert.throws(() => pipeline.lambdaEnv({ ...env, SESSION_POSTGRES_CA_SHA256: undefined }, composition));
  assert.throws(() => pipeline.lambdaEnv({ ...env, SEARCH_INDEX_BACKEND: "opensearch" }, composition));
  checks.push("missing-ca-unregistered-provider-refused");
  await db.query(`CREATE EXTENSION pg_trgm;
    CREATE SCHEMA sessions;
    CREATE TABLE sessions.session_index(org_id text,workspace_id text,harness text,session_id text,canonical_session_id text,created_at timestamptz,deleted_at timestamptz);
    CREATE TABLE sessions.session_directory(org_id text,workspace_id text,harness text,session_id text,event_projection_epoch bigint,event_projection_ready boolean);
    INSERT INTO sessions.session_index VALUES('org-a','ws-a','codex','s',NULL,now(),NULL),('org-b','ws-a','codex','s',NULL,now(),NULL);
    INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s',1,true),('org-b','ws-a','codex','s',1,true);
    CREATE ROLE portable_read LOGIN;`);
  const index = rt.deps.search.adapter.index;
  // The index's tenant-scoped calls bind their tenant on the writer's open
  // scope transaction (forced RLS); outside one they refuse (tenant_unbound).
  const pass = work => rt.deps.search.transact(() => work());
  await assert.rejects(index.query({ tenant: { orgId: "org-a", workspaceId: "ws-a" }, order: "asc", limit: 1 }), error => error.detail === "tenant_unbound");
  await index.ensureSchema();
  const state = pipeline.postgresSearchState(config);
  await state.ensureSchema(db);
  const scope = { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s" };
  const client = await db.pool.connect();
  try { await client.query("BEGIN"); await state.record(client, scope, { revision: 1, rows: 2, complete: true, at: new Date(), canonicalSessionId: "s" }); await client.query("COMMIT"); }
  catch (error) { await client.query("ROLLBACK"); throw error; }
  finally { client.release(); }
  const doc = { orgId: "org-a", workspaceId: "ws-a", tsMs: 1000, sessionId: "s", source: "codex", machineId: "m", kind: "history", eventId: "e1", type: "history", userId: "u", actorName: "", actorRole: "", subagentId: "", projectId: "p", taskTitle: "", taskRef: "{}", taskRefId: "", tags: [], filesTouched: [], toolName: "", toolStatus: "", deliveryRecordId: "", content: "İstanbul hello", fingerprint: "f1", version: 1 };
  assert.equal((await pass(() => index.upsert([doc, { ...doc, eventId: "e2", tsMs: 1001 }, { ...doc, orgId: "org-b" }, { ...doc, workspaceId: "ws-b" }]))).written, 4);
  assert.equal((await pass(() => index.upsert([{ ...doc, content: "stale", version: 0 }]))).written, 0);
  assert.equal((await pass(() => index.query({ tenant: scope, order: "asc", limit: 100, q: "i̇stanbul" }))).hits.length, 2);
  checks.push("registered-postgres-actual-search-unicode-version-tenant");
  await db.query("GRANT USAGE ON SCHEMA sessions,search_index TO portable_read; GRANT SELECT ON ALL TABLES IN SCHEMA sessions,search_index TO portable_read");
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const jwk = { ...publicKey.export({ format: "jwk" }), kid: "portable-fixture", alg: "RS256", use: "sig" };
  jwks = createServer((_req, res) => { res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ keys: [jwk] })); });
  await new Promise(resolve => jwks.listen(0, "127.0.0.1", resolve));
  const origin = "https://portable-proof.invalid", issuer = "https://issuer.invalid";
  const mint = (over = {}) => {
    const now = Math.floor(Date.now() / 1000), enc = value => Buffer.from(JSON.stringify(value)).toString("base64url");
    const unsigned = enc({ alg: "RS256", kid: jwk.kid, typ: "JWT" }) + "." + enc({ iss: issuer, iat: now, exp: now + 120, sub: "agent_relayhistory_assertion", meta: { tokenClass: "relayhistory_assertion" }, aud: [origin], org: "org-a", wks: "ws-a", sponsorId: "u", scopes: ["rth:read"], ...over });
    return unsigned + "." + sign("RSA-SHA256", Buffer.from(unsigned), privateKey).toString("base64url");
  };
  const readUrl = new URL(url); readUrl.username = "portable_read";
  let readDb;
  const readComposition = reader.createPostgresSearchComposition(config);
  const originalState = readComposition.state;
  readComposition.state = input => { readDb = input.sql; return originalState(input); };
  service = await reader.readAppFromSecret(JSON.stringify({ supabase_url: readUrl.href }), { ...env, READ_API_COMPLETE_SINCE: "epoch", READ_API_ORIGIN: origin, RELAYAUTH_ISSUER: issuer, RELAYAUTH_JWKS_URL: `http://127.0.0.1:${jwks.address().port}/jwks` }, readComposition);
  const request = (path, token = mint()) => reader.handleUrlEvent(service.app, { rawPath: path.split("?")[0], rawQueryString: path.split("?")[1], headers: token ? { authorization: `Bearer ${token}` } : {}, requestContext: { http: { method: "GET" } } });
  const first = await request("/v1/sessions/s/events?workspace=ws-a&limit=1");
  assert.equal(first.statusCode, 200);
  const firstPage = JSON.parse(first.body); assert.equal(firstPage.events.length, 1); assert.ok(firstPage.nextCursor);
  const second = JSON.parse((await request(`/v1/sessions/s/events?workspace=ws-a&limit=1&cursor=${encodeURIComponent(firstPage.nextCursor)}`)).body);
  assert.equal(second.events.length, 1); assert.notEqual(second.events[0].eventId, firstPage.events[0].eventId);
  assert.equal((await request("/v1/events?workspace=ws-b&q=hello&limit=10")).statusCode, 403);
  assert.equal((await request("/v1/events?workspace=ws-a&q=hello&limit=10", mint({ aud: ["https://other.invalid"] }))).statusCode, 401);
  assert.equal(JSON.parse((await request("/v1/events?workspace=ws-a&q=hello&limit=10&org=org-b")).body).events.length, 2);
  await assert.rejects(readDb.query("DELETE FROM search_index.transcript_events"), error => error.code === "42501");
  checks.push("actual-read-api-jwt-audience-tenant-cursor-readonly-role");
  await db.query("INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s-remapped',1,true); UPDATE sessions.session_index SET canonical_session_id='s-remapped' WHERE org_id='org-a' AND session_id='s'");
  const remapped = await request("/v1/events?workspace=ws-a&q=hello&limit=10");
  assert.equal(remapped.statusCode, 501); assert.equal(JSON.parse(remapped.body).error.reason, "session_partial");
  await db.query("UPDATE sessions.session_index SET canonical_session_id=NULL WHERE org_id='org-a' AND session_id='s'; DELETE FROM sessions.session_directory WHERE session_id='s-remapped'");
  checks.push("canonical-remap-same-epoch-refuses-complete-search");
  await db.query("UPDATE sessions.session_directory SET event_projection_epoch=2 WHERE org_id='org-a'");
  const partial = await request("/v1/events?workspace=ws-a&q=hello&limit=10");
  assert.equal(partial.statusCode, 501); assert.equal(JSON.parse(partial.body).error.reason, "session_partial");
  checks.push("pointer-epoch-change-refuses-complete-search");
  const handler = pipeline.createPipelineLambdaHandler({ loadRuntime: async () => rt });
  assert.deepEqual(await handler({ Records: [{ messageId: "test", body: JSON.stringify({ Event: "s3:TestEvent" }) }, { messageId: "bad", body: "invalid" }] }), { batchItemFailures: [{ itemIdentifier: "bad" }] });
  assert.deepEqual(await handler({ brief: true }), { ok: true });
  assert.deepEqual(await handler({ analysisCache: true }), { ok: true });
  checks.push("installed-sqs-partial-and-default-off-schedules");
  await pass(() => index.eraseScope({ orgId: "org-a", workspaceId: "ws-a", source: "codex", sessionIds: ["s"] }));
  assert.equal((await pass(() => index.query({ tenant: scope, order: "asc", limit: 100 }))).hits.length, 0);
  assert.equal((await pass(() => index.query({ tenant: { orgId: "org-b", workspaceId: "ws-a" }, order: "asc", limit: 100 }))).hits.length, 1);
  assert.equal((await pass(() => index.query({ tenant: { orgId: "org-a", workspaceId: "ws-b" }, order: "asc", limit: 100 }))).hits.length, 1);
  checks.push("synchronous-tenant-scoped-erasure");
  console.log(JSON.stringify({ proof: "portable-lambda-local-pg17-v1", sourceSha: pipelineManifest.sourceSha, sourceStatus: pipelineManifest.sourceStatus, node: process.version, platform: process.platform, arch: process.arch, generation: assets.expected.generation, checks, ok: true, cloudProof: false, provider: "postgres-only; OpenSearch/Lambda-platform conformance remains separate" }));
} finally {
  await service?.close(); await db.end();
  if (jwks) await new Promise(resolve => jwks.close(resolve));
}
