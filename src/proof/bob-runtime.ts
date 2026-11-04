/** Disposable full-flow proof. Docker resources are uniquely owned and always
 * removed. No deployment credentials, external endpoints, or secret files.
 * Run on Linux: bun run src/proof/bob-runtime.ts */
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { spawnSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createServer } from "node:http";
import { zstdDecompressSync } from "node:zlib";
import pg from "pg";
import { createS3Store, objectKey, objectStoreManifestReader, readAll, postgresPipelineQueue, type StageMessage, type ManifestV1 } from "../shared/vendor.ts";
import { runMigrations, migrationProfile } from "../hoopdb/migrate.ts";
import { s3Request } from "../locker/init.ts";
import { createIngestApp, encodeIdentity, signRequest, IDENTITY_HEADER, SIGNATURE_HEADER, SYNC_SCOPE } from "../session-ingest/app.ts";
import { sqlLedger } from "../session-ingest/ledger-sql.ts";
import { batch, record, TENANT } from "../session-ingest/__tests__/support.ts";
import { readSummaryObject } from "../bob-costas/summary-objects.ts";
import { masterKeyring } from "../shared/keyring.ts";
import { findMissingSummaries } from "../bob-costas/sweep.ts";
import { canonicalSnapshot, type CanonicalPointer } from "../session-read-api/canonical.ts";
import { createS3Eraser } from "../migration/neon-s3-erasure/eraser.ts";
import { indexSearchScope } from "../session-pipeline/search-index.ts";
import { createClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { schemaStatements } from "../session-pipeline/vendor/search-index/schema.ts";
import { derivedTick } from "../session-pipeline/derived-tick.ts";

const owned = `bob-proof-${process.pid}-${Date.now()}`;
const containers: string[] = [];
const workers: { proc: ChildProcessWithoutNullStreams; done: Promise<void> }[] = [];
const logs: string[] = [];
const checks: string[] = [];
const env: NodeJS.ProcessEnv = { PATH: process.env.PATH, HOME: process.env.HOME, LANG: "C.UTF-8" };
const adminPools: pg.Pool[] = [];
const pause = (ms: number) => new Promise(r => setTimeout(r, ms));
function check(name: string) { checks.push(name); console.log(`PASS ${name}`); }
function docker(args: string[], extraEnv = env) {
  const r = spawnSync("docker", args, { encoding: "utf8", env: extraEnv });
  if (r.status !== 0) throw new Error(`fixture_docker_failed:${args[0]}`);
  return r.stdout.trim();
}
function launch(suffix: string, image: string, port: number, args: string[], vars: Record<string, string> = {}, dockerOptions: string[] = []) {
  const name = `${owned}-${suffix}`;
  const id = docker(["run", "-d", "--name", name, "--label", `96bulls.proof=${owned}`, "-p", `127.0.0.1::${port}`,
    ...dockerOptions, ...Object.keys(vars).flatMap(k => ["-e", k]), image, ...args], { ...env, ...vars });
  assert(/^[a-f0-9]{64}$/.test(id), "fixture_container_id_missing");
  containers.push(id);
  const bound = docker(["port", name, String(port)]).match(/:(\d+)$/)?.[1];
  assert(bound, "fixture_port_missing");
  return `127.0.0.1:${bound}`;
}
async function until(fn: () => Promise<boolean>, code: string, ms = 45_000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await fn().catch(() => false)) return;
    await pause(150);
  }
  throw new Error(code);
}
async function worker(file: string, workerEnv: Record<string, string>) {
  const server = createServer();
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(resolve => server.close(() => resolve()));
  const p = spawn(process.execPath, ["run", file], {
    env: { ...env, ...workerEnv, WORKER_HEALTH_PORT: String(port) }, stdio: ["pipe", "pipe", "pipe"],
  });
  let captured = "";
  p.stdout.on("data", c => captured += c.toString());
  p.stderr.on("data", c => captured += c.toString());
  const done = new Promise<void>(resolve => p.on("close", () => { logs.push(captured); resolve(); }));
  workers.push({ proc: p, done });
  await until(async () => (await fetch(`http://127.0.0.1:${port}/readyz`)).ok, `worker_not_ready:${file}`);
}

try {
  const pgAddress = launch("pg", "pgvector/pgvector:pg17", 5432, [], { POSTGRES_HOST_AUTH_METHOD: "trust" });
  const owner = new pg.Pool({ connectionString: `postgres://postgres@${pgAddress}/postgres` });
  adminPools.push(owner);
  await until(async () => (await owner.query("SELECT 1")).rowCount === 1, "postgres_not_ready");
  await owner.query("REVOKE TEMPORARY ON DATABASE postgres FROM PUBLIC");
  await owner.query("CREATE ROLE bob_proof_runtime LOGIN");
  const profile = migrationProfile({ HOOPDB_DB_MODE: "local", HOOPDB_SUPABASE_SHIM: "true", HOOPDB_ROLE_BOOTSTRAP: "true", HOOPDB_RUNTIME_ROLE: "bob_proof_runtime", HOOPDB_RUNTIME_ROLE_LOGIN: "true" });
  const client = await owner.connect();
  try { await runMigrations(client, { profile }); } finally { client.release(); }
  const db = new pg.Pool({ connectionString: `postgres://bob_proof_runtime@${pgAddress}/postgres` });
  adminPools.push(db);
  check("full schema applied to owned PG17; dedicated worker role passes ACL validation");

  // One ephemeral MinIO root for this isolated fixture, generated in memory.
  const accessKeyId = randomBytes(12).toString("hex"), secretAccessKey = randomBytes(32).toString("hex");
  const endpoint = `http://${launch("s3", "pgsty/minio:latest", 9000,
    ["-c", "mkfifo /run/proof/keys; { read -r MINIO_ROOT_USER; read -r MINIO_ROOT_PASSWORD; } < /run/proof/keys; rm /run/proof/keys; export MINIO_ROOT_USER MINIO_ROOT_PASSWORD; exec minio server /data --address :9000"],
    {}, ["--entrypoint", "/bin/sh", "--tmpfs", "/run/proof:rw,noexec,nosuid,size=1m"])}`;
  // RAM pipe injection: neither Docker Config.Env nor arguments/files contain
  // generated fixture credentials. No production key is fetched or reused.
  const inject = spawnSync("docker", ["exec", "-i", `${owned}-s3`, "/bin/sh", "-c",
    "while [ ! -p /run/proof/keys ]; do sleep 0.01; done; cat > /run/proof/keys"],
    { input: `${accessKeyId}\n${secretAccessKey}\n`, encoding: "utf8", timeout: 10_000 });
  assert.equal(inject.status, 0, "fixture_ram_injection_failed");
  await until(async () => (await fetch(`${endpoint}/minio/health/ready`)).ok, "minio_not_ready");
  const bucket = "bob-runtime-proof";
  const req = { endpoint, region: "us-east-1", accessKeyId, secretAccessKey, path: `/${bucket}` };
  assert((await s3Request({ ...req, method: "PUT" })).ok);
  assert((await s3Request({ ...req, method: "PUT", query: "versioning", body: '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' })).ok);
  const store = createS3Store({ endpoint, bucket, region: "us-east-1", credentials: { accessKeyId, secretAccessKey } });
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket });
  const master = randomBytes(32);
  const keyring = await masterKeyring([{ id: "proof", bytes: master }]);
  const workerEnv = {
    HOOPDB_URL: `postgres://bob_proof_runtime@${pgAddress}/postgres`, HOOPDB_RUNTIME_ROLE: "bob_proof_runtime",
    LOCKER_BUCKET: bucket, LOCKER_ENDPOINT: endpoint, LOCKER_REGION: "us-east-1",
    LOCKER_ACCESS_KEY_ID: accessKeyId, LOCKER_SECRET_ACCESS_KEY: secretAccessKey,
    PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_BRIEF_ENABLED: "false",
    SUMMARY_MASTER_KEYS: `proof:${master.toString("hex")}`, BOB_COSTAS_MODEL: "off",
    BOB_COSTAS_DIRECTORY_SUMMARY_ENABLED: "false", SUMMARY_SWEEP_SECONDS: "3600",
    WORKER_IDLE_MS: "50", WORKER_BATCH_SIZE: "1", OTEL_SDK_DISABLED: "true",
  };
  // Avoid inherited static tenant pinning or deployment mode overriding the proof.
  delete env.TENANT_ORG_ID;
  await worker("src/hoopdb/indexer.ts", workerEnv);
  await worker("src/referee/main.ts", workerEnv);
  await worker("src/bob-costas/main.ts", workerEnv);
  check("real indexer, Referee and Bob processes pass readiness against owned PG/S3");

  const signingKey = randomBytes(32).toString("hex");
  const ledger = sqlLedger(async (sql, params) => {
    const c = await owner.connect();
    try { await c.query("BEGIN"); await c.query("SET LOCAL ROLE bulls96_session_ingest");
      const r = await c.query(sql, params); await c.query("COMMIT"); return r;
    } catch (e) { await c.query("ROLLBACK"); throw e; } finally { c.release(); }
  });
  const app = createIngestApp({ signingKey, deliver: { ledger, target: { store, keyRoot: "96bulls" } } });
  const fakeSecret = ["relay", "ag", "x".repeat(24)].join("_");
  const marker = "pelican-runtime-proof";
  const path = "/v1/delivery/batches";
  async function upload(revision: number, text: string) {
    const b = await batch([
      record({ record_id: "prompt", kind: "history", revision, payload: { prompt: `${marker} ${text} ${fakeSecret}`, ts_ms: Date.now() } }),
      record({ record_id: "reply", revision, payload: { role: "assistant", text: "Prepared worker verification", ts_ms: Date.now() } }),
    ], { batch_id: `proof-batch-${revision}` });
    const body = new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch: b }));
    const identity = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] });
    const response = await app.request(path, { method: "POST", headers: {
      "content-type": "application/json", [IDENTITY_HEADER]: identity,
      [SIGNATURE_HEADER]: signRequest(signingKey, Math.floor(Date.now() / 1000), "POST", path, identity, body),
    }, body });
    assert.equal(response.status, 200, `upload_refused:${response.status}`);
    return { b, body, identity };
  }
  async function onlyManifest() {
    const page = await store.list(objectKey("96bulls/org-a/v1"));
    const keys = page.items.filter(o => String(o.key).endsWith("/manifest.json"));
    assert.equal(keys.length, 1);
    const key = String(keys[0]!.key);
    return { key, manifest: (await reader.read(key))!.manifest };
  }
  async function enqueue(key: string, m: ManifestV1) {
    await postgresPipelineQueue(db).send({ v: 1, stage: "session-indexed", workKey: `${key}@proof:${m.revision}`, revision: m.revision,
      ctx: { traceId: m.trace_id, orgId: TENANT.orgId, workspaceId: m.session.workspace_id, sessionId: m.session.session_id, machineId: m.session.machine_id, harness: m.session.harness },
      payload: { manifestKey: key, workspaceId: m.session.workspace_id, sessionId: m.session.session_id, machineId: m.session.machine_id, harness: m.session.harness, provider: "s3", bucket },
    });
  }
  async function completed(revision: number) {
    return (await owner.query("SELECT count(*)::int AS n FROM sessions.session_summaries WHERE revision=$1 AND status='ready'", [revision])).rows[0].n === 2 &&
      (await owner.query("SELECT count(*)::int AS n FROM sessions.session_verdicts WHERE revision=$1", [revision])).rows[0].n > 0;
  }
  await upload(1, "first revision");
  let { key, manifest: m } = await onlyManifest();
  await enqueue(key, m);
  await until(() => completed(m.revision), "full_flow_incomplete");
  for (const obj of (await store.list(objectKey(key.replace(/manifest.json$/, "raw")))).items) {
    const text = new TextDecoder().decode(zstdDecompressSync(await readAll(await store.get(obj.key))));
    assert(!text.includes(fakeSecret)); assert(text.includes(marker));
  }
  let pointers = (await owner.query("SELECT * FROM sessions.session_summaries ORDER BY kind")).rows;
  for (const p of pointers) {
    const stored = await readAll(await store.get(objectKey(p.object_key)));
    assert(!new TextDecoder().decode(stored).includes(marker));
    const rec = await readSummaryObject({ store, keyring, ids: TENANT, key: p.object_key, sha256: p.object_sha256, kind: p.kind, revision: m.revision });
    assert(rec && rec.summary); assert(rec.summary.includes(marker)); assert(!rec.summary.includes(fakeSecret));
  }
  assert((await owner.query("SELECT bool_and(short_summary IS NULL) AS ok FROM sessions.session_directory")).rows[0].ok);
  check("signed upload -> Technical Foul -> versioned S3 -> HoopDb -> verdict + encrypted short/long pointers; no SQL summary text or planted secret");

  const indexed = (await owner.query("SELECT *, encode(content_sha256,'hex') AS content_sha256 FROM sessions.session_index")).rows[0];
  const scope = { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, harness: "claude", sessionId: indexed.session_id };
  const canonicalPointer = { ...indexed, session_id: "sess-1" } as CanonicalPointer;
  const snapshot = await canonicalSnapshot({ store, provider: "s3", bucket, prefix: workerEnv,
    pointers: async () => [canonicalPointer], decompress: (b, n) => zstdDecompressSync(b, { maxOutputLength: n }),
  }, { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId: "sess-1" });
  assert(JSON.stringify(snapshot).includes(marker)); assert(!JSON.stringify(snapshot).includes(fakeSecret));
  check("canonical read returns scrubbed current source rows");

  const chAddress = launch("ch", "clickhouse/clickhouse-server:26.8", 8123, [], { CLICKHOUSE_SKIP_USER_SETUP: "1" });
  await until(async () => (await fetch(`http://${chAddress}/ping`)).ok, "clickhouse_not_ready");
  const ch = createClickHouseClient({ url: `http://${chAddress}`, user: "default", password: "", database: "default" });
  for (const statement of schemaStatements()) await ch.command(statement);
  const searchDeps = { store, reader, ch, prefixFor: (org: string) => `96bulls/${org}`, transact: async <T>(fn: (tx: pg.PoolClient) => Promise<T>) => {
    const c = await db.connect(); try { await c.query("BEGIN"); const result = await fn(c); await c.query("COMMIT"); return result; }
    catch (e) { await c.query("ROLLBACK"); throw e; } finally { c.release(); }
  } };
  assert((await indexSearchScope(scope, searchDeps)).complete);
  assert.equal((await ch.select<{ n: number }>(`SELECT count() AS n FROM transcript_events FINAL WHERE org_id='org-a' AND position(content, '${marker}')>0`))[0]?.n, 1);
  assert.equal((await ch.select<{ n: number }>("SELECT count() AS n FROM transcript_events FINAL WHERE org_id='org-b'"))[0]?.n, 0);
  check("actual S3-to-ClickHouse search index contains scrubbed matching rows and excludes another tenant");

  const before = pointers.map(p => p.object_sha256);
  await enqueue(key, m); await pause(700);
  assert.deepEqual((await owner.query("SELECT object_sha256 FROM sessions.session_summaries ORDER BY kind")).rows.map(p => p.object_sha256), before);
  check("equal-revision replay keeps both encrypted objects current");
  await upload(2, "updated revision");
  ({ key, manifest: m } = await onlyManifest());
  await enqueue(key, m);
  await until(() => completed(m.revision), "regeneration_incomplete");
  pointers = (await owner.query("SELECT * FROM sessions.session_summaries ORDER BY kind")).rows;
  assert.notDeepEqual(pointers.map(p => p.object_sha256), before);
  assert(pointers.every(p => Number(p.revision) === m.revision));
  assert.equal((await findMissingSummaries(db as never, "summary-short", 10, "hkdf1:proof")).length, 0);
  check("updated manifest regenerates both kinds at the current revision");

  const wrong = { v: 1, stage: "summary-short", workKey: "wrong-tenant", revision: m.revision,
    ctx: { traceId: m.trace_id, orgId: "org-b", workspaceId: m.session.workspace_id, sessionId: m.session.session_id, machineId: m.session.machine_id, harness: m.session.harness }, payload: { manifestKey: key } } as StageMessage;
  await postgresPipelineQueue(db).send(wrong);
  await until(async () => (await owner.query("SELECT count(*)::int n FROM sessions.pipeline_dead_letters WHERE org_id='org-b'")).rows[0].n > 0, "wrong_tenant_not_dead_lettered");
  assert.equal((await owner.query("SELECT count(*)::int n FROM sessions.session_summaries WHERE org_id='org-b'")).rows[0].n, 0);
  check("wrong tenant refuses permanently; error and DLQ are observable without foreign output");

  // Exercise the finite-duration consumer using the same real queue and roles.
  for (const p of workers) p.proc.kill("SIGTERM");
  await Promise.all(workers.map(p => p.done));
  const short = pointers.find(p => p.kind === "short")!;
  await owner.query("UPDATE sessions.session_summaries SET revision=revision-1 WHERE kind='short'");
  const otherHarness = { ...wrong, workKey: "other-harness", ctx: { ...wrong.ctx, orgId: TENANT.orgId, harness: "codex" } };
  await postgresPipelineQueue(db).send(otherHarness);
  assert.equal((await findMissingSummaries(db as never, "summary-short", 10, "hkdf1:proof")).length, 1);
  await owner.query("DELETE FROM sessions.pipeline_jobs WHERE work_key='other-harness'");
  await owner.query("UPDATE sessions.session_summaries SET revision=$1, content_sha256=$2 WHERE kind='short'", [m.revision, "f".repeat(64)]);
  assert.equal((await findMissingSummaries(db as never, "summary-short", 10, "hkdf1:proof")).length, 1);
  await owner.query("UPDATE sessions.session_summaries SET content_sha256=$1, object_key='outside/short.json.enc' WHERE kind='short'", [m.raw.content_sha256]);
  assert.equal((await findMissingSummaries(db as never, "summary-short", 10, "hkdf1:proof")).length, 1);
  await owner.query("UPDATE sessions.session_summaries SET object_key=$1 WHERE kind='short'", [short.object_key]);
  check("sweep recovers stale revision/hash/location and another harness cannot suppress recovery");
  const stages = ["status-classify", "summary-short", "summary-long"] as const;
  await postgresPipelineQueue(db).sendBatch(stages.map(stage => ({ ...wrong, stage,
    workKey: `scheduled-${stage}`, ctx: { ...wrong.ctx, orgId: TENANT.orgId } })));
  const hoop = { pool: db, query: db.query.bind(db), end: () => db.end() };
  const withoutKey = await derivedTick({ db: hoop, store, reader, remainingMs: () => 300_000 });
  assert.equal(withoutKey.claimed, 1); assert.equal(withoutKey.summaryKeyringReady, false);
  assert.equal((await owner.query("SELECT count(*)::int n FROM sessions.pipeline_jobs WHERE work_key LIKE 'scheduled-%'")).rows[0].n, 2);
  const withKey = await derivedTick({ db: hoop, store, reader, keyring, remainingMs: () => 300_000 });
  assert.equal(withKey.acked, 2);
  const noTime = await derivedTick({ db: hoop, store, reader, keyring, remainingMs: () => 1 });
  assert(noTime.gated); assert.equal(noTime.claimed, 0);
  check("finite-duration tick validates live role gate, leaves summaries queued without keys, consumes with keys, and stops before deadline");

  // A real erasure clears every S3 version under this fixture's session prefix.
  // Fence the SQL row first, matching the required eraser ordering contract.
  await owner.query("UPDATE sessions.session_index SET deleted_at=now()");
  const eraser = createS3Eraser({ bucket, endpoint, region: "us-east-1", credentials: { accessKeyId, secretAccessKey } });
  assert((await eraser.erasePrefix(key.replace(/manifest.json$/, ""))) > 0);
  assert.equal((await eraser.listKeys(key.replace(/manifest.json$/, ""))).length, 0);
  assert((await indexSearchScope(scope, searchDeps)).complete);
  assert.equal((await ch.select<{ n: number }>("SELECT count() n FROM transcript_events FINAL"))[0]?.n, 0);
  check("fenced session erasure removes all raw/manifest/summary S3 versions and derived CH rows");

  await pause(100);
  assert(logs.every(s => !s.includes(fakeSecret) && !s.includes(marker)));
  check("worker logs contain no planted secret or source marker");
  console.log(JSON.stringify({ proof: "bob-runtime", checks, productionReady: false,
    blockers: ["canonical_fold_over_64MiB_or_100k_records", "managed_publication_authority_not_composed", "armed_lambda_assets_not_packaged", "bob_output_reader_and_search_projection_missing", "live_runtime_unverified", "full_reset_and_SQL_derived_erasure_not_proved"] }, null, 2));
} finally {
  for (const p of workers) p.proc.kill("SIGTERM");
  await Promise.all(adminPools.map(p => p.end()));
  for (const name of containers.reverse()) spawnSync("docker", ["rm", "-f", "-v", name], { stdio: "ignore" });
}
