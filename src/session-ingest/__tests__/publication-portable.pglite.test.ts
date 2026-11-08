/** Portable publication runtime end to end, offline: real migrations + verifiers on
 * PGlite (as the bulls96_session_ingest login), the real S3 store/SigV4 against a
 * MinIO-shaped path-style fake, a buffered stage from the real ingest path, then
 * runPortablePublicationRuntime publish and GC. No AWS, Supabase, IMDS or network.
 */
import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createS3Store, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { SCRUB_RULESET_VERSION } from "../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import { parseSessionKey, rawChunkKey } from "../../shared/vendor.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliverBatch } from "../deliver.ts";
import { runPortablePublicationRuntime } from "../publication-portable.ts";
import { batch, record, TENANT } from "./support.ts";
import { orderedFixtureLedger, initializeFixtureSeatClock } from "./ordered-support.ts";
import { pgliteTestPool, MinioShapedS3 } from "./portable-fakes.ts";
setDefaultTimeout(120_000);

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const GEN = "a".repeat(64), BUCKET = "selfhost-sessions", REGION = "selfhost-1", ORIGIN = "http://minio.selfhost.svc:9000";
const ACCESS = "publisher-fixture", SECRET = "publisher-fixture-secret-only";
// A long-term AWS key id shape: the current scrub ruleset must redact it before publish.
const LEAK = "AKIA" + "ABCDEFGHIJKLMNOP";
const FILES: Record<string, string> = {
  "/run/secrets/publication/access-key-id": ACCESS + "\n",
  "/run/secrets/publication/secret-access-key": SECRET + "\n",
  "/run/secrets/publication/database-url": "postgresql://bulls96_session_ingest:fixture-only@postgres.selfhost.svc:5432/sessions?sslmode=disable\n",
};
const ENV = {
  INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_GC_ENABLED: "true",
  INGEST_PUBLISH_BUCKET: BUCKET, INGEST_PUBLISH_EXPECT_GENERATION: GEN, INGEST_PUBLISH_S3_ENDPOINT: ORIGIN, ALLOW_IN_CLUSTER_HTTP: "true",
  INGEST_PUBLISH_S3_REGION: REGION, INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: "/run/secrets/publication/access-key-id",
  INGEST_PUBLISH_S3_SECRET_ACCESS_KEY_FILE: "/run/secrets/publication/secret-access-key", INGEST_PUBLISH_DATABASE_URL_FILE: "/run/secrets/publication/database-url",
  HOOPDB_DB_MODE: "selfhosted", SESSION_POSTGRES_TLS_PROFILE: "supabase", INGEST_PUBLISH_BUDGET_MS: "60000",
};
let db: PGlite, s3: MinioShapedS3, store: ObjectStore, stage: string;
const owner = async (sql: string, args: unknown[] = []) => db.query<any>(sql, args as never);
const asIngest = async (sql: string, args: unknown[] = []) => {
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await db.query<any>(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
};
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("src/hoopdb/migrations/" + f));
  // What src/hoopdb/migrate.ts does before its phase 2 verifiers (hardenAndCheck).
  await db.exec("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC");
  await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
  await owner("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')", [GEN]);
  await owner("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)VALUES($1,$2,$2,$3,clock_timestamp()+interval'1 hour',repeat('3',64))", [TENANT.orgId, "a".repeat(64), GEN]);
  await initializeFixtureSeatClock({ query: owner }, GEN);
  // The owner-side admission row naming this target (bucket/region/keyRoot); see the PR handoff contract.
  await owner("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour',$5,$6,'96bulls',$7)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, "b".repeat(64), BUCKET, REGION, GEN]);
  s3 = new MinioShapedS3(ORIGIN, BUCKET, REGION, ACCESS);
  store = createS3Store({ bucket: BUCKET, region: REGION, endpoint: ORIGIN, credentials: { accessKeyId: ACCESS, secretAccessKey: SECRET }, fetch: s3.fetch }) as unknown as ObjectStore;
  const ledger = orderedFixtureLedger(sqlLedger(asIngest, { acceptanceMode: "buffered", expectedGeneration: GEN }), { query: owner });
  const accepted = await deliverBatch({ ledger, target: { store, keyRoot: "96bulls" }, acceptanceMode: "buffered" }, TENANT,
    await batch([record({ record_id: "portable-r1", payload: { text: "deploy key " + LEAK + " pasted", ts_ms: 1761955200000 } })]));
  expect(accepted.outcome).toBe("accepted_buffered");
  stage = (await owner("SELECT active_stage_token token FROM sessions.ingest_buffer_outbox")).rows[0].token;
});
afterAll(async () => { await db?.close(); });

const assets = async () => { const real = await derivedSchemaAssets(); return { expected: { ...real.expected, generation: GEN }, verifiers: real.verifiers }; };
const overrides = (extra: Record<string, unknown> = {}) => ({
  assets, fetcher: s3.fetch, readFile: (p: string) => { if (!(p in FILES)) throw Object.assign(new Error("ENOENT"), { code: "ENOENT" }); return FILES[p]!; },
  pool: (c: any) => pgliteTestPool(db, "bulls96_session_ingest", c), ...extra,
});

test("portable publish: verifiers pass as the ingest login, scrubbed canonical image lands in the S3-compatible bucket", async () => {
  let poolConfig: any;
  const before = s3.requests.length;
  const result = await runPortablePublicationRuntime(ENV, "publish", overrides({ pool: (c: any) => { poolConfig = c; return pgliteTestPool(db, "bulls96_session_ingest", c); } }));
  expect(result).toMatchObject({ enabled: true, examined: 1, published: 1 });
  // Discrete pool fields from the mounted URL: no connectionString, URL sslmode ignored, pinned TLS profile applied.
  expect(poolConfig).toMatchObject({ host: "postgres.selfhost.svc", port: 5432, user: "bulls96_session_ingest", database: "sessions", application_name: "96bulls-canonical-publication" });
  expect(poolConfig.connectionString).toBeUndefined();
  expect(poolConfig.ssl.rejectUnauthorized).toBe(true);
  // Every store request went to the configured endpoint, path-style, signed for the configured key and region.
  const mine = s3.requests.slice(before);
  expect(mine.length).toBeGreaterThan(0);
  for (const r of mine) { expect(r.host).toBe("minio.selfhost.svc:9000"); expect(r.scope).toMatch(new RegExp(`^${ACCESS}/\\d{8}/${REGION}/s3/aws4_request$`)); }
  const pointer = (await owner("SELECT * FROM sessions.session_index")).rows[0];
  expect(pointer).toMatchObject({ org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, bucket: BUCKET, storage_provider: "s3", scrubbed: true });
  expect(pointer.manifest_key).toStartWith(`96bulls/${TENANT.orgId}/upload-generations/g=`);
  const manifest = JSON.parse(new TextDecoder().decode(s3.objects.get(pointer.manifest_key)!.body));
  expect(manifest.raw.scrub).toMatchObject({ version: SCRUB_RULESET_VERSION });
  const chunk = manifest.raw.chunks.at(-1);
  const stored = s3.objects.get(String(rawChunkKey(parseSessionKey(pointer.manifest_key)!, chunk.seq, chunk.start_offset, chunk.sha256, "ndjson")))!.body;
  const text = new TextDecoder().decode(zstdDecompressSync(stored));
  expect(text).toContain("[REDACTED]");
  expect(text).not.toContain(LEAK);
  // Every PUT is generation-qualified under this tenant; nothing else was written by publication.
  for (const r of mine.filter((r) => r.method === "PUT")) expect(r.key).toStartWith(`96bulls/${TENANT.orgId}/upload-generations/g=`);
  expect((await owner("SELECT state FROM sessions.ingest_publication_attempts")).rows[0].state).toBe("published");
  // Idempotent: nothing left to examine.
  expect(await runPortablePublicationRuntime(ENV, "publish", overrides())).toMatchObject({ enabled: true, examined: 0, published: 0 });
  expect(stage).toMatch(/^[a-f0-9-]{36}$/);
});

test("portable GC: retired generation is barriered in the same bucket and erased through the endpoint-bound eraser", async () => {
  const pointer = (await owner("SELECT manifest_key FROM sessions.session_index")).rows[0];
  const ledger = orderedFixtureLedger(sqlLedger(asIngest, { acceptanceMode: "buffered", expectedGeneration: GEN }), { query: owner });
  await ledger.setSessionUploads!({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: false });
  await ledger.setSessionUploads!({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: true });
  let eraserConfig: any; const erased: string[] = [];
  const result = await runPortablePublicationRuntime(ENV, "gc", overrides({ eraser: (c: any) => { eraserConfig = c; return { eraseObjectExcept: async (key: string) => { erased.push(key); return { deleted: 0 }; } }; } }));
  expect(result.enabled).toBe(true);
  expect((result as any).finished).toBeGreaterThan(0);
  expect(eraserConfig).toMatchObject({ bucket: BUCKET, region: REGION, endpoint: ORIGIN, credentials: { accessKeyId: ACCESS, secretAccessKey: SECRET } });
  expect(erased).toContain(pointer.manifest_key);
  for (const key of erased) expect(key).toStartWith(`96bulls/${TENANT.orgId}/upload-generations/g=`);
  const marker = JSON.parse(new TextDecoder().decode(s3.objects.get(pointer.manifest_key)!.body));
  expect(marker.kind).toBe("upload-generation-redaction");
});

test("portable runtime refuses a superuser, foreign login or SET SESSION AUTHORIZATION before any publication RPC", async () => {
  // [session role, authenticated login]: the last is a superuser backend that ran SET SESSION AUTHORIZATION.
  for (const [role, login] of [["postgres", "postgres"], ["bulls96_session_ingest_seeder", "bulls96_session_ingest_seeder"], ["bulls96_session_ingest", "postgres"]]) {
    const before = s3.requests.length, attempts = (await owner("SELECT count(*)::int n FROM sessions.ingest_publication_attempts")).rows[0].n;
    await expect(runPortablePublicationRuntime(ENV, "publish", overrides({ pool: (c: any) => pgliteTestPool(db, role!, c, login!) }))).rejects.toMatchObject({ code: "publication_database_role" });
    expect(s3.requests.length).toBe(before);
    expect((await owner("SELECT count(*)::int n FROM sessions.ingest_publication_attempts")).rows[0].n).toBe(attempts);
  }
});

test("portable runtime refuses a frontier for another bucket: nothing is a candidate", async () => {
  expect(await runPortablePublicationRuntime({ ...ENV, INGEST_PUBLISH_BUCKET: "other-bucket" }, "publish", overrides())).toMatchObject({ enabled: true, examined: 0 });
  expect(await runPortablePublicationRuntime({ ...ENV, INGEST_PUBLISH_S3_REGION: "us-east-1" }, "gc", overrides({ eraser: () => ({ eraseObjectExcept: async () => { throw new Error("no erase expected"); } }) }))).toMatchObject({ enabled: true, examined: 0 });
});
