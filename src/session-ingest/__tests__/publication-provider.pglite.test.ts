/** Publication stamps the install's Locker provider, and the read API serves what it stamped.
 *
 * Regression for GET /v1/sessions/:id/turns answering 501 turns_canonical_pointer_invalid on
 * GCS installs: publication-image.ts used to stamp storage_provider "s3" (pointer row) and
 * provider "s3" (landed message) whatever the Locker was, and canonical.ts refuses a pointer
 * whose storage_provider is not the provider the read API's Locker selected.
 *
 * Offline: real migrations + verifiers on PGlite, the real ingest path for the buffered stage,
 * the real publication job, and the read API's real canonical assembly (serviceCanonicalDeps,
 * pointer rows read AS bulls96_read_api through the tenant runner). S3 is the MinIO-shaped
 * SigV4 fake; GCS is a loopback XML-API stand-in serving the same objects with a bearer token.
 *
 * SQL: 20261105059000_ingest_publication_gcs_provider.sql widens ingest_publication_prepare and
 * _publish to admit 's3' or 'gcs' (NULL and every other provider still 22023; publish also requires
 * the landed provider to equal the prepared pointer row's). Both providers publish end to end here.
 */
import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { createS3Store, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliverBatch } from "../deliver.ts";
import { runPortablePublicationRuntime } from "../publication-portable.ts";
import { publicationRpc } from "../publication-rpc.ts";
import { publishBufferedStage } from "../publication-job.ts";
import type { PublicationImageSpec, PublicationRpc, PublicationTarget } from "../publication-protocol.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { serviceCanonicalDeps } from "../../session-read-api/service-canonical.ts";
import { poolTenantRunner } from "../../session-read-api/liveness.ts";
import { memoryLiveness } from "../../session-read-api/__tests__/support.ts";
import { batch, record, TENANT } from "./support.ts";
import { orderedFixtureLedger, initializeFixtureSeatClock } from "./ordered-support.ts";
import { pgliteTestPool, MinioShapedS3 } from "./portable-fakes.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
setDefaultTimeout(180_000);

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const GEN = "a".repeat(64), BUCKET = "selfhost-sessions", REGION = "selfhost-1", ORIGIN = "http://minio.selfhost.svc:9000";
const ACCESS = "publisher-fixture", SECRET = "publisher-fixture-secret-only";
const TENANT_B = { orgId: "org-b", workspaceId: "ws-b", userId: "user-2" };
const SOURCE = "claude", SESSION = "sess-1";
const FILES: Record<string, string> = {
  "/run/secrets/publication/access-key-id": ACCESS + "\n",
  "/run/secrets/publication/secret-access-key": SECRET + "\n",
  "/run/secrets/publication/database-url": "postgresql://bulls96_session_ingest:fixture-only@postgres.selfhost.svc:5432/sessions?sslmode=disable\n",
};
const ENV = {
  INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_ENABLED: "true",
  INGEST_PUBLISH_BUCKET: BUCKET, INGEST_PUBLISH_EXPECT_GENERATION: GEN, INGEST_PUBLISH_S3_ENDPOINT: ORIGIN, ALLOW_IN_CLUSTER_HTTP: "true",
  INGEST_PUBLISH_S3_REGION: REGION, INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: "/run/secrets/publication/access-key-id",
  INGEST_PUBLISH_S3_SECRET_ACCESS_KEY_FILE: "/run/secrets/publication/secret-access-key", INGEST_PUBLISH_DATABASE_URL_FILE: "/run/secrets/publication/database-url",
  HOOPDB_DB_MODE: "selfhosted", SESSION_POSTGRES_TLS_PROFILE: "supabase", INGEST_PUBLISH_BUDGET_MS: "60000",
};

// The read API's Locker selections for the same bucket (service-canonical.ts / shared/locker.ts).
const GCS_HOST = "http://127.0.0.1:9";
const GCS_TOKEN_URI = `${GCS_HOST}/computeMetadata/v1/instance/service-accounts/default/token`;
const GCS_TOKEN = "synthetic-gcs-access-token";
const READ_COMMON = { LOCKER_BUCKET: BUCKET, PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true", READ_API_TURNS_ASSUME_NO_LEGACY: "true" };
const S3_READ_ENV = { ...READ_COMMON, LOCKER_ENDPOINT: ORIGIN, LOCKER_REGION: REGION, LOCKER_ACCESS_KEY_ID: ACCESS, LOCKER_SECRET_ACCESS_KEY: SECRET };
const GCS_READ_ENV = { ...READ_COMMON, LOCKER_PROVIDER: "gcs", LOCKER_ENDPOINT: GCS_HOST, LOCKER_GCS_TOKEN_URI: GCS_TOKEN_URI };

let db: PGlite, s3: MinioShapedS3, store: ObjectStore;
/** The read pool's RESET ROLE on release (poolTenantRunner does not await release). */
let resetting: Promise<unknown> = Promise.resolve();
const owner = async (sql: string, args: unknown[] = []) => { await resetting; return db.query<any>(sql, args as never); };
const asIngest = async (sql: string, args: unknown[] = []) => {
  await resetting;
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await db.query<any>(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
};
const ledger = () => orderedFixtureLedger(sqlLedger(asIngest, { acceptanceMode: "buffered", expectedGeneration: GEN }), { query: owner });

beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("src/hoopdb/migrations/" + f));
  await db.exec("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC");
  // The read API's login (as server-selfhost-reads.pglite.test.ts): tenant-bound RLS on session_index.
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec(read("supabase/bootstrap/session_read_api_role_verify.sql"));
  await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
  await owner("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')", [GEN]);
  for (const t of [TENANT, TENANT_B]) {
    await owner("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)VALUES($1,$2,$2,$3,clock_timestamp()+interval'1 hour',repeat('3',64))", [t.orgId, "a".repeat(64), GEN]);
  }
  await initializeFixtureSeatClock({ query: owner }, GEN);
  for (const t of [TENANT, TENANT_B]) {
    await owner("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour',$5,$6,'96bulls',$7)", [t.orgId, t.workspaceId, t.userId, "b".repeat(64), BUCKET, REGION, GEN]);
  }
  s3 = new MinioShapedS3(ORIGIN, BUCKET, REGION, ACCESS);
  store = createS3Store({ bucket: BUCKET, region: REGION, endpoint: ORIGIN, credentials: { accessKeyId: ACCESS, secretAccessKey: SECRET }, fetch: s3.fetch }) as unknown as ObjectStore;
  // Only tenant A's stage exists until the GCS case delivers tenant B's, so each tick has one candidate.
  const accepted = await deliverBatch({ ledger: ledger(), target: { store, keyRoot: "96bulls" }, acceptanceMode: "buffered" }, TENANT,
    await batch([record({ record_id: "provider-a1", payload: { text: "s3 install", ts_ms: 1761955200000 } })]));
  expect(accepted.outcome).toBe("accepted_buffered");
});
afterAll(async () => { await db?.close(); });

const assets = async () => { const real = await derivedSchemaAssets(); return { expected: { ...real.expected, generation: GEN }, verifiers: real.verifiers }; };
const overrides = () => ({
  assets, fetcher: s3.fetch, readFile: (p: string) => { if (!(p in FILES)) throw Object.assign(new Error("ENOENT"), { code: "ENOENT" }); return FILES[p]!; },
  pool: (c: any) => pgliteTestPool(db, "bulls96_session_ingest", c),
});
const pointersOf = async (orgId: string) => (await owner("SELECT * FROM sessions.session_index WHERE org_id=$1", [orgId])).rows;

/** The read API's pointer reads, AS bulls96_read_api (one PGlite session, so one client at a time). */
const readPool = {
  async connect() {
    await resetting;
    await db.exec("SET ROLE bulls96_read_api");
    return { query: (text: string, params?: unknown[]) => db.query(text, params as never) as Promise<{ rows: unknown[] }>, release: () => { resetting = db.exec("RESET ROLE"); } };
  },
};

/** A loopback GCS XML-API stand-in: path-style `/<bucket>/<key>` with the metadata server's bearer, over the S3 fake's objects. */
const gcsRequests: string[] = [];
const gcsFetch = (async (input: Parameters<typeof fetch>[0], init: RequestInit = {}) => {
  const url = new URL(String(input instanceof Request ? input.url : input));
  const method = (init.method ?? "GET").toUpperCase();
  const headers = new Headers(init.headers);
  gcsRequests.push(`${method} ${decodeURIComponent(url.pathname)}`);
  if (url.origin !== GCS_HOST) return new Response("wrong endpoint", { status: 421 });
  if (headers.get("authorization") !== `Bearer ${GCS_TOKEN}`) return new Response("unauthorized", { status: 401 });
  if (method !== "GET" && method !== "HEAD") return new Response("read only", { status: 405 });
  const [, bucket, ...rest] = url.pathname.split("/");
  if (bucket !== BUCKET) return new Response("NoSuchBucket", { status: 404 });
  const o = s3.objects.get(rest.map(decodeURIComponent).join("/"));
  if (!o) return new Response(null, { status: 404 });
  const h = new Headers({ etag: o.etag, "x-goog-generation": o.version.replace(/\D/g, "") || "1", "content-length": String(o.body.byteLength), "content-type": o.contentType, "last-modified": new Date(0).toUTCString() });
  for (const [k, v] of Object.entries(o.metadata)) h.set(`x-goog-meta-${k}`, v);
  return new Response(method === "GET" ? (o.body as Uint8Array<ArrayBuffer>) : null, { status: 200, headers: h });
}) as typeof fetch;

/** GET /turns through the read API's real canonical assembly for the Locker env given. */
async function turns(env: Record<string, string>, tenant: { orgId: string; workspaceId: string; userId: string }) {
  const gcs = env.LOCKER_PROVIDER === "gcs";
  // gcsMetadataToken reads the token with the global fetch; answer only the trusted loopback token URI.
  const realFetch = globalThis.fetch;
  if (gcs) globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init: RequestInit = {}) => {
    const url = String(input instanceof Request ? input.url : input);
    if (url !== GCS_TOKEN_URI || new Headers(init.headers).get("metadata-flavor") !== "Google") throw new Error(`unexpected outbound fetch ${url}`);
    return Response.json({ access_token: GCS_TOKEN, expires_in: 3600, token_type: "Bearer" });
  }) as typeof fetch;
  try {
    const canonical = await serviceCanonicalDeps(env, poolTenantRunner(readPool), gcs ? gcsFetch : s3.fetch);
    const app = createReadApi({
      verify: async () => ({ ...tenant, scopes: ["rth:read"] }),
      ch: { database: "fixture", async command() {}, async insert() {}, async select<T>() { return [] as T[]; } },
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      directoryWorkspaces: new Set([tenant.workspaceId]),
      directory: { async list() { return []; }, async activity() { return []; }, async query() { return []; }, async queryActivity() { return []; } },
      turns: canonical!.turns,
    });
    const response = await app.request(`/v1/sessions/${SESSION}/turns?workspace=${tenant.workspaceId}&source=${SOURCE}`, { headers: { authorization: "Bearer fixture" } });
    return { status: response.status, body: (await response.json()) as Record<string, any> };
  } finally {
    globalThis.fetch = realFetch;
  }
}

test("S3 Locker: the real portable publish stamps s3 and /turns serves it; a GCS Locker refuses that row (the reported 501)", async () => {
  expect(await runPortablePublicationRuntime(ENV, "publish", overrides())).toMatchObject({ enabled: true, examined: 1, published: 1 });
  const [pointer] = await pointersOf(TENANT.orgId);
  expect(pointer).toMatchObject({ org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, bucket: BUCKET, storage_provider: "s3", deleted_at: null });
  expect(pointer.manifest_key).toStartWith(`96bulls/${TENANT.orgId}/upload-generations/g=`);
  expect((await landed(TENANT.orgId)).map((m) => m.payload.provider)).toEqual(["s3"]);
  const before = s3.requests.length;
  const ok = await turns(S3_READ_ENV, TENANT);
  expect(ok.status).toBe(200);
  expect(ok.body.turns).toEqual([]);
  // The canonical image was actually read (manifest + chunk through the SigV4 store).
  expect(s3.requests.slice(before).some((r) => r.method === "GET" && r.key === pointer.manifest_key)).toBe(true);
  // A row stamped s3 read by a GCS Locker: exactly the GCS-install symptom before this fix.
  const refused = await turns(GCS_READ_ENV, TENANT);
  expect(refused.status).toBe(501);
  expect(refused.body.error.reason).toBe("turns_canonical_pointer_invalid");
});

/** Runs the real publication job for tenant B's stage with the gcs target; `tamper` may rewrite what reaches the real SQL.
 *  Returns the raw SQL error of the first refused RPC (the job itself re-throws a sanitized code), or null. */
async function publishTenantB(tamper: { images?: (i: PublicationImageSpec[]) => PublicationImageSpec[]; messages?: (m: unknown[]) => unknown[] }) {
  const stage = (await owner("SELECT o.active_stage_token token FROM sessions.ingest_buffer_outbox o WHERE o.org_id=$1", [TENANT_B.orgId])).rows[0].token;
  const real = publicationRpc({ queryBounded: async (q: string, p: unknown[], ctx: { signal: AbortSignal }) => { ctx.signal.throwIfAborted(); return asIngest(q, p); } } as never, GCS_TARGET);
  let ticket: Awaited<ReturnType<PublicationRpc["claim"]>> | undefined;
  let sqlError: { code?: string; message?: string; leg?: string } | null = null;
  const seen = async <T,>(leg: string, run: () => Promise<T>) => {
    try { return await run(); } catch (e) { sqlError ??= Object.assign(e as object, { leg }); throw e; }
  };
  const rpc: PublicationRpc = {
    ...real,
    claim: async (token, signal) => (ticket = await real.claim(token, signal)),
    prepare: (t, images, signal) => seen("prepare", () => real.prepare(t, tamper.images ? tamper.images(images) : images, signal)),
    publish: (t, messages, signal) => seen("publish", () => real.publish(t, tamper.messages ? tamper.messages(messages) : messages, signal)),
  };
  try {
    await publishBufferedStage({ rpc, target: GCS_TARGET, store: () => store }, stage, { enabled: true, maxJobMs: 60_000 });
    expect(sqlError).toBeNull();
    return null;
  } catch (error) {
    expect(String((error as Error).message)).toBe(sqlError?.code ?? "unexpected");
    return sqlError;
  } finally {
    if (ticket) await real.abandon(ticket, AbortSignal.timeout(5000)).catch(() => {});
  }
}
const GCS_TARGET: PublicationTarget = { bucket: BUCKET, region: REGION, keyRoot: "96bulls", generation: GEN, provider: "gcs" };
const landed = async (orgId: string) =>
  (await owner("SELECT message FROM sessions.pipeline_jobs WHERE org_id=$1 AND message->>'stage'='object-landed' ORDER BY message->>'workKey'", [orgId])).rows.map((r) => r.message);

test("the SQL still refuses a provider outside s3/gcs (22023) and a landed provider that differs from the prepared row", async () => {
  const accepted = await deliverBatch({ ledger: ledger(), target: { store, keyRoot: "96bulls" }, acceptanceMode: "buffered" }, TENANT_B,
    await batch([record({ record_id: "provider-b1", payload: { text: "gcs install", ts_ms: 1761955200000 } })], { batch_id: "batch-b", account_id: await deliveryAccount(TENANT_B) }));
  expect(accepted.outcome).toBe("accepted_buffered");
  // The TypeScript guards admit only s3/gcs, so the pointer row is rewritten between the job and the real prepare.
  for (const provider of ["azure", null]) {
    const refused = await publishTenantB({ images: (images) => images.map((i) => ({ ...i, pointerRow: { ...i.pointerRow, storage_provider: provider } })) });
    expect(refused).toMatchObject({ leg: "prepare", code: "22023", message: "publication: pointer metadata differs from admitted image" });
  }
  // Real gcs prepare and register; the landed messages claim another provider (an allowed one, and one outside the set).
  for (const provider of ["s3", "azure"]) {
    const refused = await publishTenantB({ messages: (messages) => messages.map((m: any) => ({ ...m, payload: { ...m.payload, provider } })) });
    expect(refused).toMatchObject({ leg: "publish", code: "22023", message: "publication: notification differs from current image" });
  }
  expect(await pointersOf(TENANT_B.orgId)).toEqual([]);
  expect(await landed(TENANT_B.orgId)).toEqual([]);
});

test("GCS Locker: the real portable publish (prepare, register, publish through the SQL) stamps gcs, and /turns serves the row", async () => {
  // The provider comes from the read API's own selection (LOCKER_PROVIDER), not a publication setting.
  expect(await runPortablePublicationRuntime({ ...ENV, LOCKER_PROVIDER: "gcs" }, "publish", overrides())).toMatchObject({ enabled: true, examined: 1, published: 1 });
  const [pointer] = await pointersOf(TENANT_B.orgId);
  expect(pointer).toMatchObject({ org_id: TENANT_B.orgId, workspace_id: TENANT_B.workspaceId, storage_provider: "gcs", bucket: BUCKET, deleted_at: null });
  expect(pointer.manifest_key).toStartWith(`96bulls/${TENANT_B.orgId}/upload-generations/g=`);
  expect(s3.objects.has(pointer.manifest_key)).toBe(true);
  const messages = await landed(TENANT_B.orgId);
  expect(messages.map((m) => [m.payload.provider, m.payload.bucket, m.payload.key])).toEqual([["gcs", BUCKET, pointer.manifest_key]]);
  gcsRequests.length = 0;
  const ok = await turns(GCS_READ_ENV, TENANT_B);
  expect(ok.status).toBe(200);
  expect(ok.body.turns).toEqual([]);
  expect(gcsRequests).toContain(`GET /${BUCKET}/${pointer.manifest_key}`);
  // And the converse: an S3 Locker refuses the gcs-stamped row, so the stamp is what decides readability.
  const refused = await turns(S3_READ_ENV, TENANT_B);
  expect(refused.status).toBe(501);
  expect(refused.body.error.reason).toBe("turns_canonical_pointer_invalid");
});

test("operator UPDATE of a published row's storage_provider from s3 to gcs takes effect (the pointer guard admits it), and a GCS Locker then serves it", async () => {
  // ingest_publication_pointer_guard checks the live head (key, revision, digest, fences, generation), not the provider,
  // so an owner re-stamp of a live published row in read committed is applied, and every other column is unchanged.
  const [before] = await pointersOf(TENANT.orgId);
  expect(before.storage_provider).toBe("s3");
  expect((await owner("SHOW transaction_isolation")).rows[0].transaction_isolation).toBe("read committed");
  const updated = await owner("UPDATE sessions.session_index SET storage_provider='gcs' WHERE org_id=$1 AND storage_provider='s3' RETURNING 1", [TENANT.orgId]);
  expect(updated.rows).toHaveLength(1);
  const [after] = await pointersOf(TENANT.orgId);
  expect(after).toEqual({ ...before, storage_provider: "gcs", updated_at: after.updated_at });
  gcsRequests.length = 0;
  const ok = await turns(GCS_READ_ENV, TENANT);
  expect(ok.status).toBe(200);
  expect(gcsRequests).toContain(`GET /${BUCKET}/${after.manifest_key}`);
  expect((await turns(S3_READ_ENV, TENANT)).status).toBe(501);
});
