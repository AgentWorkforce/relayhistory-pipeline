/** Owned local PG17 + versioned MinIO vertical proof. Never accepts a live URL. */
import pg from "pg";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { runMigrations, generation } from "../src/hoopdb/migrate.ts";
import { projectSessionDirectory } from "../src/hoopdb/session-directory-projector.ts";
import { createS3Store, objectKey, readAll, type ObjectStore } from "../packages/object-store/src/index.ts";
import { signRequest } from "../packages/object-store/src/sigv4.ts";
import { sha256Hex } from "../packages/object-store/src/crypto.ts";
import { createS3Eraser } from "../src/migration/neon-s3-erasure/eraser.ts";
import { SemanticPostgresExecutor } from "../src/search-features/semantic-copy-postgres.ts";
import { sqlLedger } from "../src/session-ingest/ledger-sql.ts";
import { deliverBatch } from "../src/session-ingest/deliver.ts";
import { initializeFixtureSeatClock, orderedFixtureLedger } from "../src/session-ingest/__tests__/ordered-support.ts";
import { publicationRpc } from "../src/session-ingest/publication-rpc.ts";
import { publishBufferedStage } from "../src/session-ingest/publication-job.ts";
import { publicationGcLedger, collectPublicationObject } from "../src/session-ingest/publication-gc.ts";
import { runPublicationCanary, publicationCanaryPlanDigest, type PublicationCanaryAuthority, type PublicationCanaryPlan, type PublicationCanaryStage } from "../src/session-ingest/publication-canary.ts";
import { openPublicationCanaryOutcomeFile } from "../src/session-ingest/publication-canary-outcomes.ts";
import { produceCanonicalReplay } from "../src/session-pipeline/replay-publication.ts";
import { canonicalArtifactPage } from "../src/session-read-api/canonical-artifact-reader.ts";
import { readCanonicalHistoryPage, type CanonicalHistoryAdapterConfig } from "../src/session-read-api/canonical-history-adapter.ts";
import { deliveryAccount } from "../src/session-ingest/vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION, type HistoryExportBatch } from "../src/session-ingest/vendor/delivery/delivery-contracts.ts";

const out = process.env.PUBLICATION_CANARY_PROOF_OUT ?? "";
const pgName = process.env.PUBLICATION_CANARY_PG_CONTAINER ?? "";
const minioName = process.env.PUBLICATION_CANARY_MINIO_CONTAINER ?? "";
if (
  process.env.PUBLICATION_CANARY_DISPOSABLE !== "1" ||
  !/^\/home\/khaliqgant\/relay-briefs\/publication-canary-[A-Za-z0-9._-]+$/u.test(out) ||
  !/^publication-canary-[a-z0-9-]+$/u.test(pgName) ||
  !/^publication-canary-[a-z0-9-]+$/u.test(minioName)
) throw Error("owned_disposable_only");
const inspect = (name: string) => JSON.parse(execFileSync("docker", ["inspect", name], { encoding: "utf8" }))[0];
const port = (name: string, key: string) => {
  const value = inspect(name);
  if (value.Config.Labels["com.96bulls.proof"] !== "publication-canary-d") throw Error("container_owner");
  const binding = value.NetworkSettings.Ports[key]?.[0];
  if (!binding || binding.HostIp !== "127.0.0.1") throw Error("loopback_only");
  return Number(binding.HostPort);
};
mkdirSync(out, { recursive: false, mode: 0o700 });
const report: Record<string, unknown> & { cases: Array<Record<string, unknown>> } = {
  head: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  dirty: Boolean(execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).trim()),
  node: process.version,
  cases: [],
};
const check = async (name: string, work: () => Promise<Record<string, unknown> | void>) => {
  stage = name;
  const started = Date.now();
  const evidence = (await work()) ?? {};
  report.cases.push({ name, pass: true, ms: Date.now() - started, ...evidence });
};
const yes = (value: unknown, code: string): asserts value => { if (!value) throw Error(code); };
const credentials = { accessKeyId: "publisher-fixture-access", secretAccessKey: "publisher-fixture-only-key-20261106" };
const bucket = "publication-canary-fixture", region = "us-east-1";
const endpoint = `http://127.0.0.1:${port(minioName, "9000/tcp")}`;
const pgPort = port(pgName, "5432/tcp");
const cfg = (database: string, user = "postgres") => ({ host: "127.0.0.1", port: pgPort, database, user, connectionTimeoutMillis: 1000, application_name: "publication-canary-proof" });
async function s3(method: string, suffix = "", body = new Uint8Array()) {
  const url = new URL(`${endpoint}/${bucket}${suffix}`), headers = new Headers({ "content-type": "application/xml" });
  await signRequest({ method, url, headers, payloadSha256: await sha256Hex(body), region, credentials });
  const response = await fetch(url, { method, headers, ...(body.byteLength ? { body } : {}), signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw Error(`fixture_s3_${response.status}`);
  return response;
}
const admin = new pg.Client(cfg("postgres"));
await admin.connect();
const database = `publication_canary_${crypto.randomUUID().replaceAll("-", "")}`;
await admin.query(`CREATE DATABASE ${database}`);
const owner = new pg.Client(cfg(database));
await owner.connect();
let pool: pg.Pool | undefined;
let outcomes: Awaited<ReturnType<typeof openPublicationCanaryOutcomeFile>> | undefined;
let stage = "setup";
try {
  await runMigrations(owner, { profile: { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" } });
  const expected = await generation();
  report.schemaGeneration = expected;
  const bootstrap = readFileSync("supabase/bootstrap/session_ingest_role.sql", "utf8").split("\n").filter((line) => !line.startsWith("\\")).join("\n");
  await owner.query(bootstrap);
  await owner.query(readFileSync("supabase/bootstrap/session_ingest_verify.sql", "utf8"));
  await s3("HEAD").catch(async (error) => {
    if ((error as Error).message !== "fixture_s3_404") throw error;
    await s3("PUT");
  });
  await s3("PUT", "?versioning", new TextEncoder().encode('<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>'));
  pool = new pg.Pool({ ...cfg(database, "bulls96_session_ingest"), max: 4 });
  pool.on("error", () => {});
  const bounded = new SemanticPostgresExecutor(pool);
  const run = async (text: string, args: unknown[]) => {
    const client = await pool!.connect();
    try { return await client.query(text, args); } finally { client.release(); }
  };
  const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: expected });
  // Offline-only source-journal stand-in; production must supply stored source controls.
  const producer = orderedFixtureLedger(ledger, owner as never);
  const store = createS3Store({ endpoint, bucket, region, credentials });
  const eraser = createS3Eraser({ endpoint, bucket, region, credentials });
  const target = { bucket, region, keyRoot: "96bulls", generation: expected };
  const rpc = publicationRpc(bounded, target);
  const gc = publicationGcLedger(bounded, target);
  const origin = "origin-canary";
  const tenantA = { orgId: "canary-org-a", workspaceId: "canary-ws-a", userId: "canary-user-a" };
  const tenantB = { orgId: "held-org-b", workspaceId: "held-ws-b", userId: "held-user-b" };
  async function admit(tenant: typeof tenantA) {
    await owner.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
    try {
      await owner.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)", [JSON.stringify({ org_id: tenant.orgId, workspace_id: tenant.workspaceId, user_id: tenant.userId, deletions: [], resets: [] })]);
      await owner.query("COMMIT");
    } catch (error) { await owner.query("ROLLBACK"); throw error; }
    await owner.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,valid_until,schema_generation,control_epoch)VALUES($1,repeat('a',64),repeat('b',64),clock_timestamp()+interval'1 hour',$2,repeat('3',64))", [tenant.orgId, expected]);
    await owner.query("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,$4,repeat('c',64),repeat('d',64),clock_timestamp()+interval'1 hour',$5,$6,'96bulls',$7)", [tenant.orgId, tenant.workspaceId, tenant.userId, origin, bucket, region, expected]);
  }
  async function deliver(tenant: typeof tenantA, serial: number, revision = 1) {
    const batch: HistoryExportBatch = {
      schema_version: 1, origin_id: origin, batch_id: `batch-${tenant.orgId}-${serial}-${revision}`, job_id: `job-${serial}-${revision}`,
      generation: revision, destination_id: "fixture", instance_id: "fixture", account_id: await deliveryAccount(tenant), mapping_version: DELIVERY_MAPPING_VERSION,
      records: [{ schema_version: 1, origin_id: origin, revision_id: `revision-${serial}-${revision}`, record_id: `record-${serial}`, revision,
        kind: "session_event", source: "claude", session_id: `session-${serial}`, operation: "upsert",
        payload: { text: `synthetic transcript ${serial} revision ${revision}`, ts_ms: 1761955200000 + revision } }],
    };
    const result = await deliverBatch({ ledger, target: { store, keyRoot: "96bulls" }, acceptanceMode: "buffered" }, tenant, batch);
    yes(result.outcome === "accepted_buffered", "buffered_ack");
    const found = await owner.query("SELECT s.stage_token::text,s.user_id,s.origin_id,stage_sha256,stage_bytes::int FROM sessions.ingest_stage_intents s JOIN sessions.ingest_buffer_outbox o ON o.active_stage_token=s.stage_token WHERE o.org_id=$1 AND o.batch_id=$2", [tenant.orgId, batch.batch_id]);
    return { stageToken: found.rows[0].stage_token, tenant: { orgId: tenant.orgId, workspaceId: tenant.workspaceId }, userId: found.rows[0].user_id, originId: found.rows[0].origin_id, sha256: found.rows[0].stage_sha256, bytes: found.rows[0].stage_bytes } satisfies PublicationCanaryStage;
  }
  await admit(tenantA); await admit(tenantB);
  await initializeFixtureSeatClock(owner as never, expected);
  const inStage = await deliver(tenantA, 1), heldStage = await deliver(tenantB, 2);
  const now = Date.now();
  const plan: PublicationCanaryPlan = {
    version: 1, planId: crypto.randomUUID(), ownerId: "offline-proof-owner", createdAt: new Date(now - 1000).toISOString(), expiresAt: new Date(now + 10 * 60_000).toISOString(),
    tenant: { orgId: tenantA.orgId, workspaceId: tenantA.workspaceId }, target,
    limits: { maxStages: 2, maxRecordsPerStage: 10, maxSessionsPerStage: 4, maxStageMs: 30_000, maxPlanMs: 60_000, maxReads: 128, maxReadBytes: 32 * 1024 * 1024, maxPuts: 128, maxPutBytes: 32 * 1024 * 1024 },
    stages: [inStage, heldStage],
  };
  const planDigest = publicationCanaryPlanDigest(plan);
  await owner.query("CREATE TABLE sessions.proof_publication_canary_bindings(plan_id uuid,owner_id text,plan_digest text,stage_token uuid UNIQUE,evidence_digest text,created_at timestamptz DEFAULT clock_timestamp())");
  const inspectStage = async (token: string) => {
    const found = await owner.query("SELECT stage_token::text,org_id,workspace_id,user_id,origin_id,stage_sha256,stage_bytes::int FROM sessions.ingest_stage_intents WHERE stage_token=$1", [token]);
    yes(found.rows.length === 1, "inspection_missing");
    const row = found.rows[0];
    const value: PublicationCanaryStage = { stageToken: row.stage_token, tenant: { orgId: row.org_id, workspaceId: row.workspace_id }, userId: row.user_id, originId: row.origin_id, sha256: row.stage_sha256, bytes: row.stage_bytes };
    return { value, digest: createHash("sha256").update(JSON.stringify(value)).digest("hex") };
  };
  const authority: PublicationCanaryAuthority = {
    async inspect(input, signal) {
      signal.throwIfAborted();
      const found = await inspectStage(input.stageToken);
      const inside = found.value.tenant.orgId === plan.tenant.orgId && found.value.tenant.workspaceId === plan.tenant.workspaceId;
      return { stage: found.value, evidenceDigest: found.digest, decision: inside ? "eligible" : "hold", ...(!inside ? { holdReason: "outside_canary_tenant" as const } : {}) };
    },
    async claim(input, signal) {
      signal.throwIfAborted();
      try {
        await owner.query("BEGIN");
        const locked = await owner.query("SELECT stage_token::text,org_id,workspace_id,user_id,origin_id,stage_sha256,stage_bytes::int FROM sessions.ingest_stage_intents WHERE stage_token=$1 FOR SHARE", [input.stageToken]);
        yes(locked.rows.length === 1, "claim_missing");
        const row = locked.rows[0], value: PublicationCanaryStage = { stageToken: row.stage_token, tenant: { orgId: row.org_id, workspaceId: row.workspace_id }, userId: row.user_id, originId: row.origin_id, sha256: row.stage_sha256, bytes: row.stage_bytes };
        const digest = createHash("sha256").update(JSON.stringify(value)).digest("hex");
        yes(input.planId === plan.planId && input.ownerId === plan.ownerId && input.planDigest === planDigest && digest === input.evidenceDigest, "claim_binding");
        await owner.query("INSERT INTO sessions.proof_publication_canary_bindings(plan_id,owner_id,plan_digest,stage_token,evidence_digest)VALUES($1,$2,$3,$4,$5)", [input.planId, input.ownerId, input.planDigest, input.stageToken, input.evidenceDigest]);
        await owner.query("SET LOCAL ROLE bulls96_session_ingest");
        const claimed = await owner.query("SELECT sessions.ingest_publication_claim($1::jsonb) result", [JSON.stringify({ stageToken: input.stageToken, keyRoot: target.keyRoot, bucket: target.bucket, region: target.region, generation: target.generation })]);
        await owner.query("RESET ROLE");
        await owner.query("COMMIT");
        return { binding: input, ticket: claimed.rows[0].result };
      } catch (error) { await owner.query("ROLLBACK").catch(() => {}); throw error; }
    },
  };
  outcomes = await openPublicationCanaryOutcomeFile(`${out}/outcomes.ndjson`);
  await check("accepted buffers -> complete inspection -> atomic owner-plan-bound claim -> canonical pointer", async () => {
    const summary = await runPublicationCanary(plan, { authority, rpc, store: () => store, outcomes: outcomes! });
    yes(summary.published === 1 && summary.held === 1 && summary.refused === 0, "canary_summary");
    const pointer = (await owner.query("SELECT * FROM sessions.session_index WHERE org_id=$1 AND deleted_at IS NULL", [tenantA.orgId])).rows[0];
    yes(pointer?.manifest_key.includes("/publications/"), "publication_pointer");
    const held = (await owner.query("SELECT state,active_stage_token IS NOT NULL active FROM sessions.ingest_buffer_outbox WHERE org_id=$1", [tenantB.orgId])).rows[0];
    yes(held.state === "buffered" && held.active, "held_stage_mutated");
    return { planDigest, pointer: pointer.manifest_key, heldStage: heldStage.stageToken };
  });
  const scope = { orgId: tenantA.orgId, workspaceId: tenantA.workspaceId, harness: "claude", sessionId: "session-1" };
  await check("pointer -> directory projector -> replay CAS -> artifact reader -> History adapter", async () => {
    const projected = await projectSessionDirectory({ query: (text: string, args?: unknown[]) => owner.query(text, args) } as never, scope);
    yes(projected.projected, "directory_not_projected");
    const replay = await produceCanonicalReplay(scope, { db: { query: (text: string, args?: unknown[]) => owner.query(text, args) }, store, provider: "s3", bucket, prefix: { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" } });
    yes(replay.published && replay.publication, "replay_not_published");
    const identity = replay.publication.identity;
    const pageDeps = {
      signal: AbortSignal.timeout(5000),
      publication: async () => ({ ...replay.publication!, exists: true, current: true }),
      read: async (key: string) => (await store.getObject(objectKey(key))).body,
    };
    const page = await canonicalArtifactPage(pageDeps, identity, { table: "convergence_events", view: "replay", limit: 10 });
    yes(page.rows.length === 1 && page.rows[0]?.row.content === "synthetic transcript 1 revision 1", "artifact_replay_content");
    const adapterConfig: CanonicalHistoryAdapterConfig = { enabled: true, endpointOrigin: "https://offline.invalid", orgId: identity.orgId, workspaceId: identity.workspaceId, scopes: [{ source: identity.source, sessionId: identity.sessionId }], deadlineMs: 5000, maxResponseBytes: 2 * 1024 * 1024, maxRowsPerPage: 10 };
    const history = await readCanonicalHistoryPage(adapterConfig, { ...identity, bearer: "offline.synthetic.jwt", limit: 10 }, async (request) => {
      const url = new URL(String(request));
      yes(url.pathname.startsWith("/v1/canonical/"), "history_noncanonical_route");
      const served = await canonicalArtifactPage(pageDeps, identity, { table: "convergence_events", view: "replay", limit: Number(url.searchParams.get("limit")), order: "asc" });
      return Response.json({ ...identity, ...served, correlationId: "offline" }, { headers: { "x-relayhistory-workspace-id": identity.workspaceId } });
    });
    yes(history.events.length === 1 && history.events[0]?.content === "synthetic transcript 1 revision 1", "history_adapter_content");
    return { replayGeneration: replay.publication.generation, replayObjects: replay.objects, replayRows: replay.rows };
  });
  await check("privacy transition + GC fences delayed PUT and prevents pointer resurrection", async () => {
    const delayedStage = await deliver(tenantA, 1, 2);
    let release!: () => void, reached!: () => void, lateKey = "";
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const hit = new Promise<void>((resolve) => { reached = resolve; });
    const paused = new Proxy(store, { get(targetStore, property) {
      if (property === "put") return async (key: string, body: Uint8Array, options?: unknown) => {
        if (!lateKey) { lateKey = String(key); reached(); await gate; }
        return targetStore.put(key as never, body, options as never);
      };
      const value = Reflect.get(targetStore, property);
      return typeof value === "function" ? value.bind(targetStore) : value;
    } }) as ObjectStore;
    const pending = publishBufferedStage({ rpc, target, store: () => paused }, delayedStage.stageToken, { enabled: true, maxJobMs: 30_000, heartbeatMs: 100 }).then((value) => ({ value }), (error) => ({ error }));
    await hit;
    yes((await owner.query("SELECT count(*)::int n FROM sessions.ingest_publication_objects WHERE object_key=$1", [lateKey])).rows[0].n === 1, "late_put_unregistered");
    await producer.setSessionUploads({ tenant: tenantA, originId: origin, sessions: [["claude", "session-1"]], enabled: false });
    await producer.setSessionUploads({ tenant: tenantA, originId: origin, sessions: [["claude", "session-1"]], enabled: true });
    const tickets = (await gc.claim(100, AbortSignal.timeout(10_000))).filter((ticket) => ticket.key.startsWith(`96bulls/${tenantA.orgId}/`));
    yes(tickets.some((ticket) => ticket.key === lateKey), "late_put_gc_ticket");
    for (const ticket of tickets) yes(await collectPublicationObject(ticket, { ledger: gc, target, store, eraser }, AbortSignal.timeout(15_000)), "gc_finish");
    release();
    const result = await pending;
    yes("error" in result, "late_publish_succeeded");
    const pointer = (await owner.query("SELECT deleted_at IS NOT NULL erased FROM sessions.session_index WHERE org_id=$1", [tenantA.orgId])).rows[0];
    yes(pointer?.erased, "pointer_resurrected");
    const marker = JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(lateKey)))));
    yes(marker.kind === "upload-generation-redaction", "late_private_bytes_visible");
    return { lateKey, gcTickets: tickets.length };
  });
  report.pass = true;
  report.peakRss = process.resourceUsage().maxRSS * 1024;
} catch (error) {
  report.pass = false;
  report.stage = stage;
  report.error = (error as Error).message;
  report.code = (error as { code?: unknown }).code ?? null;
  report.reason = (error as { reason?: unknown }).reason ?? null;
  process.exitCode = 1;
} finally {
  await outcomes?.close().catch(() => {});
  await pool?.end().catch(() => {});
  await owner.end().catch(() => {});
  await admin.query(`DROP DATABASE IF EXISTS ${database} WITH(FORCE)`).catch(() => {});
  await admin.end().catch(() => {});
  writeFileSync(`${out}/result.json`, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
  console.log(JSON.stringify(report));
}
