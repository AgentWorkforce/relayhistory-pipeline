/** Actual ingest, permanent SQL fence, versioned bytes, search, metadata and read routes.
 * Uses only the disposable loopback MinIO fixture; PGlite applies every real migration. */
import { initScrubEngine } from "@rhc/lib/scrub-engine/index.ts";
import { describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";
import { createS3Store, objectKey, readAll } from "../../shared/vendor.ts";
import { createS3Eraser } from "../../migration/neon-s3-erasure/eraser.ts";
import { applySupabaseSchema, REPO_ROOT } from "../../session-ingest/__tests__/pglite-schema.ts";
import { TENANT as TEST_TENANT, batch, record } from "../../session-ingest/__tests__/support.ts";
import { createIngestApp } from "../../session-ingest/app.ts";
import { sqlLedger, type SqlRun } from "../../session-ingest/ledger-sql.ts";
import { sessionEraseConfig } from "../../session-ingest/session-erase.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { sqlLiveness, sqlOperatorErased, type TenantRunner } from "../../session-read-api/liveness.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { sqlBriefSummary } from "../../session-read-api/brief-summary.ts";
import { createPostgresSearchIndex } from "../../../packages/pipeline/src/search-index/postgres.ts";
import { syntheticDoc } from "../../../packages/pipeline/src/search-index/conformance.ts";
import { postgresSearchState } from "../../session-pipeline/postgres-search-state.ts";
import { sessionEraseWorker, sqlEraseLedger, sqlSessionWriteGuard } from "../runtime.ts";
import { stagedObjectsAbsent } from "../selfhost-runtime.ts";
import { eraseDiscoveredSessionBytes } from "../selfhost-eraser.ts";

const endpoint = process.env.FRONTIER_TEST_S3_ENDPOINT;
const bucket = process.env.FRONTIER_TEST_S3_BUCKET;
const accessKeyId = process.env.FRONTIER_TEST_S3_ACCESS_KEY;
const secretAccessKey = process.env.FRONTIER_TEST_S3_SECRET_KEY;
const enabled = !!(endpoint && bucket && accessKeyId && secretAccessKey);
if (endpoint) {
 const u = new URL(endpoint);
 if (u.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username || u.password || u.pathname !== "/" || u.search || u.hash)
  throw Error("owned_loopback_fixture_required");
}
const secret = "synthetic-erase-fixture-secret-0123456789";

describe.skipIf(!enabled)("operator hard-delete end to end", () => {
 test("real delivery -> 202 -> verified all-store receipt -> 404 exact reads; retry and re-upload cannot restore bytes", async () => {
  await initScrubEngine({ env: { SCRUB_ENGINE: "ts" } });
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const root = `eraseflow${crypto.randomUUID().replaceAll("-", "")}`;
  const storeConfig = { endpoint: endpoint!, bucket: bucket!, region: "us-east-1", credentials: { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey! } };
  const store = createS3Store(storeConfig);
  const eraser = createS3Eraser(storeConfig);
  // Pending desktop uploads must block completion even before canonical admission sees them.
  const pendingUpload = `${root}/.upload-inbox/pending`;
  await store.put(objectKey(pendingUpload), new TextEncoder().encode("fixture staged payload"));
  expect(await stagedObjectsAbsent(eraser,root,TEST_TENANT,async()=>{})).toBe(false);
  await eraser.eraseStep(`${root}/.upload-inbox/`,{scope:root,authorize:async()=>{}});
  expect(await stagedObjectsAbsent(eraser,root,TEST_TENANT,async()=>{})).toBe(true);
  const lines: Record<string, unknown>[] = [];
  let worker: ReturnType<typeof sessionEraseWorker> | undefined;
  let control: any, controlTenant: any, bridge: ReturnType<typeof createServer> | undefined;
  let TENANT = TEST_TENANT;
  let pipelineApp: ReturnType<typeof createIngestApp> | undefined;
  let membersBefore = 0;

  try {
   // Optional cross-repository proof: actual Control HTTP route, auth, store and audit schema.
   // CI remains self-contained when the separately owned checkout is not provided.
   const controlRoot = process.env.OPERATOR_ERASE_CONTROL_CHECKOUT;
   if (controlRoot) {
    const helpers = await import(pathToFileURL(join(controlRoot, "test/flows-helpers.ts")).href);
    bridge = createServer(async (req, res) => {
     try {
      const chunks: Buffer[] = []; for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const request = new Request(`http://127.0.0.1${req.url}`, { method: req.method, headers: req.headers as any,
       ...(req.method === "POST" ? { body: Buffer.concat(chunks) } : {}) });
      const response = pipelineApp ? await pipelineApp.fetch(request) : new Response(null, { status: 503 });
      res.writeHead(response.status, Object.fromEntries(response.headers)); res.end(await response.text());
     } catch { res.writeHead(503); res.end(); }
    });
    await new Promise<void>(resolve => bridge!.listen(0,"127.0.0.1",resolve));
    control = await helpers.startControlPlane({},undefined,{ sessionErasureApi: { url: `http://127.0.0.1:${(bridge.address() as AddressInfo).port}`, secret } });
    controlTenant = await helpers.seedTenant(control.api);
    TENANT = { ...TEST_TENANT, orgId: controlTenant.organizationId, workspaceId: controlTenant.workspaceId };
    membersBefore = (await control.database.query("SELECT 1 FROM organization_memberships WHERE organization_id=$1",[TENANT.orgId])).rows.length;
   }
   const key = { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId: "sess_1" };
   const erasePath = `/internal/v1/session-erase/organizations/${key.orgId}/workspaces/${key.workspaceId}/sources/${key.source}/sessions/${key.sessionId}`;
   const controlPath = `/api/v1/organizations/${key.orgId}/sessions/${key.sessionId}`;
   const controlQuery = `?workspaceId=${key.workspaceId}&source=${key.source}`;
   await applySupabaseSchema(db, { until: null });
   for (const f of readdirSync(join(REPO_ROOT, "src/hoopdb/migrations")).sort()) await db.exec(readFileSync(join(REPO_ROOT, "src/hoopdb/migrations", f), "utf8"));
   await db.exec(readFileSync(join(REPO_ROOT, "supabase/bootstrap/session_read_api_role.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"));
   let chain: Promise<unknown> = Promise.resolve();
   const exclusive = <T>(work: () => Promise<T>): Promise<T> => { const next = chain.then(work); chain = next.catch(() => {}); return next; };
   const run: SqlRun = (text, params) => exclusive(async () => {
    await db.exec("SET ROLE bulls96_session_ingest");
    try { return await db.query(text, params as never) as any; } finally { await db.exec("RESET ROLE"); }
   });
   const tenant = (role?: string): TenantRunner => (org, ws, work) => exclusive(async () => {
    await db.exec(`BEGIN${role ? `;SET LOCAL ROLE ${role}` : ""}`);
    try {
     await db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [org, ws]);
     const value = await work(db as any); await db.exec("COMMIT"); return value;
    } catch (e) { await db.exec("ROLLBACK"); throw e; }
   });
   const ownerTenant = tenant();
   const reader = tenant("bulls96_read_api");
   const index = createPostgresSearchIndex({ client: db as any, tenantRunner: ownerTenant as any });
   await index.ensureSchema();
   const state = postgresSearchState({ reader, backendId: "postgres", fromMs: 0 });
   await state.ensureSchema(db as any);
   // The actual narrow search role gets no transcript content read privilege.
   await db.exec(readFileSync(join(REPO_ROOT, "supabase/bootstrap/session_erase_search_role.sql"), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n"));
   await db.exec("SET ROLE bulls96_session_erase_search");
   await expect(db.query("SELECT content FROM search_index.transcript_events")).rejects.toMatchObject({code:"42501"});
   await expect(db.query("DELETE FROM sessions.session_index")).rejects.toMatchObject({code:"42501"});
   await db.exec("RESET ROLE");
   const eraseIndex = createPostgresSearchIndex({ client: db as any, tenantRunner: tenant("bulls96_session_erase_search") as any });
   const identity = { bucket: bucket!, keyRoot: root, storeRef: "a".repeat(64) };
   const ledger = sqlEraseLedger(run, identity);
   const guard = sqlSessionWriteGuard(run);
   const app = createIngestApp({
    identify: () => ({ ...TENANT, scopes: ["rth:sync", "rth:read"] }),
    deliver: { ledger: sqlLedger(run, { acceptanceMode: "canonical" }), acceptanceMode: "canonical", target: { store, keyRoot: root }, withSessionWrites: guard },
    sessionErase: sessionEraseConfig({ SESSION_ERASE_API_ENABLED: "1", SESSION_ERASE_API_SECRET: secret }), sessionEraseService: ledger,
    log: l => lines.push(l),
   });
   pipelineApp = app;
   const upload = async (id: string, origin = "origin-1", session = key.sessionId) => app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: id, origin_id: origin, session_id: session })], { origin_id: origin, batch_id: id, account_id: await deliveryAccount(TENANT) }) }) });
   const response = await upload("before");
   expect({ status: response.status, body: await response.json(), lines }).toMatchObject({ status: 200 });
   const listed = await store.list(objectKey(`${root}/${key.orgId}`));
   const manifestKey = listed.items.map(i => String(i.key)).find(k => k.endsWith("/manifest.json"))!;
   expect(manifestKey).toBeString();
   const rawKey = listed.items.map(i => String(i.key)).find(k => k.includes("/raw/"))!;
   expect(rawKey).toBeString();
   const machine = /\/m=([^/]+)\//.exec(manifestKey)![1]!;
   // Project the canonical pointer and derived fixtures as the consumer would. Separate SQL tests
   // prove the enforced consumer upsert cannot register a new canonical alias after the fence.
   await db.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id)
    VALUES($1,$2,'claude','sess_1',$3,$4,now()-interval '1 hour',now(),'s3',$5,$6,decode(repeat('ab',32),'hex'),'jsonl',$7)`, [TENANT.orgId, TENANT.workspaceId, machine, TENANT.userId, bucket, manifestKey, crypto.randomUUID()]);
   await db.query(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,title,event_count,projected_at)
    VALUES($1,$2,'claude','sess_1',now(),now(),'private title',1,now())`, [TENANT.orgId, TENANT.workspaceId]);
   await db.query(`INSERT INTO sessions.session_briefs(org_id,workspace_id,harness,session_id,status,revision,title,description,model,generated_at,brief_revision,brief_source_revisions,brief_fingerprint,scrub_ruleset,scrub_version,prompt_version,engine_version)
    VALUES($1,$2,'claude','sess_1','ready',1,'private title','private summary','fixture-model',now(),1,'{}'::jsonb,repeat('a',64),'relayhistory-scrub',2,'session-summary-v2','fixture')`, [TENANT.orgId, TENANT.workspaceId]);
   await db.query("UPDATE sessions.session_briefs SET brief_source_revisions=(SELECT jsonb_object_agg(machine_id,revision) FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2) WHERE org_id=$1 AND workspace_id=$2", [TENANT.orgId,TENANT.workspaceId]);
   await index.upsert([syntheticDoc({ ...key, eventId: "e-1", tsMs: Date.now() - 1000, content: "eraseflowneedle", machineId: machine })]);
   await db.query("UPDATE sessions.session_directory SET event_projection_ready=true,event_projection_version=1,event_projected_at=now(),event_projection_sources=jsonb_build_object('fixture',1) WHERE org_id=$1 AND workspace_id=$2", [key.orgId,key.workspaceId]);
   await ownerTenant(key.orgId, key.workspaceId, tx => state.record(tx as any, { ...key, harness: key.source }, { revision: 1, rows: 1, complete: true, at: new Date(), canonicalSessionId: key.sessionId }));
   const readIndex = createPostgresSearchIndex({ client: db as any, tenantRunner: reader as any });
   const readApi = createReadApi({ verify: async () => ({ ...TENANT, scopes: ["rth:read"] }), adapter: { index: readIndex, state }, liveness: sqlLiveness(reader), operatorErased: sqlOperatorErased(reader),
    directory: sqlDirectory(reader), directoryWorkspaces: "*", briefSummary: sqlBriefSummary(reader), completeSinceMs: 0 });
   const read = (path: string) => readApi.request(path, { headers: { authorization: "Bearer fixture" } });
   const events = `/v1/sessions/sess_1/events?workspace=${TENANT.workspaceId}&source=claude`;
   const summary = `/v1/sessions/sess_1/summary?workspace=${TENANT.workspaceId}&source=claude&kind=short`;
   const beforeEvents = await read(events); expect({ status: beforeEvents.status, body: await beforeEvents.text() }).toMatchObject({ status: 200 });
   expect(await (await read(summary)).json()).toMatchObject({ summary: { status: "ready", summary: "private summary" } });
   expect((await read(`/v1/sessions?workspace=${TENANT.workspaceId}`)).status).toBe(200);
   expect(await (await read(`/v1/events?workspace=${TENANT.workspaceId}&q=eraseflowneedle&limit=50`)).text()).toContain("eraseflowneedle");
   expect((await readAll(await store.get(objectKey(rawKey)))).byteLength).toBeGreaterThan(0);
   const neighbour = rawKey.replace("s=sess_1/", "s=other-session/");
   await store.put(objectKey(neighbour), new TextEncoder().encode("neighbor control"));
   const directEraseRequest = () => app.request(erasePath, { method: "POST", headers: { authorization: `Bearer ${secret}`, "content-type": "application/json" }, body: JSON.stringify({ requestId: "control-request", actor: { kind: "user", id: "operator" } }) });
   const eraseRequest = async () => {
    if (!control) return directEraseRequest();
    const result = await control.api(controlPath+controlQuery,{ method:"DELETE",token:controlTenant.ownerToken });
    return Response.json(result.body,{status:result.status});
   };
   if (control) {
    expect((await control.api(controlPath+controlQuery,{method:"DELETE",token:controlTenant.memberToken})).status).toBe(403);
    expect((await db.query("SELECT 1 FROM sessions.session_erasure_receipts")).rows).toEqual([]);
    expect((await db.query("SELECT 1 FROM sessions.session_erasure_fences")).rows).toEqual([]);
   }
   expect((await eraseRequest()).status).toBe(202);
   expect((await read(events)).status).toBe(404);
   worker = sessionEraseWorker({ ledger, identity, bytes: {
    stagesAbsent: (key, authorize) => stagedObjectsAbsent(eraser, root, key, authorize),
    erase: (k, authorize) => eraseDiscoveredSessionBytes(eraser, k, { keyRoot: root, bucket: bucket!, authorize, log: () => {} }),
   }, search: { erase: (k, sessionIds) => eraseIndex.eraseScope({ ...k, sessionIds }) }, searchLock: async (_w, action) => action(), log: l => lines.push(l) });
   // A deleted current upload still has old payload versions + a delete marker.
   // One scan must surface a distinct refusal rather than retry forever or erase mixed data.
   await store.put(objectKey(pendingUpload),new TextEncoder().encode("mixed staged fixture"));
   await store.delete(objectKey(pendingUpload));
   expect(await worker.scan()).toBe(true);
   expect(await ledger.receipt(key)).toMatchObject({status:"failed",bytesErased:false,
    failure:{step:"stagedObjects",code:"staged_objects_require_drain",retryable:true}});
   if(control) expect((await control.api(controlPath+"/erasure"+controlQuery,{token:controlTenant.ownerToken})).body)
    .toMatchObject({status:"failed",failure:{code:"staged_objects_require_drain"}});
   expect((await readAll(await store.get(objectKey(rawKey)))).byteLength).toBeGreaterThan(0);
   expect(await worker.scan()).toBe(false);
   // Controlled fixture only: a separately authorized operator drain is required in a real store.
   await eraser.eraseStep(`${root}/.upload-inbox/`,{scope:root,authorize:async()=>{}});
   expect((await eraseRequest()).status).toBe(202);
   expect(await worker.scan()).toBe(true);
   const controlReceipt = control ? await control.api(controlPath+"/erasure"+controlQuery,{token:controlTenant.ownerToken}) : null;
   const receiptResponse = controlReceipt ? Response.json(controlReceipt.body,{status:controlReceipt.status}) : await app.request(erasePath, { headers: { authorization: `Bearer ${secret}` } });
   const receipt = await receiptResponse.json() as any;
   expect([receiptResponse.status, receipt.status, receipt.bytesErased, receipt.uploadsFenced]).toEqual([200, "completed", true, true]);
   for (const p of [events, summary, `/v1/sessions/sess_1/metadata?workspace=${TENANT.workspaceId}&source=claude`, `/v1/sessions/sess_1/turns?workspace=${TENANT.workspaceId}&source=claude`]) expect((await read(p)).status).toBe(404);
   expect(await (await read(`/v1/sessions?workspace=${TENANT.workspaceId}`)).json()).toMatchObject({ sessions: [] });
   expect(await (await read(`/v1/events?workspace=${TENANT.workspaceId}&q=eraseflowneedle&limit=50`)).text()).not.toContain("eraseflowneedle");
   for (const k of [rawKey, manifestKey]) await expect(store.get(objectKey(k))).rejects.toMatchObject({ code: "not_found" });
   expect(new TextDecoder().decode(await readAll(await store.get(objectKey(neighbour))))).toBe("neighbor control");
   expect((await eraseRequest()).status).toBe(200);
   const after = await upload("after");
   expect(after.status).toBe(200);
   expect(lines.findLast(l => l.route === "delivery.batches")).toMatchObject({ deleted: 1, manifests: 0 });
   expect((await upload("new-origin", "new-origin")).status).toBe(200);
   expect(lines.findLast(l => l.route === "delivery.batches")).toMatchObject({ deleted: 1, manifests: 0 });
   // A new canonical spelling that encodes to the erased physical ID must fail
   // before object I/O, even though claim's logical key differs.
   expect((await upload("layout-collision","alias-origin","sess:1")).status).toBe(409);
   expect((await ledger.receipt(key))?.status).toBe("completed");
   const inventory = await eraser.verifyErased(manifestKey.slice(0, manifestKey.lastIndexOf("/") + 1), { authorize: async () => {} });
   expect(inventory.status).toBe("erased");
   expect((await db.query("SELECT 1 FROM sessions.session_index WHERE session_id='sess_1'")).rows).toEqual([]);
   expect((await db.query("SELECT 1 FROM search_index.transcript_events")).rows).toEqual([]);
   expect(JSON.stringify(lines)).not.toContain("private summary");
   expect(JSON.stringify(lines)).not.toContain(secret);
   if (control) {
    expect(receipt.audit.action).toBe("session.hard_deleted");
    await control.api(controlPath+"/erasure"+controlQuery,{token:controlTenant.ownerToken});
    const audits = await control.database.query("SELECT action FROM admin_audit_events WHERE organization_id=$1 AND action='session.hard_deleted'",[TENANT.orgId]);
    expect(audits.rows).toHaveLength(1);
    expect((await control.database.query("SELECT 1 FROM organization_memberships WHERE organization_id=$1",[TENANT.orgId])).rows).toHaveLength(membersBefore);
   }
  } finally { if(control) await control.stop().catch(async (error: any) => {
    // Node's helper closes sockets first; Bun may report its subsequent close as already stopped.
    if(error.code!=="ERR_SERVER_NOT_RUNNING") throw error;
    await control.database.close();
   }); if(bridge) await new Promise<void>(resolve=>{bridge!.closeAllConnections();bridge!.close(()=>resolve());}); await worker?.stop(); await eraser.eraseStep(`${root}/`, { scope: root, authorize: async () => {} }); await db.close(); }
 }, 120_000);
});
