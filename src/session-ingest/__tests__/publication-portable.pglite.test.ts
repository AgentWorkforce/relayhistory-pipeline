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
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { createS3Store, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { SCRUB_RULESET_VERSION } from "../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import { parseSessionKey, rawChunkKey } from "../../shared/vendor.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
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
const asRole = async (role: "bulls96_session_ingest" | "bulls96_publication_scoped", sql: string, args: unknown[] = []) => {
  await db.exec("BEGIN; SET LOCAL ROLE "+role);
  try { const r = await db.query<any>(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
};
const asIngest = (sql: string,args: unknown[] = []) => asRole("bulls96_session_ingest",sql,args);
const asPublisher = (sql: string,args: unknown[] = []) => asRole("bulls96_publication_scoped",sql,args);
async function installScopedProposal() {
  if ((await owner("SELECT to_regnamespace('publication_private') AS installed")).rows[0].installed) return;
  await db.exec("CREATE ROLE bulls96_publication_scoped NOLOGIN; CREATE ROLE bulls96_publication_recovery NOLOGIN; GRANT USAGE ON SCHEMA sessions TO bulls96_publication_scoped");
  await db.exec(read("supabase/proposals/continuous-publication/062280-scoped-kernels.up.sql"));
}
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  // Single-sourced bootstrap chain: the grant migration FAILs closed without the bare consumer grantee.
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
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
  // Pause with accepted work already waiting. A fresh disabled runtime must
  // leave even that eligible stage unclaimed, without touching the store.
  const paused = { ...ENV, INGEST_PUBLISH_ENABLED: "false" };
  expect(await runPortablePublicationRuntime(paused, "publish", overrides())).toEqual({ enabled: false });
  expect(s3.requests.length).toBe(before);
  expect((await owner("SELECT count(*)::int AS n FROM sessions.ingest_publication_attempts")).rows[0].n).toBe(0);
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
  const paused = { ...ENV, INGEST_PUBLISH_ENABLED: "false" };
  const before = s3.requests.length;
  expect(await runPortablePublicationRuntime(paused, "publish", overrides())).toEqual({ enabled: false });
  expect(s3.requests.length).toBe(before);
  let eraserConfig: any; const erased: string[] = [];
  const result = await runPortablePublicationRuntime(paused, "gc", overrides({ eraser: (c: any) => { eraserConfig = c; return { eraseObjectExcept: async (key: string) => { erased.push(key); return { deleted: 0 }; } }; } }));
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

test("disposable scoped v1 binds candidates, claim and final commit to one admitted stage and live authority", async () => {
  const { scopedPublicationCycle } = await import("../publication-scoped-rpc.ts");
  const target = {bucket:BUCKET,region:REGION,keyRoot:"96bulls",generation:GEN,provider:"s3" as const};
  const ledger = orderedFixtureLedger(sqlLedger(asIngest, { acceptanceMode:"buffered", expectedGeneration:GEN }),{ query:owner });
  const submit=async(id:string)=>{
    const payload=await batch([record({record_id:id,session_id:id,payload:{text:"scoped offline fixture",ts_ms:1761955200000}})],{batch_id:id});
    const accepted=await deliverBatch({ledger,target:{store,keyRoot:"96bulls"},acceptanceMode:"buffered"},TENANT,payload);
    expect(accepted.outcome).toBe("accepted_buffered");
    return (await owner("SELECT active_stage_token token FROM sessions.ingest_buffer_outbox WHERE batch_id=$1",[payload.batch_id])).rows[0].token as string;
  };
  const admitted=await submit("scoped-once"), other=await submit("not-admitted");
  await installScopedProposal();
  const binding={planSha256:"c".repeat(64),admissionId:"77777777-1111-2222-3333-444444444444",frontierSha256:"e".repeat(64),
    tenant:{...TENANT,originId:"origin-1"},target};
  await owner(`INSERT INTO publication_private.admission(id,binding,stage,frontier_image,admission_image,stage_sha,enabled,valid_until)
    SELECT $1::uuid,$2::jsonb,s.stage_token,to_jsonb(f),to_jsonb(d),s.stage_sha256,true,clock_timestamp()+interval '5 minutes' FROM sessions.ingest_stage_intents s
    JOIN sessions.ingest_publication_frontiers f USING(org_id,workspace_id,user_id,origin_id)
    JOIN sessions.ingest_buffer_admission d USING(org_id) WHERE s.stage_token=$3::uuid`,[binding.admissionId,JSON.stringify(binding),admitted]);
  const scopeArgs={generation:GEN,keyRoot:"96bulls",bucket:BUCKET,region:REGION,binding};
  // A buffered receipt alone cannot authorize an image. Remove the first-ACK
  // seat in a disposable transaction before the real candidate can be claimed.
  await db.exec("BEGIN");
  try {
    const removed=await owner("DELETE FROM sessions.ingest_receipt_ord_seats WHERE claim_token=(SELECT claim_token FROM sessions.ingest_stage_intents WHERE stage_token=$1) RETURNING ord",[admitted]);
    expect(removed.rows).toHaveLength(1);
    await db.exec("SET LOCAL ROLE bulls96_publication_scoped");
    await expect(owner("SELECT sessions.ingest_publication_scoped_claim_v1($1::jsonb)",[JSON.stringify({...scopeArgs,stageToken:admitted})]))
      .rejects.toMatchObject({code:"55000"});
  } finally { await db.exec("ROLLBACK"); }
  expect((await owner("SELECT 1 FROM sessions.ingest_publication_attempts WHERE stage_token=$1",[admitted])).rows).toHaveLength(0);
  const call=async(name:string,p:unknown)=>(await asPublisher("SELECT sessions.ingest_publication_"+name+"($1::jsonb) AS result",[JSON.stringify(p)])).rows[0].result;
  expect(await call("scoped_candidates_v1",{...scopeArgs,limit:1})).toEqual({stageTokens:[admitted]});
  await expect(call("scoped_claim_v1",{...scopeArgs,stageToken:other})).rejects.toThrow();
  await expect(call("scoped_candidates_v1",{...scopeArgs,limit:1,binding:{...binding,planSha256:"b".repeat(64)}})).rejects.toThrow();
  await expect(call("claim",{stageToken:other,generation:GEN,keyRoot:"96bulls",bucket:BUCKET,region:REGION})).rejects.toThrow("permission denied");

  // Forward the host adapter's parameter and function selection unchanged to SQL.
  const calls:string[]=[];
  const transport={queryBounded:async(text:string,args:unknown[])=>{
    const p=JSON.parse(String(args[0]));
    const method=text.match(/^SELECT sessions\.ingest_publication_([a-z_0-9]+)\(\$1::jsonb\) AS result$/)![1]!;
    calls.push(method);
    if(method==="scoped_publish_v1"){
      const final=p;
      // A lost claim response must not allocate another attempt. Even when its
      // lease has expired, this one-shot admission is held for audited recovery.
      await db.exec("BEGIN");
      try {
        await owner("UPDATE sessions.ingest_publication_attempts SET lease_until=clock_timestamp()-interval '1s' WHERE attempt_token=$1",[p.attemptToken]);
        await db.exec("SET LOCAL ROLE bulls96_publication_scoped");
        await expect(owner("SELECT sessions.ingest_publication_scoped_claim_v1($1::jsonb)",[JSON.stringify({...scopeArgs,stageToken:admitted})])).rejects.toThrow("admission busy");
      } finally { await db.exec("ROLLBACK"); }
      // Model a real source authority/retention invalidation just before commit.
      // Roll the complete scenario back to exercise the independent success case.
      await db.exec("BEGIN");
      try {
        await owner("UPDATE sessions.ingest_buffer_admission SET privacy_baseline_sha256=repeat('f',64) WHERE org_id=$1",[TENANT.orgId]);
        await db.exec("SET LOCAL ROLE bulls96_publication_scoped");
        await expect(owner("SELECT sessions.ingest_publication_scoped_publish_v1($1::jsonb)",[JSON.stringify(final)])).rejects.toThrow("authority stale");
      } finally { await db.exec("ROLLBACK"); }
      // Two transactions interleave after the live claim and before the final write.
      await owner("UPDATE publication_private.admission SET enabled=false WHERE id=$1",[binding.admissionId]);
      await expect(call("scoped_publish_v1",final)).rejects.toThrow("authority stale");
      expect((await owner("SELECT state FROM sessions.ingest_publication_attempts WHERE attempt_token=$1",[p.attemptToken])).rows[0].state).not.toBe("published");
      await owner("UPDATE publication_private.admission SET enabled=true WHERE id=$1",[binding.admissionId]);
      await expect(call("scoped_publish_v1",{...final,stageToken:other})).rejects.toThrow();
      await expect(call("scoped_publish_v1",{...final,leaseToken:binding.admissionId})).rejects.toThrow();
      const result=await call("scoped_publish_v1",final);
      expect(await call("scoped_publish_v1",final)).toMatchObject({published:true,replay:true});
      return {rows:[{result}]};
    }
    return {rows:[{result:await call(method,p)}]};
  }} as never;
  const deps={target,store:()=>store};
  expect(await scopedPublicationCycle(transport,binding,deps)).toMatchObject({enabled:false});
  expect(calls).toEqual([]);
  expect(await scopedPublicationCycle(transport,binding,deps,{enabled:true})).toMatchObject({examined:1,published:1});
  expect(calls).toContain("scoped_claim_v1");
  expect(calls).toContain("scoped_publish_v1");
  expect(new Set(calls)).toEqual(new Set(["scoped_candidates_v1","scoped_claim_v1","scoped_heartbeat_v1","scoped_prepare_v1","scoped_register_objects_v1","scoped_publish_v1"]));
  const activeCalls=calls.length, objectRequests=s3.requests.length;
  expect(await scopedPublicationCycle(transport,binding,deps,{enabled:false})).toMatchObject({enabled:false});
  expect(calls).toHaveLength(activeCalls);
  expect(s3.requests).toHaveLength(objectRequests);
  expect(await call("scoped_candidates_v1",{...scopeArgs,limit:1})).toEqual({stageTokens:[]});
  expect((await owner("SELECT count(*)::int n FROM sessions.ingest_publication_attempts WHERE stage_token=$1",[other])).rows[0].n).toBe(0);
});

test("generation PUT before scoped finalization cannot promote a committed or invalidated stage", async () => {
  const { processBatch } = await import("../../session-pipeline/consumer.ts");
  const { scopedPublicationCycle } = await import("../publication-scoped-rpc.ts");
  const { relayhistoryNewestAcrossOrigins } = await import("../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts");
  const target = { bucket: BUCKET, region: REGION, keyRoot: "96bulls", generation: GEN, provider: "s3" as const };
  const cohort={...TENANT,workspaceId:"ws-pickup"};
  await owner("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour',$5,$6,'96bulls',$7)", [cohort.orgId,cohort.workspaceId,cohort.userId,"b".repeat(64),BUCKET,REGION,GEN]);
  const ledger = orderedFixtureLedger(sqlLedger(asIngest, { acceptanceMode: "buffered", expectedGeneration: GEN }), { query: owner });
  await installScopedProposal();

  for (const commit of [false, true]) {
    const id = commit ? "early-final" : "early-abort";
    const payload = await batch([record({ record_id: id, session_id: id, payload: {text:"needle publication proof", ts_ms:1761955200000} })], { batch_id: id,account_id:await deliveryAccount(cohort) });
    expect((await deliverBatch({ ledger, target: { store, keyRoot: "96bulls" }, acceptanceMode: "buffered" }, cohort, payload)).outcome).toBe("accepted_buffered");
    const binding = { planSha256: "c".repeat(64), admissionId: crypto.randomUUID(), frontierSha256: "e".repeat(64),
      tenant: { ...cohort, originId: "origin-1" }, target };
    await owner(`INSERT INTO publication_private.admission(id,binding,stage,frontier_image,admission_image,stage_sha,enabled,valid_until)
      SELECT $1::uuid,$2::jsonb,s.stage_token,to_jsonb(f),to_jsonb(d),s.stage_sha256,true,clock_timestamp()+interval '5 minutes' FROM sessions.ingest_stage_intents s
      JOIN sessions.ingest_publication_frontiers f USING(org_id,workspace_id,user_id,origin_id)
      JOIN sessions.ingest_buffer_admission d USING(org_id) WHERE s.batch_id=$3`, [binding.admissionId, JSON.stringify(binding), id]);

    const counts = async () => (await owner(`SELECT
      (SELECT count(*)::int FROM sessions.session_index WHERE manifest_key=$1) AS pointers,
      (SELECT count(*)::int FROM sessions.ingest_record_revisions WHERE record_id=$2) AS revisions,
      (SELECT count(*)::int FROM sessions.pipeline_jobs WHERE stage='object-landed' AND work_key LIKE $1 || '@%') AS jobs`, [key, id])).rows[0];
    let key = "";
    const before = s3.requests.length;
    let notifications = 0;
    async function deliverPut() {
      // processBatch uses only the reader provider to decode the S3 envelope.
      // Any manifest read, DB lookup, search or fanout caused by this event fails the test.
      const pointer = new Proxy({ reader: { provider: "s3" } }, {
        get(target, prop) { if (prop === "reader") return target.reader; throw Error("early event accessed " + String(prop)); },
      });
      const result = await processBatch([{messageId: id, body: JSON.stringify({Records:[{
        eventSource:"aws:s3", eventName:"ObjectCreated:Put", eventTime:"2026-11-01T00:00:00Z",
        s3:{bucket:{name:BUCKET},object:{key:encodeURIComponent(key),eTag:"fixture-etag",sequencer:"01"}},
      }]})}], {pointer: pointer as never});
      expect(result.batchItemFailures).toEqual([]);
      notifications++;
    }
    const transport = { queryBounded: async (sql: string, args: unknown[]) => {
      if (sql.includes("ingest_publication_scoped_publish_v1(")) {
        const manifests = s3.requests.slice(before).filter(r => r.method === "PUT" && r.key.includes("/upload-generations/") && r.key.endsWith("/manifest.json"));
        expect(manifests).toHaveLength(1);
        key = manifests[0]!.key;
        expect(s3.objects.has(key)).toBe(true);
        await deliverPut();
        expect(await counts()).toEqual({ pointers: 0, revisions: 0, jobs: 0 });
        if (!commit) await owner("UPDATE publication_private.admission SET enabled=false WHERE id=$1", [binding.admissionId]);
      }
      return asPublisher(sql, args);
    }};
    const cycle = scopedPublicationCycle(transport as never, binding, {target, store: () => store}, {enabled: true});
    if (commit) expect(await cycle).toMatchObject({published:1});
    else await expect(cycle).rejects.toThrow("P0001");
    expect(notifications).toBe(1);
    expect((await owner(`SELECT a.state FROM sessions.ingest_publication_attempts a
      JOIN publication_private.admission f ON f.stage=a.stage_token WHERE f.id=$1`, [binding.admissionId])).rows)
      .toEqual([{state: commit ? "published" : "prepared"}]);
    const expected = commit ? 1 : 0;
    await deliverPut(); // duplicate may arrive after the outcome
    expect(await counts()).toEqual({pointers: expected, revisions: expected, jobs: expected});
    if (commit) await (await import("./publication-pickup.ts")).checkPublicationPickup(db,store,key,GEN,read("supabase/bootstrap/session_read_api_role.sql"));
  }

  // Separate projection boundary: a queue hint cannot supply an acceptance seat
  // for two contending records. This is the actual fold used by the Reader.
  expect(() => relayhistoryNewestAcrossOrigins([
    {kind:"history",record_id:"contender",accepted_seq:"1"},
    {kind:"history",record_id:"contender"},
  ])).toThrow(expect.objectContaining({code:"relayhistory_acceptance_missing"}));
});
