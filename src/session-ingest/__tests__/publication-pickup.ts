/** Disposable worker and Reader check for a committed, generation-qualified manifest. */
import { expect } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectStoreManifestReader, type ObjectStore, type StageMessage } from "../../shared/vendor.ts";
import { pollPostgresStage, postgresPipelineQueue, postgresStageConsumer } from "@rhc/lib/pipeline/queue.ts";
import { objectLandedHandler, sessionIndexedHandler } from "@rhc/lib/pipeline/stages.ts";
import { InMemoryPipelineEventSink } from "@rhc/lib/pipeline/events.ts";
import { InMemoryDeadLetterStore, type StageQueueDeps } from "@rhc/lib/pipeline/dlq.ts";
import { hoopDbSessionIndex } from "../../hoopdb/index.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { TENANT_SETTINGS_SQL, sqlLiveness, type TenantRunner } from "../../session-read-api/liveness.ts";
import { composeSearchService, createPostgresSearchComposition } from "../../shared/search-composition.ts";
import { postgresSearchState } from "../../session-pipeline/postgres-search-state.ts";
import { reconcileStaleProjections } from "../../session-pipeline/consumer-service.ts";
import { processBatch } from "../../session-pipeline/consumer.ts";
import { indexSearchScope } from "../../session-pipeline/search-index.ts";
import { AuthError } from "../../session-read-api/app.ts";
import type { Transact } from "../../shared/db.ts";

export async function checkPublicationPickup(pg:PGlite, store:ObjectStore, key:string, generation:string, readerBootstrap:string) {
  const rows = (await pg.query<{message:StageMessage}>("SELECT message FROM sessions.pipeline_jobs WHERE stage='object-landed' AND message->'payload'->>'key'=$1",[key])).rows;
  expect(rows).toHaveLength(1);
  const job=rows[0]!.message;
  const transact:Transact = work=>pg.transaction(tx=>work(tx));
  const reader=objectStoreManifestReader(store,{provider:"s3",bucket:(job.payload as {bucket:string}).bucket});
  const queue=postgresPipelineQueue(pg);
  const index=hoopDbSessionIndex({db:pg as never,reader,queue,current:()=>undefined,publication:{schemaGeneration:generation,transact}});
  const deps:StageQueueDeps={sink:new InMemoryPipelineEventSink(),deadLetters:new InMemoryDeadLetterStore(),handlers:{
    "object-landed":objectLandedHandler({senders:{send:(_stage,msg)=>queue.send(msg)},quarantine:async()=>{throw Error("unexpected quarantine");},transformEnabled:false}),
    "session-indexed":sessionIndexedHandler(index),
  }};
  // Other scenarios may already have committed jobs. Give this fixture job the earliest timestamp.
  await pg.query("UPDATE sessions.pipeline_jobs SET visible_at='2000-01-01' WHERE work_key=$1 AND stage='object-landed'",[job.workKey]);
  const options={stage:"object-landed" as const,sql:pg,deps,batchSize:1,leaseSeconds:30};
  const crashed=postgresStageConsumer(options);
  expect(await crashed.source.claim(1)).toHaveLength(1);
  expect((await pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE work_key=$1 AND lease_id IS NOT NULL",[job.workKey])).rows).toHaveLength(1);
  await pg.query("UPDATE sessions.pipeline_jobs SET visible_at='2000-01-01' WHERE work_key=$1",[job.workKey]);
  expect(await pollPostgresStage(options)).toMatchObject({claimed:1,acked:1,retried:0});
  await pg.query("UPDATE sessions.pipeline_jobs SET visible_at='2000-01-01' WHERE work_key=$1 AND stage='session-indexed'",[job.workKey]);
  // With the same committed message already queued, a retirement ordered before
  // the worker's final lock must prevent directory projection and fanout.
  const indexed = (await pg.query<{message:StageMessage}>("SELECT message FROM sessions.pipeline_jobs WHERE work_key=$1 AND stage='session-indexed'",[job.workKey])).rows[0]!.message;
  const rollback = Error("rollback retirement fixture");
  await expect(pg.transaction(async tx=>{
    await tx.query("UPDATE sessions.ingest_publication_generations SET state='retired',retired_at=clock_timestamp() WHERE generation=(SELECT generation FROM sessions.ingest_publication_heads WHERE current_key=$1)",[key]);
    const fenced=hoopDbSessionIndex({db:tx as never,reader,queue,current:()=>indexed,publication:{schemaGeneration:generation,transact:work=>work(tx)}});
    expect(await sessionIndexedHandler(fenced)(indexed,{} as never)).toEqual({status:"skipped",reason:"publication_retired"});
    expect((await tx.query("SELECT 1 FROM sessions.pipeline_jobs WHERE work_key=$1 AND stage='summary-short'",[job.workKey])).rows).toHaveLength(0);
    throw rollback;
  })).rejects.toBe(rollback);
  expect(await pollPostgresStage({...options,stage:"session-indexed"})).toMatchObject({claimed:1,acked:1,retried:0});
  const scope=(await pg.query<{orgId:string;workspaceId:string;sessionId:string;harness:string}>(`SELECT org_id AS "orgId",workspace_id AS "workspaceId",session_id AS "sessionId",harness FROM sessions.session_index WHERE manifest_key=$1`,[key])).rows[0]!;
  expect((await pg.query("SELECT 1 FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND session_id=$3",[scope.orgId,scope.workspaceId,scope.sessionId])).rows).toHaveLength(1);

  const search = new PGlite({extensions:{pg_trgm}});
  try {
    await search.exec("CREATE EXTENSION pg_trgm");
    await pg.exec(readerBootstrap);
    const tenant:TenantRunner=(org,ws,work)=>pg.transaction(async tx=>{
      await tx.exec("SET LOCAL ROLE bulls96_read_api");
      await tx.query(TENANT_SETTINGS_SQL,[org,ws]); return work(tx);
    });
    const searchTenant:TenantRunner=(org,ws,work)=>search.transaction(async tx=>{
      await tx.query(TENANT_SETTINGS_SQL,[org,ws]); return work(tx);
    });
    const backendId="publication-peer-proof";
    const composition=createPostgresSearchComposition({backendId,fromMs:0});
    const writer=composeSearchService(composition,{env:{SEARCH_INDEX_BACKEND:"postgres"},sql:search},"writer",searchTenant);
    await writer.index.ensureSchema();
    await postgresSearchState({backendId,fromMs:0}).ensureSchema(pg);
    const state=postgresSearchState({backendId,fromMs:0,reader:tenant});
    const searchDeps={transact,reader,store,adapter:writer,prefixFor:(org:string)=>"96bulls/"+org};
    const collected:string[]=[];
    await reconcileStaleProjections(pg,(t,after)=>state.partials(t,{limit:100,after}),"96bulls",async body=>{
      if (body.includes(key)) collected.push(body);
    });
    expect(collected).toHaveLength(1);
    const pointer=new Proxy({reader},{get(target,prop) {
      if(prop==="reader")return target.reader;
      throw Error("managed reconcile hint attempted pointer work: "+String(prop));
    }});
    expect(await processBatch([{messageId:"freshness",body:collected[0]!}],{pointer:pointer as never,search:searchDeps}))
      .toEqual({batchItemFailures:[]});
    // The local service's freshness message follows the same managed-hint
    // path as an S3 event. It acknowledged the hint without projecting search.
    const afterWake=(await state.partials(scope,{limit:10})).sessions;
    // Opt into the rollout acceptance condition to reproduce the missing trigger.
    if (process.env.PUBLICATION_REQUIRE_AUTOMATIC_SEARCH === "1") expect(afterWake).not.toContain(scope.sessionId);
    else expect(afterWake).toContain(scope.sessionId);
    // Explicit one-scope qualification of the writer, independent of its trigger.
    expect(await indexSearchScope(scope,searchDeps)).toMatchObject({complete:true});
    expect((await state.partials(scope,{limit:10})).sessions).not.toContain(scope.sessionId);
    await search.exec("CREATE ROLE bulls96_read_api NOLOGIN; GRANT USAGE ON SCHEMA search_index TO bulls96_read_api; GRANT SELECT ON search_index.transcript_events TO bulls96_read_api");
    const peerSearch:TenantRunner=(org,ws,work)=>search.transaction(async tx=>{
      await tx.exec("SET LOCAL ROLE bulls96_read_api");
      await tx.query(TENANT_SETTINGS_SQL,[org,ws]); return work(tx);
    });
    const peerIndex=composeSearchService(composition,{env:{SEARCH_INDEX_BACKEND:"postgres"},sql:search},"reader",peerSearch);
    const adapter={index:peerIndex.index,state:postgresSearchState({backendId,fromMs:0,reader:tenant})};
    const app=createReadApi({verify:async token=>{
      if(token!=="fixture-peer")throw new AuthError(401,"invalid_token","fixture");
      return {orgId:scope.orgId,workspaceId:scope.workspaceId,userId:"peer-fixture",scopes:["rth:read"]};
    },adapter,directory:sqlDirectory(tenant),directoryWorkspaces:new Set([scope.workspaceId]),liveness:sqlLiveness(tenant),completeSinceMs:0});
    const paths=["/v1/sessions","/v1/sessions/"+scope.sessionId+"/events","/v1/events"];
    for(const path of paths){
      const url=(ws:string)=>path+"?workspace="+ws+(path==="/v1/events"?"&q=needle&limit=20":"");
      const get=(ws:string,token:string)=>app.request(url(ws),{headers:{authorization:"Bearer "+token}});
      const visible=await get(scope.workspaceId,"fixture-peer");
      const body=await visible.json() as {sessions?:unknown[];events?:unknown[]};
      expect({path,status:visible.status,body}).toMatchObject({path,status:200});
      expect(body.sessions??body.events).toEqual(expect.arrayContaining([expect.objectContaining({sessionId:scope.sessionId})]));
      expect((await get("other-workspace","fixture-peer")).status).toBe(403);
      expect((await get(scope.workspaceId,"invalid")).status).toBe(401);
    }
  } finally {await search.close();}
}
