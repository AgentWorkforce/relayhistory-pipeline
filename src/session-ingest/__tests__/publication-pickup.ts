/** Disposable worker and Reader check for a committed, generation-qualified manifest. */
import { expect } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectStoreManifestReader, type ObjectStore, type StageMessage } from "../../shared/vendor.ts";
import { MemoryPipelineQueue, pollPostgresStage, postgresPipelineQueue, postgresStageConsumer } from "@rhc/lib/pipeline/queue.ts";
import { objectLandedHandler, sessionIndexedHandler } from "@rhc/lib/pipeline/stages.ts";
import { InMemoryPipelineEventSink } from "@rhc/lib/pipeline/events.ts";
import { isStageMessage, InMemoryDeadLetterStore, type StageQueueDeps } from "@rhc/lib/pipeline/dlq.ts";
import { hoopDbSessionIndex } from "../../hoopdb/index.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { TENANT_SETTINGS_SQL, sqlLiveness, type TenantRunner } from "../../session-read-api/liveness.ts";
import { composeSearchService, createPostgresSearchComposition } from "../../shared/search-composition.ts";
import { postgresSearchState } from "../../session-pipeline/postgres-search-state.ts";
import { reconcileStaleProjections } from "../../session-pipeline/consumer-service.ts";
import { processBatch } from "../../session-pipeline/consumer.ts";
import { indexSearchScope } from "../../session-pipeline/search-index.ts";
import { drainSearchJobs } from "../../session-pipeline/lambda.ts";
import { AuthError } from "../../session-read-api/app.ts";
import type { Transact } from "../../shared/db.ts";

export async function checkPublicationPickup(pg:PGlite, store:ObjectStore, key:string, generation:string, readerBootstrap:string) {
  const rows = (await pg.query<{message:StageMessage}>("SELECT message FROM sessions.pipeline_jobs WHERE stage='object-landed' AND message->'payload'->>'key'=$1",[key])).rows;
  expect(rows).toHaveLength(1);
  const job=rows[0]!.message;
  const transact:Transact = work=>pg.transaction(tx=>work(tx));
  const reader=objectStoreManifestReader(store,{provider:"s3",bucket:(job.payload as {bucket:string}).bucket});
  const queue=postgresPipelineQueue(pg);
  const index=hoopDbSessionIndex({db:pg as never,reader,queue,current:()=>undefined,publication:{schemaGeneration:generation,enqueueSearchWake:true,transact}});
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
    const fenced=hoopDbSessionIndex({db:tx as never,reader,queue,current:()=>indexed,publication:{schemaGeneration:generation,enqueueSearchWake:true,transact:work=>work(tx)}});
    expect(await sessionIndexedHandler(fenced)(indexed,{} as never)).toEqual({status:"skipped",reason:"publication_retired"});
    expect((await tx.query("SELECT 1 FROM sessions.pipeline_jobs WHERE work_key=$1 AND stage='summary-short'",[job.workKey])).rows).toHaveLength(0);
    throw rollback;
  })).rejects.toBe(rollback);
  const wakes=()=>pg.query<{work_key:string;message:StageMessage<{epoch:number;clock:{source:"publish";at:string}}>}>(
    "SELECT work_key,message FROM sessions.pipeline_jobs WHERE stage='search-index' AND org_id=$1 AND workspace_id=$2",[job.ctx.orgId,job.ctx.workspaceId]);
  expect((await wakes()).rows).toHaveLength(0);
  const {rows:[current]}=await pg.query<{evidence:Record<string,string>}>(`SELECT jsonb_build_object(
    'orgId',org_id,'workspaceId',workspace_id,'harness',physical_source,'sessionId',physical_session,
    'machineId',machine_id,'manifestKey',current_key,'revision',pointer_revision::text,'schemaGeneration',$2::text) AS evidence
    FROM sessions.ingest_publication_heads WHERE current_key=$1`,[key,generation]);
  expect(current).toBeDefined();
  for (const [evidence,trace] of [
    [current!.evidence,"00000000-0000-0000-0000-000000000000"],
    [{...current!.evidence,orgId:""},job.ctx.traceId],
    [{...current!.evidence,workspaceId:""},job.ctx.traceId],
  ] as const) {
    await expect(pg.query("SELECT sessions.enqueue_managed_search_wake_v1($1,$2)",[evidence,trace]))
      .rejects.toMatchObject({code:"22023",message:"search wake: invalid trace or tenant context"});
  }
  expect((await pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage='search-index'")).rows).toHaveLength(0);
  // Reject wake persistence, and prove projection, queue fanout and wake roll
  // back together. Remove the fixture constraint and retry the same job.
  await pg.exec("ALTER TABLE sessions.pipeline_jobs ADD CONSTRAINT fixture_reject_search CHECK(stage<>'search-index') NOT VALID");
  expect(await pollPostgresStage({...options,stage:"session-indexed"})).toMatchObject({claimed:1,acked:0,retried:1});
  expect((await pg.query("SELECT 1 FROM sessions.session_directory WHERE workspace_id=$1 AND session_id=$2",[job.ctx.workspaceId,job.ctx.sessionId])).rows).toHaveLength(0);
  expect((await wakes()).rows).toHaveLength(0);
  await pg.exec("ALTER TABLE sessions.pipeline_jobs DROP CONSTRAINT fixture_reject_search");
  await pg.query("UPDATE sessions.pipeline_jobs SET visible_at='2000-01-01' WHERE stage='session-indexed' AND work_key=$1",[job.workKey]);
  expect(await pollPostgresStage({...options,stage:"session-indexed"})).toMatchObject({claimed:1,acked:1,retried:0});
  const scope=(await pg.query<{orgId:string;workspaceId:string;sessionId:string;harness:string}>(`SELECT org_id AS "orgId",workspace_id AS "workspaceId",session_id AS "sessionId",harness FROM sessions.session_index WHERE manifest_key=$1`,[key])).rows[0]!;
  const {rows:[head]}=await pg.query<{generation:string;revision:number;epoch:number;physical:string;canonical:string}>(`SELECT h.generation,h.pointer_revision::int AS revision,d.event_projection_epoch::int AS epoch,h.physical_session AS physical,d.session_id AS canonical
    FROM sessions.ingest_publication_heads h JOIN sessions.session_directory d ON (d.org_id,d.workspace_id,d.harness,d.session_id)=(h.org_id,h.workspace_id,h.physical_source,h.session_id) WHERE h.current_key=$1`,[key]);
  expect(head).toBeDefined();
  expect(head!.physical).not.toBe(head!.canonical);
  const wk=["search",scope.orgId,scope.workspaceId,scope.harness,head!.canonical].join("\u001f");
  const beforeReplay=(await wakes()).rows;
  expect(beforeReplay).toHaveLength(1);
  const emitted=beforeReplay[0]!.message;
  expect(isStageMessage(emitted)).toBe(true);
  await new MemoryPipelineQueue().send(emitted);
  expect(emitted).toMatchObject({v:1,stage:"search-index",workKey:wk,ctx:{traceId:job.ctx.traceId,orgId:scope.orgId,
    workspaceId:scope.workspaceId,harness:scope.harness,sessionId:head!.canonical},revision:head!.revision,
    payload:{clock:{source:"publish"},epoch:head!.epoch,generation:head!.generation,schema_generation:generation}});
  const clock=await pg.query("SELECT a.published_at=$1::text::timestamptz AS exact FROM sessions.ingest_publication_attempts a JOIN sessions.ingest_publication_images i USING(attempt_token) WHERE i.manifest_key=$2 AND i.generation=$3 AND i.pointer_revision=$4",
    [emitted.payload.clock.at,key,head!.generation,head!.revision]);
  expect(clock.rows).toEqual([{exact:true}]);
  // Session-indexed ACK and search ACK are independent. Same-revision redelivery
  // passes the fanout early-return path and cannot duplicate the open wake.
  await queue.send(indexed);
  expect(await pollPostgresStage({...options,stage:"session-indexed"})).toMatchObject({acked:1});
  expect((await wakes()).rows).toEqual(beforeReplay);
  // Exercise the same database enqueue with increasing and decreasing committed
  // epochs and a leased predecessor. Restore the durable fixture after the proof.
  const rollbackEpoch = Error("restore directory epoch");
  await expect(pg.transaction(async tx=>{
    const evidence={orgId:scope.orgId,workspaceId:scope.workspaceId,harness:scope.harness,
      sessionId:scope.sessionId,machineId:indexed.ctx.machineId,manifestKey:key,
      revision:String(head!.revision),schemaGeneration:generation};
    const enqueue=()=>tx.query("SELECT sessions.enqueue_managed_search_wake_v1($1,$2)",[evidence,job.ctx.traceId]);
    const advance=async(delta:number)=>{
      await tx.query("UPDATE sessions.session_directory SET event_projection_epoch=$1 WHERE org_id=$2 AND workspace_id=$3 AND harness=$4 AND session_id=$5",
        [head!.epoch+delta,scope.orgId,scope.workspaceId,scope.harness,head!.canonical]);
      await enqueue();
    };
    await advance(2); await advance(1);
    const jobRow=async()=>(await tx.query<{id:string;epoch:number;lease:string|null;visible:string;dlq:boolean;attempts:number}>(
      "SELECT id::text,(message->'payload'->>'epoch')::int AS epoch,lease_id::text AS lease,visible_at::text AS visible,dlq,attempts FROM sessions.pipeline_jobs WHERE stage='search-index' AND work_key=$1",[wk])).rows;
    const [prior]=await jobRow();
    expect(prior!.epoch).toBe(head!.epoch+2);
    await tx.query("UPDATE sessions.pipeline_jobs SET lease_id=gen_random_uuid(),visible_at=now()+interval '5 minutes',attempts=2 WHERE id=$1",[prior!.id]);
    const [claimed]=await jobRow();
    await advance(3); await advance(4); await advance(0);
    expect(await jobRow()).toEqual([{...claimed!,attempts:0,epoch:head!.epoch+4}]);
    // The consumer contract's guarded ACK cannot delete a newer wanted epoch.
    const deleted=await tx.query("DELETE FROM sessions.pipeline_jobs WHERE id=$1 AND lease_id=$2 AND (message->'payload'->>'epoch')::bigint <= $3 RETURNING id",
      [claimed!.id,claimed!.lease,claimed!.epoch]);
    expect(deleted.rows).toHaveLength(0);
    await tx.query("UPDATE sessions.pipeline_jobs SET lease_id=NULL,visible_at=now(),attempts=0 WHERE id=$1 AND lease_id=$2",[claimed!.id,claimed!.lease]);
    expect(await jobRow()).toMatchObject([{id:claimed!.id,lease:null,epoch:head!.epoch+4,attempts:0}]);
    // A retried leased job reopens the same row without a uniqueness conflict.
    await tx.query("UPDATE sessions.pipeline_jobs SET lease_id=gen_random_uuid(),attempts=3 WHERE id=$1",[claimed!.id]);
    await advance(5);
    await tx.query("UPDATE sessions.pipeline_jobs SET lease_id=NULL,visible_at=now()+interval '30 seconds' WHERE id=$1",[claimed!.id]);
    expect(await jobRow()).toMatchObject([{id:claimed!.id,lease:null,epoch:head!.epoch+5,attempts:0}]);
    // Equal or older epochs preserve quarantine and backoff. Only new work reopens it.
    await tx.query("UPDATE sessions.pipeline_jobs SET dlq=true,attempts=12 WHERE id=$1",[claimed!.id]);
    const [parked]=await jobRow();
    await advance(5); await advance(1);
    expect(await jobRow()).toEqual([parked!]);
    await advance(6);
    expect(await jobRow()).toMatchObject([{id:parked!.id,lease:null,dlq:false,attempts:0,epoch:head!.epoch+6}]);
    expect((await jobRow())[0]!.visible).not.toBe(parked!.visible);
    // Add a second machine with a later successful publish. Enqueue through
    // the older machine and vary the sibling's canonical membership.
    const siblingKey = key + ".sibling";
    const siblingAttempt = crypto.randomUUID();
    const later = "2090-01-01T00:00:00.000000Z";
    await tx.query(`INSERT INTO sessions.ingest_publication_attempts
      SELECT (jsonb_populate_record(a,jsonb_build_object('attempt_token',$2::uuid,'published_at',$3::text))).*
      FROM sessions.ingest_publication_attempts a JOIN sessions.ingest_publication_images i USING(attempt_token)
      WHERE i.manifest_key=$1`,[key,siblingAttempt,later]);
    await tx.query(`INSERT INTO sessions.ingest_publication_images
      SELECT (jsonb_populate_record(i,jsonb_build_object('attempt_token',$2::uuid,'manifest_key',$3::text))).*
      FROM sessions.ingest_publication_images i WHERE manifest_key=$1`,[key,siblingAttempt,siblingKey]);
    await tx.query(`INSERT INTO sessions.ingest_publication_heads
      SELECT (jsonb_populate_record(h,jsonb_build_object('source','sibling','machine_id','sibling','current_key',$2::text))).*
      FROM sessions.ingest_publication_heads h WHERE current_key=$1`,[key,siblingKey]);
    await tx.query(`INSERT INTO sessions.session_index
      SELECT (jsonb_populate_record(p,jsonb_build_object('machine_id','sibling','manifest_key',$2::text))).*
      FROM sessions.session_index p WHERE manifest_key=$1`,[key,siblingKey]);
    const currentClock=async()=>(await tx.query<{at:string}>(
      "SELECT message #>> '{payload,clock,at}' AS at FROM sessions.pipeline_jobs WHERE stage='search-index' AND work_key=$1",[wk])).rows[0]!.at;
    await advance(7);
    expect(await currentClock()).toBe(later);
    await tx.query("UPDATE sessions.session_index SET canonical_session_id='unrelated' WHERE manifest_key=$1",[siblingKey]);
    await advance(8);
    expect(await currentClock()).toBe(emitted.payload.clock.at);
    await tx.query("UPDATE sessions.session_index SET canonical_session_id=$2 WHERE manifest_key=$1",[siblingKey,head!.canonical]);
    await tx.query("UPDATE sessions.ingest_publication_heads SET state='retired' WHERE current_key=$1",[siblingKey]);
    await advance(9);
    expect(await currentClock()).toBe(emitted.payload.clock.at);
    await tx.query("UPDATE sessions.ingest_publication_heads SET state='live' WHERE current_key=$1",[siblingKey]);
    for (const manifest of [key,siblingKey]) {
      await tx.exec("SAVEPOINT missing_clock");
      await tx.query(`UPDATE sessions.ingest_publication_attempts a SET published_at=NULL
        FROM sessions.ingest_publication_images i WHERE i.attempt_token=a.attempt_token AND i.manifest_key=$1`,[manifest]);
      await expect(advance(10)).rejects.toMatchObject({code:"P0002"});
      await tx.exec("ROLLBACK TO SAVEPOINT missing_clock");
      await tx.exec("RELEASE SAVEPOINT missing_clock");
    }
    throw rollbackEpoch;
  })).rejects.toBe(rollbackEpoch);
  // When the real source clock is absent, fail before touching the existing
  // job. The transaction rolls the fixture clock change back.
  await expect(pg.transaction(async tx=>{
    const cleared=await tx.query("UPDATE sessions.ingest_publication_attempts a SET published_at=NULL FROM sessions.ingest_publication_images i WHERE a.attempt_token=i.attempt_token AND i.manifest_key=$1 RETURNING a.attempt_token",[key]);
    expect(cleared.rows).toHaveLength(1);
    await tx.query("SELECT sessions.enqueue_managed_search_wake_v1($1,$2)",[
      {orgId:scope.orgId,workspaceId:scope.workspaceId,harness:scope.harness,sessionId:scope.sessionId,machineId:indexed.ctx.machineId,
        manifestKey:key,revision:String(head!.revision),schemaGeneration:generation},job.ctx.traceId]);
  })).rejects.toMatchObject({code:"P0002"});
  await expect(pg.transaction(async tx=>{
    await tx.query("UPDATE sessions.session_directory SET event_projection_epoch=9007199254740992 WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
      [scope.orgId,scope.workspaceId,scope.harness,head!.canonical]);
    await tx.query("SELECT sessions.enqueue_managed_search_wake_v1($1,$2)",[
      {orgId:scope.orgId,workspaceId:scope.workspaceId,harness:scope.harness,sessionId:scope.sessionId,machineId:indexed.ctx.machineId,
        manifestKey:key,revision:String(head!.revision),schemaGeneration:generation},job.ctx.traceId]);
  })).rejects.toThrow("ambiguous directory scope");
  await expect(pg.transaction(async tx=>{
    const row=await tx.query("UPDATE sessions.session_index SET scrubbed=false WHERE manifest_key=$1 RETURNING scrub_quarantined",[key]);
    expect(row.rows).toEqual([{scrub_quarantined:true}]);
    await tx.query("SELECT sessions.enqueue_managed_search_wake_v1($1,$2)",[
      {orgId:scope.orgId,workspaceId:scope.workspaceId,harness:scope.harness,sessionId:scope.sessionId,machineId:indexed.ctx.machineId,
        manifestKey:key,revision:String(head!.revision),schemaGeneration:generation},job.ctx.traceId]);
  })).rejects.toMatchObject({code:"P0002"});
  // A normal job writer can queue an ordinary stage, but cannot insert or
  // replace protected search work even when it knows the correct scope key.
  await pg.exec("CREATE ROLE fixture_queue_writer NOLOGIN; CREATE POLICY fixture_job_writer ON sessions.pipeline_jobs TO fixture_queue_writer USING(true) WITH CHECK(true); GRANT USAGE ON SCHEMA sessions TO fixture_queue_writer; GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON sessions.pipeline_jobs TO fixture_queue_writer; GRANT USAGE ON ALL SEQUENCES IN SCHEMA sessions TO fixture_queue_writer");
  const forge=(sql:string)=>pg.transaction(async tx=>{await tx.exec("SET LOCAL ROLE fixture_queue_writer");return tx.query(sql);});
  await expect(forge("UPDATE sessions.pipeline_jobs SET message='{}' WHERE stage='search-index'")).rejects.toMatchObject({code:"42501"});
  await expect(forge("DELETE FROM sessions.pipeline_jobs WHERE stage='search-index'")).rejects.toMatchObject({code:"42501"});
  await expect(forge("UPDATE sessions.pipeline_jobs SET lease_id=gen_random_uuid() WHERE stage='search-index'")).rejects.toMatchObject({code:"42501"});
  await expect(forge("TRUNCATE sessions.pipeline_jobs")).rejects.toMatchObject({code:"42501"});
  await expect(forge("INSERT INTO sessions.pipeline_jobs(stage,work_key,trace_id,org_id,workspace_id,session_id,message) VALUES ('search-index','forged',gen_random_uuid(),'f','f','f','{}')")).rejects.toMatchObject({code:"42501"});

  expect((await wakes()).rows).toEqual(beforeReplay);
  // The independent search role can lease and ACK, but cannot change the
  // message on an existing row. Roll back the synthetic ACK for later checks.
  await pg.exec("GRANT USAGE ON SCHEMA sessions TO bulls96_search_wake_consumer; GRANT SELECT,UPDATE,DELETE ON sessions.pipeline_jobs TO bulls96_search_wake_consumer; CREATE POLICY fixture_search_consumer ON sessions.pipeline_jobs TO bulls96_search_wake_consumer USING(true) WITH CHECK(true)");
  const restoreAck=Error("restore pending search wake");
  await expect(pg.transaction(async tx=>{
    await tx.exec("SET LOCAL ROLE bulls96_search_wake_consumer");
    const leased=await tx.query("UPDATE sessions.pipeline_jobs SET lease_id=gen_random_uuid() WHERE stage='search-index' AND work_key=$1 RETURNING id",[wk]);
    expect(leased.rows).toHaveLength(1);
    const acked=await tx.query("DELETE FROM sessions.pipeline_jobs WHERE stage='search-index' AND work_key=$1 AND lease_id IS NOT NULL RETURNING id",[wk]);
    expect(acked.rows).toEqual(leased.rows);
    throw restoreAck;
  })).rejects.toBe(restoreAck);
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
    // The rollout acceptance condition opts the consumer in (SEARCH_JOBS_ENABLED on the Lambda): the batch's
    // follow-on claim runs the committed search-index wake through the stage-specific loop. Off, no search job runs.
    const automatic = process.env.PUBLICATION_REQUIRE_AUTOMATIC_SEARCH === "1";
    expect(await processBatch([{messageId:"freshness",body:collected[0]!}],{pointer:pointer as never,search:searchDeps,
      ...(automatic ? {searchJobs:{drain:()=>drainSearchJobs({sql:pg,search:searchDeps,claim:5})}} : {})}))
      .toEqual({batchItemFailures:[]});
    // The local service's freshness message follows the same managed-hint
    // path as an S3 event. It acknowledged the hint without projecting search.
    const afterWake=(await state.partials(scope,{limit:10})).sessions;
    if (automatic) {
      // The rollout acceptance condition: the committed wake alone made the scope searchable. NO explicit
      // indexSearchScope call runs in this mode, so the authorized peer list/point/search reads below can only
      // see what the automatic search-index path wrote.
      expect(afterWake).not.toContain(head!.canonical);
    } else {
      // The positive control: without the consumer the hint projects nothing, and the explicit one-scope
      // qualification of the writer, independent of its trigger, makes the scope searchable.
      expect(afterWake).toContain(head!.canonical);
      expect(await indexSearchScope(scope,searchDeps)).toMatchObject({complete:true});
      expect((await state.partials(scope,{limit:10})).sessions).not.toContain(head!.canonical);
    }
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
    const paths=["/v1/sessions","/v1/sessions/"+encodeURIComponent(head!.canonical)+"/events","/v1/events"];
    for(const path of paths){
      const url=(ws:string)=>path+"?workspace="+ws+(path==="/v1/events"?"&q=needle&limit=20":"");
      const get=(ws:string,token:string)=>app.request(url(ws),{headers:{authorization:"Bearer "+token}});
      const visible=await get(scope.workspaceId,"fixture-peer");
      const body=await visible.json() as {sessions?:unknown[];events?:unknown[]};
      expect({path,status:visible.status,body}).toMatchObject({path,status:200});
      expect(body.sessions??body.events).toEqual(expect.arrayContaining([expect.objectContaining({sessionId:head!.canonical})]));
      expect((await get("other-workspace","fixture-peer")).status).toBe(403);
      expect((await get(scope.workspaceId,"invalid")).status).toBe(401);
    }
  } finally {await search.close();}
}
