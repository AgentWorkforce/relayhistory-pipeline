import {afterEach,expect,test} from "bun:test";
import {mkdtemp,rm} from "node:fs/promises";import {tmpdir} from "node:os";import {join} from "node:path";
import {replayDatabase,exportReplayFixture,createFsStore,REPLAY_FIXTURE_IDENTITY as i} from "./replay-fixture.ts";
import {recordingReplayClickHouse} from "./replay-clickhouse-fixture.ts";
import {objectStoreManifestReader} from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import {largeReadMain} from '../large-read-main.ts';
import {runLargeReadOnce} from "../large-read-worker.ts";
const cleanup:Array<()=>Promise<unknown>>=[];afterEach(async()=>{for(const c of cleanup.splice(0))await c()});
async function fixture(){const x=await replayDatabase();cleanup.push(()=>x.pg.close());const root=await mkdtemp(join(tmpdir(),"large-read-runner-"));cleanup.push(()=>rm(root,{recursive:true,force:true}));const store=createFsStore({root});await exportReplayFixture(store,x.db,6,2);const observed=recordingReplayClickHouse();const search={transact:<T>(work:any)=>x.pg.transaction((tx:any)=>work({query:(sql:string,params?:unknown[])=>tx.query(sql,params)})) as Promise<T>,reader:objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"}),store,ch:observed.ch,prefixFor:(org:string)=>`96bulls/${org}`};const replay={db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}};return {...x,store,search,replay,observed}}
test("real leased worker selects validated physical scope and publishes CH,metadata and artifacts atomically under its nonce",async()=>{const f=await fixture();await f.db.query("SELECT sessions.enqueue_session_large_read($1,$2,$3,$4)",[i.orgId,i.workspaceId,i.source,i.sessionId]);expect(await runLargeReadOnce({db:f.db,search:f.search,replay:f.replay})).toBe("published");expect(f.observed.stats().uniqueRows).toBe(6);expect((await f.pg.query("SELECT state FROM sessions.session_large_read_jobs")).rows[0].state).toBe("complete");expect((await f.pg.query("SELECT event_count::text n,event_projection_ready ready FROM sessions.session_directory")).rows[0]).toEqual({n:"6",ready:true});expect((await f.pg.query("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0].n).toBe(1)});
test("an aborted worker and an empty queue do no source work",async()=>{const f=await fixture(),stop=new AbortController();stop.abort();expect(await runLargeReadOnce({db:f.db,search:f.search,replay:f.replay,signal:stop.signal})).toBe("idle");expect(await runLargeReadOnce({db:f.db,search:f.search,replay:f.replay})).toBe("idle");expect(f.observed.stats().rows).toBe(0)});

test('actual Node entry discovers, leases and publishes a real source before shutdown',async()=>{const f=await fixture();let closed=0;expect(await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},()=>({deps:{db:f.db,search:f.search,replay:f.replay},close:async()=>{closed++}}))).toMatchObject({enabled:true,processed:1,published:1,failed:0});expect(closed).toBe(1);expect(f.observed.stats().uniqueRows).toBe(6);expect((await f.pg.query('SELECT state FROM sessions.session_large_read_jobs')).rows[0].state).toBe('complete')});

test("managed mode refuses a missing history factory before claiming any job",async()=>{
 let calls=0;const db:any={query:async()=>{calls++;throw Error("unexpected_query")}};
 await expect(runLargeReadOnce({db,search:{}as any,replay:{}as any,expectedGeneration:"a".repeat(64)})).rejects.toThrow("large_history_configuration_required");
 expect(calls).toBe(0);
});


test("managed leased runner publishes the official graph under only the ingest function role",async()=>{
 const f=await fixture();
 const {generation}=await import("../../hoopdb/migrate.ts");
 const {sqlHistoryMaskStore,updateHistoryMask}=await import("../../shared/history-mask.ts");
 const {captureHistorySource,readHistoryReplayDescriptor}=await import("../../shared/history-source-authority.ts");
 const generationSha=await generation(),authority="62345678-1234-1234-1234-123456789abc";
 const db:any={query:async(q:string,p:unknown[])=>{
  await f.pg.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
  try{const r=await f.db.query(q,p);await f.pg.exec("COMMIT");return r}catch(e){await f.pg.exec("ROLLBACK");throw e}
 }};
 const mask=await updateHistoryMask(sqlHistoryMaskStore(db,generationSha,i.orgId,i.workspaceId),{orgId:i.orgId,workspaceId:i.workspaceId},null,Array.from({length:5},(_,n)=>({recordId:"event-"+String(n+1).padStart(12,"0"),winner:null})));
 const anchor="96bulls/"+i.orgId+"/upload-generations/g="+authority+"/v1/ws="+i.workspaceId+"/m=history_scope/h="+i.source+"/d=2026/11/01/s=logical/publications/"+authority+"/manifest.json";
 await f.db.query("INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot_sha256) SELECT $1,org_id,workspace_id,harness,session_id,epoch,$2,5,$3,'replay-fixture','us-east-1','96bulls','ready',repeat('a',64) FROM sessions.session_replay_publications WHERE org_id=$4",[authority,mask.sha256,anchor,i.orgId]);
 await f.db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,$3,$4,$5,true)",[i.orgId,i.workspaceId,i.source,i.sessionId,authority]);
 await f.db.query("UPDATE sessions.ingest_history_scope_versions SET source_fingerprint=encode(sha256(convert_to(sessions.session_replay_source_snapshot(org_id,workspace_id,harness,session_id)::text,'UTF8')),'hex') WHERE authority_id=$1",[authority]);
 const history=()=>captureHistorySource(db,i,{schemaGeneration:generationSha,profile:"large"});
 const d=(await db.query("SELECT sessions.session_history_large_job($1::jsonb) r",[JSON.stringify({generation:generationSha,action:"discover"})])).rows[0].r;
 expect(d).toMatchObject({changed:1,scanned:1});
 expect(await runLargeReadOnce({db,expectedGeneration:generationSha,history,search:f.search,replay:{...f.replay,db}})).toBe("published");
 expect((await readHistoryReplayDescriptor(db,await history()))?.index.bytes).toBeGreaterThan(0);
 expect((await f.db.query("SELECT state,history_generation FROM sessions.session_large_read_jobs")).rows[0]).toEqual({state:"complete",history_generation:generationSha});
 expect(f.observed.stats().rows).toBe(0);
});
