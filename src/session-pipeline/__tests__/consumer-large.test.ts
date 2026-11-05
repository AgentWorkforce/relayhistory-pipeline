// deps.large routing in the event consumer (SESSION_LARGE_READ_HANDOFF_ENABLED): real pointer
// stage, real search index (PGlite + recorded ClickHouse transport), stubbed or real route.
import {afterEach,expect,test} from "bun:test";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {objectKey} from "../../../packages/object-store/src/index.ts";
import {objectStoreManifestReader,prefixAllowed,prefixOrgResolver} from "../../shared/vendor.ts";
import {processBatch,type ConsumerDeps} from "../consumer.ts";
import {droppedFanout} from "../pointer.ts";
import {replayDatabase,exportReplayFixture,createFsStore} from "./replay-fixture.ts";
import {recordingReplayClickHouse} from "./replay-clickhouse-fixture.ts";
const cleanup:Array<()=>Promise<unknown>>=[];
afterEach(async()=>{for(const c of cleanup.splice(0).reverse())await c()});
const bindings={PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"};
async function fixture(){
 const x=await replayDatabase();cleanup.push(()=>x.pg.close());
 const root=await mkdtemp(join(tmpdir(),"consumer-large-"));cleanup.push(()=>rm(root,{recursive:true,force:true}));
 const store=createFsStore({root}),source=await exportReplayFixture(store,x.db,3);
 const reader=objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"});
 let searches=0;
 const deps:ConsumerDeps={
  pointer:{db:{...x.db,pool:undefined,end:async()=>{}} as any,lock:async(_s:any,work:any)=>work(),reader,exists:async(k:any)=>(await store.head(objectKey(k)))!==undefined,orgs:prefixOrgResolver,allowPrefix:(p:any,o:any)=>prefixAllowed(bindings,p,o),fanout:droppedFanout},
  search:{transact:<T>(work:any)=>{searches++;return x.pg.transaction((tx:any)=>work({query:(sql:string,params?:unknown[])=>tx.query(sql,params)})) as Promise<T>},reader,store,ch:recordingReplayClickHouse().ch,prefixFor:(org:string)=>`96bulls/${org}`},
 };
 const messages=[{messageId:"m1",body:JSON.stringify({reconcile:{key:source.manifests[0]!.manifestKey}})}];
 const run=async()=>{
  const lines:any[]=[],out=process.stdout.write,err=process.stderr.write;
  const tap=(w:any,stream:NodeJS.WriteStream)=>(chunk:any,...rest:any[])=>{for(const l of String(chunk).split("\n"))if(l.startsWith("{"))try{const j=JSON.parse(l);if(j.stage==="large-read")lines.push(j)}catch{}return w.call(stream,chunk,...rest)};
  process.stdout.write=tap(out,process.stdout) as any;process.stderr.write=tap(err,process.stderr) as any;
  const before=searches;
  try{const r=await processBatch(messages,deps);return {failures:r.batchItemFailures.map(f=>f.itemIdentifier),searched:searches>before,lines}}
  finally{process.stdout.write=out;process.stderr.write=err}
 };
 return {x,deps,run};
}
const pgError=(code:string,message:string)=>Object.assign(new Error(message),{code});

test("flag off: no deps.large, no routing call, search runs as before",async()=>{
 const f=await fixture();
 expect(f.deps.large).toBeUndefined();
 const r=await f.run();
 expect(r).toMatchObject({failures:[],searched:true,lines:[]});
 // The real route would delegate this fixture (0/NULL counts): it must not have been consulted.
 expect((await f.x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_large_read_jobs")).rows[0]!.n).toBe(0);
},60000);

test("flag on + delegated: search skipped",async()=>{
 const f=await fixture();let routes=0;
 f.deps.large={route:async()=>{routes++;return true}};
 const r=await f.run();
 expect(r.failures).toEqual([]);expect(r.searched).toBe(false);expect(routes).toBe(1);
},60000);

test("flag on + not delegated: search runs",async()=>{
 const f=await fixture();
 f.deps.large={route:async()=>false};
 expect(await f.run()).toMatchObject({failures:[],searched:true,lines:[]});
},60000);

test.each([
 ["54000 pointer capacity",pgError("54000","replay capture: pointer capacity exceeded")],
 ["54000 metadata capacity",pgError("54000","replay capture: metadata capacity exceeded")],
])("flag on + capacity refusal (%s): logged, not failed, search runs",async(_name,error)=>{
 const f=await fixture();
 f.deps.large={route:async()=>{throw error}};
 const r=await f.run();
 expect(r.failures).toEqual([]);expect(r.searched).toBe(true);
 expect(r.lines).toEqual([expect.objectContaining({level:"warn",message:"large scope handoff refused",reason:"replay_capacity_refused",error_code:"54000"})]);
},60000);

test.each([
 ["unexpected error",new Error("boom")],
 ["55000 durable handoff unavailable (enqueue infrastructure, retried)",pgError("55000","large read: durable handoff unavailable")],
 ["invalid route result",new Error("large_handoff_invalid")],
])("flag on + %s: batch failure, search skipped",async(_name,error)=>{
 const f=await fixture();
 f.deps.large={route:async()=>{throw error}};
 const r=await f.run();
 expect(r.failures).toEqual(["m1"]);expect(r.searched).toBe(false);
 expect(r.lines).toEqual([expect.objectContaining({level:"error",message:"large scope handoff failed"})]);
},60000);

