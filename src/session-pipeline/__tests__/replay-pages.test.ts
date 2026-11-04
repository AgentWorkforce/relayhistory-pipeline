import {afterEach,expect,test} from "bun:test";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {objectKey,readAll,type ObjectStore} from "../../../packages/object-store/src/index.ts";
import {materializeCanonicalArtifacts,artifactDigest,compareArtifactRows} from "../replay-pages.ts";
import {produceCanonicalReplay} from "../replay-publication.ts";
import type {ArtifactRangeRef,ArtifactEntry,ArtifactIndex} from "../../session-read-api/canonical-artifact.ts";
import {replayDatabase,exportReplayFixture,createFsStore,REPLAY_FIXTURE_IDENTITY as identity} from "./replay-fixture.ts";
const roots:string[]=[],dbs:Awaited<ReturnType<typeof replayDatabase>>[]=[];
afterEach(async()=>{for(const x of dbs.splice(0))await x.pg.close();for(const r of roots.splice(0))await rm(r,{recursive:true,force:true})});
async function storage(){const root=await mkdtemp(join(tmpdir(),"replay-artifact-"));roots.push(root);return createFsStore({root})}
const input=(rows:any[])=>({identity,generation:"a".repeat(64),source:{epoch:"1",fingerprint:"b".repeat(64)},prefix:"96bulls/org_replay/normalized/test/",rows,legacyCoverage:null});
async function read(store:ObjectStore,ref:{key:string,sha256:string,bytes:number}){const b=await readAll(await store.get(objectKey(ref.key)));expect(b.length).toBe(ref.bytes);expect(artifactDigest(b)).toBe(ref.sha256);return b}
async function leaves(store:ObjectStore,ref:ArtifactRangeRef):Promise<ArtifactRangeRef[]>{if(ref.kind!=="node")return[ref];const node=JSON.parse(new TextDecoder().decode(await read(store,ref)));expect(node.children.length).toBeLessThanOrEqual(128);const result=[];for(const c of node.children)result.push(...await leaves(store,c));return result}
async function entries(store:ObjectStore,index:ArtifactIndex,table:string,view="raw"){const root=index.tables[table]![view as "raw"]!.root;if(!root)return[];const result:ArtifactEntry[]=[];for(const p of await leaves(store,root)){const page=JSON.parse(new TextDecoder().decode(await read(store,p)));expect(page.rows.length).toBe(p.count);result.push(...page.rows)}return result}
async function row(store:ObjectStore,e:ArtifactEntry){let r=e.row;if(e.rowJson){const parts=[];for(const s of await leaves(store,e.rowJson.root))parts.push(await read(store,s));const all=Buffer.concat(parts);expect(artifactDigest(all)).toBe(e.rowJson.sha256);r=JSON.parse(all.toString("utf8"))}if(e.content){let content="";for(const s of await leaves(store,e.content.root))content+=JSON.parse(new TextDecoder().decode(await read(store,s)));expect(content.length).toBe(e.content.chars);expect(artifactDigest(JSON.stringify(content))).toBe(e.content.jsonSha256);r={...r,content}}return r!}
test("arbitrary nested arrays and replay content segment losslessly including lone surrogates",async()=>{const store=await storage(),content=("text\ud800😀\udfff".repeat(9000)),tools=Array.from({length:12000},(_,n)=>({name:"Read",input:{n,path:"src/test.ts"}})),original={ts:"2026-11-01T10:00:00.123456Z",event_id:"native",machine_id:"m",kind:"session_event",content,tool_calls:tools,record:{payload:{message_id:"native-msg",text:content}}};const made=await materializeCanonicalArtifacts(store,input([{table:"convergence_events",row:original}]));const e=(await entries(store,made.document,"convergence_events","replay"))[0]!;expect(e.rowJson).toBeDefined();expect(e.content).toBeDefined();expect(await row(store,e)).toEqual({...original,record:{payload:{message_id:"native-msg"}}});expect(await row(store,(await entries(store,made.document,"convergence_events"))[0]!)).toEqual(original)});
test("hierarchical pages retain exact microsecond and numeric BIGSERIAL ordering",async()=>{const store=await storage();const rows=Array.from({length:3000},(_,n)=>({table:"session_links",row:{id:String(9007199254740993n+BigInt(2999-n)),link_ts:"2026-11-01T10:00:00.123456Z",metadata:{kept:true}}}));const made=await materializeCanonicalArtifacts(store,input(rows));const all=await entries(store,made.document,"session_links");expect(all.length).toBe(3000);expect((await row(store,all[0]!)).id).toBe("9007199254740993");expect((await row(store,all.at(-1)!)).id).toBe("9007199254743992");expect(made.document.tables.convergence_events!.bounds).toEqual({count:0,root:null});expect(compareArtifactRows({table:"convergence_events",row:{ts:"2026-11-01T10:00:00.123455Z",event_id:"z"}},{table:"convergence_events",row:{ts:"2026-11-01T10:00:00.123456Z",event_id:"a"}})).toBeLessThan(0)});
test("actual producer hydrates full union, read-verifies objects and publishes idempotently",async()=>{const x=await replayDatabase();dbs.push(x);const store=await storage();await exportReplayFixture(store,x.db,36,2);const deps={db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}};const scope={orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId};const first=await produceCanonicalReplay(scope,deps),second=await produceCanonicalReplay(scope,deps);expect(first.published).toBe(true);expect(first.rows).toBe(36);expect(second.publication).toEqual(first.publication);const index=JSON.parse(new TextDecoder().decode(await read(store,first.publication!.index)));expect((await entries(store,index,"convergence_events","replay")).length).toBe(36);expect(first.publication!.prefix).toContain("/normalized/read-projection-v1/")});
test.each(["write","ack","readback","source_change"])("failed %s publishes no current generation",async mode=>{const x=await replayDatabase();dbs.push(x);const store=await storage();await exportReplayFixture(store,x.db,3);let changed=false;const guarded=new Proxy(store,{get(target,key){if(key==="put")return async(k:any,b:any,o:any)=>{if(mode==="write")throw Error("fixture write failed");const ack=await target.put(k,b,o);return mode==="ack"?{...ack,size:1}:ack};if(key==="get")return async(k:any)=>{if(String(k).includes("normalized/read-projection-v1/")){if(mode==="readback")return new ReadableStream({start(c){c.enqueue(new Uint8Array([1]));c.close()}});if(mode==="source_change"&&!changed){changed=true;await x.pg.exec("UPDATE sessions.session_index SET user_id='raced'")}}return target.get(k)};const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value}});await expect(produceCanonicalReplay({orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId},{db:x.db,store:guarded,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}})).rejects.toThrow();expect((await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n).toBe(0)});
test("existing event consumer leaves pages dormant by default and publishes when explicitly wired",async()=>{
 const x=await replayDatabase();dbs.push(x);const store=await storage();const source=await exportReplayFixture(store,x.db,3);
 const {processBatch}=await import("../consumer.ts");const {droppedFanout}=await import("../pointer.ts");
 const {objectStoreManifestReader,prefixOrgResolver,prefixAllowed}=await import("../../shared/vendor.ts");
 const key=source.manifests[0]!.manifestKey,bindings={PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"};
 const deps:any={pointer:{db:{...x.db,pool:undefined,end:async()=>{}},lock:async(_scope:any,work:any)=>work(),reader:objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"}),exists:async(k:any)=>(await store.head(objectKey(k)))!==undefined,orgs:prefixOrgResolver,allowPrefix:(p:any,o:any)=>prefixAllowed(bindings,p,o),fanout:droppedFanout}};
 const messages=[{messageId:"fixture",body:JSON.stringify({reconcile:{key}})}];
 expect(await processBatch(messages,deps)).toEqual({batchItemFailures:[]});
 expect((await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n).toBe(0);
 deps.replay={db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:bindings};
 expect(await processBatch(messages,deps)).toEqual({batchItemFailures:[]});
 expect((await x.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_replay_publications WHERE index_key IS NOT NULL")).rows[0]!.n).toBe(1);
});

test("legacy turn ordinals follow public turn_index and exact ID order, not timestamp",()=>{const rows=[{table:"conversation_turns",row:{turn_index:1,id:"9007199254740993",ts:"2025-01-01T00:00:00Z"}},{table:"conversation_turns",row:{turn_index:0,id:"9007199254740994",ts:"2030-01-01T00:00:00Z"}},{table:"conversation_turns",row:{turn_index:0,id:"9007199254740993",ts:"2040-01-01T00:00:00Z"}}];rows.sort(compareArtifactRows);expect(rows.map(r=>r.row.id)).toEqual(["9007199254740993","9007199254740994","9007199254740993"]);expect(rows.map(r=>r.row.turn_index)).toEqual([0,0,1])});
test("actual multi-machine union honors per-origin revision, acceptance order and cross-origin tombstones",async()=>{
 const x=await replayDatabase();dbs.push(x);const store=await storage();
 const base={kind:"session_event",record_id:"event-000000000001",revision:"2",operation:"upsert" as const,received_at:"2020-01-01T00:00:00Z",payload:{ts_ms:Date.parse("2026-11-01T10:00:00Z"),text:"new per-origin revision",role:"assistant",message_id:"revision-native"}};
 await exportReplayFixture(store,x.db,6,2,[{...base,origin_id:"capacity:1",accepted_seq:"900001"},{...base,origin_id:"capacity:other",revision:"1",accepted_seq:"900002",payload:{...base.payload,text:"last accepted independent source",message_id:"accepted-native"}},{...base,origin_id:"capacity:other",record_id:"event-000000000002",revision:"1",operation:"delete",payload:null,accepted_seq:"900003"}]);
 const result=await produceCanonicalReplay({orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId},{db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}});
 const index=JSON.parse(new TextDecoder().decode(await read(store,result.publication!.index))),rows=[];
 for(const e of await entries(store,index,"convergence_events"))rows.push(await row(store,e));
 expect(rows.length).toBe(5);expect(rows.find(r=>r.delivery_record_id==="event-000000000001")?.content).toBe("last accepted independent source");expect(rows.some(r=>r.delivery_record_id==="event-000000000002")).toBe(false);
});

test('publisher refuses a row beyond the shared hydrated consumer bound before any artifact write',async()=>{
 const {ARTIFACT_MAX_ROW_BYTES}=await import('../../session-read-api/canonical-artifact.ts');
 let writes=0;const forbidden={put:async()=>{writes++;throw Error('unexpected write')}} as unknown as ObjectStore;
 await expect(materializeCanonicalArtifacts(forbidden,input([{table:'convergence_events',row:{content:'x'.repeat(ARTIFACT_MAX_ROW_BYTES)}}]))).rejects.toMatchObject({code:'artifact_row_bound'});
 expect(writes).toBe(0);
});

test("actual materializer preserves public persisted-source order after canonical turn attribution",async()=>{const s=await storage(),small="00000000-0000-4000-8000-000000000001",large="ffffffff-ffff-4fff-8fff-ffffffffffff",turnRow=(id:string,stored:string)=>({table:"conversation_turns",row:{org_id:identity.orgId,workspace_id:identity.workspaceId,source:identity.source,session_id:identity.sessionId,id,legacy_stored_source:stored,turn_index:0,session_owner:"owner",content:"kept",role:"assistant",actor_name:"actor",actor_role:"assistant",metadata:{nativeCli:"claude"},ts:"2026-11-01T00:00:00.000001Z"}}),made=await materializeCanonicalArtifacts(s,{identity,source:{epoch:"1",fingerprint:"b".repeat(64)},generation:"d".repeat(64),prefix:"fixture/",rows:[turnRow(small,"z-label"),turnRow(large,"a-label")],legacyCoverage:null});const all=await entries(s,made.document,"conversation_turns");expect((await Promise.all(all.map(e=>row(s,e)))).map(r=>r.id)).toEqual([large,small]);expect((await Promise.all(all.map(e=>row(s,e)))).map(r=>r.legacy_stored_source)).toEqual(["a-label","z-label"])});

test("managed replay requires exact mask, registers graph before PUT and publishes only qualified namespace",async()=>{
 const x=await replayDatabase();dbs.push(x);const store=await storage();
 await exportReplayFixture(store,x.db,1,1,[{kind:"session_event",origin_id:"moved-origin",record_id:"moved-native",revision:"1",accepted_seq:"2",operation:"upsert",received_at:"2026-11-01T10:00:00Z",payload:{text:"must stay hidden",role:"assistant",ts_ms:Date.parse("2026-11-01T10:00:00Z")}}]);
 const {generation}=await import("../../hoopdb/migrate.ts"),{captureHistorySource,readHistoryReplayDescriptor,publishHistoryReplay}=await import("../../shared/history-source-authority.ts");
 const gen=await generation(),id="12345678-1234-1234-1234-123456789abc",scope={orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId};
 await x.pg.exec("SET ROLE bulls96_session_ingest");
 const mask=(await x.db.query<any>("SELECT sessions.ingest_history_mask_register($1::jsonb) r",[JSON.stringify({generation:gen,document:{protocol:"history-mask-v1",orgId:identity.orgId,workspaceId:identity.workspaceId,prefix:"",kind:"leaf",count:"1",items:[{recordId:"moved-native",winner:null}]}})])).rows[0].r;
 await x.pg.exec("RESET ROLE");
 const anchor="96bulls/org_replay/upload-generations/g="+id+"/v1/ws=ws_replay/m=history_scope/h=claude/d=2026/11/01/s=logical/publications/"+id+"/manifest.json";
 await x.db.query("INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot_sha256) SELECT $1,org_id,workspace_id,harness,session_id,epoch,$2,1,$3,'replay-fixture','us-east-1','96bulls','ready',repeat('a',64) FROM sessions.session_replay_publications WHERE org_id=$4",[id,mask.sha256,anchor,identity.orgId]);
 await x.db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,$3,$4,$5,true)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,id]);
 await x.db.query("UPDATE sessions.ingest_history_scope_versions SET source_fingerprint=encode(sha256(convert_to(sessions.session_replay_source_snapshot(org_id,workspace_id,harness,session_id)::text,'UTF8')),'hex') WHERE authority_id=$1",[id]);
 const history=await captureHistorySource(x.db,identity,{schemaGeneration:gen}),deps={db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}};
 await expect(produceCanonicalReplay(scope,deps)).rejects.toMatchObject({code:"artifact_history_authority_required"});
 const cap=(await x.db.query<any>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0].c;
 const forgedGeneration="f".repeat(64),forgedHash="e".repeat(64),forgedPrefix=anchor.slice(0,-"manifest.json".length)+"derived/replay/"+forgedGeneration+"/";
 expect(await publishHistoryReplay(x.db,history,[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,history.source.epoch,history.source.fingerprint,JSON.stringify(cap.snapshot),forgedGeneration,"d".repeat(64),forgedPrefix,forgedPrefix+forgedHash+".json",forgedHash,100])).toBe(false);

 let checked=0;
 const registered=new Proxy(store,{get(target,key){if(key==="put")return async(k:any,b:any,o:any)=>{
   const rows=(await x.db.query("SELECT object_key FROM sessions.ingest_history_derived_objects WHERE object_key=$1 AND cleanup_state='live'",[String(k)])).rows;
   expect(rows.length).toBe(1);checked++;return target.put(k,b,o);
 };const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value}});
 const result=await produceCanonicalReplay(scope,{...deps,history,store:registered});
 expect(result.published).toBe(true);expect(result.rows).toBe(1);expect(checked).toBeGreaterThan(0);expect(result.publication!.prefix).toContain("/derived/replay/");
 expect(await readHistoryReplayDescriptor(x.db,history)).toEqual(result.publication!);
 const doc=JSON.parse(new TextDecoder().decode(await read(store,result.publication!.index)));
 expect((await entries(store,doc,"convergence_events")).length).toBe(1);
 const old=(await x.db.query<any>("SELECT sessions.publish_session_replay_pages($1,$2,$3,$4,$5::bigint,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::integer) ok",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,history.source.epoch,history.source.fingerprint,"{}",result.publication!.generation,"c".repeat(64),result.publication!.prefix,result.publication!.index.key,result.publication!.index.sha256,result.publication!.index.bytes])).rows[0].ok;
 expect(old).toBe(false);
});
