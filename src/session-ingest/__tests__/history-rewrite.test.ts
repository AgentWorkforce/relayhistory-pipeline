import {test,expect} from "bun:test";
import {mkdtemp,rm}from"node:fs/promises";import{tmpdir}from"node:os";import{join}from"node:path";
import{zstdCompressSync,zstdDecompressSync}from"node:zlib";
import{createFsStore}from"../../../packages/object-store/src/fs.ts";
import{computeContentSha256,rawChunkKey,parseManifest,manifestKey,type ManifestV1,type ChunkEntry}from"../../../packages/object-store/src/index.ts";
import{createHash}from"node:crypto";
import{rewriteVerifiedCarrier}from"../history-rewrite.ts";
const sha=(v:Uint8Array)=>createHash("sha256").update(v).digest("hex"),E=new TextEncoder(),D=new TextDecoder(),AT="2026-11-01T00:00:00.000Z";
const loc={prefix:"96bulls/org",workspaceId:"ws",machineId:"srv_fixture",harness:"claude",sessionId:"session",startDate:"2026-11-01"};
const dst={...loc,prefix:"96bulls/org/upload-generations/g=12345678-1234-1234-1234-123456789abc"};
const line=(origin:string,id:string,revision="1",text="kept")=>JSON.stringify({kind:"session_event",origin_id:origin,record_id:id,revision,operation:"upsert",received_at:AT,payload:{text}})+"\n";
async function fixture(texts:string[],epochs=[0,0],backfill=false){
 const root=await mkdtemp(join(tmpdir(),"history-rewrite-")),store=createFsStore({root}),chunks:ChunkEntry[]=[];let offset=0;
 for(const [seq,text]of texts.entries()){const raw=E.encode(text),stored=new Uint8Array(zstdCompressSync(raw)),hash=sha(raw);if(seq&&epochs[seq]!==epochs[seq-1])offset=0;
  const c:ChunkEntry={seq,epoch:epochs[seq]!,start_offset:offset,bytes:raw.length,stored_bytes:stored.length,sha256:hash,sha256_8:hash.slice(0,8),stored_sha256:sha(stored),stored_raw_bytes:raw.length,raw_sha256:hash,compression:"zstd"};chunks.push(c);await store.put(rawChunkKey(loc,seq,offset,hash,"ndjson"),stored);offset+=raw.length;}
 const m:ManifestV1={schema_v:1,layout:"v1",trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",revision:1,created_at:AT,updated_at:AT,
  session:{workspace_id:loc.workspaceId,machine_id:loc.machineId,harness:loc.harness,session_id:loc.sessionId,started_at:AT},writer:{delivery_instance_id:"srv_fixture",plugin_version:"fixture"},
  raw:{owner:"srv_fixture",state:"open",format:"ndjson",compression:"zstd",updated_at:AT,raw_bytes:chunks.reduce((n,c)=>n+c.bytes,0),stored_bytes:chunks.reduce((n,c)=>n+c.stored_bytes,0),chunk_count:chunks.length,chunks,next_offset:offset,epoch:backfill?1:0,epochs:[{epoch:0,first_seq:0,reason:"initial",opened_by:"srv_fixture",opened_at:AT},...(backfill?[{epoch:1,first_seq:1,reason:"backfill"as const,opened_by:"migration:fixture",opened_at:AT}]:[])],content_sha256:await computeContentSha256(chunks),scrub:{ruleset:"fixture",version:1}},extensions:{"96bulls":{user_id:"user"}}};
 await parseManifest(m);return{root,store,m};}
async function run(f:Awaited<ReturnType<typeof fixture>>,remove:{originId:string;recordId:string}[],extra:Record<string,unknown>={}){
 const writes:{key:string;raw:string}[]=[],intents:string[]=[];
 const out=await rewriteVerifiedCarrier({manifest:f.m,sourcePrefix:loc.prefix,location:dst,remove,store:f.store,decompress:(s,n)=>new Uint8Array(zstdDecompressSync(s,{maxOutputLength:n})),signal:new AbortController().signal,
 register:async(spec)=>{intents.push(spec.key)},write:async(spec,body)=>{expect(intents.includes(spec.key)).toBe(true);writes.push({key:spec.key,raw:D.decode(zstdDecompressSync(body))});},...extra});
 return{out,writes};}
test("multi-move filters every revision in one carrier pass; unrelated origins, NULL payload and raw lexemes survive",async()=>{
 const exact=line("other","native").replace('"kept"','"literal \\u00e9 ?state=[REDACTED]"').replace('"payload":{','"payload":{"number":9007199254740993,"decimal":1.00000000000000001,');
 const f=await fixture([line("move","native")+line("move","native","2")+exact,line("move","second")+line("keep","second")]);try{
  let gets=0;const wrapped=new Proxy(f.store,{get(t,k){if(k==="get")return async(...args:any[])=>{gets++;return(t.get as any)(...args)};return Reflect.get(t,k)}});f.store=wrapped;
  const r=await run(f,[{originId:"move",recordId:"native"},{originId:"move",recordId:"second"}]);expect(gets).toBe(2);expect(r.out.removedRecords).toBe(3);expect(r.out.keptRecords).toBe(2);expect(r.writes.map(x=>x.raw).join("")).toBe(exact+line("keep","second"));await parseManifest({...f.m,raw:r.out.raw});
 }finally{await rm(f.root,{recursive:true,force:true})}
});
test("empty backfill epoch retains epoch provenance with blank LF and no phantom record",async()=>{
 const f=await fixture([line("keep","one"),line("gone","two")],[0,1],true);try{const r=await run(f,[{originId:"gone",recordId:"two"}]);expect(r.writes[1]!.raw).toBe("\n");expect(r.out.raw.epoch).toBe(1);expect(r.out.raw.epochs).toEqual(f.m.raw.epochs);expect(r.out.raw.chunks[1]!.records).toBe(0);await parseManifest({...f.m,raw:r.out.raw});}finally{await rm(f.root,{recursive:true,force:true})}
});
test("corrupt stored digest refuses before destination intent/write; record and byte bounds refuse without partial publication",async()=>{
 const f=await fixture([line("keep","one")],[0]);try{const old=f.m.raw.chunks[0]!.stored_sha256;f.m.raw.chunks[0]!.stored_sha256="a".repeat(64);let calls=0;await expect(run(f,[],{register:async()=>{calls++}})).rejects.toThrow("wrong stored digest");expect(calls).toBe(0);f.m.raw.chunks[0]!.stored_sha256=old;
 await expect(run(f,[],{maxRawBytes:1})).rejects.toThrow("scope limit");await expect(run(f,[],{maxRecords:0})).rejects.toThrow("record count");
 }finally{await rm(f.root,{recursive:true,force:true})}
});

const{scanHistoryCarrier}=await import("../history-inventory.ts");
test("frontier scan validates complete manifest authority before admitting paged locations; no foreign filtered absence",async()=>{
 const f=await fixture([line("one","native")+line("two","native","7")],[0]);try{
  const key=String(manifestKey(loc));await f.store.put(key as never,E.encode(JSON.stringify(f.m)));
  const pointer={org_id:"org",workspace_id:"ws",user_id:"user",harness:loc.harness,session_id:loc.sessionId,machine_id:loc.machineId,storage_provider:"s3",bucket:"fixture",manifest_key:key,content_sha256:"\\x"+f.m.raw.content_sha256,revision:1};
  const t={carrierSha256:"a".repeat(64),physicalRoot:key.slice(0,-"manifest.json".length),bucket:"fixture",scanToken:"12345678-1234-1234-1234-123456789abc",leaseToken:"12345678-1234-1234-1234-123456789abd",epoch:"1",referenceCount:"1",leaseUntil:"2026-11-04T23:00:00Z"};
  const records:any[]=[],finished:any[]=[];let beats=0;const rpc:any={refs:async()=>({references:[{ordinal:"0",pointer,pointerSha256:"b".repeat(64)}],total:"1",next:null}),page:async(_t:any,p:string,r:any[])=>{records.push(...r)},finish:async(...args:any[])=>{finished.push(args)},heartbeat:async()=>{beats++}};
  const deps={rpc,store:f.store,keyRoot:"96bulls",decompress:(s:Uint8Array,n:number)=>new Uint8Array(zstdDecompressSync(s,{maxOutputLength:n}))};
  const r=await scanHistoryCarrier(t,deps,new AbortController().signal);expect(r.records).toBe("2");expect(records.map(r=>[r.originId,r.revision])).toEqual([["one","1"],["two","7"]]);expect(finished).toHaveLength(1);expect(beats).toBe(1);
  pointer.user_id="foreign";records.length=0;finished.length=0;await expect(scanHistoryCarrier(t,deps,new AbortController().signal)).rejects.toThrow("history_inventory_manifest_authority");expect(records).toHaveLength(0);expect(finished).toHaveLength(0);
 }finally{await rm(f.root,{recursive:true,force:true})}
});

test("actual shared canonical fold hides globally moved copy in unchanged A carrier and restores independent fallback",async()=>{
 const{readCanonicalScope}=await import("../../session-read-api/canonical-projection.ts");const{objectStoreManifestReader}=await import("../../../packages/pipeline/src/lib/pipeline/adapter.ts");
 const{chooseHistoryCurrentWinners,historyRecordContentSha256}=await import("../../shared/history-visibility.ts");const{parseRelayhistoryRecord}=await import("../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts");
 const a=line("a","native","1","retained A").replace(/}\n$/,',"accepted_seq":"10"}\n'),u=line("a","unrelated","1","U").replace(/}\n$/,',"accepted_seq":"11"}\n'),b=line("b","native","1","B").replace(/}\n$/,',"accepted_seq":"12"}\n');
 const f=await fixture([a+u],[0]);try{
  const key=String(manifestKey(loc));await f.store.put(key as never,E.encode(JSON.stringify(f.m)));
  const seat=(raw:string,sessionId:string)=>{const r=parseRelayhistoryRecord(raw);return{originId:r.origin_id,recordId:r.record_id,kind:r.kind,revision:r.revision,acceptedSeq:r.accepted_seq!,operation:r.operation,userId:"user",source:"claude",sessionId,contentSha256:historyRecordContentSha256(r),lineage:"1"}};
  const scope={orgId:"org",workspaceId:"ws",harness:"claude",sessionId:"session"};
  const read=async(winners:any)=>readCanonicalScope(scope,[key],{store:f.store,reader:objectStoreManifestReader(f.store,{provider:"s3",bucket:"fixture"}),prefixFor:()=>loc.prefix,decompress:(s,n)=>new Uint8Array(zstdDecompressSync(s,{maxOutputLength:n})),historyMask:{scope:{orgId:"org",workspaceId:"ws",source:"claude",sessionId:"session"},source:{epoch:"1",fingerprint:"a".repeat(64)},winners}},{maxScopeBytes:2**30,maxScopeEvents:100,maxRows:100,insertBatch:100},2);
  const moved=await read(chooseHistoryCurrentWinners([seat(a,"session"),seat(u,"session"),seat(b,"B")]));expect(moved.complete).toBe(true);expect(moved.projectionRows.filter(r=>r.table==="convergence_events").map(r=>r.row.content)).toEqual(["U"]);
  const restored=await read(chooseHistoryCurrentWinners([seat(a,"session"),seat(u,"session")]));expect(restored.complete).toBe(true);expect(restored.projectionRows.filter(r=>r.table==="convergence_events").map(r=>r.row.content).sort()).toEqual(["U","retained A"]);
 }finally{await rm(f.root,{recursive:true,force:true})}
});
