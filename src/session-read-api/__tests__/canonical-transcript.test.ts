import { expect,test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync,zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { computeContentSha256,manifestKey,objectKey,rawChunkKey,type ManifestV1 } from "../../../packages/object-store/src/index.ts";
import { canonicalTranscriptSnapshot } from "../canonical-transcript.ts";
import { canonicalSnapshot,type CanonicalDeps,type CanonicalPointer } from "../canonical.ts";
import { briefInputFromTranscript } from "../../session-brief/canonical-input.ts";

const ID={orgId:"org-a",workspaceId:"ws-a",source:"claude",sessionId:"s-1"};
const KEY={orgId:ID.orgId,workspaceId:ID.workspaceId,harness:ID.source,sessionId:ID.sessionId};
const LOC={prefix:"org-a",workspaceId:"ws-a",machineId:"m-1",harness:"claude",sessionId:"s-1",startDate:"2026-10-29"};
const AT="2026-10-29T10:00:00.000Z", hash=(b:Uint8Array)=>createHash("sha256").update(b).digest("hex");
const messages=[{type:"user",sessionId:"s-1",uuid:"u1",timestamp:AT,message:{role:"user",content:[{type:"text",text:"Investigate the owned queue race."}]}},{type:"assistant",sessionId:"s-1",uuid:"a1",timestamp:"2026-10-29T10:00:01.000Z",message:{role:"assistant",content:[{type:"text",text:"Awaiting the lease fixed the race."}],model:"fixture"}}];
async function fixture(raw=new TextEncoder().encode(messages.map(x=>JSON.stringify(x)).join("\n")+"\n"),split?:number|number[],declaredRecords=messages.length,session="s-1"){
 const location={...LOC,sessionId:session};
 const store=createFsStore({root:mkdtempSync(join(tmpdir(),"canonical-jsonl-"))});
 const chunks=[];let offset=0;
 // An array split: chunk boundaries at those offsets, each chunk declaring the records it ends.
 const pieces=split===undefined?[raw]:Array.isArray(split)?[0,...split].map((at,i,all)=>raw.slice(at,all[i+1]??raw.length)):[raw.slice(0,split),raw.slice(split)];
 for(const [seq,bytes] of pieces.entries()){
  const stored=new Uint8Array(zstdCompressSync(bytes)),digest=hash(bytes);
  await store.put(rawChunkKey(location,seq,offset,digest,"jsonl"),stored);
  chunks.push({seq,epoch:0,start_offset:offset,bytes:bytes.length,stored_bytes:stored.length,sha256:digest,sha256_8:digest.slice(0,8),stored_sha256:hash(stored),stored_raw_bytes:bytes.length,raw_sha256:digest,compression:"zstd",records:Array.isArray(split)?bytes.filter(b=>b===10).length:seq===0&&split!==undefined?0:declaredRecords});offset+=bytes.length;
 }
 const manifest={schema_v:1,layout:"v1",trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",revision:1,created_at:AT,updated_at:AT,session:{workspace_id:"ws-a",machine_id:"m-1",harness:"claude",session_id:session,started_at:AT},writer:{delivery_instance_id:"m-1",plugin_version:"test"},raw:{owner:"m-1",state:"open",updated_at:AT,format:"jsonl",compression:"zstd",chunk_count:chunks.length,raw_bytes:raw.length,stored_bytes:chunks.reduce((n,c)=>n+c.stored_bytes,0),content_sha256:await computeContentSha256(chunks as never),epoch:0,epochs:[{epoch:0,opened_at:AT,reason:"initial",first_seq:0,opened_by:"m-1"}],next_offset:raw.length,chunks,scrub:{ruleset:"relayhistory-scrub",version:1}},extensions:{"96bulls":{user_id:"u-1"}}} as unknown as ManifestV1;
 const path=manifestKey(location);let reads=0;
 const save=()=>store.put(objectKey(path),new TextEncoder().encode(JSON.stringify(manifest)));
 await save();
 const pointer:CanonicalPointer={org_id:"org-a",workspace_id:"ws-a",harness:"claude",session_id:session,machine_id:"m-1",user_id:"u-1",storage_provider:"s3",bucket:"bucket",manifest_key:path,revision:1,content_sha256:manifest.raw.content_sha256,raw_format:"jsonl",deleted_at:null};
 const watched=new Proxy(store,{get(target,prop){const v=Reflect.get(target,prop);if(prop==='getObject')return (...args:Parameters<typeof store.getObject>)=>{if(String(args[0]).endsWith('.zst'))reads++;return target.getObject(...args)};return typeof v==='function'?v.bind(target):v}});
 const deps:CanonicalDeps={store:watched,provider:"s3",bucket:"bucket",prefix:{PIPELINE_ORG_FROM_PREFIX:"true"},pointers:async()=>[pointer],decompress:(b,max)=>new Uint8Array(zstdDecompressSync(b,{maxOutputLength:max}))};
 return {store,manifest,pointer,deps,path,save,reads:()=>reads};
}

test("verified native JSONL yields bounded real user/assistant brief input; public ndjson reader unchanged",async()=>{
 const f=await fixture();const snapshot=await canonicalTranscriptSnapshot(f.deps,ID);expect(snapshot?.records).toHaveLength(2);
 const brief=briefInputFromTranscript(KEY,[{machineId:"m-1",manifestKey:f.path,revision:1}],snapshot!);expect(brief.kind).toBe("ready");if(brief.kind==='ready')expect(brief.input.messages.map(x=>x.text)).toEqual([messages[0]!.message.content[0]!.text,messages[1]!.message.content[0]!.text]);
 await expect(canonicalSnapshot(f.deps,ID)).rejects.toMatchObject({reason:"pointer_invalid"});
});
test("ndjson remains delegated and mixed/foreign/deleted/invalid pointers refuse before chunks",async()=>{
 const f=await fixture();f.pointer.raw_format='ndjson';expect(await canonicalTranscriptSnapshot(f.deps,ID)).toBeUndefined();expect(f.reads()).toBe(0);f.pointer.raw_format='jsonl';
 for(const changed of [{org_id:'foreign'},{workspace_id:'foreign'},{harness:'codex'},{session_id:'foreign'},{bucket:'foreign'},{storage_provider:'foreign'},{deleted_at:AT},{revision:0},{content_sha256:'bad'}]){
  const deps={...f.deps,pointers:async()=>[{...f.pointer,...changed} as CanonicalPointer]};await expect(canonicalTranscriptSnapshot(deps,ID)).rejects.toBeInstanceOf(Error);
 }
 await expect(canonicalTranscriptSnapshot({...f.deps,pointers:async()=>[f.pointer,{...f.pointer,raw_format:'ndjson',manifest_key:'other'}]},ID)).rejects.toMatchObject({reason:'pointer_invalid'});
 await expect(canonicalTranscriptSnapshot({...f.deps,pointers:async()=>Array.from({length:64},()=>f.pointer)},ID)).rejects.toMatchObject({reason:'pointer_bound'});expect(f.reads()).toBe(0);
});
test("manifest owner/revision/key/format/digest and scrub checks precede chunk reads",async()=>{
 for(const change of [(f:Awaited<ReturnType<typeof fixture>>)=>{(f.manifest.extensions!["96bulls"] as any).user_id='foreign'},(f:Awaited<ReturnType<typeof fixture>>)=>{f.pointer.revision=2},(f:Awaited<ReturnType<typeof fixture>>)=>{f.pointer.content_sha256='f'.repeat(64)},(f:Awaited<ReturnType<typeof fixture>>)=>{f.manifest.raw.format='ndjson'},(f:Awaited<ReturnType<typeof fixture>>)=>{delete f.manifest.raw.scrub}]){
  const f=await fixture();change(f);await f.save();await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toBeInstanceOf(Error);expect(f.reads()).toBe(0);
 }
});
test("stored/raw chunk digest corruption refuses instead of returning partial messages",async()=>{
 for(const raw of [false,true]){const f=await fixture();if(raw){f.manifest.raw.chunks[0]!.raw_sha256='f'.repeat(64);await f.save()}else{const c=f.manifest.raw.chunks[0]!;await f.store.put(rawChunkKey(LOC,c.seq,c.start_offset,c.sha256_8,'jsonl'),new Uint8Array([1,2,3]))}await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'transcript_chunk_digest_mismatch'});}
});
test("authenticated malformed JSON/non-object/UTF8 or declared record count never gets silently skipped",async()=>{
 for(const [raw,count,reason] of [[new TextEncoder().encode('{}\n{\n'),2,'transcript_json_invalid'],[new TextEncoder().encode('[]\n'),1,'transcript_record_invalid'],[new Uint8Array([0xff,0x0a]),1,'transcript_utf8_invalid'],[new TextEncoder().encode('{}\n'),2,'transcript_record_count_mismatch']] as const){const f=await fixture(raw,undefined,count);await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason});}
});
test("valid JSONL and UTF8 may cross chunk boundaries without losing a record",async()=>{
 const row={...messages[0],message:{role:'user',content:[{type:'text',text:'café🙂'}]}};const raw=new TextEncoder().encode(JSON.stringify(row)+'\n');const at=raw.indexOf(0xf0)+2;const f=await fixture(raw,at,1);const s=await canonicalTranscriptSnapshot(f.deps,ID);expect(s?.records).toHaveLength(1);expect((s!.records[0]!.record.message as any).content[0].text).toBe('café🙂');
});
test("pointer revision change/erasure after object read invalidates the generation",async()=>{
 for(const erased of [false,true]){const f=await fixture();let calls=0;f.deps.pointers=async()=>++calls===1?[f.pointer]:erased?[]:[{...f.pointer,revision:2}];await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'source_generation_changed'});}
});
test("foreign declared transcript session cannot become this session's brief",async()=>{
 const f=await fixture(new TextEncoder().encode(JSON.stringify({...messages[0],sessionId:'foreign'})+'\n'),undefined,1);await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'transcript_session_mismatch'});
});

test("org-prefix, admitted machine and same-response manifest etag remain mandatory",async()=>{
 const f=await fixture();
 for(const changed of [{user_id:'foreign'},{machine_id:'foreign'},{manifest_etag:'foreign'}])await expect(canonicalTranscriptSnapshot({...f.deps,pointers:async()=>[{...f.pointer,...changed}]},ID)).rejects.toMatchObject({reason:'manifest_mismatch'});
 const wrong=manifestKey({...LOC,prefix:'org-foreign'});await f.store.put(objectKey(wrong),new TextEncoder().encode(JSON.stringify(f.manifest)));f.pointer.manifest_key=wrong;
 await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'prefix_mismatch'});expect(f.reads()).toBe(0);
});
test("admitted physical-to-logical JSONL alias preserves source identity and revisions",async()=>{
 const raw=new TextEncoder().encode(JSON.stringify({...messages[0],sessionId:'s:1'})+'\n');const f=await fixture(raw,undefined,1,'s_1');(f.manifest.extensions!['96bulls'] as any).canonical_session_id='s:1';f.pointer.session_id='s:1';await f.save();const identity={...ID,sessionId:'s:1'};const snapshot=await canonicalTranscriptSnapshot(f.deps,identity);
 const built=briefInputFromTranscript({...KEY,sessionId:'s:1'},[{machineId:'m-1',manifestKey:f.path,revision:1}],snapshot!);expect(built.kind).toBe('ready');expect(built.sourceRevisions).toEqual({'m-1':1});if(built.kind==='ready')expect(built.input.sessionId).toBe('s:1');
});
test("declared and actual count/byte bounds refuse without increasing canonical limits",async()=>{
 const f=await fixture();f.manifest.raw.chunks[0]!.bytes=16*1024*1024+1;f.manifest.raw.raw_bytes=16*1024*1024+1;f.manifest.raw.next_offset=16*1024*1024+1;f.manifest.raw.content_sha256=await computeContentSha256(f.manifest.raw.chunks);f.pointer.content_sha256=f.manifest.raw.content_sha256;await f.save();await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'transcript_scope_too_large'});expect(f.reads()).toBe(0);
 const tooMany=await fixture(new TextEncoder().encode('{}\n'.repeat(100001)),undefined,1);await expect(canonicalTranscriptSnapshot(tooMany.deps,ID)).rejects.toMatchObject({reason:'transcript_scope_too_large'});
});
test("valid metadata, tools, sidechains and thinking never enter model messages",async()=>{
 const rows=[...messages,{type:'assistant',isMeta:true,message:{role:'assistant',content:'META_NOT_MODEL'}},{type:'assistant',isSidechain:true,sessionId:'child',message:{role:'assistant',content:'CHILD_NOT_MODEL'}},{type:'tool',message:{role:'tool',content:'TOOL_NOT_MODEL'}},{type:'assistant',message:{role:'assistant',content:[{type:'thinking',thinking:'PRIVATE_NOT_MODEL'}]}}];const f=await fixture(new TextEncoder().encode(rows.map(x=>JSON.stringify(x)).join('\n')+'\n'),undefined,rows.length);const snapshot=await canonicalTranscriptSnapshot(f.deps,ID);const brief=briefInputFromTranscript(KEY,[{machineId:'m-1',manifestKey:f.path,revision:1}],snapshot!);expect(brief.kind).toBe('ready');if(brief.kind==='ready'){expect(brief.input.messages).toHaveLength(2);expect(JSON.stringify(brief.input)).not.toContain('NOT_MODEL');}
});

async function nextEpoch(f:Awaited<ReturnType<typeof fixture>>,raw:Uint8Array,reason:'rewritten'|'truncated',records:number){
 const m=f.manifest,seq=m.raw.chunks.length,epoch=m.raw.epoch+1;
 if(raw.length){const stored=new Uint8Array(zstdCompressSync(raw)),digest=hash(raw);await f.store.put(rawChunkKey(LOC,seq,0,digest,'jsonl'),stored);m.raw.chunks.push({seq,epoch,start_offset:0,bytes:raw.length,stored_bytes:stored.length,sha256:digest,sha256_8:digest.slice(0,8),stored_sha256:hash(stored),stored_raw_bytes:raw.length,raw_sha256:digest,compression:'zstd',records});}
 m.revision++;m.raw.epoch=epoch;m.raw.epochs.push({epoch,opened_at:AT,reason,first_seq:seq,opened_by:'m-1'});m.raw.chunk_count=m.raw.chunks.length;m.raw.raw_bytes=m.raw.chunks.reduce((n,c)=>n+c.bytes,0);m.raw.stored_bytes=m.raw.chunks.reduce((n,c)=>n+c.stored_bytes,0);m.raw.next_offset=raw.length;m.raw.content_sha256=await computeContentSha256(m.raw.chunks);f.pointer.revision=m.revision;f.pointer.content_sha256=m.raw.content_sha256;await f.save();
}
test("rewrite and truncation exclude even malformed superseded bytes from live input",async()=>{
 for(const reason of ['rewritten','truncated'] as const){const f=await fixture(new TextEncoder().encode('{"OLD_SUPERSEDED_SECRET":'),undefined,0);await nextEpoch(f,new TextEncoder().encode(messages.map(x=>JSON.stringify(x)).join('\n')+'\n'),reason,2);const s=await canonicalTranscriptSnapshot(f.deps,ID);expect(s?.records).toHaveLength(2);expect(f.reads()).toBe(1);const brief=briefInputFromTranscript(KEY,[{machineId:'m-1',manifestKey:f.path,revision:2}],s!);expect(brief.kind).toBe('ready');expect(JSON.stringify(brief)).not.toContain('OLD_SUPERSEDED_SECRET');}
});
test("a current fragment cannot be completed using a superseded epoch's prefix",async()=>{
 const f=await fixture(new TextEncoder().encode('{"type":"user","message":{"role":"user","content":"'),undefined,0);await nextEpoch(f,new TextEncoder().encode('CROSS_EPOCH_NOT_A_RECORD"}}\n'),'rewritten',1);await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'transcript_json_invalid'});expect(f.reads()).toBe(1);
});
test("truncation to an empty live epoch does not resurrect old messages",async()=>{
 const f=await fixture();await nextEpoch(f,new Uint8Array(),'truncated',0);const s=await canonicalTranscriptSnapshot(f.deps,ID);expect(s?.records).toEqual([]);expect(f.reads()).toBe(0);expect(briefInputFromTranscript(KEY,[{machineId:'m-1',manifestKey:f.path,revision:2}],s!).kind).toBe('empty');
});
test("budget and declared counts describe live chunks while the full historical chain is validated",async()=>{
 const old=new TextEncoder().encode(JSON.stringify({...messages[0],message:{role:'user',content:'OLD'.repeat(6*1024*1024)}})+'\n');const f=await fixture(old,undefined,1);await nextEpoch(f,new TextEncoder().encode(JSON.stringify(messages[1])+'\n'),'rewritten',1);const s=await canonicalTranscriptSnapshot(f.deps,ID);expect(s?.records).toHaveLength(1);expect(f.reads()).toBe(1);
 const tampered=await fixture();await nextEpoch(tampered,new TextEncoder().encode(JSON.stringify(messages[1])+'\n'),'rewritten',1);tampered.manifest.raw.chunks[0]!.sha256='f'.repeat(64);tampered.manifest.raw.chunks[0]!.sha256_8='f'.repeat(8);await tampered.save();await expect(canonicalTranscriptSnapshot(tampered.deps,ID)).rejects.toBeInstanceOf(Error);expect(tampered.reads()).toBe(0);
});


test("JSONL brief input enforces the reader ceiling before chunk hydration", async () => {
 const f=await fixture(); f.manifest.raw.scrub={ruleset:"relayhistory-scrub",version:10}; await f.save();
 await expect(canonicalTranscriptSnapshot({...f.deps,readerMaxRuleset:8},ID)).rejects.toMatchObject({code:"scrub_ruleset_unsupported",retryable:false});
 expect(f.reads()).toBe(0);
 const c=f.manifest.raw.chunks[0]!; const chunkKey=rawChunkKey(LOC,c.seq,c.start_offset,c.sha256,"jsonl");
 const body=new Uint8Array(await new Response(await f.store.get(chunkKey)).arrayBuffer());
 await f.store.put(chunkKey,body,{metadata:{"scrub-ruleset":"relayhistory-scrub","scrub-version":"10"}});
 const result=await canonicalTranscriptSnapshot({...f.deps,readerMaxRuleset:10},ID);
 expect(result?.records).toHaveLength(2);
});


test("unset JSONL policy retains main's version-9 and foreign-family transcript reads",async()=>{
 const f=await fixture();const baseline=await canonicalTranscriptSnapshot(f.deps,ID);
 for(const marker of [{ruleset:"relayhistory-scrub",version:9},{ruleset:"foreign-family",version:8}]){
  f.manifest.raw.scrub=marker;await f.save();
  expect((await canonicalTranscriptSnapshot(f.deps,ID))?.records).toEqual(baseline?.records);
  await expect(canonicalTranscriptSnapshot({...f.deps,readerMaxRuleset:8},ID)).rejects.toMatchObject({code:"scrub_ruleset_unsupported"});
 }
});

/** READ_API_CANONICAL_FETCH_CONCURRENCY (CanonicalDeps.fetchConcurrency, bounded-prefetch.ts) on the JSONL reader. */
async function manyChunks(n=40){
 // n chunks of one record each, the last record split across two chunks so a line still crosses a boundary.
 const rows=Array.from({length:n},(_,i)=>({...messages[i%2]!,uuid:`m${i}`}));const lines=rows.map(x=>JSON.stringify(x)+"\n");
 const raw=new TextEncoder().encode(lines.join(""));const cuts:number[]=[];let at=0;for(const l of lines.slice(0,-1)){at+=new TextEncoder().encode(l).length;cuts.push(at);}cuts.push(at+5);
 return fixture(raw,cuts);
}
function slow(deps:CanonicalDeps,fetchConcurrency?:number,abort?:AbortController,abortAt?:number){
 const state={inFlight:0,peak:0,started:0};
 const store=new Proxy(deps.store,{get(target,prop){const v=Reflect.get(target,prop);if(prop!=='getObject')return typeof v==='function'?v.bind(target):v;
  return async(...args:Parameters<typeof target.getObject>)=>{if(!String(args[0]).endsWith('.zst'))return target.getObject(...args);state.started++;if(state.started===abortAt)abort?.abort();state.peak=Math.max(state.peak,++state.inFlight);try{await new Promise(r=>setTimeout(r,2));return await target.getObject(...args);}finally{state.inFlight--;}};}});
 return {state,deps:{...deps,store,...(fetchConcurrency!==undefined?{fetchConcurrency}:{})} as CanonicalDeps};
}
function watchUnhandled(){const seen:unknown[]=[];const on=(r:unknown)=>{seen.push(r)};process.on("unhandledRejection",on);return {seen,off:()=>process.off("unhandledRejection",on)};}
test("fetchConcurrency 8 over 41 chunks yields the sequential records, at most 8 chunk reads in flight",async()=>{
 const f=await manyChunks();expect(f.manifest.raw.chunks).toHaveLength(41);
 const one=slow(f.deps),par=slow(f.deps,8);
 const sequential=await canonicalTranscriptSnapshot(one.deps,ID);expect(sequential?.records).toHaveLength(40);expect(one.state.peak).toBe(1);
 expect(await canonicalTranscriptSnapshot(par.deps,ID)).toEqual(sequential);expect(par.state.peak).toBeGreaterThan(1);expect(par.state.peak).toBeLessThanOrEqual(8);expect(par.state.started).toBe(41);
 const explicitOne=slow(f.deps,1);expect(await canonicalTranscriptSnapshot(explicitOne.deps,ID)).toEqual(sequential);expect(explicitOne.state.peak).toBe(1);
});
test("fetchConcurrency 8: a digest-mismatched middle chunk still refuses transcript_chunk_digest_mismatch, no unhandled rejection",async()=>{
 const f=await manyChunks();const c=f.manifest.raw.chunks[20]!;await f.store.put(rawChunkKey(LOC,c.seq,c.start_offset,c.sha256,'jsonl'),new Uint8Array([1,2,3]));
 const watch=watchUnhandled();
 try{
  await expect(canonicalTranscriptSnapshot(f.deps,ID)).rejects.toMatchObject({reason:'transcript_chunk_digest_mismatch'});
  const par=slow(f.deps,8);await expect(canonicalTranscriptSnapshot(par.deps,ID)).rejects.toMatchObject({reason:'transcript_chunk_digest_mismatch'});
  expect(par.state.started).toBeLessThanOrEqual(21+7);await new Promise(r=>setTimeout(r,30));expect(par.state.inFlight).toBe(0);expect(watch.seen).toEqual([]);
 }finally{watch.off();}
});
test("fetchConcurrency 8: an aborted signal still stops with the AbortError, starting no chunk read after the abort",async()=>{
 const f=await manyChunks();const watch=watchUnhandled();
 try{
  for(const n of [undefined,8]){
   const controller=new AbortController();const run=slow(f.deps,n,controller,10);
   const error=await canonicalTranscriptSnapshot(run.deps,ID,controller.signal).catch(e=>e);
   expect(error).toBeInstanceOf(DOMException);expect((error as DOMException).name).toBe('AbortError');
   const started=run.state.started;expect(started).toBeLessThanOrEqual(10+(n??1)-1);
   await new Promise(r=>setTimeout(r,30));expect(run.state.started).toBe(started);expect(run.state.inFlight).toBe(0);
  }
  expect(watch.seen).toEqual([]);
 }finally{watch.off();}
});
