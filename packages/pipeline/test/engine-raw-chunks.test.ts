import { afterEach, expect, test } from 'vitest';
import { mkdtemp,rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { zstdDecompressSync } from 'node:zlib';
import { createFsStore } from '../../object-store/src/fs.js';
import { sha256Hex } from '../../object-store/src/crypto.js';
import { readAll, type ObjectStore } from '../src/lib/pipeline/object-layout.js';
import { sealRawChunk as historical, type SealRawChunk } from '../src/lib/pipeline/raw-chunks.js';
import { bindEngineRawSealer } from '../src/lib/pipeline/engine-raw-chunks.js';
import { ScrubberError } from '../src/lib/scrub-engine/scrubber-error.js';
import { activeScrubEngine,resetScrubEngineForTests,sealRawChunk as wrapper,selectScrubEngine } from '../src/lib/scrub-engine/index.js';
import { fixtureSealer,verifiedFixtureEngine } from './support/engine-sealer.js';
const enc=new TextEncoder();
const dirs:string[]=[];
afterEach(async()=>{resetScrubEngineForTests();await Promise.all(dirs.splice(0).map(d=>rm(d,{recursive:true,force:true})));});
async function store(){const root=await mkdtemp(join(tmpdir(),'engine-seal-'));dirs.push(root);const inner=createFsStore({root});return {backend:inner.backend,head:inner.head.bind(inner),get:inner.get.bind(inner),getObject:inner.getObject.bind(inner),list:inner.list.bind(inner),presignGet:inner.presignGet.bind(inner),put:inner.put.bind(inner),delete:inner.delete.bind(inner)} satisfies ObjectStore;}
function input(text='{"text":"VISIBLE"}\n'):SealRawChunk{return {location:{prefix:'96bulls/o',workspaceId:'w',machineId:'m',harness:'h',sessionId:'s',startDate:'2026-11-10'},format:'ndjson',seq:0,epoch:0,startOffset:0,source:enc.encode(text)};}
async function object(store:ObjectStore,key:any){const o=await store.getObject(key);return {body:await readAll(o.body),meta:o.meta.metadata};}
test('engine output and its actual marker/profile are persisted, even unchanged',async()=>{
 for(const unchanged of [false,true]){const s=await store(),f=fixtureSealer(t=>unchanged?t:t.replaceAll('VISIBLE','SEALED'));const i=input(),out=await f.sealer(s,i),o=await object(s,out.key);
 expect(new TextDecoder().decode(zstdDecompressSync(o.body))).toBe(unchanged?'{"text":"VISIBLE"}\n':'{"text":"SEALED"}\n');
 expect(f.state.calls).toBe(1);expect(out.scrub).toEqual(f.engine.marker);expect(o.meta['scrub-profile']).toBe('fixture-v10');expect(o.meta['scrub-version']).toBe('10');}
});
test('private source/location snapshot survives caller mutation and selection attempts while hashing awaits',async()=>{
 const s=await store(),f=fixtureSealer(),i=input(),original=new Uint8Array(i.source);const pending=f.sealer(s,i);
 i.source.fill(0);i.location.sessionId='other';selectScrubEngine({SCRUB_ENGINE:'ts'});
 expect(()=>selectScrubEngine({SCRUB_ENGINE:'wasm-bindgen'})).toThrow();const out=await pending;
 expect(out.entry.sha256).toBe(await sha256Hex(original));expect(f.state.seen[0]).toEqual(original);expect(out.key).toContain('/s=s/');expect(f.state.calls).toBe(1);
});
for(const cause of [new ScrubberError('invalid_json','call_refusal'),new ScrubberError('scrub_admission_unavailable','call_refusal',undefined,true),new Error('private trap detail')])test(`scrub failure writes nothing (${cause.name}/${'code' in cause?cause.code:'unknown'})`,async()=>{
 const s=await store(),f=fixtureSealer();f.engine.scrubRecords=()=>{throw cause;};let puts=0;const counting={...s,put:async(...a:Parameters<ObjectStore['put']>)=>{puts++;return s.put(...a);}};
 await expect(f.sealer(counting,input())).rejects.toMatchObject(cause instanceof ScrubberError?{code:cause.code}:{code:'scrubber_instance_poisoned',retryable:true});expect(puts).toBe(0);
 if(!(cause instanceof ScrubberError)){f.engine.scrubRecords=()=>({scrubbed:enc.encode('x'),records:1,scrub:f.engine.marker});await expect(f.sealer(s,input())).rejects.toMatchObject({retryable:true});}
});
test('failed instance at capture or during invocation refuses without writes',async()=>{
 const s=await store(),f=fixtureSealer();f.state.failed=true;expect(()=>bindEngineRawSealer(f.engine,{retained:'same-family-up-to-writer'})).toThrow(ScrubberError);f.state.failed=false;
 f.engine.scrubRecords=()=>{f.state.failed=true;return {scrubbed:enc.encode('x'),records:1,scrub:f.engine.marker};};await expect(f.sealer(s,input())).rejects.toMatchObject({retryable:true});expect((await s.list('96bulls' as never)).items).toHaveLength(0);
});
test('result marker mismatch poisons binding and never persists',async()=>{
 const s=await store(),f=fixtureSealer();f.engine.scrubRecords=()=>({scrubbed:enc.encode('x'),records:1,scrub:{...f.engine.marker,version:8}});await expect(f.sealer(s,input())).rejects.toMatchObject({code:'scrubber_instance_poisoned'});expect((await s.list('96bulls' as never)).items).toHaveLength(0);
});
for(const [name,source] of [['empty',new Uint8Array()],['malformed',new Uint8Array([255])],['shared',new Uint8Array(new SharedArrayBuffer(2))]] as const)test(`source ${name} refuses before engine or writes`,async()=>{
 const s=await store(),f=fixtureSealer();await expect(f.sealer(s,{...input(),source})).rejects.toBeDefined();expect(f.state.calls).toBe(0);expect((await s.list('96bulls' as never)).items).toHaveLength(0);
});
for(const text of ['\n',' \r\n','\u2003\n'])test(`blank-only stream is a call refusal, not shared-instance poison (${JSON.stringify(text)})`,async()=>{
 const s=await store(),f=fixtureSealer();await expect(f.sealer(s,input(text))).rejects.toMatchObject({code:'chunk_empty',retryable:false});expect(f.state.calls).toBe(0);expect(f.engine.failed).toBe(false);await expect(f.sealer(s,input())).resolves.toMatchObject({created:true});
});
for(const [name,metadata,code] of [
 ['legacy8',{'scrub-version':'8'},null],['same10',{'scrub-version':'10','scrub-profile':'old-profile'},null],
 ['newer',{'scrub-version':'11'},'chunk_stored_newer'],['foreign',{'scrub-ruleset':'other'},'chunk_stored_foreign'],
 ['missing',null,'chunk_stored_unscrubbed'],['bad-hash',{'raw-sha256':'no'},'chunk_stored_unscrubbed'],['bad-size',{'raw-bytes':'NaN'},'chunk_stored_unscrubbed'],['bad-version',{'scrub-version':'0'},'chunk_stored_unscrubbed'],
 ['exponent-version',{'scrub-version':'1e1'},'chunk_stored_unscrubbed'],['hex-version',{'scrub-version':'0xa'},'chunk_stored_unscrubbed'],['leading-zero-version',{'scrub-version':'010'},'chunk_stored_unscrubbed'],['exponent-size',{'raw-bytes':'1e1'},'chunk_stored_unscrubbed'],
] as const)test(`retained ${name} is described from same GET, never overwritten/relabelled`,async()=>{
 const s=await store(),f=fixtureSealer(),i=input(),out=await f.sealer(s,i),old=enc.encode('different compressed historical bytes');await s.delete(out.key as never);
 await s.put(out.key as never,old,{ifNoneMatch:true,metadata:metadata===null?{}:Object.assign({'raw-sha256':'a'.repeat(64),'raw-bytes':'7','scrub-ruleset':'relayhistory-scrub','scrub-version':'8'},metadata)});
 if(code)await expect(f.sealer(s,i)).rejects.toMatchObject({code});else{const retained=await f.sealer(s,i);expect(retained.created).toBe(false);expect(retained.scrub.version).toBe(Number(metadata!['scrub-version']));expect(retained.scrub.profile).toBe('scrub-profile' in metadata!?metadata['scrub-profile']:undefined);expect(retained.entry.stored_sha256).toBe(await sha256Hex(old));expect(retained.entry.stored_bytes).toBe(old.length);}
 expect((await object(s,out.key)).body).toEqual(old);expect(f.state.calls).toBe(2);
});
test('lost put acknowledgement retries against truthful retained object',async()=>{
 const s=await store(),f=fixtureSealer();let once=true;const faulty={...s,async put(...args:Parameters<ObjectStore['put']>){const ack=await s.put(...args);if(once){once=false;throw Error('ack lost');}return ack;}};
 await expect(f.sealer(faulty,input())).rejects.toThrow('ack lost');const result=await f.sealer(faulty,input());expect(result.created).toBe(false);expect(result.scrub).toEqual(f.engine.marker);expect(f.state.calls).toBe(2);
});
for(const text of ['', 'plain unchanged\n','{"text":"é 😀"}\r\n','{"text":"password=example-secret-value"}\n',String.fromCharCode(255)])test(`default wrapper equals frozen sealer bytes/metadata/entry (${JSON.stringify(text)})`,async()=>{
 const a=await store(),b=await store();selectScrubEngine({});const i=input(text);
 let left:any,right:any;try{left=await historical(a,i);}catch(e){left=e;}try{right=await wrapper(b,i);}catch(e){right=e;}
 if(left instanceof Error){expect(right).toMatchObject({name:left.name,message:left.message});return;}
 expect(right).toEqual(left);expect(await object(a,left.key)).toEqual(await object(b,(right as any).key));expect(await wrapper(b,i)).toEqual(await historical(a,i));
});
test('competing create acknowledgement describes the winner without comparing new-engine bytes',async()=>{
 const s=await store(),f=fixtureSealer(),i=input();let puts=0;
 const race={...s,async put(...args:Parameters<ObjectStore['put']>){puts++;await s.put(args[0],enc.encode('old stored body'),{ifNoneMatch:true,metadata:{'raw-sha256':'c'.repeat(64),'raw-bytes':'12','scrub-ruleset':'relayhistory-scrub','scrub-version':'8'}});return {key:args[0],version:{eTag:'winner'},size:15,contentSha256:'d'.repeat(64),created:false};}};
 const result=await f.sealer(race,i);expect(result.created).toBe(false);expect(result.scrub).toEqual({ruleset:'relayhistory-scrub',version:8});expect(result.entry.stored_sha256).toBe(await sha256Hex(enc.encode('old stored body')));expect(puts).toBe(1);expect(f.state.calls).toBe(1);
});
test('malformed output refuses and poisons the binding rather than mislabelling an engine defect as input refusal',async()=>{
 const s=await store(),f=fixtureSealer();f.engine.scrubRecords=()=>({scrubbed:new Uint8Array([255]),records:1,scrub:f.engine.marker});
 await expect(f.sealer(s,input())).rejects.toMatchObject({code:'scrubber_instance_poisoned',retryable:true});expect((await s.list('96bulls' as never)).items).toHaveLength(0);
});
test('stream size admission precedes hashing or engine calls',async()=>{
 const s=await store(),f=fixtureSealer();await expect(f.sealer(s,{...input(),source:new Uint8Array(64*1024*1024+1)})).rejects.toMatchObject({code:'scrub_input_too_large'});expect(f.state.calls).toBe(0);
});
test('retained reads are bounded even if object metadata claims a tiny body',async()=>{
 const s=await store(),f=fixtureSealer(),i=input(),first=await f.sealer(s,i);let cancelled=false;
 const oversized={...s,async getObject(key:any){const o=await s.getObject(key);return {...o,body:new ReadableStream<Uint8Array>({start(c){c.enqueue(new Uint8Array(64*1024*1024+4097));},cancel(){cancelled=true;}})};}};
 await expect(f.sealer(oversized,i)).rejects.toMatchObject({code:'chunk_stored_too_large'});expect(cancelled).toBe(true);expect((await object(s,first.key)).meta['scrub-version']).toBe('10');
});
test('typed poison is sticky even when an engine incorrectly leaves failed=false',async()=>{
 const s=await store(),f=fixtureSealer();f.engine.scrubRecords=()=>{throw new ScrubberError('scrubber_engine_failure','instance_poison');};
 await expect(f.sealer(s,input())).rejects.toMatchObject({code:'scrubber_instance_poisoned',retryable:true});
 f.engine.scrubRecords=()=>({scrubbed:enc.encode('x'),records:1,scrub:f.engine.marker});
 await expect(f.sealer(s,input())).rejects.toMatchObject({code:'scrubber_instance_poisoned'});
});
test('correct output expansion beyond the input record limit is preserved',async()=>{
 const s=await store(),f=fixtureSealer(()=> 'x'.repeat(16*1024*1024+1)+'\n');const out=await f.sealer(s,input());expect(out.entry.stored_raw_bytes).toBe(16*1024*1024+2);expect(f.state.failed).toBe(false);
});
test('output exceeding the staging bound refuses permanently and leaves a healthy engine reusable',async()=>{
 const s=await store(),f=fixtureSealer();f.engine.scrubRecords=()=>({scrubbed:new Uint8Array(64*1024*1024+1),records:1,scrub:f.engine.marker});
 await expect(f.sealer(s,input())).rejects.toMatchObject({code:'scrub_output_too_large',retryable:false});
 f.engine.scrubRecords=()=>({scrubbed:enc.encode('x'),records:1,scrub:f.engine.marker});await expect(f.sealer(s,input())).resolves.toMatchObject({created:true});
});

for(const [text,recordCount] of [['',1],['x',0],['',0]] as const)test(`invalid empty result poisons the shared active instance (${text.length}/${recordCount})`,async()=>{
 const s=await store(),engine=await verifiedFixtureEngine(()=>({text,recordCount}));
 const first=bindEngineRawSealer(engine,{retained:'same-family-up-to-writer'}),other=bindEngineRawSealer(engine,{retained:'same-family-up-to-writer'});
 expect(activeScrubEngine()).toBe(engine);
 await expect(first(s,input())).rejects.toMatchObject({code:'scrubber_instance_poisoned'});
 expect(engine.failed).toBe(true);expect(()=>activeScrubEngine()).toThrow(ScrubberError);expect(()=>engine.scrubText('later')).toThrow(ScrubberError);
 await expect(other(s,input())).rejects.toMatchObject({code:'scrubber_instance_poisoned'});expect((await s.list('96bulls' as never)).items).toHaveLength(0);
});

/** Eight valid JSON records, all below the per-record limit. Their UTF-8
 * payloads straddle the admission decoder's 65,536-byte read boundary. */
function aggregateBytes(size:number,scalar:string):Uint8Array {
 const bytes=new Uint8Array(size),prefix=enc.encode('{"text":"'),suffix=enc.encode('"}\n'),fill=enc.encode(scalar.repeat(32768));let offset=0;
 for(let record=0;record<8;record++){
  const length=Math.floor(size/8)+(record<size%8?1:0);bytes.set(prefix,offset);let at=offset+prefix.length;const end=offset+length-suffix.length;
  while(end-at>=fill.length){bytes.set(fill,at);at+=fill.length;}
  const whole=Math.floor((end-at)/enc.encode(scalar).length)*enc.encode(scalar).length;bytes.set(fill.subarray(0,whole),at);at+=whole;bytes.fill(0x61,at,end);bytes.set(suffix,end);offset+=length;
 }
 return bytes;
}
async function countingStore(){const s=await store();let puts=0;return {store:{...s,async put(key:any,body:any){puts++;return {key,version:{eTag:'fixture'},size:body.byteLength,contentSha256:await sha256Hex(body),created:true};}} as ObjectStore,puts:()=>puts};}
const LIMIT=64*1024*1024;
for(const scalar of ['é','😀'])for(const delta of [-1,0,1]){
 test(`input aggregate ${scalar.length===1?2:4}-byte fill limit${delta>=0?'+':''}${delta}`,async()=>{
  const s=await countingStore(),f=fixtureSealer(),bytes=aggregateBytes(LIMIT+delta,scalar);let calls=0;
  f.engine.scrubRecords=source=>{calls++;return {scrubbed:source,records:8,scrub:f.engine.marker};};
  expect(()=>new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes.subarray(0,65536))).toThrow();
  if(delta>0){await expect(f.sealer(s.store,{...input(),source:bytes})).rejects.toMatchObject({code:'scrub_input_too_large',retryable:false});expect(calls).toBe(0);expect(s.puts()).toBe(0);}
  else{await expect(f.sealer(s.store,{...input(),source:bytes})).resolves.toMatchObject({entry:{stored_raw_bytes:LIMIT+delta,records:8}});expect(calls).toBe(1);expect(s.puts()).toBe(1);}
 },30_000);
 test(`output aggregate ${scalar.length===1?2:4}-byte fill limit${delta>=0?'+':''}${delta}`,async()=>{
  const s=await countingStore(),f=fixtureSealer(),bytes=aggregateBytes(LIMIT+delta,scalar);
  f.engine.scrubRecords=()=>({scrubbed:bytes,records:8,scrub:f.engine.marker});
  expect(()=>new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes.subarray(0,65536))).toThrow();
  if(delta>0){await expect(f.sealer(s.store,input())).rejects.toMatchObject({code:'scrub_output_too_large',retryable:false});expect(s.puts()).toBe(0);}
  else{await expect(f.sealer(s.store,input())).resolves.toMatchObject({entry:{stored_raw_bytes:LIMIT+delta}});expect(s.puts()).toBe(1);}
  expect(f.engine.failed).toBe(false);
 },30_000);
}
for(const scalar of ['é','😀'])for(const side of ['input','output'] as const)test(`${side} ending in a split ${scalar.length===1?2:4}-byte codepoint refuses at the aggregate limit`,async()=>{
 const s=await countingStore(),f=fixtureSealer(),bytes=aggregateBytes(LIMIT,scalar),partial=enc.encode(scalar).subarray(0,enc.encode(scalar).length-1);bytes.set(partial,bytes.length-partial.length);
 if(side==='output')f.engine.scrubRecords=()=>({scrubbed:bytes,records:8,scrub:f.engine.marker});
 await expect(f.sealer(s.store,side==='input'?{...input(),source:bytes}:input())).rejects.toMatchObject(side==='input'?{code:'chunk_not_utf8',retryable:false}:{code:'scrubber_instance_poisoned',retryable:true});expect(s.puts()).toBe(0);
},30_000);

test('retained whitespace version from GET is not leniently parsed',async()=>{
 const s=await store(),f=fixtureSealer(),i=input();await f.sealer(s,i);const changed={...s,async getObject(key:any){const o=await s.getObject(key);return {...o,meta:{...o.meta,metadata:{...o.meta.metadata,'scrub-version':' 10'}}};}};await expect(f.sealer(changed,i)).rejects.toMatchObject({code:'chunk_stored_unscrubbed'});
});
