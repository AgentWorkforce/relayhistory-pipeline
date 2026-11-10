/** Real writer ports with an explicit distinguishable test engine, never env cutover. */
import { afterEach,expect,test } from 'bun:test';
import { Hono } from 'hono';
import { zstdDecompressSync } from 'node:zlib';
import { fixtureSealer } from '../../../packages/pipeline/test/support/engine-sealer.ts';
import { createPipelineRoutes } from '../../../packages/pipeline/src/routes/pipeline.ts';
import type { AppEnv } from '../../../packages/pipeline/src/env.ts';
import { readAll,objectKey,type ObjectStore } from '../../../packages/object-store/src/index.ts';
import { ScrubberError } from '../../../packages/pipeline/src/lib/scrub-engine/scrubber-error.ts';
import { writeGroups } from '../writer.ts';
import { preparePublicationImage,writePublicationImage } from '../publication-image.ts';
import { parseReceiptSeatSnapshot } from '../publication-seats.ts';
import { exportUnit } from '../../migration/neon-s3-sync/export-session.ts';
import { scratchStore } from './support.ts';
const enc=new TextEncoder(),HEX='a'.repeat(64),OTHER='b'.repeat(64);
const cleanup:(()=>void)[]=[];
afterEach(()=>{for(const f of cleanup.splice(0))f();});
function scratch(){const s=scratchStore();cleanup.push(s.done);return s.store;}
const line=JSON.stringify({kind:'session_event',origin_id:'origin-a',record_id:'native-r',revision:'1',operation:'upsert',received_at:'2026-11-04T01:00:00Z',payload:{text:'VISIBLE',ts_ms:1762218000000}});
function publication(){
 const record={ord:0,source:'claude',session_id:'session:1',record_id:'native-r',revision_id:'revision-1',revision:1,digest:HEX};
 const stage={key:'unused',sha256:HEX,bytes:1,groupOrdinals:[[0]]};
 const receiptSeats=parseReceiptSeatSnapshot({controlEpoch:HEX,sourceIdentitySha256:OTHER,allocatedHighWater:'9007199254740993',lastAssignedSeat:'9007199254740999',records:[{ord:0,seat:'9007199254740994',bodyAccepted:true,candidateDigest:HEX,firstDigest:HEX,resetEpoch:'0',fenceEpoch:'2'}]},[record],stage);
 const generation='12345678-1234-1234-1234-123456789abc',attemptToken='abcdefab-1234-1234-1234-123456789abc';
 const head={groupIndex:0,ordinals:[0],generation,prefix:`96bulls/org-a/upload-generations/g=${generation}`,headVersion:'1',revision:'1',previousKey:null,previousRawSha256:null,basePointer:null,source:'claude',sessionId:'session:1',physicalWorkspace:'ws-a',physicalSource:'claude',physicalSession:'session_1',machineId:'srv_fixture',fenceEpoch:'2',resetEpoch:'0'};
 const ticket={schemaGeneration:HEX,attemptToken,leaseToken:'11111111-1111-1111-1111-111111111111',leaseUntil:'2026-11-05T00:05:00.000000Z',stageToken:'22222222-2222-2222-2222-222222222222',stageVersion:'1',outboxVersion:'1',tenant:{orgId:'org-a',workspaceId:'ws-a',userId:'user-a'},originId:'origin-a',batchId:'batch-a',batchDigest:HEX,claimToken:'33333333-3333-3333-3333-333333333333',stage,records:[record],receiptSeats,acceptedOrdinals:[0],heads:[head]};
 const decoded={header:{v:1 as const,publication_state:'buffered' as const,claim_token:ticket.claimToken,batch_digest:HEX,received_at:'2026-11-04T01:00:00.000Z',org_id:'org-a',workspace_id:'ws-a',origin_id:'origin-a',user_id:'user-a',groups:[['claude','session:1'] as [string,string]]},groups:[{source:'claude',sessionId:'session:1',lines:[line]}],spec:stage};
 const target={bucket:'fixture',region:'us-east-1',keyRoot:'96bulls',generation:HEX};
 return {ticket,head,decoded,target};
}
async function packed(s:ObjectStore,key:string){const o=await s.getObject(objectKey(key));return {text:new TextDecoder().decode(zstdDecompressSync(await readAll(o.body))),meta:o.meta.metadata};}
for(const poisoned of [false,true])for(const caller of ['http','writer','publication','export'] as const)test(`${caller}: bound output/provenance or retryable poison, no fallback (${poisoned})`,async()=>{
 const s=scratch(),f=fixtureSealer();
 if(poisoned)f.engine.scrubRecords=()=>{f.state.failed=true;throw new ScrubberError('scrubber_instance_poisoned','instance_poison');};
 let result:Promise<unknown>;
 let key:string|undefined;
 if(caller==='http'){
  const app=new Hono<AppEnv>();app.use('*',async(c,next)=>{c.set('auth' as never,{orgId:'org-a',workspaceId:'ws-a',scopes:['rth:sync']} as never);c.set('correlationId','corr');await next();});
  app.route('/v1',createPipelineRoutes({rawChunkSealer:f.sealer,rawStore:()=>({store:s,provider:'s3',bucket:'fixture'}),sink:()=>({record:async()=>{}}) as never,deadLetters:()=>({}) as never,send:async()=>{}}));
  const response=await app.request('/v1/pipeline/chunks?machine=m&harness=claude&session=s&date=2026-11-04&seq=0&epoch=0&offset=0&format=ndjson',{method:'POST',body:line+'\n'},{PIPELINE_ORG_ID:'org-a'});
  expect(response.status).toBe(poisoned?503:201);const body=await response.json() as any;
  if(poisoned){expect((await s.list(objectKey('v1'))).items.length).toBe(0);return;}key=body.key;expect(body.scrub.profile).toBe('fixture-v10');
 }else{
  if(caller==='writer')result=writeGroups({store:s,keyRoot:'96bulls',rawChunkSealer:f.sealer},{orgId:'org-a',workspaceId:'ws-a',userId:'user-a',originId:'origin-a',batchDigest:HEX},[{source:'claude',sessionId:'session:1',lines:[line]}],new Date('2026-11-04T01:00:00Z')).then(r=>{key=r.parts[0]!.chunkKey;expect(r.manifests[0]!.manifest.raw.scrub!.version).toBe(10);});
  else if(caller==='publication'){
   const p=publication(),signal=new AbortController().signal;
   result=preparePublicationImage(p.ticket,p.head,p.decoded,p.target,s,signal,undefined,f.sealer).then(async image=>{
    expect(new TextDecoder().decode(zstdDecompressSync(image.newChunk.body))).toContain('SEALED');
    expect(image.newChunk.options?.metadata?.['scrub-profile']).toBe('fixture-v10');
    // Image construction captures bytes/options; no durable put precedes prepare/register.
    expect((await s.list(objectKey('96bulls'))).items.length).toBe(0);
    const order:string[]=[];const rpc={prepare:async()=>{order.push('prepare');},register:async()=>{order.push('register');}};
    await writePublicationImage(p.ticket,image,rpc as never,s,signal,p.target);expect(order[0]).toBe('prepare');key=image.newChunk.spec.key;
   });
  }else{
   const before=process.env.S3X_OBLIGATIONS;process.env.S3X_OBLIGATIONS='off';
   result=exportUnit({store:s,keyRoot:'96bulls',grantId:'test-grant',rawChunkSealer:f.sealer},{orgId:'org-a',workspaceId:'ws-a',source:'claude',sessionId:'session:1',firstMs:Date.parse('2026-11-04T01:00:00Z'),lastMs:Date.parse('2026-11-04T01:01:00Z'),machineIds:[],totals:{event_count:'1',models:[]}},{fenced:async()=>false,async *lines(){yield {machine:'machine',bytes:enc.encode(line+'\n'),userId:'user-a'};}}).then(async r=>{
    expect(r.manifests[0]!.chunksSealed).toBe(1);const raw=r.manifests[0]!.referenced.find(k=>k.includes('/raw/'));expect(raw).toBeDefined();key=raw;
    const m=JSON.parse(new TextDecoder().decode(await readAll(await s.get(objectKey(r.manifests[0]!.manifestKey)))));expect(m.raw.scrub.version).toBe(10);
   }).finally(()=>{if(before===undefined)delete process.env.S3X_OBLIGATIONS;else process.env.S3X_OBLIGATIONS=before;});
  }
  if(poisoned){await expect(result!).rejects.toMatchObject({code:'scrubber_instance_poisoned',retryable:true});const remaining=(await s.list(objectKey('96bulls'))).items;expect(remaining.every(o=>o.key.includes('/live-sessions/'))).toBe(true);return;}
  await result!;
 }
 expect(f.state.calls).toBe(1);expect(key).toBeDefined();const o=await packed(s,key!);expect(o.text).toContain('SEALED');expect(o.text).not.toContain('VISIBLE');expect(o.meta['scrub-version']).toBe('10');expect(o.meta['scrub-profile']).toBe('fixture-v10');
});
