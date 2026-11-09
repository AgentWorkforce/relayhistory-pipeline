import {afterEach,beforeEach,expect,spyOn,test} from 'bun:test';
import {BufferedGcDiagnostics,BufferedGcError,bufferedGcTick,collectBufferedTicket,sqlBufferedGcLedger} from '../buffered-gc.ts';
import {runBufferedGcJob,type GcJobOverrides} from '../buffered-gc-job.ts';
import {handler} from '../buffered-gc-lambda.ts';
import {SemanticPostgresExecutor} from '../../search-features/semantic-copy-postgres.ts';
import {derivedSchemaAssets} from '../../session-pipeline/derived-schema.ts';
import type {BufferedGcTicket} from '../buffered-protocol.ts';

const uuid=(n:number)=>'00000000-0000-4000-8000-'+String(n).padStart(12,'0');
const ticket=():BufferedGcTicket=>({key:'96bulls/synthetic/buffered-batches/v1/ws=w/m=srv_'+'0'.repeat(32)+'/b='+'1'.repeat(32)+'-'+'2'.repeat(16)+'-'+'3'.repeat(32)+'.staged.ndjson.zst',sha256:'a'.repeat(64),bytes:10,groupOrdinals:[[0]],stageToken:uuid(1),claimToken:uuid(2),leaseToken:uuid(3),rowVersion:'2',resetEpoch:'0',receiptState:'acknowledged',phase:'cleanup',cleanupState:'retired',keepOrdinals:[],keepGroupIndexes:[]});
const ACCOUNT='111122223333',REF='abcdefghijklmnopqrst';
const env={INGEST_GC_AWS_ACCOUNT_ID:ACCOUNT,INGEST_GC_SUPABASE_PROJECT_REF:REF,AWS_REGION:'us-east-1',HOOPDB_DB_MODE:'supabase',INGEST_GC_BUCKET:'synthetic-bucket',INGEST_GC_KMS_KEY_ARN:`arn:aws:kms:us-east-1:${ACCOUNT}:key/11111111-1111-4111-8111-111111111111`,INGEST_GC_SECRET_ARN:`arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:96bulls/prod/buffered-gc-abcdef`,INGEST_GC_EXPECT_GENERATION:'a'.repeat(64)};
const privateMarker='synthetic-private-diagnostic-canary';
const url=`postgresql://bulls96_session_ingest.${REF}:${privateMarker}@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require`;
const assets=async()=>({expected:{names:['synthetic'],generation:env.INGEST_GC_EXPECT_GENERATION},verifiers:{searchWorker:'SELECT 1;',tokenVerifier:'SELECT 2;',ingestSeeder:'SELECT 3;',ingest:'SELECT 4;'}});
const error=()=>Object.assign(new Error(privateMarker),{code:'57014',phase:privateMarker,GcPhase:privateMarker});
let restore:Array<()=>void>=[];
beforeEach(()=>{
 // Test process is launched with a clean, credential-free environment. These are synthetic RAM-only fixtures.
 for(const [key,value] of Object.entries({AWS_ACCESS_KEY_ID:'ASIA'+'A'.repeat(16),AWS_SECRET_ACCESS_KEY:privateMarker,AWS_SESSION_TOKEN:privateMarker})){
  const old=process.env[key];process.env[key]=value;restore.push(()=>{if(old===undefined)delete process.env[key];else process.env[key]=old});
 }
 const fetcher=spyOn(globalThis,'fetch').mockImplementation(async()=>{throw Error('network forbidden')});restore.push(()=>fetcher.mockRestore());
});
afterEach(()=>{for(const reset of restore.reverse())reset();restore=[]});
function queries(failAt?:string,failure=error()){
 const calls:string[]=[];
 const query=spyOn(SemanticPostgresExecutor.prototype,'queryBounded').mockImplementation(async(sql:string)=>{
  calls.push(sql);
  const phase=sql.startsWith('SELECT current_user')?'role_check':sql.startsWith('SET LOCAL hoopdb.db_mode')?'readiness':sql.includes('ingest_buffer_gc_claim')?'claim':'other';
  if(phase===failAt)throw failure;
  return {rows:phase==='role_check'?[{me:'bulls96_session_ingest',session:'bulls96_session_ingest'}]:phase==='claim'?[{result:{tickets:[]}}]:[]};
 });restore.push(()=>query.mockRestore());return calls;
}
function factories(end:()=>Promise<void>=async()=>{}):GcJobOverrides{
 return {assets,secret:async()=>JSON.stringify({hoopdb_url:url}),pool:()=>({options:{connectionTimeoutMillis:1000},on(){},end} as never),store:()=>({} as never),eraser:()=>({} as never)};
}

test('phase is invocation-owned, finite, ignores hostile error metadata and survives normal cleanup',()=>{
 const first=new BufferedGcDiagnostics(),second=new BufferedGcDiagnostics(),e=error();
 Object.defineProperty(e,'diagnosticPhase',{get(){throw Error('must not inspect error')}});
 first.phase('claim');first.capture(e);first.phase('cleanup');first.capture(e);
 second.phase('readiness');second.capture(e);
 expect(first.failurePhase(e)).toBe('claim');expect(second.failurePhase(e)).toBe('readiness');
 expect(first.failurePhase(new Error())).toBe('unknown');
 const hostile={toString(){throw Error('must not serialize phase')}};
 for(const value of [privateMarker,hostile,null,undefined,123]){
  const d=new BufferedGcDiagnostics();d.phase(value);d.capture(value);expect(d.failurePhase(value)).toBe('unknown');
 }
 const d=new BufferedGcDiagnostics();(d as any).current=privateMarker;(d as any).failures=new Map([[e,privateMarker]]);
 d.capture(e);expect(d.failurePhase(e)).toBe('unknown');
});
test('each fixed ledger RPC retains its exact wrapped code/message and distinct phase',async()=>{
 for(const phase of ['claim','prepare','swap','finish'] as const){
  const d=new BufferedGcDiagnostics(),ledger=sqlBufferedGcLedger({query:async()=>{throw error()}},'a'.repeat(64),d),t=ticket();
  try{
   await (phase==='claim'?ledger.claim(1):phase==='prepare'?ledger.prepare(t,t):phase==='swap'?ledger.swap(t,null):ledger.finish(t,'barrier-version'));
   throw Error('must fail');
  }catch(e){
   expect(e).toBeInstanceOf(BufferedGcError);expect((e as BufferedGcError).code).toBe('sql_57014');
   expect((e as Error).message).toBe('buffer gc refused: sql_57014');d.capture(e);expect(d.failurePhase(e)).toBe(phase);
  }
 }
});
test('job preserves raw role/readiness errors and captures their phase before pool cleanup',async()=>{
 for(const phase of ['role_check','readiness']){
  const d=new BufferedGcDiagnostics(),e=error();let closed=0;queries(phase,e);
  await expect(runBufferedGcJob(env,factories(async()=>{closed++}),d)).rejects.toBe(e);
  expect(e.message).toBe(privateMarker);expect(e.code).toBe('57014');expect(d.failurePhase(e)).toBe(phase);expect(closed).toBe(1);
 }
});
test('job claim timeout propagates with claim phase and the original wrapping',async()=>{
 const d=new BufferedGcDiagnostics();const calls=queries('claim');
 try{await runBufferedGcJob(env,factories(),d);throw Error('must fail')}
 catch(e){expect(e).toMatchObject({code:'sql_57014',message:'buffer gc refused: sql_57014'});expect(d.failurePhase(e)).toBe('claim')}
 expect(calls.filter(s=>s.includes('ingest_buffer_gc_claim'))).toHaveLength(1);
});
test('config/assets/secret/pool/store setup failures do not become readiness or claim failures',async()=>{
 for(const phase of ['config','assets','secret','pool','store_setup'] as const){
  const d=new BufferedGcDiagnostics(),e=error(),f=factories();queries();
  let input:NodeJS.ProcessEnv=env;
  if(phase==='config')input={...env,AWS_REGION:'elsewhere'};
  if(phase==='assets')f.assets=async()=>{throw e};
  if(phase==='secret')f.secret=async()=>{throw e};
  if(phase==='pool')f.pool=()=>{throw e};
  if(phase==='store_setup')f.store=()=>{throw e};
  try{await runBufferedGcJob(input,f,d);throw Error('must fail')}
  catch(caught){expect(d.failurePhase(caught)).toBe(phase);if(phase!=='config')expect(caught).toBe(e)}
 }
});
test('pool-end failure keeps the existing thrown outcome while retaining both bounded phase associations',async()=>{
 const d=new BufferedGcDiagnostics(),primary=error(),cleanup=error();queries('role_check',primary);
 await expect(runBufferedGcJob(env,factories(async()=>{throw cleanup}),d)).rejects.toBe(cleanup);
 expect(d.failurePhase(primary)).toBe('role_check');expect(d.failurePhase(cleanup)).toBe('cleanup');
});
test('collection returns to collect phase after successful swap; object errors are unchanged',async()=>{
 const d=new BufferedGcDiagnostics(),t={...ticket(),phase:'plan' as const,cleanupState:'live' as const},e=error();
 const ledger=sqlBufferedGcLedger({query:async()=>({rows:[{result:{swapped:true}}]})},'a'.repeat(64),d);
 try{await collectBufferedTicket(t,{ledger,diagnostics:d,keyRoot:'96bulls',store:{getObject:async()=>{throw e}} as never,eraser:{} as never});throw Error('must fail')}
 catch(caught){expect(caught).toBe(e);d.capture(caught);expect(d.failurePhase(caught)).toBe('collect')}
});
test('tick still defers known collect errors and fails every claim error; all claims stay one ticket',async()=>{
 const d=new BufferedGcDiagnostics(),limits:number[]=[],t=ticket();
 const ledger={claim:async(n:number)=>{limits.push(n);return[t]},prepare:async()=>'',swap:async()=>true,finish:async()=>true};
 const deps={ledger,diagnostics:d,keyRoot:'96bulls' as const,store:{getObject:async()=>{throw new BufferedGcError('sql_57014')}} as never,eraser:{} as never};
 expect(await bufferedGcTick(deps,2)).toEqual({examined:2,finished:0,deferred:2});expect(limits).toEqual([1,1]);
 const e=new BufferedGcError('sql_57014');ledger.claim=async()=>{throw e};
 await expect(bufferedGcTick(deps,2)).rejects.toBe(e);d.capture(e);expect(d.failurePhase(e)).toBe('claim');
});
async function handlerEnv(){
 const generation=(await derivedSchemaAssets()).expected.generation;
 for(const [key,value]of Object.entries({...env,INGEST_GC_EXPECT_GENERATION:generation,INGEST_GC_ENABLED:'true'})){
  const old=process.env[key];process.env[key]=value;restore.push(()=>{if(old===undefined)delete process.env[key];else process.env[key]=old});
 }
 const f=spyOn(globalThis,'fetch').mockImplementation(async()=>new Response(JSON.stringify({SecretString:JSON.stringify({hoopdb_url:url})})));restore.push(()=>f.mockRestore());return f;
}
test('actual handler logs only a fixed phase and preserves raw/wrapped terminal messages',async()=>{
 await handlerEnv();const logs:string[]=[];const log=spyOn(console,'log').mockImplementation((s:string)=>{logs.push(s)});restore.push(()=>log.mockRestore());
 for(const phase of ['role_check','readiness','claim']){
  queries(phase);await expect(handler({diagnostics:{phase:privateMarker},overrides:{}})).rejects.toThrow(phase==='claim'?'buffered_gc_sql_57014':'buffered_gc_57014');
  const entry=JSON.parse(logs.at(-1)!);expect(entry).toMatchObject({Healthy:0,failed:1,GcPhase:phase});
  expect(entry._aws.CloudWatchMetrics[0].Dimensions).toEqual([['FunctionName']]);
  expect(logs.at(-1)).not.toContain(privateMarker);expect(logs.at(-1)).not.toContain('SELECT');expect(logs.at(-1)).not.toContain('hoopdb_url');
 }
});
test('successful handler keeps the old return/count shape without a failure phase',async()=>{
 const fetcher=await handlerEnv();queries();const logs:string[]=[];const log=spyOn(console,'log').mockImplementation((s:string)=>logs.push(s));restore.push(()=>log.mockRestore());
 expect(await handler()).toEqual({enabled:true,examined:0,finished:0,deferred:0});
 const entry=JSON.parse(logs.at(-1)!);expect(entry).toMatchObject({Healthy:1,examined:0,finished:0,deferred:0});expect(entry.GcPhase).toBeUndefined();expect(fetcher).toHaveBeenCalledTimes(1);
});
test('default-off handler does not accept invocation phase/factory controls or emit telemetry',async()=>{
 const old=process.env.INGEST_GC_ENABLED;delete process.env.INGEST_GC_ENABLED;restore.push(()=>{if(old!==undefined)process.env.INGEST_GC_ENABLED=old});
 const log=spyOn(console,'log').mockImplementation(()=>{throw Error('must not log')});restore.push(()=>log.mockRestore());
 expect(await handler({env:{INGEST_GC_ENABLED:'true'},phase:privateMarker,factories:{}})).toEqual({enabled:false});expect(log).not.toHaveBeenCalled();
});
