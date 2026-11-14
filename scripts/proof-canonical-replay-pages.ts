/** Offline synthetic capacity proof. PGlite + local fs only, no environment credentials. */
import {mkdir,writeFile,readdir} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {replayDatabase,exportReplayFixture,createFsStore,REPLAY_FIXTURE_IDENTITY as identity} from '../src/session-pipeline/__tests__/replay-fixture.ts';
import {objectKey,readAll} from '../packages/object-store/src/index.ts';
import {produceCanonicalReplay} from '../src/session-pipeline/replay-publication.ts';
import {readerMaxRuleset} from '../packages/pipeline/src/lib/scrub-engine/reader-policy.ts';
import { initScrubEngine } from '../packages/pipeline/src/lib/scrub-engine/index.ts';
// Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
await initScrubEngine();
const args=process.argv.slice(2),outAt=args.indexOf('--out'),eventsAt=args.indexOf('--events');
if(outAt<0||!args[outAt+1])throw Error('proof requires --out <new-empty-local-directory>');
const output=resolve(args[outAt+1]!),count=eventsAt<0?300001:Number(args[eventsAt+1]);
if(!Number.isSafeInteger(count)||count<1||count>900000)throw Error('invalid synthetic event count');
await mkdir(output,{recursive:true});if((await readdir(output)).length)throw Error('proof output must be empty');
const store=createFsStore({root:join(output,'objects')}),x=await replayDatabase();
const started=performance.now();let peakRss=process.memoryUsage().rss;
const sample=setInterval(()=>{peakRss=Math.max(peakRss,process.memoryUsage().rss)},100);
try{
 const coverage={kind:'legacy_scope',origin_id:'legacy:scope',record_id:'legacy:scope:coverage',revision:'1',operation:'upsert' as const,received_at:'2026-11-01T10:00:00Z',payload:{org_id:identity.orgId,workspace_id:identity.workspaceId,source:identity.source,session_id:identity.sessionId,version:1,tables:['conversation_turns','session_links','session_outcomes']}};
 const source=await exportReplayFixture(store,x.db,count,2,[coverage]);
 console.log(JSON.stringify({phase:'source_exported',events:count,rawBytes:source.manifests.reduce((n,m)=>n+m.rawBytes,0),manifests:source.manifests.length}));
 const result=await produceCanonicalReplay({orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId},{readerMaxRuleset:readerMaxRuleset(process.env),db:x.db,store,provider:'s3',bucket:'replay-fixture',prefix:{PIPELINE_KEY_PREFIX:'96bulls',PIPELINE_ORG_FROM_PREFIX:'true'}});
 if(!result.published||result.rows!==count)throw Error('actual producer did not publish all source events');
 await x.pg.exec("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://replay.invalid','replay-test')");
 const now=Math.floor(Date.now()/1000);await x.pg.exec('BEGIN; SET LOCAL ROLE session_directory_search_worker');
 await x.pg.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify({role:'session_directory_search_worker',iss:'https://replay.invalid',aud:'replay-test',iat:now,exp:now+30,app_metadata:{org_id:identity.orgId,workspace_ids:[identity.workspaceId]}})]);
 const claim=(await x.pg.query<{claim:unknown}>('SELECT sessions.session_directory_replay_pages($1,$2,$3) claim',[identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.claim;
 await x.pg.exec('COMMIT');
 const load=async(ref:any)=>JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(ref.key)))));
 const index=await load(result.publication!.index);
 const replayRoot=index.tables.convergence_events.replay.root;
 const firstLeaf=async(ref:any):Promise<any>=>ref.kind==='node'?firstLeaf((await load(ref)).children[0]):ref;
 const firstPage=await firstLeaf(replayRoot);
 const locate=async(position:number)=>{let ref=replayRoot;while(ref.kind==='node'){const node=await load(ref);ref=node.children.find((r:any)=>position>=r.start&&position<r.start+r.count);if(!ref)throw Error('missing ordinal')}const page=await load(ref),entry=page.rows[position-ref.start];let row=entry.row;if(entry.rowJson){const parts:Uint8Array[]=[];const walk=async(r:any)=>{if(r.kind==='node'){for(const c of(await load(r)).children)await walk(c)}else parts.push(await readAll(await store.get(objectKey(r.key))))};await walk(entry.rowJson.root);row=JSON.parse(Buffer.concat(parts).toString('utf8'))}return {position,eventId:row.event_id,machineId:row.machine_id,ts:row.ts,nativeMessageId:row.record?.payload?.message_id,contentChars:entry.content?.chars??row.content?.length,toolCalls:row.tool_calls?.length,inputTokens:row.input_tokens}};
 const positions=[...new Set([0,1,firstPage.count-1,firstPage.count,Math.floor(count/2),count-1,...(count>=300000?[1800]:[])])].filter(n=>n>=0&&n<count);
 const expectedPositions=[];for(const p of positions)expectedPositions.push(await locate(p));
 const proof={expectedPositions,firstPageCount:firstPage.count,head:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),runtime:process.version,events:count,sourceRawBytes:source.manifests.reduce((n,m)=>n+m.rawBytes,0),sourceManifests:source.manifests.length,result,claim,seconds:(performance.now()-started)/1000,peakRssBytes:Math.max(peakRss,process.memoryUsage().rss,process.resourceUsage().maxRSS*1024),resourceUsage:process.resourceUsage()};
 await writeFile(join(output,'proof.json'),JSON.stringify(proof,null,2)+'\n');
 await writeFile(join(output,'claim.json'),JSON.stringify(claim)+'\n');
 console.log(JSON.stringify({phase:'published',events:count,seconds:proof.seconds,peakRssBytes:proof.peakRssBytes,indexBytes:result.publication!.index.bytes,objects:result.objects,artifactBytes:result.bytes}));
}finally{clearInterval(sample);await x.pg.close()}
