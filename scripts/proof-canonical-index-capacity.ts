/** Offline full Node index/#171/artifact proof, reusing immutable synthetic raw objects. */
import {mkdir,readdir,writeFile} from "node:fs/promises";
import {resolve,join} from "node:path";
import {execFileSync} from "node:child_process";
import {objectKey,readAll,parseManifest,parseSessionKey,chunkKeys} from "../packages/object-store/src/index.ts";
import {createFsStore,replayDatabase,addReplayPointer,REPLAY_FIXTURE_IDENTITY as identity} from "../src/session-pipeline/__tests__/replay-fixture.ts";
import {projectSessionDirectory} from "../src/hoopdb/session-directory-projector.ts";
import {indexSearchScope} from "../src/session-pipeline/search-index.ts";
import {produceCanonicalReplay} from "../src/session-pipeline/replay-publication.ts";
import {recordingReplayClickHouse} from "../src/session-pipeline/__tests__/replay-clickhouse-fixture.ts";
const args=process.argv.slice(2),arg=(name:string)=>{const i=args.indexOf(name);return i<0?undefined:args[i+1]};
const input=arg("--source"),outputArg=arg("--out");if(!input||!outputArg)throw Error("requires --source <existing-synthetic-fixture> --out <new-empty-directory>");
const output=resolve(outputArg);await mkdir(output,{recursive:true});if((await readdir(output)).length)throw Error("output must be empty");
const from=createFsStore({root:join(resolve(input),"objects")}),store=createFsStore({root:join(output,"objects")}),x=await replayDatabase();
let cursor:string|undefined;const manifests:string[]=[];
do{const page=await from.list(objectKey("96bulls/org_replay/v1"),cursor);for(const item of page.items)if(String(item.key).endsWith("/manifest.json"))manifests.push(String(item.key));cursor=page.nextCursor}while(cursor);
if(!manifests.length)throw Error("no synthetic source manifests");
for(const key of manifests){
 const object=await from.getObject(objectKey(key)),bytes=await readAll(object.body),manifest=await parseManifest(new TextDecoder().decode(bytes),key);
 await store.put(objectKey(key),bytes,{ifNoneMatch:true,metadata:Object.fromEntries(Object.entries(object.meta.metadata??{}).filter(([k])=>k!=="content-sha256"))});
 for(const {key:rawKey} of chunkKeys(manifest,{prefix:parseSessionKey(key)!.prefix})){const obj=await from.getObject(rawKey);await store.put(rawKey,await readAll(obj.body),{ifNoneMatch:true,metadata:Object.fromEntries(Object.entries(obj.meta.metadata??{}).filter(([k])=>k!=="content-sha256"))});}
 const ext=manifest.extensions!["96bulls"] as {canonical_machine_id:string};await addReplayPointer(x.db,{key,digest:manifest.raw.content_sha256,physicalSession:manifest.session.session_id,physicalMachine:manifest.session.machine_id,canonicalMachine:ext.canonical_machine_id,revision:manifest.revision,trace:manifest.trace_id});
}
const first=await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(manifests[0]!)))),manifests[0]!);
const scope={orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:first.session.session_id};await projectSessionDirectory(x.db,scope);
const observed=recordingReplayClickHouse(),started=performance.now();let peakRss=process.memoryUsage().rss;const sampler=setInterval(()=>peakRss=Math.max(peakRss,process.memoryUsage().rss),100);
try{
 const db=x.db,reader=(await import("../packages/pipeline/src/lib/pipeline/adapter.ts")).objectStoreManifestReader(store,{provider:"s3",bucket:"replay-fixture"});
 const result=await indexSearchScope(scope,{transact:work=>x.pg.transaction(async tx=>{await tx.exec("SET LOCAL statement_timeout='60s'");return work({query:<T>(sql:string,params?:unknown[])=>tx.query<T>(sql,params)})}),reader,store,ch:observed.ch,prefixFor:org=>`96bulls/${org}`});
 const metadata=(await db.query<{event_count:string;event_projection_ready:boolean;event_projection_epoch:string;times:string;dirty:string}>(`SELECT event_count::text,event_projection_ready,event_projection_epoch::text,(SELECT count(*)::text FROM sessions.session_directory_event_times) times,(SELECT count(*)::text FROM sessions.session_directory_event_pending WHERE dirty) dirty FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4`,[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0]!;
 if(!result.complete||result.partial||Number(metadata.event_count)!==300001||!metadata.event_projection_ready||metadata.dirty!=="0"||observed.stats().rows!==300001||observed.stats().uniqueRows!==300001)throw Error("full Node index or actual metadata not ready");
 console.log(JSON.stringify({phase:"index_and_metadata_ready",result,metadata,ch:observed.stats()}));
 const publication=await produceCanonicalReplay(scope,{db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}});
 if(!publication.published)throw Error("no artifact publication");
 await x.pg.exec("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://replay.invalid','replay-test')");const now=Math.floor(Date.now()/1000);await x.pg.exec("BEGIN;SET LOCAL ROLE session_directory_search_worker");await x.pg.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify({role:"session_directory_search_worker",iss:"https://replay.invalid",aud:"replay-test",iat:now,exp:now+30,app_metadata:{org_id:identity.orgId,workspace_ids:[identity.workspaceId]}})]);const claim=(await x.pg.query<{claim:unknown}>("SELECT sessions.session_directory_replay_pages($1,$2,$3) claim",[identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.claim;await x.pg.exec("COMMIT");
 const prior=JSON.parse(await(await import("node:fs/promises")).readFile(join(resolve(input),"proof.json"),"utf8"));
 const proof={head:execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),runtime:process.version,events:300001,sourceRawBytes:prior.sourceRawBytes,sourceManifests:manifests.length,result,metadata,ch:observed.stats(),publication,claim,expectedPositions:prior.expectedPositions,firstPageCount:prior.firstPageCount,seconds:(performance.now()-started)/1000,peakRssBytes:Math.max(peakRss,process.memoryUsage().rss,process.resourceUsage().maxRSS*1024),resourceUsage:process.resourceUsage()};
 await writeFile(join(output,"proof.json"),JSON.stringify(proof,null,2)+"\n");await writeFile(join(output,"claim.json"),JSON.stringify(claim)+"\n");console.log(JSON.stringify({phase:"published",events:300001,seconds:proof.seconds,peakRssBytes:proof.peakRssBytes,indexBytes:publication.publication!.index.bytes}));
}finally{clearInterval(sampler);await x.pg.close()}
