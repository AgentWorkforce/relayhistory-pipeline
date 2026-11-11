/** Offline actual exporter/fold/publication proof; no service credentials. */
import {mkdir,readdir,writeFile} from "node:fs/promises";
import {join,resolve} from "node:path";
import {replayDatabase,exportReplayFixture,createFsStore,REPLAY_FIXTURE_IDENTITY as identity} from "../src/session-pipeline/__tests__/replay-fixture.ts";
import {produceCanonicalReplay} from "../src/session-pipeline/replay-publication.ts";
import {readerMaxRuleset} from "../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import {objectKey,readAll} from "../packages/object-store/src/index.ts";
const output=resolve(process.argv[2]??"");
if(!process.argv[2])throw Error("output required");
await mkdir(output,{recursive:true});if((await readdir(output)).length)throw Error("output must be empty");
const x=await replayDatabase(),store=createFsStore({root:join(output,"objects")});
try{
 const content="\u00a0\uFEFF \n\t".repeat(2000)+"Faithful opening 😀 "+"word ".repeat(500000),project="/private/原文/project";
 const direct={kind:"convergence_event",origin_id:"machine:machine:0",record_id:"original-display",revision:"1",accepted_seq:"9000",operation:"upsert" as const,received_at:"2026-11-01T10:00:00Z",payload:{source:identity.source,session_id:identity.sessionId,machine_id:"machine:0",user_id:"user_replay",event_id:"original-display",kind:"history",ts:"2020-01-01T00:00:00.123456Z",ingested_at:"2026-11-01T10:00:00.000000Z",content,project_id:project,record:{}}};
 await exportReplayFixture(store,x.db,1001,2,[direct]);
 const result=await produceCanonicalReplay({orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId},{readerMaxRuleset:readerMaxRuleset(process.env),db:x.db,store,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}});
 if(!result.published)throw Error("publication failed");
 const load=async(ref:any)=>JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(ref.key)))));
 const index=await load(result.publication!.index),ref=index.tables.session_directory_event_values.raw.root;
 const display=await load(ref);if(display.rows.length!==1)throw Error("wrong display inventory");
 const row=display.rows[0].row;
 if(row.event.ts!=="2020-01-01T00:00:00.123456Z"||row.event.machine_id!=="machine:0"||row.event.session_id!==identity.sessionId||row.project_id!==project||!row.opening_prefix.startsWith("Faithful opening 😀 word"))throw Error("display mismatch");
 await x.pg.exec("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://display.invalid','display-test'); BEGIN; SET LOCAL ROLE session_directory_search_worker");
 const now=Math.floor(Date.now()/1000);
 await x.pg.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify({role:"session_directory_search_worker",iss:"https://display.invalid",aud:"display-test",iat:now,exp:now+30,app_metadata:{org_id:identity.orgId,workspace_ids:[identity.workspaceId]}})]);
 const claim=(await x.pg.query<{claim:unknown}>("SELECT sessions.session_directory_replay_pages($1,$2,$3) claim",[identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.claim;
 await x.pg.exec("COMMIT");
 await writeFile(join(output,"claim.json"),JSON.stringify(claim));
 await writeFile(join(output,"proof.json"),JSON.stringify({identity,expected:row,eventCount:1002,sourceContentChars:content.length,indexBytes:result.publication!.index.bytes,displayBytes:ref.bytes,rows:result.rows},null,2));
 console.log(JSON.stringify({published:true,eventCount:1002,displayBytes:ref.bytes,indexBytes:result.publication!.index.bytes,contentChars:content.length}));
}finally{await x.pg.close()}
