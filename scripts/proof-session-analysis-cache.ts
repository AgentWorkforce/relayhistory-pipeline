/** Offline real 0031 SQL -> Node builder -> Supabase pointer proof. Synthetic only. */
import {mkdir,readdir,writeFile} from "node:fs/promises";
import {resolve,join} from "node:path";
import {execFileSync} from "node:child_process";
import {legacyAnalysisOracle,LEGACY_BRIEF} from "../src/session-pipeline/__tests__/analysis-oracle-fixture.ts";
import {replayDatabase,addReplayPointer,createFsStore,REPLAY_FIXTURE_IDENTITY as identity} from "../src/session-pipeline/__tests__/replay-fixture.ts";
import {projectSessionDirectory} from "../src/hoopdb/session-directory-projector.ts";
import {analysisRefreshTick} from "../src/session-pipeline/analysis-refresh.ts";
const outArg=process.argv[process.argv.indexOf("--out")+1];if(!process.argv.includes("--out")||!outArg)throw Error("requires --out <new-empty-directory>");const output=resolve(outArg);await mkdir(output,{recursive:true});if((await readdir(output)).length)throw Error("output must be empty");
const legacy=await legacyAnalysisOracle(),supabase=await replayDatabase(),store=createFsStore({root:join(output,"objects")});
try{
 for(const table of ["convergence_events","conversation_turns","session_analysis_requests"]){await legacy.pg.query(`UPDATE sessions.${table} SET org_id=$1,workspace_id=$2,session_id=$3`,[identity.orgId,identity.workspaceId,identity.sessionId]);}
 await legacy.pg.query("UPDATE sessions.session_analysis_jobs SET org_id=$1,session_id=$2,source=$3::jsonb",[identity.orgId,identity.sessionId,JSON.stringify({evidenceWorkspaceIds:[identity.workspaceId]})]);
 const large={title:"Actual 0031 nested brief",description:"source-analysis-content 😀 ".repeat(200000),metadata:{tools:Array.from({length:10000},(_,i)=>({id:`tool-${i}`,args:{value:i,nested:[true,null,"保留"]}}))}};
 await legacy.pg.query("UPDATE sessions.session_analysis_jobs SET result=$1::jsonb WHERE id=$2",[JSON.stringify(large),LEGACY_BRIEF]);
 await addReplayPointer(supabase.db,{key:"96bulls/org_replay/v1/ws=ws_replay/m=m/h=claude/d=2026-11-01/s=physical/manifest.json",digest:"a".repeat(64),physicalSession:"physical",physicalMachine:"m",canonicalMachine:"m"});
 await projectSessionDirectory(supabase.db,{orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:"physical"});
 await supabase.db.query("SELECT sessions.enqueue_session_analysis_cache($1,$2,$3,$4)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
 await legacy.pg.exec("CREATE ROLE proof_source_reader NOINHERIT;GRANT USAGE ON SCHEMA sessions TO proof_source_reader;GRANT SELECT ON ALL TABLES IN SCHEMA sessions TO proof_source_reader;REVOKE EXECUTE ON FUNCTION sessions.read_session_analysis_v1(text,text),sessions.session_requests_v1_projection(text,text) FROM PUBLIC");
 const readonlySource={snapshot:<T>(work:any)=>legacy.pg.transaction(async tx=>{await tx.exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;SET LOCAL ROLE proof_source_reader");return work({query:(sql:string,params?:unknown[])=>tx.query(sql,params)})}) as Promise<T>};
 const started=performance.now(),summary=await analysisRefreshTick({db:supabase.db,store,source:readonlySource},{maxJobs:1});const publication={published:summary.published===1,summary};if(!publication.published||summary.failed||summary.lost)throw Error("analysis publication incomplete");
 await supabase.pg.exec("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://replay.invalid','replay-test');BEGIN;SET LOCAL ROLE session_directory_search_worker");const now=Math.floor(Date.now()/1000);await supabase.pg.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify({role:"session_directory_search_worker",iss:"https://replay.invalid",aud:"replay-test",iat:now,exp:now+30,app_metadata:{org_id:identity.orgId,workspace_ids:[identity.workspaceId]}})]);
 const claim=(await supabase.pg.query<{c:any}>("SELECT sessions.session_directory_analysis_cache($1,$2,$3) c",[identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.c;await supabase.pg.exec("COMMIT");if(!claim.current)throw Error("no current analysis claim");
 const payload=(await legacy.pg.query<{p:any}>("SELECT sessions.read_session_analysis_v1($1,$2) p",[identity.orgId,identity.sessionId])).rows[0]!.p;
 const proof={head:execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),runtime:process.version,sourceOracleHead:"9ff3a285cf2d7ac617ad4a2a59ffc6931a60fecb",seconds:(performance.now()-started)/1000,peakRssBytes:process.resourceUsage().maxRSS*1024,publication,claim,expected:{requests:payload.requests.map((r:any)=>r.id),analyses:payload.analyses.map((a:any)=>a.id),status:payload.analyses[0].status,title:large.title,descriptionChars:large.description.length,toolCount:large.metadata.tools.length,snapshotSha256:claim.snapshotSha256}};
 await writeFile(join(output,"proof.json"),JSON.stringify(proof,null,2)+"\n");await writeFile(join(output,"claim.json"),JSON.stringify(claim)+"\n");await writeFile(join(output,"expected-payload.json"),JSON.stringify(payload)+"\n");console.log(JSON.stringify(proof));
}finally{await legacy.pg.close();await supabase.pg.close()}
