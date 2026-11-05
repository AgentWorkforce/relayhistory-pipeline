
import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { captureHistorySource, withHistorySourceAuthority, historyDerivedKey, registerHistoryDerivedObject, publishHistorySummary, publishHistoryState, readHistorySummary } from "../../shared/history-source-authority.ts";
import { sqlHistoryMaskStore, updateHistoryMask } from "../../shared/history-mask.ts";
setDefaultTimeout(30000);
const root=join(import.meta.dir,"../../.."),gen="a".repeat(64),scope={orgId:"mask-org",workspaceId:"mask-ws"};
let db:PGlite;
beforeAll(async()=>{
 db=new PGlite({extensions:{btree_gin,pg_trgm}});
 const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.query("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience)VALUES('https://history-fixture.test','session-directory-search')");
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+gen+"','pass')");
});
afterAll(async()=>{await db?.close()});
const run:any={query:async(sql:string,args:unknown[])=>{
 await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
 try{const r=await db.query(sql,args as never);await db.exec("COMMIT");return r}catch(e){await db.exec("ROLLBACK");throw e}
}};
async function publicRpc(identity:{orgId:string;workspaceId:string;source:string;sessionId:string},fn:"session_directory_history_summary"|"session_directory_pointers"|"session_directory_replay_pages",args:string[],claimedWorkspaces=[identity.workspaceId]){
 const now=Math.floor(Date.now()/1000),claims={role:"session_directory_search_worker",iss:"https://history-fixture.test",aud:"session-directory-search",iat:now,exp:now+60,app_metadata:{org_id:identity.orgId,workspace_ids:claimedWorkspaces}};
 await db.exec("BEGIN;SET LOCAL ROLE session_directory_search_worker");
 try{await db.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify(claims)]);
  const r=(await db.query("SELECT sessions."+fn+"("+args.map((_,n)=>"$"+(n+1)).join(",")+") r",args)).rows[0]?.r;
  await db.exec("COMMIT");return r as any;
 }catch(e){await db.exec("ROLLBACK");throw e}
}
const publicSummary=(identity:Parameters<typeof publicRpc>[0],kind="short",claimedWorkspaces=[identity.workspaceId])=>publicRpc(identity,"session_directory_history_summary",[identity.workspaceId,identity.source,identity.sessionId,kind],claimedWorkspaces);
test("actual SQL COW tree seeks exact ordinal pages and updates only changed leaf/root",async()=>{
 const store=sqlHistoryMaskStore(run,gen,scope.orgId,scope.workspaceId);
 let writes=0;const counted={...store,register:async(d:any)=>{writes++;return store.register(d)}};
 const items=Array.from({length:1200},(_,i)=>({recordId:"native-"+i,winner:null}));
 const first=await updateHistoryMask(counted,scope,null,items);expect(first.count).toBe("1200");expect(writes).toBe(17);
 const rows:any[]=[];
 for(let after=0;after<1200;after+=499)rows.push(...(await db.query("SELECT ordinal::text,record_id,winner FROM sessions.ingest_history_mask_rows($1,$2,499)",[first.sha256,after])).rows);
 const sha=(s:string)=>createHash("sha256").update(s).digest("hex");
 expect(rows.map(r=>r.record_id)).toEqual(items.map(x=>x.recordId).sort((a,b)=>sha(a)<sha(b)?-1:1));
 expect(rows.map(r=>r.ordinal)).toEqual(Array.from({length:1200},(_,i)=>String(i)));
 writes=0;
 const second=await updateHistoryMask(counted,scope,first,[{recordId:"native-19",winner:null}]);
 expect(writes).toBe(2);expect(second.sha256).toBe(first.sha256);
 const third=await updateHistoryMask(counted,scope,second,[{recordId:"additional-native",winner:null}]);
 expect(third.count).toBe("1201");expect(third.sha256).not.toBe(first.sha256);
 expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_history_mask_rows($1,1199,500)",[first.sha256])).rows[0]!.n).toBe(1);
 await expect(sqlHistoryMaskStore(run,gen,"foreign","ws").read(first.sha256)).rejects.toThrow("unknown mask");
});
test("actual SQL refuses NULL operation, foreign child and false counts rather than admitting malformed visibility",async()=>{
 const emptyWorkspace=sqlHistoryMaskStore(run,gen,scope.orgId,"");
 const empty=await updateHistoryMask(emptyWorkspace,{orgId:scope.orgId,workspaceId:""},null,[]);
 expect((await emptyWorkspace.read(empty.sha256)).workspaceId).toBe("");
 const store=sqlHistoryMaskStore(run,gen,scope.orgId,scope.workspaceId);
 const winner={originId:"origin",recordId:"bad",kind:"history",revision:"1",acceptedSeq:"2",operation:null,userId:"owner",source:"claude",sessionId:"s",contentSha256:"c".repeat(64),lineage:"0"};
 await expect(store.register({protocol:"history-mask-v1",...scope,prefix:"",kind:"leaf",count:"1",items:[{recordId:"bad",winner:winner as any}]})).rejects.toThrow("mask current custody");
 await expect(store.register({protocol:"history-mask-v1",...scope,prefix:"",kind:"leaf",count:"2",items:[]})).rejects.toThrow("mask leaf count");
 const other=sqlHistoryMaskStore(run,gen,"other","workspace");
 const id=Array.from({length:100},(_,i)=>"foreign-"+i).find(x=>createHash("sha256").update(x).digest("hex")[0]==="a")!;
 const child=await other.register({protocol:"history-mask-v1",orgId:"other",workspaceId:"workspace",prefix:"a",kind:"leaf",count:"1",items:[{recordId:id,winner:null}]});
 await expect(store.register({protocol:"history-mask-v1",...scope,prefix:"",kind:"node",count:"1",items:[{digit:"a",sha256:child.sha256}]})).rejects.toThrow("mask child proof");
});

test("logical source locks bind summary intent/final CAS and native changes invalidate unchanged pointers",async()=>{
 const maskStore=sqlHistoryMaskStore(run,gen,scope.orgId,scope.workspaceId);
 const root=await updateHistoryMask(maskStore,scope,null,[{recordId:"native-19",winner:null}]);
 const identity={...scope,source:"claude",sessionId:"history-session"},id="12345678-1234-1234-1234-123456789abc";
 const row={org_id:scope.orgId,workspace_id:scope.workspaceId,user_id:"history-user",harness:identity.source,session_id:identity.sessionId,machine_id:"srv_origin",created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"history-fixture",manifest_key:"96bulls/"+scope.orgId+"/v1/ws="+scope.workspaceId+"/m=srv_origin/h=claude/d=2026/11/01/s=history-session/manifest.json",content_sha256:"\\x"+"a".repeat(64),raw_format:"ndjson",revision:1,trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"};
 await db.query("SELECT sessions.upsert_session_index($1::jsonb)",[JSON.stringify([row])]);
 await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,$3,$4,now(),now())",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
 const anchor="96bulls/"+scope.orgId+"/upload-generations/g="+id+"/v1/ws="+scope.workspaceId+"/m=history_scope/h=claude/d=2026/11/01/s=history-session/publications/"+id+"/manifest.json";
 await db.query("INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot_sha256) SELECT $1,$2,$3,$4,$5,epoch,$6,1,$7,'history-fixture','us-east-1','96bulls','ready',repeat('a',64) FROM sessions.session_replay_publications WHERE org_id=$2 AND workspace_id=$3 AND harness=$4 AND session_id=$5",[id,identity.orgId,identity.workspaceId,identity.source,identity.sessionId,root.sha256,anchor]);
 await db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,$3,$4,$5,true)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,id]);
 await db.query("UPDATE sessions.ingest_history_scope_versions SET source_fingerprint=encode(sha256(convert_to(sessions.session_replay_source_snapshot(org_id,workspace_id,harness,session_id)::text,'UTF8')),'hex') WHERE authority_id=$1",[id]);
 await db.query("INSERT INTO sessions.ingest_history_scope_keys VALUES($1,$2,'native-19',$3,$4)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
 const service:any={query:async(q:string,p:unknown[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(q,p as never);await db.exec("COMMIT");return r}catch(e){await db.exec("ROLLBACK");throw e}}};
 const captured=await captureHistorySource(service,identity,{schemaGeneration:gen});
 expect(captured.mask.winners.has("native-19")).toBe(true);
 const objectKey=historyDerivedKey(captured,"summary-short",id);
 await registerHistoryDerivedObject(service,captured,{key:objectKey,sha256:"b".repeat(64),bytes:100});
 const transact:any=async(work:any)=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await work(db);await db.exec("COMMIT");return r}catch(e){await db.exec("ROLLBACK");throw e}};
 const pointer={kind:"short" as const,status:"ready" as const,objectKey,objectSha256:"b".repeat(64),objectBytes:100,keyId:"fixture",model:"fixture-model",builtAt:"2026-11-04T00:00:00Z"};
 await expect(publishHistorySummary(service,captured,pointer)).rejects.toThrow("transaction permit required");
 expect(await withHistorySourceAuthority(transact,captured,async tx=>publishHistorySummary(tx,captured,pointer))).toEqual({current:true,value:true});
 expect((await readHistorySummary(service,captured,"short"))?.objectKey).toBe(objectKey);
 await withHistorySourceAuthority(transact,captured,async tx=>publishHistoryState(tx,captured,"done","2026-11-04T00:00:00Z"));
 expect((await db.query("SELECT state FROM sessions.session_directory WHERE org_id=$1",[identity.orgId])).rows[0]!.state).toBe("done");
 expect((await db.query("SELECT sessions.set_session_state($1,$2,$3,$4,'srv_origin','active',clock_timestamp()) applied",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.applied).toBe(false);
 await db.query("UPDATE sessions.session_directory SET state='active',short_summary='stale physical summary' WHERE org_id=$1",[identity.orgId]);
 expect((await db.query("SELECT state,short_summary FROM sessions.session_directory WHERE org_id=$1",[identity.orgId])).rows[0]).toEqual({state:"done",short_summary:null});

 // Existing selected readers understand171ready only. A late physical
 // repair must not advertise unmasked events/work signals as current.
 await db.query("UPDATE sessions.session_directory SET event_projection_ready=true,event_projection_version=1,event_projected_at=clock_timestamp(),event_projection_sources=jsonb_build_object('fixture','{}'::jsonb),work_status='active',work_status_at=clock_timestamp() WHERE org_id=$1",[identity.orgId]);
 expect((await db.query("SELECT event_projection_ready,work_status,work_status_at FROM sessions.session_directory WHERE org_id=$1",[identity.orgId])).rows[0]).toEqual({event_projection_ready:false,work_status:null,work_status_at:null});

 expect(await publicRpc(identity,"session_directory_replay_pages",[identity.workspaceId,identity.source,identity.sessionId])).toMatchObject({exists:true,current:false});
 await expect(publicRpc(identity,"session_directory_pointers",[identity.workspaceId,identity.source,identity.sessionId])).rejects.toMatchObject({code:"0A000"});
 const publicClaim=await publicSummary(identity);
 expect(publicClaim).toMatchObject({version:1,kind:"short",exists:true,required:true,ready:true,identity,source:captured.source,
  authority:{authorityId:id,maskSha256:captured.maskSha256,recordCount:"1",derivedAnchorKey:captured.derivedAnchorKey,schemaGeneration:gen},
  storage:{provider:"s3",bucket:"history-fixture",region:"us-east-1"},pointer:{kind:"short",status:"ready",objectKey},state:{state:"done"}});
 expect(Object.keys(publicClaim).sort()).toEqual(["version","kind","exists","required","ready","identity","source","authority","storage","pointer","state"].sort());
 expect((await publicSummary(identity,"long")).pointer).toBe(null);
 expect(publicClaim.pointer.builtAt).toMatch(/Z$/);expect(publicClaim.state.classifiedAt).toMatch(/Z$/);
 await db.exec("UPDATE sessions.__96bulls_role_validation SET status='pending'");
 expect(await publicSummary(identity)).toMatchObject({required:true,ready:false,authority:null,storage:null,pointer:null,state:null});
 await db.exec("UPDATE sessions.__96bulls_role_validation SET status='pass'");

 expect(await publicSummary({...identity,orgId:"foreign-org"})).toMatchObject({exists:false,ready:false,required:false,authority:null,storage:null,pointer:null,state:null});
 await expect(publicSummary(identity,"short",["foreign-workspace"])).rejects.toMatchObject({code:"42501"});
 await expect(publicSummary(identity,"arbitrary-key")).rejects.toMatchObject({code:"22023"});
 await db.exec("BEGIN;SET LOCAL ROLE authenticated");try{await expect(db.query("SELECT sessions.session_directory_history_summary($1,$2,$3,'short')",[identity.workspaceId,identity.source,identity.sessionId])).rejects.toMatchObject({code:"42501"});}finally{await db.exec("ROLLBACK")}

 const sliceKey=historyDerivedKey(captured,"bob-slice","32345678-1234-1234-1234-123456789abc");
 await registerHistoryDerivedObject(service,captured,{key:sliceKey,sha256:"b".repeat(64),bytes:100});
 await expect(withHistorySourceAuthority(transact,captured,async tx=>publishHistorySummary(tx,captured,{...pointer,objectKey:sliceKey}))).rejects.toThrow("registered encrypted summary required");
 expect((await readHistorySummary(service,captured,"short"))?.objectKey).toBe(objectKey);

 // The managed worker uses only generation-bound JSON entrypoints; old
 // service workers must leave these jobs queued.
 const discovery=(await service.query("SELECT sessions.session_history_large_job($1::jsonb) r",[JSON.stringify({generation:gen,action:"discover"})])).rows[0].r;
 expect(discovery).toMatchObject({busy:false,scanned:1,changed:1,cycleComplete:true});
 expect((await db.query("SELECT sessions.claim_session_large_read($1::uuid,120) j",["42345678-1234-1234-1234-123456789abc"])).rows[0]!.j).toBe(null);
 const leaseNonce="52345678-1234-1234-1234-123456789abc";
 const job=async(action:string)=> (await service.query("SELECT sessions.session_history_large_job($1::jsonb) r",[JSON.stringify({generation:gen,action,leaseNonce,...(action==="claim"?{}:{identity})})])).rows[0].r;
 expect(await job("claim")).toMatchObject({identity,source:captured.source,schemaGeneration:gen,leaseNonce});
 expect(await job("heartbeat")).toEqual({ok:true});
 await expect(service.query("SELECT sessions.claim_session_large_read($1::uuid,120)",[leaseNonce])).rejects.toThrow("permission denied");
 await expect(service.query("SELECT sessions.session_history_large_job($1::jsonb)",[JSON.stringify({generation:"b".repeat(64),action:"heartbeat",leaseNonce,identity})])).rejects.toThrow();
 await db.query("UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()-interval'1 second'WHERE org_id=$1",[identity.orgId]);
 expect(await job("heartbeat")).toEqual({ok:false});
 expect(await job("retry")).toEqual({ok:false});
 expect(await job("claim")).toMatchObject({leaseNonce,schemaGeneration:gen});
 expect(await job("retry")).toEqual({ok:true});

 // A fresh TTL/nonce and matching current source still cannot qualify
 // an analysis built through the old unmasked physical-source publisher.
 const raw=(await db.query("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.c as any;
 const legacyNonce="72345678-1234-1234-1234-123456789abc",oldAnchor=raw.snapshot.pointers[0],oldGeneration="d".repeat(64),oldHash="b".repeat(64);
 const oldPrefix=oldAnchor.manifest_key.slice(0,-"manifest.json".length)+"normalized/read-projection-v1/"+oldAnchor.content_sha256+"/"+oldGeneration+"/";
 await db.query("INSERT INTO sessions.session_analysis_wakeups(org_id,workspace_id,harness,session_id,source_epoch,source_fingerprint,demand_until,last_demand_at,next_refresh_at,lease_nonce,lease_expires_at,lease_generation)VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()+interval'120 seconds',clock_timestamp(),clock_timestamp(),$7,clock_timestamp()+interval'120 seconds',1)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,raw.source.epoch,raw.source.fingerprint,legacyNonce]);
 const accepted=(await db.query("SELECT sessions.publish_session_analysis_cache($1,$2,$3,$4,$5::bigint,$6,$7::timestamptz,$8,$9,$10,$11,$12,$13,$14::integer,$15::uuid) ok",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,raw.source.epoch,raw.source.fingerprint,new Date().toISOString(),"a".repeat(64),"b".repeat(64),oldGeneration,oldPrefix,oldPrefix+oldHash+".json",oldHash,100,legacyNonce])).rows[0]!.ok;
 expect(accepted).toBe(false);

 const before=(await db.query("SELECT manifest_key FROM sessions.session_index WHERE org_id=$1",[identity.orgId])).rows;
 await db.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES(repeat('e',64),'[]',$1,$2,'origin','native-19',repeat('c',64),repeat('d',64),repeat('f',64))",[identity.orgId,identity.workspaceId]);
 expect(await publicSummary(identity)).toMatchObject({required:true,ready:false,authority:null,storage:null,pointer:null,state:null});
 const oldCapture=(await db.query("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.c as any;
 expect(oldCapture.blocked).toBe(true);
 await db.query("INSERT INTO sessions.session_briefs(org_id,workspace_id,harness,session_id,status,revision,title,description,brief_source_revisions,brief_revision,brief_fingerprint,scrub_ruleset,scrub_version,prompt_version,engine_version,model,generated_at,state)VALUES($1,$2,$3,$4,'ready',1,'OLD-MOVED-PRIVATE','Unqualified old raw brief',jsonb_build_object('srv_origin',1),1,repeat('a',64),'relayhistory-scrub',1,'session-summary-v3','fixture','fixture',clock_timestamp(),'completed')",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
 const now=Math.floor(Date.now()/1000),claims={role:"session_directory_search_worker",iss:"https://history-fixture.test",aud:"session-directory-search",iat:now,exp:now+60,app_metadata:{org_id:identity.orgId,workspace_ids:[identity.workspaceId]}};
 await db.exec("BEGIN;SET LOCAL ROLE session_directory_search_worker");
 try{
  await db.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify(claims)]);
  await expect(db.query("SELECT *FROM sessions.session_brief($1,$2,$3)",[identity.workspaceId,identity.source,identity.sessionId])).rejects.toThrow("qualified brief metadata not ready");
 }finally{await db.exec("ROLLBACK")}
 let calls=0;expect(await withHistorySourceAuthority(transact,captured,async()=>{calls++})).toEqual({current:false});expect(calls).toBe(0);
 expect((await db.query("SELECT manifest_key FROM sessions.session_index WHERE org_id=$1",[identity.orgId])).rows).toEqual(before);
 expect((await db.query("SELECT state FROM sessions.ingest_history_reconcile_outbox WHERE native_key_sha256=repeat('e',64)")).rows[0]!.state).toBe("pending");
 expect((await db.query("SELECT state FROM sessions.session_directory WHERE org_id=$1",[identity.orgId])).rows[0]!.state).toBe("unknown");
 expect((await db.query("SELECT cleanup_state FROM sessions.ingest_history_derived_objects WHERE object_key=$1",[objectKey])).rows[0]!.cleanup_state).toBe("retired");
 // Simulate the next immutable rebuilt version for the same unchanged pointer.
 // No currently owned native slot is required to invalidate legacy source data.
 const nextId="22345678-1234-1234-1234-123456789abc";
 await db.query("INSERT INTO sessions.ingest_history_scope_versions SELECT $1,org_id,workspace_id,harness,session_id,(SELECT epoch FROM sessions.session_replay_publications a WHERE a.org_id=v.org_id AND a.workspace_id=v.workspace_id AND a.harness=v.harness AND a.session_id=v.session_id),NULL,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,created_at,retired_at,pointer_snapshot_sha256 FROM sessions.ingest_history_scope_versions v WHERE authority_id=$2",[nextId,id]);
 await db.query("UPDATE sessions.ingest_history_scope_current SET authority_id=$1,ready=true WHERE authority_id=$2",[nextId,id]);
 await db.query("UPDATE sessions.ingest_history_scope_versions SET source_fingerprint=encode(sha256(convert_to(sessions.session_replay_source_snapshot(org_id,workspace_id,harness,session_id)::text,'UTF8')),'hex') WHERE authority_id=$1",[nextId]);
 await db.query("INSERT INTO sessions.ingest_history_scope_owners VALUES($1,$2,'history-user',$3,$4,repeat('d',64),0)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
 const rebuilt=await captureHistorySource(service,identity,{schemaGeneration:gen});
 expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_lazy_native_slots WHERE current_owner='history-user'")).rows[0]!.n).toBe(0);
 await db.query("INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation) VALUES($1,$2,'history-user',repeat('d',64),1,repeat('a',64),'reset','{}',$3)",[identity.orgId,identity.workspaceId,gen]);
 expect(await withHistorySourceAuthority(transact,rebuilt,async()=>{throw Error("legacy-only stale callback")})).toEqual({current:false});
 expect((await db.query("SELECT state,control_version::text FROM sessions.ingest_history_privacy_outbox WHERE org_id=$1 AND user_id='history-user'",[identity.orgId])).rows[0]).toEqual({state:"pending",control_version:"1"});

});

test("a new destination invalidates before a record has any registered scope dependency",async()=>{
 const h="9".repeat(64);
 expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_history_scope_keys WHERE record_id='never-registered'")).rows[0]!.n).toBe(0);
 await db.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256,current_owner,current_source,current_session,current_revision,current_digest,current_reset_epoch,current_fence_epoch) VALUES($1,'[]','brand-org','brand-ws','origin','never-registered',repeat('c',64),repeat('d',64),repeat('f',64),'user','claude','new-session',1,repeat('a',64),0,0)",[h]);
 expect((await db.query("SELECT count(*)::int n FROM sessions.ingest_history_required_scopes WHERE org_id='brand-org' AND session_id='new-session'")).rows[0]!.n).toBe(1);
 const snapshot=(await db.query("SELECT sessions.session_replay_source_snapshot('brand-org','brand-ws','claude','new-session') s")).rows[0]!.s as any;
 expect(snapshot.history).toEqual({version:1,required:true,ready:false});
 await db.query("UPDATE sessions.ingest_lazy_native_slots SET current_session='moved-session',current_revision=2,lineage=1 WHERE key_sha256=$1",[h]);
 expect((await db.query("SELECT session_id FROM sessions.ingest_history_required_scopes WHERE org_id='brand-org' ORDER BY session_id")).rows).toEqual([{session_id:"moved-session"},{session_id:"new-session"}]);
});


test("unused foundation rolls down without discarding authority, restores legacy claimant, and reapplies",async()=>{
 await expect(db.exec(readFileSync(join(root,"supabase/rollbacks/20261104062000_canonical_history_lineage.down.sql"),"utf8"))).rejects.toThrow("retain authority/object ledgers");
 const clean=new PGlite({extensions:{btree_gin,pg_trgm}}),read=(p:string)=>readFileSync(join(root,p),"utf8");
 try{
  await clean.exec(read("supabase/tests/supabase_shim.sql"));
  for(const f of["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await clean.exec(read("supabase/bootstrap/"+f+".sql"));
  for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await clean.exec(read("supabase/migrations/"+f));
  await clean.exec(read("supabase/rollbacks/20261104062000_canonical_history_lineage.down.sql"));
  expect((await clean.query("SELECT to_regprocedure('sessions.session_history_large_job(jsonb)') p,to_regclass('sessions.ingest_history_scope_current') t")).rows[0]).toEqual({p:null,t:null});
  expect((await clean.query("SELECT prosrc LIKE '%ingest_history_%' managed,pronargdefaults FROM pg_proc WHERE oid='sessions.claim_session_large_read(uuid,integer)'::regprocedure")).rows[0]).toEqual({managed:false,pronargdefaults:1});
  expect((await clean.query("SELECT count(*)::int n FROM pg_attribute WHERE attrelid='sessions.session_large_read_jobs'::regclass AND attname='history_generation'AND NOT attisdropped")).rows[0]!.n).toBe(0);
  await clean.exec(read("supabase/migrations/20261104062000_canonical_history_lineage.sql"));
  expect((await clean.query("SELECT to_regprocedure('sessions.session_history_large_job(jsonb)') IS NOT NULL yes")).rows[0]!.yes).toBe(true);
 }finally{await clean.close()}
});


test("generation-bound capture preserves an empty workspace and blocks an unready required mask",async()=>{
 const identity={orgId:"empty-ws-org",workspaceId:"",source:"claude",sessionId:"empty-workspace-session"};
 const row={org_id:identity.orgId,workspace_id:"",user_id:"empty-user",harness:"claude",session_id:identity.sessionId,machine_id:"srv_empty",created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"history-fixture",manifest_key:"96bulls/empty-ws-org/v1/ws=_/m=srv_empty/h=claude/d=2026/11/01/s=empty-workspace-session/manifest.json",content_sha256:"\\x"+"a".repeat(64),raw_format:"ndjson",revision:1,trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"};
 await db.query("SELECT sessions.upsert_session_index($1::jsonb)",[JSON.stringify([row])]);
 await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)VALUES($1,'','claude',$2,now(),now())",[identity.orgId,identity.sessionId]);
 const capture=async()=>(await run.query("SELECT sessions.session_history_capture($1::jsonb) c",[JSON.stringify({generation:gen,identity})])).rows[0].c;
 expect(await capture()).toMatchObject({exists:true,blocked:false,identity});
 await db.query("SELECT sessions.ingest_history_invalidate_scope($1,'','claude',$2)",[identity.orgId,identity.sessionId]);
 expect(await capture()).toMatchObject({exists:true,blocked:true,identity});
 const old=(await db.query("SELECT sessions.capture_session_replay_pages($1,'','claude',$2) c",[identity.orgId,identity.sessionId])).rows[0]!.c as any;
 expect(old.blocked).toBe(true);
});
