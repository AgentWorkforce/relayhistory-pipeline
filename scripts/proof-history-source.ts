/** Owned actual PG proof; no production URL discovery. */
import pg from "pg";import assert from "node:assert/strict";import{execFileSync}from"node:child_process";import{readFileSync,writeFileSync}from"node:fs";import{createHash}from"node:crypto";
import{runMigrations,generation}from"../src/hoopdb/migrate.ts";
import{sqlHistoryMaskStore,updateHistoryMask}from"../src/shared/history-mask.ts";
import{captureHistorySource,withHistorySourceAuthority,historyDerivedKey,registerHistoryDerivedObject,publishHistorySummary,readHistorySummary}from"../src/shared/history-source-authority.ts";
if(process.env.HISTORY_DISPOSABLE!=="1")throw Error("owned_fixture_required");
const fixtureContainer=process.argv[2]??"canonical-history-source-pg17-20261104";
if(!["canonical-history-source-pg17-20261104","canonical-history-source-final-pg17-20261104"].includes(fixtureContainer))throw Error("unowned_fixture");
const container=JSON.parse(execFileSync("docker",["inspect",fixtureContainer],{encoding:"utf8"}))[0];assert.equal(container.Config.Labels["com.96bulls.proof"],"canonical-history-source");
const port=container.NetworkSettings.Ports["5432/tcp"][0];assert.equal(port.HostIp,"127.0.0.1");
const cfg=(user="postgres",database="history_source")=>({host:"127.0.0.1",port:Number(port.HostPort),user,database,connectionTimeoutMillis:1000});
const admin=new pg.Client(cfg("postgres","postgres"));await admin.connect();await admin.query("CREATE DATABASE history_source");await admin.end();
const owner=new pg.Client(cfg());await owner.connect();
const db:any={query:async(q:string,p?:unknown[])=>owner.query(q,p),exec:async(q:string)=>owner.query(q)};
const proof:any={head:execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),dirty:execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim(),node:process.version,cases:[]};
let ingest:pg.Client,gen:string;const scope={orgId:"mask-org",workspaceId:"mask-ws"};
const check=async(name:string,work:()=>Promise<void>)=>{const at=Date.now();await work();proof.cases.push({name,pass:true,ms:Date.now()-at});console.log("PASS "+name)};
function expect(v:any):any{return {toBe:(x:any)=>assert.equal(v,x),toEqual:(x:any)=>assert.deepEqual(v,x),get not(){return{toBe:(x:any)=>assert.notEqual(v,x)}},rejects:{toThrow:async(part:string)=>assert.rejects(v,(e:any)=>String(e.message).includes(part))}}}
try {
 await runMigrations(db,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 await owner.query(readFileSync("supabase/bootstrap/session_ingest_role.sql","utf8").split("\n").filter(s=>!s.startsWith("\\")).join("\n"));
 await owner.query(readFileSync("supabase/bootstrap/session_ingest_verify.sql","utf8"));
 gen=await generation();proof.schemaGeneration=gen;ingest=new pg.Client(cfg("bulls96_session_ingest"));await ingest.connect();
 const run:any={query:(q:string,p:unknown[])=>ingest.query(q,p)};
await check("actual SQL COW tree seeks exact ordinal pages and updates only changed leaf/root",async()=>{
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
await check("actual SQL refuses NULL operation, foreign child and false counts rather than admitting malformed visibility",async()=>{
 const store=sqlHistoryMaskStore(run,gen,scope.orgId,scope.workspaceId);
 const winner={originId:"origin",recordId:"bad",kind:"history",revision:"1",acceptedSeq:"2",operation:null,userId:"owner",source:"claude",sessionId:"s",contentSha256:"c".repeat(64),lineage:"0"};
 await expect(store.register({protocol:"history-mask-v1",...scope,prefix:"",kind:"leaf",count:"1",items:[{recordId:"bad",winner:winner as any}]})).rejects.toThrow("mask current custody");
 await expect(store.register({protocol:"history-mask-v1",...scope,prefix:"",kind:"leaf",count:"2",items:[]})).rejects.toThrow("mask leaf count");
 const other=sqlHistoryMaskStore(run,gen,"other","workspace");
 const id=Array.from({length:100},(_,i)=>"foreign-"+i).find(x=>createHash("sha256").update(x).digest("hex")[0]==="a")!;
 const child=await other.register({protocol:"history-mask-v1",orgId:"other",workspaceId:"workspace",prefix:"a",kind:"leaf",count:"1",items:[{recordId:id,winner:null}]});
 await expect(store.register({protocol:"history-mask-v1",...scope,prefix:"",kind:"node",count:"1",items:[{digit:"a",sha256:child.sha256}]})).rejects.toThrow("mask child proof");
});
await check("logical source locks bind summary intent/final CAS and native changes invalidate unchanged pointers",async()=>{
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
 const service:any=ingest;
 const captured=await captureHistorySource(service,identity,{schemaGeneration:gen});
 expect(captured.mask.winners.has("native-19")).toBe(true);
 const objectKey=historyDerivedKey(captured,"summary-short",id);
 await registerHistoryDerivedObject(service,captured,{key:objectKey,sha256:"b".repeat(64),bytes:100});
 const transact:any=async(work:any)=>{await ingest.query("BEGIN");try{const r=await work(ingest);await ingest.query("COMMIT");return r}catch(e){await ingest.query("ROLLBACK");throw e}};
 const pointer={kind:"short" as const,status:"ready" as const,objectKey,objectSha256:"b".repeat(64),objectBytes:100,keyId:"fixture",model:"fixture-model",builtAt:"2026-11-04T00:00:00Z"};
 await expect(publishHistorySummary(service,captured,pointer)).rejects.toThrow("transaction permit required");
 expect(await withHistorySourceAuthority(transact,captured,async tx=>publishHistorySummary(tx,captured,pointer))).toEqual({current:true,value:true});
 expect((await readHistorySummary(service,captured,"short"))?.objectKey).toBe(objectKey);
 const sliceKey=historyDerivedKey(captured,"bob-slice","32345678-1234-1234-1234-123456789abc");
 await registerHistoryDerivedObject(service,captured,{key:sliceKey,sha256:"b".repeat(64),bytes:100});
 await expect(withHistorySourceAuthority(transact,captured,async tx=>publishHistorySummary(tx,captured,{...pointer,objectKey:sliceKey}))).rejects.toThrow("registered encrypted summary required");
 expect((await readHistorySummary(service,captured,"short"))?.objectKey).toBe(objectKey);

 await owner.query("BEGIN");await owner.query("SELECT pg_advisory_xact_lock(hashtextextended(concat_ws(chr(31),'96bulls:directory-events',$1::text,$2::text,$3::text,$4::text),0))",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
 await assert.rejects(()=>withHistorySourceAuthority(transact,captured,async()=>{throw Error("must_not_run")}),(e:any)=>e.code==="40001");await owner.query("ROLLBACK");
 const before=(await db.query("SELECT manifest_key FROM sessions.session_index WHERE org_id=$1",[identity.orgId])).rows;
 await db.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES(repeat('e',64),'[]',$1,$2,'origin','native-19',repeat('c',64),repeat('d',64),repeat('f',64))",[identity.orgId,identity.workspaceId]);
 let calls=0;expect(await withHistorySourceAuthority(transact,captured,async()=>{calls++})).toEqual({current:false});expect(calls).toBe(0);
 expect((await db.query("SELECT manifest_key FROM sessions.session_index WHERE org_id=$1",[identity.orgId])).rows).toEqual(before);
 expect((await db.query("SELECT state FROM sessions.ingest_history_reconcile_outbox WHERE native_key_sha256=repeat('e',64)")).rows[0]!.state).toBe("pending");
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
proof.runtime=(await ingest.query("SELECT session_user,current_user,(SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname=current_user) privileged")).rows[0];assert.equal(proof.runtime.session_user,"bulls96_session_ingest");assert.equal(proof.runtime.privileged,false);proof.pass=true;
}catch(e:any){proof.pass=false;proof.failure={code:e.code,message:e.message};console.error(proof.failure);process.exitCode=1}
finally{await ingest?.end();await owner.end();writeFileSync("/home/khaliqgant/relay-briefs/history-source-actual-pg-20261104.json",JSON.stringify(proof,null,2));}
