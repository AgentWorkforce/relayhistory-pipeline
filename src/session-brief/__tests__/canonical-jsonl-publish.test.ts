import {expect,spyOn,test} from "bun:test";
import {objectKey,manifestKey} from "../../../packages/object-store/src/index.ts";
import {canonicalBriefInputFromEnv} from "../canonical-input.ts";
import {briefTick} from "../worker.ts";
import {recordBrief,sweepUnbriefed} from "../store.ts";
import {TENANT_SETTINGS_SQL} from "../../session-read-api/liveness.ts";
import {database,hoopDb,pointer,transcript,tempStore,loc,KEY,FakeModel,testConfig,read} from "./harness.ts";

test("native factory verifies JSONL before fake model publication and existing read-role freshness",async()=>{
 const pg=await database(),db=hoopDb(pg),store=tempStore(),model=new FakeModel();
 const prefix=KEY.orgId,place={...loc(),prefix};
 // The existing fixture writes unprefixed chunks; copy those exact sealed bytes
 // to the org-bound layout to exercise the real service Locker implementation.
 const manifest=await transcript(store,[{type:'user',sessionId:KEY.sessionId,timestamp:'2026-10-29T10:00:00Z',message:{role:'user',content:'Investigate the synthetic queue race.'}},{type:'assistant',sessionId:KEY.sessionId,timestamp:'2026-10-29T10:00:01Z',message:{role:'assistant',content:'Awaiting the lease fixed the race.'}}]);
 manifest.extensions={'96bulls':{user_id:'u-1'}};
 const {rawChunkKey,readAll}=await import('../../../packages/object-store/src/index.ts');
 for(const c of manifest.raw.chunks){const original=rawChunkKey(loc(),c.seq,c.start_offset,c.sha256_8,'jsonl');await store.put(rawChunkKey(place,c.seq,c.start_offset,c.sha256_8,'jsonl'),await readAll(await store.get(original)));}
 const path=manifestKey(place);await store.put(objectKey(path),new TextEncoder().encode(JSON.stringify(manifest)));
 const fake=spyOn(globalThis,'fetch').mockImplementation(async(input,init)=>{
  const url=new URL(input instanceof Request?input.url:String(input));expect(url.origin).toBe('http://127.0.0.1:9000');expect(init?.method??'GET').toBe('GET');
  const key=decodeURIComponent(url.pathname.replace(/^\/bucket\//,''));
  try{const object=await store.getObject(objectKey(key));return new Response(object.body,{status:200,headers:{etag:object.meta.version.eTag??'', 'content-length':String(object.meta.size)}});}catch{return new Response('<Error><Code>NoSuchKey</Code></Error>',{status:404});}
 });
 try{
  await pg.exec(read('supabase/bootstrap/session_read_api_role.sql').split('\n').filter(l=>!l.startsWith('\\')).join('\n'));
  await pointer(pg,{manifestKey:path});await pg.query("UPDATE sessions.session_index SET storage_provider='s3',raw_format='jsonl',content_sha256=decode($1,'hex')",[manifest.raw.content_sha256]);
  const input=canonicalBriefInputFromEnv({SESSION_BRIEF_CANONICAL_INPUT:'true',SESSION_BRIEF_ENABLED:'true',SESSION_BRIEF_WORKSPACE_IDS:KEY.workspaceId,LOCKER_BUCKET:'bucket',PIPELINE_ORG_FROM_PREFIX:'true',LOCKER_ENDPOINT:'http://127.0.0.1:9000',LOCKER_ACCESS_KEY_ID:'fixture',LOCKER_SECRET_ACCESS_KEY:'fixture'});
  // An exhausted old-format failure has no wakeup left. Its existing spend
  // is retained; the normal native tick admits one recovery, not a reset.
  await recordBrief(db,KEY,{'m-1':1},{status:'failed',error:'canonical_input_refused'});
  await pg.query(`INSERT INTO sessions.session_brief_attempts(org_id,workspace_id,harness,session_id,day,attempts) VALUES($1,$2,$3,$4,(now() AT TIME ZONE 'UTC')::date,2)`,[KEY.orgId,KEY.workspaceId,KEY.harness,KEY.sessionId]);
  expect(await sweepUnbriefed(db,10,{includeChanged:true})).toBe(0);
  const tick=briefTick({reader:{read:async()=>{throw Error('legacy reader cannot run')}} as never,store,transport:model,config:testConfig({throttleSeconds:0,sweepSeconds:1,sessionDailyCap:3,workspaceIds:[KEY.workspaceId]}),canonicalInput:input});
  expect(await tick(db,new Date())).toEqual({generated:1});expect(model.requests).toHaveLength(1);
  expect((await pg.query<{n:number}>("SELECT attempts AS n FROM sessions.session_brief_attempts")).rows[0]?.n).toBe(3);
  expect(await sweepUnbriefed(db,10,{includeChanged:true,recoverJsonlInputFailures:true,workspaceIds:[KEY.workspaceId]})).toBe(0);
  async function summary(org=KEY.orgId){await pg.exec('SET ROLE bulls96_read_api;BEGIN READ ONLY');try{await pg.query(TENANT_SETTINGS_SQL,[org,KEY.workspaceId]);const x=await pg.query('SELECT * FROM sessions.session_brief_summary_for($1,$2,$3)',[KEY.workspaceId,KEY.harness,KEY.sessionId]);await pg.exec('COMMIT');return x.rows;}catch(e){await pg.exec('ROLLBACK');throw e;}finally{await pg.exec('RESET ROLE');}}
  expect((await summary())[0]).toMatchObject({description:model.reply.description,model:'gpt-5.6-luna'});expect(await summary('foreign-org')).toEqual([]);
  // Corrupt current source bytes refuse before a second model call. Persisting
  // that new error cannot match the historical recovery predicate again.
  const chunk=manifest.raw.chunks[0]!;await store.put(rawChunkKey(place,chunk.seq,chunk.start_offset,chunk.sha256_8,'jsonl'),new Uint8Array([0]));
  await expect(input!(db,KEY,[{machineId:'m-1',manifestKey:path,revision:1}])).rejects.toMatchObject({code:'canonical_jsonl_input_refused'});
  await recordBrief(db,KEY,{'m-1':1},{status:'failed',error:'canonical_jsonl_input_refused'});
  expect(await sweepUnbriefed(db,10,{includeChanged:true,recoverJsonlInputFailures:true,workspaceIds:[KEY.workspaceId]})).toBe(0);expect(model.requests).toHaveLength(1);

  await pg.exec('UPDATE sessions.session_index SET revision=2');expect(await summary()).toEqual([]);await pg.exec('UPDATE sessions.session_index SET deleted_at=now()');expect(await summary()).toEqual([]);
 }finally{fake.mockRestore();await pg.close();}
},60000);
