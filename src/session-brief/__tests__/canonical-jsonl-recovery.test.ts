import {expect,test} from "bun:test";
import {recordBrief,sweepUnbriefed,claimWakeups} from "../store.ts";
import {database,hoopDb,pointer,KEY} from "./harness.ts";

test("historical JSONL recovery is native-only, scoped, exact-error and excludes active wakeups",async()=>{
 const pg=await database(),db=hoopDb(pg);
 const native={includeChanged:true,refreshCanonicalInput:true,recoverJsonlInputFailures:true,workspaceIds:[KEY.workspaceId]};
 try {
  await pointer(pg);await recordBrief(db,KEY,{'m-1':1},{status:'failed',error:'canonical_input_refused'});
  expect(await sweepUnbriefed(db,10,{includeChanged:true})).toBe(0);
  expect(await sweepUnbriefed(db,10,{...native,workspaceIds:['foreign']})).toBe(0);
  await pg.exec("UPDATE sessions.session_index SET raw_format='ndjson'");expect(await sweepUnbriefed(db,10,native)).toBe(0);
  await pg.exec("UPDATE sessions.session_index SET raw_format='jsonl'");
  await pointer(pg,{machine:'m-2'});await pg.exec("UPDATE sessions.session_index SET raw_format='ndjson' WHERE machine_id='m-2'");
  await recordBrief(db,KEY,{'m-1':1,'m-2':1},{status:'failed',error:'canonical_input_refused'});
  expect(await sweepUnbriefed(db,10,native)).toBe(0);
  await pg.exec("UPDATE sessions.session_index SET raw_format='jsonl'");
  for(const error of [null,'model_timeout','canonical_jsonl_input_refused']) {
   await pg.query('UPDATE sessions.session_briefs SET error=$1',[error]);expect(await sweepUnbriefed(db,10,native)).toBe(0);
  }
  await pg.exec("UPDATE sessions.session_briefs SET error='canonical_input_refused'");
  expect(await sweepUnbriefed(db,10,native)).toBe(1);
  expect(await sweepUnbriefed(db,10,native)).toBe(0);
  expect(await claimWakeups(db,1,300,[KEY.workspaceId])).toHaveLength(1);
  expect(await sweepUnbriefed(db,10,native)).toBe(0);
 }finally{await pg.close();}
},60000);
