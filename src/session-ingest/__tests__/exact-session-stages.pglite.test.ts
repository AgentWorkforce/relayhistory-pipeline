
import {test,expect} from "bun:test";
import {fixture} from "./exact-session-stages.fixture.ts";
test("050030: whole-stage malformed/mixed groups and privacy match the frozen exact 044090 function",async()=>{
 const f=await fixture();
 await f.deliver([["droid","fixture-target",["a","b"]],["droid","fixture-other",["c","d"]],["codex","fixture-target",["e","f"]]]);
 await f.enableAll();await f.prove();
 const a=(await f.db.query("SELECT claim_token,records,epochs FROM sessions.ingest_claim_attempts WHERE batch_id='b-1'")).rows[0] as any;
 const st=(await f.db.query("SELECT stage_token,group_ordinals FROM sessions.ingest_stage_intents WHERE batch_id='b-1'")).rows[0] as any;
 const attempt=async(name:string,bind={})=>{try{return {rows:await f.stages(name,"droid","fixture-target",bind)}}catch(e){return {code:(e as any).code??"unknown"}}};
 const check=async(label:string)=>{const before=await attempt("ref044090.stages"),after=await attempt("sessions.buffered_session_stages_for_target");expect([label,after]).toEqual([label,before]);return before};
 const reset=async()=>{
  await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$1::jsonb,epochs=$2::jsonb WHERE claim_token=$3",[JSON.stringify(a.records),JSON.stringify(a.epochs),a.claim_token]);
  await f.db.query("UPDATE sessions.ingest_stage_intents SET group_ordinals=$1::jsonb WHERE stage_token=$2",[JSON.stringify(st.group_ordinals),st.stage_token]);
  await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=0 WHERE claim_token=$1",[a.claim_token]);
 };
 const results=[{label:"baseline",result:await check("baseline")}];
 const groupCases=[
  ["unrelated empty group",[st.group_ordinals[0],[],st.group_ordinals[2]]],
  ["unrelated malformed group",[st.group_ordinals[0],"invalid",st.group_ordinals[2]]],
  ["duplicate ordinal outside target",[st.group_ordinals[0],[2,2],st.group_ordinals[2]]],
  ["ordinal duplicated across groups",[st.group_ordinals[0],[0,3],st.group_ordinals[2]]],
  ["target group mixed sessions",[[0,2],[1,3],st.group_ordinals[2]]],
  ["target group mixed providers",[[0,4],[2,3],[1,5]]],
  ["target missing ordinal",[[0,99],st.group_ordinals[1],st.group_ordinals[2]]],
 ];
 for(const [label,groups]of groupCases){await reset();await f.db.query("UPDATE sessions.ingest_stage_intents SET group_ordinals=$1::jsonb WHERE stage_token=$2",[JSON.stringify(groups),st.stage_token]);results.push({label,result:await check(label as string)});}
 const recordCases=[
  ["duplicate target record ordinal",[...a.records,a.records[0]]],
  ["duplicate unrelated record ordinal",[...a.records,a.records[2]]],
  ["unresolved unrelated ordinal",a.records.filter((_:unknown,i:number)=>i!==2)],
  ["null target source",a.records.map((x:any,i:number)=>i===0?{...x,source:null}:x)],
  ["malformed unrelated record",a.records.map((x:any,i:number)=>i===2?0:x)],
 ];
 for(const[label,records]of recordCases){await reset();await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$1::jsonb WHERE claim_token=$2",[JSON.stringify(records),a.claim_token]);results.push({label,result:await check(label as string)});}
 await reset();await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=5 WHERE claim_token=$1 AND ord=0",[a.claim_token]);results.push({label:"target ACK admission moved",result:await check("target ACK admission moved")});
 await reset();expect(await attempt("sessions.buffered_session_stages_for_target",{user:"fixture-other-user"})).toEqual(await attempt("ref044090.stages",{user:"fixture-other-user"}));
 console.log(JSON.stringify({qualification:"Synthetic PGlite only; frozen exact044090 function row/error equivalence",cases:results.map(x=>({label:x.label,rows:(x.result as any).rows?.length,code:(x.result as any).code}))}));
});

import {readFileSync} from "node:fs";
import {BUFFERED_STAGES_SQL,BUFFERED_TARGET_STAGES_SQL,ownerBufferedExactSessionEnabled,sqlOwnerBufferedReads} from "../../session-read-api/owner-buffered.ts";
test("050030 changes only the target-group prefilter in a new RPC",()=>{
 const old=readFileSync("supabase/migrations/20261105044090_claim_sessions_reader.sql","utf8").match(/CREATE OR REPLACE FUNCTION sessions\.buffered_session_stages_for\([\s\S]*?END\n\$buffered_stages\$;/)![0];
 const candidate=readFileSync("supabase/migrations/20261105050030_buffered_exact_session_stages.sql","utf8").match(/CREATE FUNCTION sessions\.buffered_session_stages_for_target\([\s\S]*?END\n\$buffered_stages\$;/)![0];
 const restored=candidate.replace("CREATE FUNCTION sessions.buffered_session_stages_for_target(","CREATE OR REPLACE FUNCTION sessions.buffered_session_stages_for(")
  .replace(/  \), target_groups AS MATERIALIZED \([\s\S]*?\n  \), judged AS \(/,"  ), judged AS (")
  .replace("      JOIN target_groups AS tg ON tg.stage_token = gr.stage_token AND tg.group_index = gr.group_index\n","");
 expect(restored).toBe(old);
 expect(BUFFERED_TARGET_STAGES_SQL.replace("_for_target(","_for(")).toBe(BUFFERED_STAGES_SQL);
});
test("050030 runtime opt-in is exact and default-off; source/session arguments remain bound",async()=>{
 for(const flag of [undefined,"","off","true","1","ON"," on "])expect(ownerBufferedExactSessionEnabled(flag)).toBe(false);
 expect(ownerBufferedExactSessionEnabled("on")).toBe(true);
 for(const exactSession of [undefined,false,true]){
  const calls:Array<{sql:string;params:unknown[]}> = [];
  const run:any=async(_o:string,_w:string,read:any)=>read({query:async(sql:string,params:unknown[]=[])=>{calls.push({sql,params});return {rows:[]}}});
  const reads=sqlOwnerBufferedReads(run,{exactSession});
  await reads.stages({orgId:"fixture-org",workspaceId:"fixture-workspace",userId:"fixture-user"} as any,{cutoverEpoch:"1".repeat(64),headEpoch:null,headVersion:null,headDigest:null,headKind:null,headPending:false,privacyBaselineSha256:"a".repeat(64)} as any,"droid","fixture-session");
  const q=calls.at(-1)!;expect(q.sql).toBe(exactSession?BUFFERED_TARGET_STAGES_SQL:BUFFERED_STAGES_SQL);
  expect(q.params.slice(-2)).toEqual(["droid","fixture-session"]);
  expect(q.params[0]).toBe("fixture-workspace");
 }
});


test("050030 requires the frozen owned reader and grants only the read role", async () => {
 const f=await fixture();
 const signature="sessions.buffered_session_stages_for_target(text,text,text,text,text,text,text,text,boolean,text)";
 const attrs=(await f.db.query(`SELECT p.prosecdef,p.provolatile,p.proconfig,l.lanname,
  (SELECT array_agg(r.rolname ORDER BY r.rolname) FROM aclexplode(p.proacl) a JOIN pg_roles r ON r.oid=a.grantee WHERE a.grantee<>p.proowner) AS grantees
  FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=$1::regprocedure`,[signature])).rows;
 expect(attrs).toEqual([{prosecdef:true,provolatile:"s",proconfig:["search_path=pg_catalog, sessions, pg_temp"],lanname:"plpgsql",grantees:["bulls96_read_api"]}]);
 const migration=readFileSync("supabase/migrations/20261105050030_buffered_exact_session_stages.sql","utf8");
 await expect(f.db.exec(migration)).rejects.toMatchObject({code:"55000"});
 await f.db.exec("DROP FUNCTION "+signature);
 await f.db.exec("ALTER FUNCTION sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text) VOLATILE");
 await expect(f.db.exec(migration)).rejects.toMatchObject({code:"55000"});
 expect((await f.db.query("SELECT to_regprocedure($1) IS NULL AS absent",[signature])).rows).toEqual([{absent:true}]);
});


test("050030 down requires its ledger, exact checksum/body and no later migration", async () => {
 const f=await fixture();
 const down=readFileSync("supabase/rollbacks/20261105050030_buffered_exact_session_stages.down.sql","utf8");
 const migration=readFileSync("supabase/migrations/20261105050030_buffered_exact_session_stages.sql","utf8");
 const sha=new Bun.CryptoHasher("sha256").update(migration).digest("hex");
 const own="upstream:20261105050030_buffered_exact_session_stages.sql";
 const signature="sessions.buffered_session_stages_for_target(text,text,text,text,text,text,text,text,boolean,text)";
 const refuse=async()=>{await expect(f.db.exec(down)).rejects.toMatchObject({code:"55000"});await f.db.exec("ROLLBACK");};
 await refuse();
 await f.db.exec("CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text)");
 await f.db.query("INSERT INTO sessions.__96bulls_migrations VALUES($1,$2)",[own,"wrong"]);await refuse();
 await f.db.query("UPDATE sessions.__96bulls_migrations SET sha256=$1",[sha]);
 await f.db.exec("ALTER FUNCTION "+signature+" VOLATILE");await refuse();
 await f.db.exec("ALTER FUNCTION "+signature+" STABLE");
 await f.db.exec("INSERT INTO sessions.__96bulls_migrations VALUES('upstream:20990101000000_later.sql','fixture')");await refuse();
 await f.db.exec("DELETE FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:2099%'");
 const before=(await f.db.query("SELECT prosrc FROM pg_proc WHERE oid='sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)'::regprocedure")).rows;
 await f.db.exec(down);
 expect((await f.db.query("SELECT to_regprocedure($1) IS NULL AS absent",[signature])).rows).toEqual([{absent:true}]);
 expect((await f.db.query("SELECT * FROM sessions.__96bulls_migrations")).rows).toEqual([]);
 expect((await f.db.query("SELECT prosrc FROM pg_proc WHERE oid='sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)'::regprocedure")).rows).toEqual(before);
});

test("050030 mixed-claim regression: 17 claims / 6939 records return only the requested groups", async () => {
 const f=await fixture();
 for(let batch=0;batch<17;batch++){
  const groups:Array<[string,string,string[]]>=[["droid","fixture-wide-target",[`${batch}-target-a`,`${batch}-target-b`]]];
  for(let session=0;session<81;session++)groups.push(["droid",`fixture-wide-other-${session}`,Array.from({length:5},(_,record)=>`${batch}-${session}-${record}`)]);
  groups.push(["codex","fixture-wide-extra",Array.from({length:batch===16?4:1},(_,i)=>`${batch}-extra-${i}`)]);
  await f.deliver(groups);
 }
 await f.enableAll();await f.prove();
 expect((await f.db.query("SELECT count(*)::int claims,sum(jsonb_array_length(records))::int records FROM sessions.ingest_claim_attempts")).rows).toEqual([{claims:17,records:6939}]);
 const start=performance.now();
 const before=await f.stages("ref044090.stages","droid","fixture-wide-target");
 const frozenMs=performance.now()-start;
 const next=performance.now();
 const after=await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-wide-target");
 const targetMs=performance.now()-next;
 expect(after).toEqual(before);
 expect(after).toHaveLength(17);
 expect(after.reduce((n,r)=>n+r.kept_ordinals.length,0)).toBe(34);
 expect(after.every(r=>r.group_indexes.length===1&&r.group_indexes[0]===0)).toBe(true);
 console.log(JSON.stringify({qualification:"synthetic PGlite full-function, timing diagnostic only; no latency assertion",claims:17,expandedRecords:6939,targetRecords:34,frozenMs,targetMs}));
});
