/**
 * TEST-ONLY functional evidence for the ledger v8 Phase 1 prototype (packed ACK vectors).
 * Backends: ACK_V2_BACKENDS=pglite[,pg] (pg needs ACK_V2_TEST_PG_URL to an owned, disposable PG17).
 * Never points at a deployed database.
 */
import {describe,test,expect,setDefaultTimeout} from "bun:test";
import {readFileSync} from "node:fs";
import {join} from "node:path";
import {fixture,record,meta,TENANT,attemptSha,root,FIXTURE,type F,type Mode} from "./ack-v2-support.ts";
setDefaultTimeout(600000);
const backends=(process.env.ACK_V2_BACKENDS??"pglite").split(",") as ("pglite"|"pg")[];
const T2={orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:"user-2"};
const code=(e:any,f?:F)=>[e?.code,e?.stage,e?.cause_code,f?.lastSqlError()].filter(Boolean).join(" ")||String(e?.message??e);
async function expectCode(p:Promise<unknown>,c:string|RegExp,f?:F){let err:any=null;try{await p}catch(e){err=e}expect(err).not.toBeNull();
 const s=[err?.code,err?.cause_code,err?.message,f?.lastSqlError()].filter(Boolean).join(" ");if(typeof c==="string")expect(s).toContain(c);else expect(s).toMatch(c)}

/** One deterministic history exercising accept, stale reuse, legacy custody (equal/lower/higher), in-batch twin
 *  revisions, cross-claim reuse, selection clear, cross-user refusal, reset, a zero-allocation claim and GC readers. */
async function scenario(f:F){
 const out:any[]=[];
 const step=async(name:string,fn:()=>Promise<unknown>)=>{try{const r:any=await fn();out.push([name,"ok",r?.kind??r?.status??null])}catch(e){out.push([name,"err",code(e,f)])}};
 const old=record({record_id:"old",revision:8,session_id:"sess-L"}),old2=record({record_id:"old2",revision:5,session_id:"sess-L"}),old3=record({record_id:"old3",revision:2,session_id:"sess-L"});
 await step("b1",()=>f.send("b1",[...[1,2,3,4,5].map(i=>record({record_id:"r"+i,session_id:"sess-A"})),record({record_id:"r9",session_id:"sess-B"})]));
 await step("b2",()=>f.send("b2",[record({record_id:"r1",revision:2,session_id:"sess-A"}),record({record_id:"r2",session_id:"sess-A"}),record({record_id:"r6",session_id:"sess-A"})]));
 await step("b9-zero-alloc",()=>f.send("b9",[record({record_id:"r1",revision:1,session_id:"sess-A"})]));
 await step("b3",async()=>f.send("b3",[old,record({record_id:"old2",revision:1,session_id:"sess-L"}),record({record_id:"old3",revision:9,session_id:"sess-L"})],
  {old:await meta(old),old2:await meta(old2),old3:await meta(old3)}));
 await step("b4",()=>f.send("b4",[record({record_id:"r7",revision:1,session_id:"sess-A"}),record({record_id:"r7",revision:2,session_id:"sess-A"})]));
 await step("b5",()=>f.send("b5",[record({record_id:"r6",session_id:"sess-A"}),record({record_id:"r8",session_id:"sess-A"})]));
 await step("sel",()=>f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"sess-B"}],enabled:false}));
 await step("b6",()=>f.send("b6",[record({record_id:"r9",session_id:"sess-C"})]));
 await step("b7-cross-user",()=>f.send("b7",[record({record_id:"r1",revision:3,session_id:"sess-A"})],{},T2));
 await step("reset",()=>f.control("reset",{request_id:"reset-1"}));
 await step("b8",()=>f.send("b8",[record({record_id:"r2",revision:5,session_id:"sess-A"})]));
 await step("b10-again",()=>f.send("b10",[record({record_id:"r3",revision:4,session_id:"sess-A"}),record({record_id:"old3",revision:9,session_id:"sess-L"})]));
 for(const b of ["b1","b2","b3","b4","b5","b6","b8","b9","b10"]){try{out.push(["seats",b,await f.seats(b)])}catch(e){out.push(["seats",b,"err",code(e)])}}
 try{out.push(["gc",await f.gcOrdinals()])}catch(e){out.push(["gc","err",code(e)])}
 out.push(["slots",await f.slots()]);
 return out;
}

for(const kind of backends)describe("ack v2 prototype on "+kind,()=>{
 test("P1/P4/P8(T1-T5) differential: baseline v1 ≡ bridge(v1 mode) ≡ bridge(v2 mode) over the real delivery path",async()=>{
  const runs:Record<Mode,any[]>={} as never;
  for(const m of ["baseline","bridge-v1","bridge-v2"] as Mode[]){const f=await fixture(kind,m);try{runs[m]=await scenario(f);
   if(m==="bridge-v2"){const v=(await f.db.query("SELECT count(*)::int n,sum(allocated_count)::int alloc,count(*) FILTER(WHERE allocated_count=0)::int zero FROM sessions.ingest_ack_vectors")).rows[0];
    const d=(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_lazy_ack_decisions)::int d,(SELECT count(*) FROM sessions.ingest_receipt_seats)::int s")).rows[0];
    console.log("v2 vectors",JSON.stringify(v),"v1 rows written in v2 mode",JSON.stringify(d));expect(d).toEqual({d:0,s:0});expect(v.zero).toBeGreaterThan(0)}}
   finally{await f.close()}}
  console.log("transcript steps",JSON.stringify(runs.baseline.filter(x=>x[0]!=="seats"&&x[0]!=="slots"&&x[0]!=="gc")));
  expect(runs["bridge-v1"]).toEqual(runs.baseline);
  expect(runs["bridge-v2"]).toEqual(runs.baseline);
  // the scenario actually exercised the intended paths
  const steps=Object.fromEntries(runs.baseline.filter(x=>x.length===3&&x[1]!=="seats").map(x=>[x[0],x[1]+":"+x[2]]));
  expect(steps["b7-cross-user"]).toContain("err");
  const seatRows=runs.baseline.filter(x=>x[0]==="seats").flatMap(x=>x[2]?.records??[]);
  expect(seatRows.some((r:any)=>r.bodyAccepted===false)).toBe(true);
 });

 test("P3 decoder fuzz: every corruption refuses 55000",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   await f.send("fz",[record({record_id:"a",session_id:"s"}),record({record_id:"b",session_id:"s"})]);
   await f.send("fz2",[record({record_id:"a",session_id:"s"})]); // reused-only (zero allocation) vector
   const vrow=async(b:string)=>(await f.db.query("SELECT v.claim_token,encode(v.vec,'hex') vec FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token) WHERE a.batch_id=$1",[b])).rows[0];
   const base=await vrow("fz"),reuse=await vrow("fz2");
   await f.db.exec("ALTER TABLE sessions.ingest_ack_vectors DISABLE TRIGGER ack_guard_vector");
   const mutate=async(ct:string,hex:string,fixHash=true,extra="")=>{await f.db.query("UPDATE sessions.ingest_ack_vectors SET vec=decode($2,'hex')"+(fixHash?",vec_sha256=sha256(decode($2,'hex'))":"")+extra+" WHERE claim_token=$1",[ct,hex])};
   const restore=async(ct:string,hex:string)=>mutate(ct,hex,true);
   const B=base.vec as string,R=reuse.vec as string;
   const cases:[string,string,string,boolean?][]=[
    ["truncated",base.claim_token,B.slice(0,-2)],["trailing",base.claim_token,B+"00"],["version",base.claim_token,"02"+B.slice(2)],["kind",base.claim_token,B.slice(0,2)+"02"+B.slice(4)],
    ["n",base.claim_token,B.slice(0,4)+"0003"+B.slice(8)],["alloc",base.claim_token,B.slice(0,8)+"0001"+B.slice(12)],
    ["reserved-flag",base.claim_token,B.slice(0,16)+"0b"+B.slice(18)],["flags-none",base.claim_token,B.slice(0,16)+"00"+B.slice(18)],
    ["accepted-reused",base.claim_token,B.slice(0,16)+"05"+B.slice(18)],["dup-ord",base.claim_token,B.slice(0,22)+B.slice(12,16)+B.slice(26)],
    ["noncanonical-varint",base.claim_token,B.slice(0,18)+"8100"+B.slice(20)],["offset",base.claim_token,B.slice(0,20)+"02"+B.slice(22)],
    ["hash-mismatch",base.claim_token,B.slice(0,-2)+(B.endsWith("00")?"01":"00"),false],
    ["reuse-truncated",reuse.claim_token,R.slice(0,-4)],["reuse-kind",reuse.claim_token,R.slice(0,36)+"07"+R.slice(38)],["reuse-seat-zero",reuse.claim_token,R.slice(0,20)+"0000000000000000"+R.slice(36)],
   ];
   for(const [name,ct,hex,fix] of cases){await mutate(ct,hex,fix!==false);
    await expectCode(f.db.query("SELECT count(*) FROM sessions.ingest_ack_entries($1)",[ct]),"55000").catch(e=>{throw Error(name+": "+e)});
    await restore(ct,ct===base.claim_token?B:R)}
   // attempt identity and clock identity bindings recompute from live rows (§11.1)
   await f.db.exec("ALTER TABLE sessions.ingest_claim_attempts DISABLE TRIGGER ack_attempt_immutable");
   await f.db.query("UPDATE sessions.ingest_claim_attempts SET digest=repeat('e',64) WHERE claim_token=$1",[base.claim_token]);
   await expectCode(f.db.query("SELECT count(*) FROM sessions.ingest_ack_entries($1)",[base.claim_token]),"attempt identity changed");
   await f.db.exec("ALTER TABLE sessions.ingest_receipt_seat_clock DISABLE TRIGGER ack_clock_immutable");
   await f.db.query("UPDATE sessions.ingest_receipt_seat_clock SET initial_generation=repeat('f',64)");
   await expectCode(f.db.query("SELECT count(*) FROM sessions.ingest_ack_entries($1)",[reuse.claim_token]),"clock identity mismatch");
  }finally{await f.close()}
 });

 test("P5/P9 sequential + §12.1 guards: unconditional v2 refusal of old v1 paths, immutability, discriminator",async()=>{
  const f=await fixture(kind,"bridge-v1");try{
   await f.send("pre",[record({record_id:"p1",session_id:"s"})]);          // ACKed as v1, stamped format 1
   expect((await f.db.query("SELECT ack_format FROM sessions.ingest_claim_attempts WHERE batch_id='pre'")).rows[0].ack_format).toBe(1);
   await f.db.query("SELECT sessions.ingest_ack_format_set('v2')");
   await f.send("post",[record({record_id:"p2",session_id:"s"})]);
   const pre=(await f.db.query("SELECT a.claim_token,t.stage_token AS ack_stage_token FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_stage_intents t USING(claim_token) WHERE a.batch_id='pre'")).rows[0];
   const post=(await f.db.query("SELECT * FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token) WHERE a.batch_id='post'")).rows[0];
   // old v1 seat / decision inserts are refused in mode v2 (stale old body)
   await expectCode(f.db.query("INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch) VALUES(repeat('1',64),'[]',9007199254749999,repeat('2',64),'claude','s',0)"),"refused in mode v2");
   await expectCode(f.db.query("INSERT INTO sessions.ingest_lazy_ack_decisions SELECT * FROM sessions.ingest_lazy_ack_decisions LIMIT 1"),"refused in mode v2");
   // the original v1 body, called directly after the flip, cannot write
   await expectCode(f.db.query("SELECT sessions.ingest_lazy_ack_batch_v1(to_jsonb(b)) FROM sessions.ingest_batches b WHERE batch_id='post'"),/refused in mode v2|stage required|55000|40001/);
   // vectors immutable
   await expectCode(f.db.query("UPDATE sessions.ingest_ack_vectors SET n=n WHERE claim_token=$1",[post.claim_token]),"immutable");
   await expectCode(f.db.query("DELETE FROM sessions.ingest_ack_vectors WHERE claim_token=$1",[post.claim_token]),"immutable");
   await expectCode(f.db.query("TRUNCATE sessions.ingest_ack_vectors CASCADE"),"truncate refused");
   // a v1-stamped claim cannot get a vector (discriminator)
   await expectCode(f.db.query(`INSERT INTO sessions.ingest_ack_vectors(claim_token,ack_stage_token,format_version,n,allocated_count,clock_before,seat_lo,seat_hi,attempt_sha256,clock_identity_sha256,vec,vec_sha256)
    VALUES($1,$2,1,1,0,1,NULL,NULL,sha256('x'),sha256('y'),'\\x010100010000','\\x00')`,[pre.claim_token,pre.ack_stage_token]),/another format|vec_sha256|55000|23514/);
   // write-once discriminator and hashed-field immutability
   await expectCode(f.db.query("UPDATE sessions.ingest_claim_attempts SET ack_format=1 WHERE claim_token=$1",[post.claim_token]),"immutable");
   await expectCode(f.db.query("UPDATE sessions.ingest_claim_attempts SET records=records||'[]' , batch_id=batch_id||'' , digest=repeat('9',64) WHERE claim_token=$1",[post.claim_token]),"immutable");
   // clock identity immutable
   await expectCode(f.db.query("UPDATE sessions.ingest_receipt_seat_clock SET allocated_high_water=allocated_high_water+1"),"immutable");
   // prototype limit: rollback writer not implemented => refuse
   await expectCode(f.db.query("SELECT sessions.ingest_ack_format_set('v1')"),"rollback writer not implemented");
   // readers still serve both formats
   expect((await f.seats("pre")).records.length).toBe(1);expect((await f.seats("post")).records.length).toBe(1);
  }finally{await f.close()}
 });

 test("P10 three-pointer CHECK and P10b set validator negatives (binding, source authority, cutover)",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   const old2=record({record_id:"L",revision:5,session_id:"s"});
   await f.send("v1",[record({record_id:"x",session_id:"s"}),record({record_id:"y",session_id:"s"})]);
   await f.send("v2",[record({record_id:"L",revision:1,session_id:"s"})],{L:await meta(old2)}); // legacy: not-accepted allocated seat
   const sl=async(rid:string)=>(await f.db.query("SELECT * FROM sessions.ingest_lazy_native_slots WHERE record_id=$1",[rid])).rows[0];
   const x=await sl("x"),y=await sl("y"),L=await sl("L");
   const notAccepted=(await f.db.query("SELECT e.seat::text FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token),LATERAL sessions.ingest_ack_entries(v.claim_token) e WHERE a.batch_id='v2' AND NOT e.accepted AND e.allocated")).rows[0].seat;
   const upd=(set:string,key:string)=>f.db.query("UPDATE sessions.ingest_lazy_native_slots SET "+set+" WHERE key_sha256=$1",[key]);
   // P10: pointer cardinality
   await expectCode(upd("current_seat_key=repeat('a',64)",x.key_sha256),/23514|ack_slot_pointer/);
   await expectCode(upd("current_legacy_key='"+L.current_legacy_key+"'",x.key_sha256),/23514|ack_slot_pointer/);
   await expectCode(upd("current_seat_key=repeat('a',64),current_seat=NULL,current_legacy_key='"+L.current_legacy_key+"'",x.key_sha256),/23514|ack_slot_pointer/);
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_owner=NULL WHERE key_sha256=$1",[x.key_sha256]),/23514|ack_slot_pointer/);
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_seat=NULL WHERE key_sha256=$1",[x.key_sha256]),/23514|ack_slot_pointer/);
   // P10b: binding negatives
   const bad:[string,string][]=[["another slot's accepted seat","current_seat="+y.current_seat],["non-accepted allocated seat","current_seat="+notAccepted],
    ["wrong owner","current_owner='user-9'"],["wrong revision","current_revision=current_revision+1"],["wrong digest","current_digest=repeat('0',64)"],
    ["wrong source","current_source='codex'"],["wrong session","current_session='other'"],["NULL session","current_session=NULL"],
    ["wrong reset epoch","current_reset_epoch=current_reset_epoch+1"],["wrong fence epoch","current_fence_epoch=current_fence_epoch+1"],
    ["wrong lineage","lineage=lineage+1"],["wrong source identity","source_identity_sha256=repeat('e',64)"],["wrong control epoch","control_epoch=repeat('e',64)"],
    ["seat in no vector","current_seat=current_seat+1000000"]];
   for(const [name,set] of bad)await expectCode(upd(set,x.key_sha256),/55000|binding refused|23514|23503/).catch(e=>{throw Error(name+": "+e)});
   // multi-row statement mixing valid and invalid refuses entirely
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+(CASE WHEN key_sha256=$1 THEN 1 ELSE 0 END) WHERE key_sha256 IN($1,$2)",[x.key_sha256,y.key_sha256]),"binding refused");
   // valid no-op multi-row update passes
   await f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage WHERE key_sha256 IN($1,$2)",[x.key_sha256,y.key_sha256]);
   // seat at or below cutover is refused even if it would otherwise bind (set cutover above it)
   await f.db.query("UPDATE sessions.ingest_ack_format SET cutover_seat=$1",[x.current_seat]);
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage WHERE key_sha256=$1",[x.key_sha256]),"binding refused");
   expect(L.current_legacy_key).not.toBeNull();
  }finally{await f.close()}
 });

 test("P10b writer coverage fault injection: omitted, extra and wrong-tuple updates roll back the whole ACK",async()=>{
  const src=readFileSync(join(root,FIXTURE),"utf8");
  const v2=src.slice(src.indexOf("CREATE FUNCTION sessions.ingest_lazy_ack_batch_v2"),src.indexOf("END $v2$;")+9).replace("CREATE FUNCTION","CREATE OR REPLACE FUNCTION");
  const faults:[string,string,string][]=[
   ["omit one accepted entry","FROM want w WHERE n.key_sha256=w.native_key RETURNING","FROM want w WHERE n.key_sha256=w.native_key AND w.idx<>(SELECT min(idx) FROM want) RETURNING"],
   ["extra unrelated slot","FROM want w WHERE n.key_sha256=w.native_key RETURNING","FROM want w WHERE n.key_sha256=w.native_key OR (n.record_id='bystander' AND w.idx=(SELECT min(idx) FROM want)) RETURNING"],
   ["wrong lineage","SET lineage=w.lineage,","SET lineage=w.lineage+1,"],
   ["wrong seat","current_seat=w.seat,","current_seat=w.seat+(CASE WHEN w.idx=(SELECT max(idx) FROM want) THEN 1000 ELSE 0 END),"],
  ];
  for(const [name,a,b] of faults){
   const f=await fixture(kind,"bridge-v2");try{
    await f.send("pre",[record({record_id:"bystander",session_id:"s"})]);
    expect(v2.includes(a)).toBe(true);await f.db.exec(v2.replace(a,b));
    const before=(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_ack_vectors)::int v,(SELECT last_seat::text FROM sessions.ingest_receipt_seat_clock) s")).rows[0];
    await expectCode(f.send("hit",[record({record_id:"m1",session_id:"s"}),record({record_id:"m2",session_id:"s"})]),/coverage mismatch|binding refused/,f).catch(e=>{throw Error(name+": "+e)});
    const after=(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_ack_vectors)::int v,(SELECT last_seat::text FROM sessions.ingest_receipt_seat_clock) s")).rows[0];
    expect(after).toEqual(before);
   }finally{await f.close()}
  }
 });

 test("P11 isolation: RR/SERIALIZABLE refused for flip, ACK bridge and readers",async()=>{
  const f=await fixture(kind,"bridge-v1");try{
   for(const lvl of ["REPEATABLE READ","SERIALIZABLE"]){
    await f.db.exec("BEGIN ISOLATION LEVEL "+lvl);
    await expectCode(f.db.query("SELECT sessions.ingest_ack_format_set('v2')"),"READ COMMITTED required");await f.db.exec("ROLLBACK");
    await f.db.exec("BEGIN ISOLATION LEVEL "+lvl);
    await expectCode(f.db.query("SELECT sessions.ack_require_rc()"),"READ COMMITTED required");await f.db.exec("ROLLBACK");
   }
  }finally{await f.close()}
 });

 test("P13 golden vectors: TypeScript preimage ≡ SQL preimage for real claim rows",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   await f.send("g1",[record({record_id:"Σİ😀",session_id:"s"}),record({record_id:"plain",session_id:null as any})]);
   await f.send("g2",[record({record_id:"plain",revision:2,session_id:null as any})]);
   const rows=(await f.db.query(`SELECT a.claim_token::text,a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest,a.reset_epoch::text,a.schema_generation,
    a.source_control_version::text,a.source_identity_sha256,a.source_witness_sha256,a.records::text records,a.epochs::text epochs,a.source_processing_ranks::text source_processing_ranks,
    encode(sessions.ack_attempt_sha(a),'hex') sql,encode(v.attempt_sha256,'hex') stored FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_ack_vectors v USING(claim_token)`)).rows;
   expect(rows.length).toBe(2);
   for(const r of rows){expect(attemptSha(r)).toBe(r.sql);expect(r.stored).toBe(r.sql)}
  }finally{await f.close()}
 });

 test("P14 max seat: allocation ending exactly at 2^63-1 is allowed (no narrowing), next refuses exhausted; zero-allocation at max",async()=>{
  for(const m of ["baseline","bridge-v2"] as Mode[]){
   const f=await fixture(kind,m,{highWater:"9223372036854775805"});try{console.log("P14 mode",m);
    await f.send("m1",[record({record_id:"a",session_id:"s"}),record({record_id:"b",session_id:"s"})]);
    expect((await f.seats("m1")).records.map((r:any)=>r.seat)).toEqual(["9223372036854775806","9223372036854775807"]);
    await f.send("m2",[record({record_id:"a",session_id:"s"})]); // stale -> reuse only, zero allocation at the max clock
    await expectCode(f.send("m3",[record({record_id:"c",session_id:"s"})]),"ACK seats exhausted",f);
    if(m==="bridge-v2")expect((await f.db.query("SELECT seat_hi::text FROM sessions.ingest_ack_vectors WHERE seat_hi IS NOT NULL")).rows[0].seat_hi).toBe("9223372036854775807");
   }finally{await f.close()}
  }
 });
});
