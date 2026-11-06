/**
 * TEST-ONLY functional evidence for the ledger v8 Phase 1 prototype (packed ACK vectors).
 * Backends: ACK_V2_BACKENDS=pglite[,pg] (pg needs ACK_V2_TEST_PG_URL to an owned, disposable PG17).
 * Never points at a deployed database.
 */
import {describe,test,expect,setDefaultTimeout} from "bun:test";
import {readFileSync} from "node:fs";
import {join} from "node:path";
import {fixture,record,meta,TENANT,attemptSha,clockSha,root,FIXTURE,type F,type Mode} from "./ack-v2-support.ts";
setDefaultTimeout(600000);
const backends=(process.env.ACK_V2_BACKENDS??"pglite").split(",") as ("pglite"|"pg")[];
const T2={orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:"user-2"};
const code=(e:any,f?:F)=>[e?.code,e?.stage,e?.cause_code,f?.lastSqlError()].filter(Boolean).join(" ")||String(e?.message??e);
async function expectCode(p:Promise<unknown>,c:string|RegExp,f?:F){let err:any=null;try{await p}catch(e){err=e}expect(err).not.toBeNull();
 const s=[err?.code,err?.cause_code,err?.message,f?.lastSqlError()].filter(Boolean).join(" ");if(typeof c==="string")expect(s).toContain(c);else expect(s).toMatch(c)}

/** One deterministic history exercising accept, stale reuse, legacy custody (equal/lower/higher), in-batch twin
 *  revisions, cross-claim reuse, selection clear, cross-user refusal, reset, a zero-allocation claim and GC readers. */
const GEN="a".repeat(64);
/** strip per-run random identifiers (uuids, leases, timestamps) so outputs compare across independent databases */
const UUID=/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;
const norm=(v:any):any=>Array.isArray(v)?v.map(norm):v&&typeof v==="object"?Object.fromEntries(Object.entries(v)
  .filter(([k])=>!/token|lease|attempt|until|expires|_at$|At$|generation_id|^initialized_at$/i.test(k))
  // the stage object's S3 key and byte hash derive from the random claim token (per-run), not from the ledger format
  .filter(([k])=>!("groupOrdinals" in v&&(k==="key"||k==="sha256")))
  .map(([k,x])=>[k,norm(x)]))
 :typeof v==="string"?v.replace(UUID,"<uuid>"):v;
async function scenario(f:F,flips:Record<string,"v1"|"v2">={}){
 const out:any[]=[];
 const step=async(name:string,fn:()=>Promise<unknown>)=>{if(flips[name])await f.db.query("SELECT sessions.ingest_ack_format_set($1)",[flips[name]]);try{const r:any=await fn();out.push([name,"ok",r?.kind??r?.status??null])}catch(e){out.push([name,"err",code(e,f)])}};
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
 // P4: 620 whole-row native snapshot (byte-compatible projection) and a real publication claim, per live stage
 const stages=(await f.db.query("SELECT o.batch_id,o.active_stage_token::text st FROM sessions.ingest_buffer_outbox o WHERE o.state='buffered' ORDER BY 1")).rows;
 for(const r of stages){try{const j=(await f.db.query("SELECT sessions.ingest_history_native_snapshot($1)::text j",[r.st])).rows[0].j;out.push(["hist",r.batch_id,norm(JSON.parse(j))])}
  catch(e:any){out.push(["hist",r.batch_id,"err",e.message])}}
 await f.db.query("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour','synthetic','us-east-1','96bulls',$5)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,"b".repeat(64),GEN]);
 for(const r of stages){try{const res:any=await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest").then(()=>f.db.query("SELECT sessions.ingest_publication_claim($1::jsonb) r",[JSON.stringify({stageToken:r.st,keyRoot:"96bulls",bucket:"synthetic",region:"us-east-1",generation:GEN})]));
   await f.db.exec("COMMIT");out.push(["pub",r.batch_id,norm(res.rows[0].r)])}catch(e:any){await f.db.exec("ROLLBACK").catch(()=>{});out.push(["pub",r.batch_id,"err",e.message])}}
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

 test("P4 non-vacuous: 620 snapshot and a real publication claim succeed on clean stages and are byte-equal v1 vs (v1→v2 cutover)",async()=>{
  const runP4=async(m:Mode,flip:boolean)=>{const f=await fixture(kind,m);const out:any[]=[];try{
   await f.send("p1",[record({record_id:"a",session_id:"s1"}),record({record_id:"b",session_id:"s1"})]);
   if(flip)await f.db.query("SELECT sessions.ingest_ack_format_set('v2')");
   await f.send("p2",[record({record_id:"c",session_id:"s2"}),record({record_id:"d",session_id:"s2"}),record({record_id:"d",revision:2,session_id:"s2"})]);
   await f.send("p3",[record({record_id:"e",session_id:"s3"}),record({record_id:"b",revision:2,session_id:"s1"})]);
   await f.db.query("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour','synthetic','us-east-1','96bulls',$5)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,"b".repeat(64),GEN]);
   for(const b of ["p1","p2","p3"]){const st=(await f.db.query("SELECT active_stage_token::text st FROM sessions.ingest_buffer_outbox WHERE batch_id=$1",[b])).rows[0].st;
    try{out.push(["hist",b,norm(JSON.parse((await f.db.query("SELECT sessions.ingest_history_native_snapshot($1)::text j",[st])).rows[0].j))])}catch(e:any){out.push(["hist",b,"err",e.message])}
    try{await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");const r=await f.db.query("SELECT sessions.ingest_publication_claim($1::jsonb) r",[JSON.stringify({stageToken:st,keyRoot:"96bulls",bucket:"synthetic",region:"us-east-1",generation:GEN})]);
     await f.db.exec("COMMIT");out.push(["pub",b,norm(r.rows[0].r)])}catch(e:any){await f.db.exec("ROLLBACK").catch(()=>{});out.push(["pub",b,"err",e.message])}}
   return out}finally{await f.close()}};
  const v1=await runP4("baseline",false),v2=await runP4("bridge-v1",true);
  for(let i=0;i<v1.length;i++)if(JSON.stringify(v1[i])!==JSON.stringify(v2[i]))console.log("P4 DIFF",i,JSON.stringify(v1[i]).slice(0,1500),"\n  v2:",JSON.stringify(v2[i]).slice(0,1500));
  console.log("P4",JSON.stringify(v1.map(x=>[x[0],x[1],x[2]==="err"?"err: "+x[3]:"ok"])));
  expect(v2).toEqual(v1);
  // non-vacuous: all three 620 snapshots and two independent publication claims (one v1-format, one v2-format stage) succeed;
  // p3 shares p1's session head, so its claim is legitimately refused identically in both formats
  for(const b of ["p1","p2","p3"])expect(v1.find(x=>x[0]==="hist"&&x[1]===b)![2]).not.toBe("err");
  for(const b of ["p1","p2"])expect(v1.find(x=>x[0]==="pub"&&x[1]===b)![2]).not.toBe("err");
 });

 test("P2/T6c rollback: v1 → v2 → v1 (dual-aware writer) → v2 … over the same keys ≡ all-v1",async()=>{
  const base=await fixture(kind,"baseline");let ref:any[];try{ref=await scenario(base)}finally{await base.close()}
  const f=await fixture(kind,"bridge-v1");try{
   const got=await scenario(f,{b2:"v2",b5:"v1",b6:"v2",b8:"v1","b10-again":"v2"});
   const eras=(await f.db.query("SELECT mode FROM sessions.ingest_ack_format_epochs ORDER BY epoch")).rows.map(r=>r.mode);
   const compat=(await f.db.query("SELECT count(*)::int n FROM sessions.ingest_receipt_seats WHERE compat_vector_claim IS NOT NULL")).rows[0].n;
   const fmts=(await f.db.query("SELECT ack_format,count(*)::int n FROM sessions.ingest_claim_attempts WHERE ack_format IS NOT NULL GROUP BY 1 ORDER BY 1")).rows;
   console.log("T6c eras",JSON.stringify(eras),"compat aliases",compat,"formats",JSON.stringify(fmts));
   expect(eras).toEqual(["v1","v2","v1","v2","v1","v2"]);expect(fmts.length).toBe(2);expect(compat).toBeGreaterThan(0);
   expect(got).toEqual(ref!);
  }finally{await f.close()}
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
    ["accepted-reused",base.claim_token,B.slice(0,16)+"05"+B.slice(18)],["dup-ord",base.claim_token,B.slice(0,22)+B.slice(12,16)+B.slice(26)],["entry-order-not-processing-order",base.claim_token,B.slice(0,12)+B.slice(22,26)+B.slice(16,22)+B.slice(12,16)+B.slice(26)],
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
   // rollback is implemented (P2/T6c): flipping back to v1 now succeeds and later v1-format ACKs work
   await f.db.query("SELECT sessions.ingest_ack_format_set('v1')");
   await f.send("back",[record({record_id:"p2",session_id:"s"})]);   // stale re-send: aliases the v2 seat via a compat row
   expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_receipt_seats WHERE compat_vector_claim IS NOT NULL")).rows[0].n).toBe(1);
   // a forged compat alias (wrong key for the seat) is refused
   await expectCode(f.db.query("INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch,compat_vector_claim) SELECT repeat('7',64),key_material,seat+0,first_digest,source,session_id,fence_epoch,compat_vector_claim FROM sessions.ingest_receipt_seats WHERE compat_vector_claim IS NOT NULL"),/compat seat must alias|23505/);
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
   // the same binding negatives against a test-only validator copy WITHOUT the transition state machine:
   // each must still be refused by the BINDING itself (review M1: the state machine would otherwise shadow them)
   {const vd=(await f.db.query("SELECT pg_get_functiondef('sessions.ack_slot_validate()'::regprocedure) d")).rows[0].d as string;
    const a0=vd.indexOf(" IF TG_OP='UPDATE' THEN"),a1=vd.indexOf(" ELSIF EXISTS(SELECT 1 FROM nr WHERE current_seat IS NOT NULL",a0);expect(a0>0&&a1>a0).toBe(true);
    const orig=vd;await f.db.exec(vd.slice(0,a0)+" IF TG_OP='UPDATE' THEN NULL;\n"+vd.slice(a1));
    for(const [name,set] of bad.filter(([n])=>!/source identity|control epoch/.test(n)))
     await expectCode(upd(set,x.key_sha256),"binding refused").catch(e=>{throw Error("isolated binding, "+name+": "+e)});
    await f.db.exec(orig)}
   // multi-row statement mixing valid and invalid refuses entirely
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+(CASE WHEN key_sha256=$1 THEN 1 ELSE 0 END) WHERE key_sha256 IN($1,$2)",[x.key_sha256,y.key_sha256]),/binding refused|slot regression refused|coverage mismatch/);
   // valid no-op multi-row update passes
   await f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage WHERE key_sha256 IN($1,$2)",[x.key_sha256,y.key_sha256]);
   // seat at or below cutover is refused even if it would otherwise bind (set cutover above it)
   // the era table that gates the validator is append-only and bound to the format row + clock (review M2)
   await expectCode(f.db.query("INSERT INTO sessions.ingest_ack_format_epochs(epoch,mode,first_seat) VALUES(999,'v1',$1)",[x.current_seat]),"format era must match");
   await expectCode(f.db.query("UPDATE sessions.ingest_ack_format_epochs SET mode='v1'"),"append-only");
   await expectCode(f.db.query("DELETE FROM sessions.ingest_ack_format_epochs"),"append-only");
   await expectCode(f.db.query("TRUNCATE sessions.ingest_ack_format_epochs"),"append-only");
   expect(L.current_legacy_key).not.toBeNull();
  }finally{await f.close()}
 });

 test("P10b final-winner rule: earlier accepted entry of the same vector cannot be installed (manual, all fields matching)",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   await f.send("fw",[record({record_id:"R",revision:1,session_id:"s"}),record({record_id:"R",revision:2,session_id:"s"}),record({record_id:"Q",session_id:"s"})]);
   const e=(await f.db.query("SELECT e.idx,e.accepted,e.allocated,e.seat::text seat,e.lineage::text lineage,e.revision::text revision,e.candidate_digest,e.source,e.session_id,e.reset_epoch::text reset_epoch,e.fence_epoch::text fence_epoch FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token),LATERAL sessions.ingest_ack_entries(v.claim_token) e WHERE a.batch_id='fw' AND e.record_id='R' ORDER BY e.idx")).rows;
   expect(e.map((x:any)=>[x.accepted,x.allocated])).toEqual([[true,true],[true,true]]);   // two accepted entries, one native key: legitimate
   const snap=async()=>(await f.db.query("SELECT (to_jsonb(n)-'current_seat'-'lineage')::text j,current_seat::text seat,lineage::text lin FROM sessions.ingest_lazy_native_slots n WHERE record_id='R'")).rows[0];
   const before=await snap();
   expect(before.seat).toBe(String(e[1].seat));                                             // slot = final winner
   const first=e[0];
   await expectCode(f.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_seat=$1,lineage=$2,current_revision=$3,current_digest=$4,current_source=$5,
     current_session=$6,current_reset_epoch=$7,current_fence_epoch=$8 WHERE record_id='R'`,[first.seat,first.lineage,first.revision,first.candidate_digest,first.source,first.session_id,first.reset_epoch,first.fence_epoch]),"55000");
   expect(await snap()).toEqual(before);
   // isolate the final-winner BINDING from the no-regression rule (which also refuses any earlier seat): run the same
   // update against a test-only validator copy whose regression block is removed; the binding alone must still refuse
   const vd=(await f.db.query("SELECT pg_get_functiondef('sessions.ack_slot_validate()'::regprocedure) d")).rows[0].d as string;
   const a0=vd.indexOf(" IF TG_OP='UPDATE' THEN"),a1=vd.indexOf(" ELSIF EXISTS(SELECT 1 FROM nr WHERE current_seat IS NOT NULL",a0);
   expect(a0>0&&a1>a0).toBe(true);
   await f.db.exec(vd.slice(0,a0)+" IF TG_OP='UPDATE' THEN NULL;\n"+vd.slice(a1));
   await expectCode(f.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_seat=$1,lineage=$2,current_revision=$3,current_digest=$4 WHERE record_id='R'`,
     [first.seat,first.lineage,first.revision,first.candidate_digest]),"binding refused");
   expect(await snap()).toEqual(before);
   // and the slot's own (final) values pass unchanged
   await f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage WHERE record_id='R'");
  }finally{await f.close()}
 });

 test("P10b no regression (review M1): cross-claim winner, v2->v1 pointer, DELETE, identity change, INSERT with a v2 seat",async()=>{
  const f=await fixture(kind,"bridge-v1");try{
   await f.send("c0",[record({record_id:"R",revision:1,session_id:"s"})]);                     // v1 seat (key), lineage 1
   const v1key=(await f.db.query("SELECT current_seat_key k FROM sessions.ingest_lazy_native_slots WHERE record_id='R'")).rows[0].k;
   await f.db.query("SELECT sessions.ingest_ack_format_set('v2')");
   await f.send("c1",[record({record_id:"R",revision:2,session_id:"s"})]);                     // v2 winner, lineage 2
   await f.send("c2",[record({record_id:"R",revision:3,session_id:"s"})]);                     // v2 winner, lineage 3
   const c1=(await f.db.query("SELECT e.seat::text seat,e.lineage::text lineage,e.revision::text revision,e.candidate_digest,e.source,e.session_id,e.reset_epoch::text re,e.fence_epoch::text fe FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token),LATERAL sessions.ingest_ack_entries(v.claim_token) e WHERE a.batch_id='c1'")).rows[0];
   const snap=async()=>(await f.db.query("SELECT (to_jsonb(n)-'current_seat'-'lineage')::text j,current_seat::text s,lineage::text l FROM sessions.ingest_lazy_native_slots n WHERE record_id='R'")).rows[0];
   const before=await snap();
   // earlier CLAIM's final winner, every field matching it (incl. its lineage): binding alone would pass; no-regression refuses
   await expectCode(f.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_seat=$1,lineage=$2,current_revision=$3,current_digest=$4,current_source=$5,current_session=$6,
    current_reset_epoch=$7,current_fence_epoch=$8 WHERE record_id='R'`,[c1.seat,c1.lineage,c1.revision,c1.candidate_digest,c1.source,c1.session_id,c1.re,c1.fe]),"slot regression refused");
   // v2 -> older v1 pointer, keeping lineage: effective seat moves backwards
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_seat=NULL,current_seat_key=$1 WHERE record_id='R'",[v1key]),"slot regression refused");
   // v2 -> older v1 pointer with its lower lineage
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_seat=NULL,current_seat_key=$1,lineage=1 WHERE record_id='R'",[v1key]),"slot regression refused");
   await expectCode(f.db.query("DELETE FROM sessions.ingest_lazy_native_slots WHERE record_id='R'"),"never deleted");
   await expectCode(f.db.query("TRUNCATE sessions.ingest_lazy_native_slots CASCADE"),"never deleted");
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET key_sha256=repeat('9',64) WHERE record_id='R'"),"identity is immutable");
   await expectCode(f.db.query(`INSERT INTO sessions.ingest_lazy_native_slots SELECT (jsonb_populate_record(n,jsonb_build_object('key_sha256',repeat('8',64),'record_id','R2'))).* FROM sessions.ingest_lazy_native_slots n WHERE record_id='R'`),"insert with a v2 seat refused");
   expect(await snap()).toEqual(before);
  }finally{await f.close()}
 });

 test("P10b transition state machine (review v9 HIGH 2): forbidden edges refuse, nothing changes",async()=>{
  const f=await fixture(kind,"bridge-v1");try{
   const L=record({record_id:"L",revision:5,session_id:"s"});
   await f.send("k0",[record({record_id:"K",session_id:"s"})]);                                   // K: v1 seat key
   await f.send("l0",[record({record_id:"L",revision:1,session_id:"s"})],{L:await meta(L)});      // L: legacy (not accepted)
   const legacyKey=(await f.db.query("SELECT current_legacy_key k FROM sessions.ingest_lazy_native_slots WHERE record_id='L'")).rows[0].k;
   expect(legacyKey).not.toBeNull();
   await f.db.query("SELECT sessions.ingest_ack_format_set('v2')");
   await f.send("s0",[record({record_id:"S",session_id:"s"})]);                                   // S: v2 seat
   await f.send("u0",[record({record_id:"U",session_id:"s"})]);
   const snap=async()=>(await f.db.query("SELECT record_id,(to_jsonb(n)-'current_seat'-'lineage')::text j,current_seat::text s,lineage::text l FROM sessions.ingest_lazy_native_slots n ORDER BY 1")).rows;
   const before=await snap();
   const neg:[string,string,string][]=[
    ["v2 -> legacy (same lineage)","current_seat=NULL,current_legacy_key=$1","S"],
    ["v2 -> legacy (lineage+1)","current_seat=NULL,current_legacy_key=$1,lineage=lineage+1","S"],
    ["v1 -> legacy","current_seat_key=NULL,current_legacy_key=$1","K"],
    ["clear without lineage bump","current_owner=NULL,current_source=NULL,current_session=NULL,current_revision=NULL,current_digest=NULL,current_reset_epoch=NULL,current_fence_epoch=NULL,current_seat=NULL","S"],
    ["clear with lineage+2","current_owner=NULL,current_source=NULL,current_session=NULL,current_revision=NULL,current_digest=NULL,current_reset_epoch=NULL,current_fence_epoch=NULL,current_seat=NULL,lineage=lineage+2","S"],
    ["clear leaving residual custody fields","current_owner=NULL,current_seat=NULL,lineage=lineage+1","S"],
    ["direct owner transfer (v2)","current_owner='user-9'","S"],
    ["direct owner transfer (v1)","current_owner='user-9'","K"],
    ["legacy owner transfer","current_owner='user-9'","L"],
    ["lineage change without custody change","lineage=lineage+1","S"],
   ];
   for(const [name,set,rid] of neg){const usesKey=set.includes("$1");
    await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET "+set+" WHERE record_id='"+rid+"'",usesKey?[legacyKey]:[]),/slot regression refused|binding refused|23514/).catch(e=>{throw Error(name+": "+e)})}
   expect(await snap()).toEqual(before);
   // K accepts are bound (review v9-H1): a forged K pointer is refused even with lineage+1 and a higher seat
   const kRow=(await f.db.query("SELECT current_seat_key k,lineage::text l FROM sessions.ingest_lazy_native_slots WHERE record_id='K'")).rows[0];
   const otherSeat=(await f.db.query("SELECT key_sha256 k FROM sessions.ingest_receipt_seats WHERE key_sha256<>$1 ORDER BY seat DESC LIMIT 1",[kRow.k])).rows[0].k; // L's not-accepted seat (other record)
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_seat_key=$1,lineage=lineage+1 WHERE record_id='K'",[otherSeat]),"slot regression refused");
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1000,current_revision=current_revision+1 WHERE record_id='K'"),"slot regression refused");
   expect(await snap()).toEqual(before);
   // unowned -> legacy, and unowned lineage drift
   await f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"s"}],enabled:false});   // lazy_clear positive: K,L?,S,U -> U (+1)
   const cleared=(await f.db.query("SELECT count(*)::int n FROM sessions.ingest_lazy_native_slots WHERE current_owner IS NULL")).rows[0].n;expect(cleared).toBeGreaterThan(0);
   const u=(await f.db.query("SELECT record_id FROM sessions.ingest_lazy_native_slots WHERE current_owner IS NULL LIMIT 1")).rows[0].record_id;
   const mid=await snap();
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_owner='user-1',current_legacy_key=$1 WHERE record_id=$2",[legacyKey,u]),/slot regression refused|23514/);
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1 WHERE record_id=$1",[u]),"slot regression refused");
   expect(await snap()).toEqual(mid);
   // erasure resurrection: after lazy_clear, re-point the cleared K slot at its pre-clear v1 seat with its old custody, lineage+1
   const kc=(await f.db.query("SELECT current_owner o FROM sessions.ingest_lazy_native_slots WHERE record_id='K'")).rows[0];
   if(kc.o===null){const s0=(await f.db.query("SELECT key_sha256,key_material,first_digest,source,session_id,fence_epoch FROM sessions.ingest_receipt_seats WHERE key_sha256=$1",[kRow.k])).rows[0];
    await expectCode(f.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_owner='user-1',current_seat_key=$1,current_revision=1,current_digest=$2,current_source=$3,current_session=$4,
      current_reset_epoch=0,current_fence_epoch=$5,lineage=lineage+1 WHERE record_id='K'`,[s0.key_sha256,s0.first_digest,s0.source,s0.session_id,s0.fence_epoch]),"slot regression refused")}
   expect(kc.o).toBeNull();
  }finally{await f.close()}
 });

 test("K-edge binding, single-cause negatives against a fresh self-consistent receipt seat (second review MAJORs)",async()=>{
  const f=await fixture(kind,"bridge-v1");try{
   await f.send("k1",[record({record_id:"K",session_id:"s"})]);
   const k=(await f.db.query("SELECT n.current_seat_key,n.lineage::text lineage,s.key_material,s.seat::text seat FROM sessions.ingest_lazy_native_slots n JOIN sessions.ingest_receipt_seats s ON s.key_sha256=n.current_seat_key WHERE n.record_id='K'")).rows[0];
   const forge=async(rev:string,rid="K",digest="a".repeat(64))=>{const km=[...k.key_material];km[8]=rev;km[7]=rid;
    const r=(await f.db.query(`WITH x AS(SELECT $1::jsonb km,(SELECT last_seat+3+$2::int FROM sessions.ingest_receipt_seat_clock) seat)
     INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
     SELECT encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',encode(sha256(convert_to(km::text,'UTF8')),'hex'),seat::text)::text,'UTF8')),'hex'),km,seat,$3,'claude','s',0 FROM x
     RETURNING key_sha256`,[JSON.stringify(km),Math.floor(Math.random()*1000),digest])).rows[0];return r.key_sha256 as string};
   const good=await forge("2");
   const base={key:good,rev:"2",digest:"a".repeat(64),owner:"user-1",source:"claude",session:"s",reset:"0",fence:"0",lin:String(Number(k.lineage)+1)};
   const upd=(o:any)=>f.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_seat_key=$1,current_revision=$2,current_digest=$3,current_owner=$4,current_source=$5,current_session=$6,
     current_reset_epoch=$7,current_fence_epoch=$8,lineage=$9 WHERE record_id='K'`,[o.key,o.rev,o.digest,o.owner,o.source,o.session,o.reset,o.fence,o.lin]);
   const single:[string,any][]=[["revision",{rev:"3"}],["digest",{digest:"b".repeat(64)}],["owner",{owner:"user-9"}],["source",{source:"codex"}],["session",{session:"other"}],
    ["session NULL",{session:null}],["reset epoch",{reset:"1"}],["fence epoch",{fence:"1"}],["lineage +2",{lin:String(Number(k.lineage)+2)}],["lineage unchanged",{lin:k.lineage}]];
   for(const [name,d] of single)await expectCode(upd({...base,...d}),"slot regression refused").catch(e=>{throw Error("K single-cause "+name+": "+e)});
   await expectCode(upd({...base,key:await forge("2","OTHER")}),"slot regression refused");                 // another record's fresh seat
   await expectCode(upd({...base,key:await forge("1"),rev:"1"}),"slot regression refused");                  // fresh self-consistent seat, revision not increasing
   // PROVENANCE (addendum 6a09d424): the former pinned positive is now REFUSED at commit: a receipt-only winner with no
   // accepted decision, ahead of the committed clock
   const snap=async()=>(await f.db.query("SELECT to_jsonb(n)::text j FROM sessions.ingest_lazy_native_slots n WHERE record_id='K'")).rows[0].j;
   const before=await snap();
   await expectCode(upd(base),/ahead of the committed clock|no accepted v1 decision/);
   expect(await snap()).toBe(before);
   // a forged decision row for that seat, clock NOT advanced: still ahead of the clock (future collision)
   const forgedSeat=(await f.db.query("SELECT seat::text s FROM sessions.ingest_receipt_seats WHERE key_sha256=$1",[good])).rows[0].s;
   const dec=(await f.db.query("SELECT * FROM sessions.ingest_lazy_ack_decisions WHERE native_key_sha256=(SELECT key_sha256 FROM sessions.ingest_lazy_native_slots WHERE record_id='K')")).rows[0];
   const forgeTx=async(over:any,advance:boolean)=>{await f.db.exec("BEGIN");try{
     await upd(base);   // writer order: seat (already inserted) -> slot update -> decision -> clock advance -> commit
     const d={...dec,ord:1+Math.floor(Math.random()*400),seat_key_sha256:good,native_lineage:Number(k.lineage)+1,candidate_digest:"a".repeat(64),first_digest:"a".repeat(64),body_accepted:true,
      candidate_key_sha256:(await f.db.query("SELECT encode(sha256(convert_to(key_material::text,'UTF8')),'hex') h FROM sessions.ingest_receipt_seats WHERE key_sha256=$1",[good])).rows[0].h,...over};
     await f.db.query(`INSERT INTO sessions.ingest_lazy_ack_decisions(claim_token,ord,native_key_sha256,native_lineage,candidate_key_sha256,candidate_digest,first_digest,body_accepted,seat_key_sha256,
       reset_epoch,fence_epoch,generation,source_control_version,source_witness_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
      [d.claim_token,d.ord,d.native_key_sha256,d.native_lineage,d.candidate_key_sha256,d.candidate_digest,d.first_digest,d.body_accepted,d.seat_key_sha256,d.reset_epoch,d.fence_epoch,d.generation,d.source_control_version,d.source_witness_sha256]);
     if(advance)await f.db.query("UPDATE sessions.ingest_receipt_seat_clock SET last_seat=$1",[forgedSeat]);
     await f.db.exec("COMMIT")}catch(e){await f.db.exec("ROLLBACK").catch(()=>{});throw e}};
   await expectCode(forgeTx({},false),"ahead of the committed clock");
   // per-field decision mismatches with the clock advanced: each refused at commit ONLY because the decision binding fails
   for(const [name,over] of [["not accepted",{body_accepted:false}],["lineage",{native_lineage:Number(k.lineage)+5}],["digest",{candidate_digest:"b".repeat(64),first_digest:"b".repeat(64)}],
     ["reset epoch",{reset_epoch:1}],["fence epoch",{fence_epoch:1}],["native key",{native_key_sha256:"9".repeat(64)}],["candidate key",{candidate_key_sha256:"8".repeat(64)}]] as [string,any][])
    await expectCode(forgeTx(over,true),/no accepted v1 decision|23503/).catch(e=>{throw Error("decision mismatch "+name+": "+e)});
   expect(await snap()).toBe(before);
   // after every refusal nothing leaked: a normal ACK allocates the next seat without colliding
   await f.send("k2",[record({record_id:"K",revision:2,session_id:"s"})]);
   expect((await f.db.query("SELECT current_revision::text r FROM sessions.ingest_lazy_native_slots WHERE record_id='K'")).rows[0].r).toBe("2");
  }finally{await f.close()}
 });

 test("provenance writer faults: a v1 or v2 writer that skips the clock advance cannot commit a winner",async()=>{
  for(const [mode,fn] of [["bridge-v1","sessions.ingest_lazy_ack_batch_v1(jsonb)"],["bridge-v2","sessions.ingest_lazy_ack_batch_v2(jsonb)"]] as [Mode,string][]){
   const f=await fixture(kind,mode);try{
    const d=(await f.db.query("SELECT pg_get_functiondef($1::regprocedure) d",[fn])).rows[0].d as string;
    const at=" UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;";expect(d.includes(at)).toBe(true);
    await f.db.exec(d.replace(at," NULL;"));
    await expectCode(f.send("fault",[record({record_id:"z",session_id:"s"})]),/ahead of the committed clock/,f).catch(e=>{throw Error(mode+": "+e)});
    expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id='fault' AND state='committed'")).rows[0].n).toBe(0);
   }finally{await f.close()}
  }
 });

 test("receipt evidence is append-only (review v9 MAJOR 3), also under session_replication_role=replica",async()=>{
  const f=await fixture(kind,"bridge-v1");try{
   await f.send("a0",[record({record_id:"A",session_id:"s"})]);
   await f.db.query("SELECT sessions.ingest_ack_format_set('v2')");
   await f.send("a1",[record({record_id:"A",revision:2,session_id:"s"})]);
   await f.db.query("SELECT sessions.ingest_ack_format_set('v1')");
   await f.send("a2",[record({record_id:"A",revision:1,session_id:"s"})]);   // stale: v1r compat alias of the v2 seat
   expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_receipt_seats WHERE compat_vector_claim IS NOT NULL")).rows[0].n).toBe(1);
   // S/K -> K pointing at a compat alias is refused (compat rows are never a K winner)
   const cmp=(await f.db.query("SELECT key_sha256 k FROM sessions.ingest_receipt_seats WHERE compat_vector_claim IS NOT NULL")).rows[0].k;
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_seat=NULL,current_seat_key=$1,lineage=lineage+1 WHERE record_id='A'",[cmp]),/slot regression refused|23514/);
   for(const role of ["origin","replica"]){await f.db.exec("SET session_replication_role="+role);
    await expectCode(f.db.query("UPDATE sessions.ingest_receipt_seats SET first_digest=repeat('0',64) WHERE compat_vector_claim IS NOT NULL"),"append-only");
    await expectCode(f.db.query("UPDATE sessions.ingest_receipt_seats SET first_digest=repeat('0',64) WHERE compat_vector_claim IS NULL"),"append-only");
    await expectCode(f.db.query("DELETE FROM sessions.ingest_receipt_seats"),"append-only");
    await expectCode(f.db.query("TRUNCATE sessions.ingest_receipt_seats CASCADE"),"append-only");
    await expectCode(f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET body_accepted=NOT body_accepted"),"append-only");
    await expectCode(f.db.query("DELETE FROM sessions.ingest_lazy_ack_decisions"),"append-only");
    await expectCode(f.db.query("TRUNCATE sessions.ingest_lazy_ack_decisions CASCADE"),"append-only")}
   await f.db.exec("SET session_replication_role=origin");
  }finally{await f.close()}
 });

 test("processing ranks must be a total order: SQL proof at claim (witness) and in the v2 writer/decoder",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   for(const bad of ['[0,0]','[1,2]','[0]','[0,"1"]','[0,-1]','{}'])
    await expectCode(f.db.query("SELECT sessions.ack_require_rank_permutation($1::jsonb,2)",[bad]),"not a permutation").catch(e=>{throw Error(bad+": "+e)});
   await f.db.query("SELECT sessions.ack_require_rank_permutation('[1,0]'::jsonb,2)");
   await expectCode(f.send("dup",[record({record_id:"x",session_id:"s"}),record({record_id:"y",session_id:"s"})],{},TENANT,undefined as any,[0,0] as any),"rank coverage",f);
  }finally{await f.close()}
 });

 test("P10b writer coverage fault injection: omitted, extra and wrong-tuple updates roll back the whole ACK",async()=>{
  const src=readFileSync(join(root,FIXTURE),"utf8");
  const v2=src.slice(src.indexOf("CREATE FUNCTION sessions.ingest_lazy_ack_batch_v2"),src.indexOf("END $v2$;")+9).replace("CREATE FUNCTION","CREATE OR REPLACE FUNCTION");
  const faults:[string,string,string][]=[
   ["omit one final winner","FROM want w WHERE n.key_sha256=w.native_key RETURNING","FROM want w WHERE n.key_sha256=w.native_key AND w.native_key<>(SELECT min(native_key) FROM want) RETURNING"],
   ["extra unrelated slot","FROM want w WHERE n.key_sha256=w.native_key RETURNING","FROM want w WHERE n.key_sha256=w.native_key OR (n.record_id='bystander' AND w.native_key=(SELECT min(native_key) FROM want)) RETURNING"],
   ["wrong lineage","SET lineage=w.lineage,","SET lineage=w.lineage+1,"],
   ["wrong seat","current_seat=w.seat,","current_seat=w.seat+(CASE WHEN w.native_key=(SELECT max(native_key) FROM want) THEN 1000 ELSE 0 END),"],
   ["writer installs the earlier accepted entry (overlay keeps the first winner)",
    "    prev[j]:=(r.key_material->>8)::bigint;pdig[j]:=r.candidate_digest;plin[j]:=v_lin;pseat[j]:=v_seat;pord[j]:=r.ord;",
    "    IF prev[j] IS NULL THEN prev[j]:=(r.key_material->>8)::bigint;pdig[j]:=r.candidate_digest;plin[j]:=v_lin;pseat[j]:=v_seat;pord[j]:=r.ord;END IF;"],
  ];
  for(const [name,a,b] of faults){
   const f=await fixture(kind,"bridge-v2");try{
    await f.send("pre",[record({record_id:"bystander",session_id:"s"})]);
    expect(v2.includes(a)).toBe(true);await f.db.exec(v2.replace(a,b));
    const before=(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_ack_vectors)::int v,(SELECT last_seat::text FROM sessions.ingest_receipt_seat_clock) s")).rows[0];
    await expectCode(f.send("hit",[record({record_id:"m1",session_id:"s"}),record({record_id:"m1",revision:2,session_id:"s"}),record({record_id:"m2",session_id:"s"})]),/coverage mismatch|binding refused|slot regression refused/,f).catch(e=>{throw Error(name+": "+e)});
    const after=(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_ack_vectors)::int v,(SELECT last_seat::text FROM sessions.ingest_receipt_seat_clock) s")).rows[0];
    expect(after).toEqual(before);
   }finally{await f.close()}
  }
 });

 test("P6 replacement map: range overlap and FK RESTRICT refusals",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   await f.send("o1",[record({record_id:"a",session_id:"s"}),record({record_id:"b",session_id:"s"})]);
   await f.send("o2",[record({record_id:"c",session_id:"s"})]);
   const v=(await f.db.query("SELECT v.*,a.batch_id FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token) ORDER BY seat_lo")).rows;
   await f.db.exec("ALTER TABLE sessions.ingest_ack_vectors DISABLE TRIGGER ack_guard_vector");
   // overlapping seat range on a second vector row (guard disabled to reach the constraint itself)
   await expectCode(f.db.query("UPDATE sessions.ingest_ack_vectors SET seat_lo=$1::bigint,seat_hi=$1::bigint,clock_before=$1::bigint-1 WHERE claim_token=$2",[v[0].seat_lo,v[1].claim_token]),/23P01|exclusion|ack_vectors_numrange/);
   await expectCode(f.db.query("UPDATE sessions.ingest_ack_vectors SET seat_hi=seat_hi+1 WHERE claim_token=$1",[v[1].claim_token]),/23514|check/i);  // range length bound to count
   await expectCode(f.db.query("UPDATE sessions.ingest_ack_vectors SET seat_lo=seat_lo+1 WHERE claim_token=$1",[v[1].claim_token]),/23514|check/i);  // seat_lo = clock_before+1
   await f.db.exec("ALTER TABLE sessions.ingest_ack_vectors ENABLE TRIGGER ack_guard_vector");
   // FK RESTRICT: neither the claim nor the ACK stage of a vector can be deleted
   await f.db.exec("ALTER TABLE sessions.ingest_claim_attempts DISABLE TRIGGER USER");
   await expectCode(f.db.query("DELETE FROM sessions.ingest_claim_attempts WHERE claim_token=$1",[v[0].claim_token]),/23503|foreign key/i);
   await expectCode(f.db.query("DELETE FROM sessions.ingest_stage_intents WHERE stage_token=$1",[v[0].ack_stage_token]),/23503|foreign key/i);
   expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_ack_vectors")).rows[0].n).toBe(2);
  }finally{await f.close()}
 });

 test("T6b pinned: a revision_conflict claim records no ownership/attempt rows, identically in v1 and v2",async()=>{
  const res:any[]=[];
  for(const m of ["baseline","bridge-v2"] as Mode[]){const f=await fixture(kind,m);try{
   let e:any=null;try{await f.send("tw",[record({record_id:"T",session_id:"sx"}),record({record_id:"T",revision_id:"T:alt",payload:{text:"other"},session_id:"sx"})])}catch(x){e=x}
   res.push([m,e?.code,(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_session_ownership WHERE session_id='sx')::int own,(SELECT count(*) FROM sessions.ingest_claim_attempts WHERE batch_id='tw')::int att,(SELECT count(*) FROM sessions.ingest_batches WHERE batch_id='tw')::int bat,(SELECT count(*) FROM sessions.ingest_lazy_native_slots WHERE record_id='T')::int slots")).rows[0]]);
  }finally{await f.close()}}
  console.log("T6b",JSON.stringify(res));expect(res[1].slice(1)).toEqual(res[0].slice(1));
  expect(res[0][2].own).toBe(0);expect(res[0][2].att).toBe(0);  // the current protocol gap (§8.3), pinned
 });

 test("P11 full ACK negative: a whole delivery whose ACK transaction runs at REPEATABLE READ / SERIALIZABLE refuses and changes nothing",async()=>{
  for(const iso of ["REPEATABLE READ","SERIALIZABLE"]){const f=await fixture(kind,"bridge-v2");try{
   if(kind==="pglite"){await f.db.exec("SET default_transaction_isolation='"+iso.toLowerCase()+"'")}
   const before=(await f.db.query("SELECT (SELECT last_seat::text FROM sessions.ingest_receipt_seat_clock) s,(SELECT count(*) FROM sessions.ingest_ack_vectors)::int v")).rows[0];
   if(kind==="pglite"){await expectCode(f.send("rr",[record({record_id:"z",session_id:"s"})]),"READ COMMITTED required",f);await f.db.exec("SET default_transaction_isolation='read committed'")}
   else{const pg=(await import("pg")).default;const c=new pg.Client({connectionString:f.url});await c.connect();
    try{await expectCode(f.senderOn(c,undefined,iso)("rr",[record({record_id:"z",session_id:"s"})]),"READ COMMITTED required",f)}finally{await c.end()}}
   const after=(await f.db.query("SELECT (SELECT last_seat::text FROM sessions.ingest_receipt_seat_clock) s,(SELECT count(*) FROM sessions.ingest_ack_vectors)::int v,(SELECT count(*) FROM sessions.ingest_batches WHERE batch_id='rr' AND state='committed')::int c")).rows[0];
   expect({s:after.s,v:after.v}).toEqual(before);expect(after.c).toBe(0);
  }finally{await f.close()}}
 });

 test("P10c (container only): session_replication_role=replica does not skip guards or the validator (ENABLE ALWAYS)",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   await f.send("rp",[record({record_id:"q1",session_id:"s"}),record({record_id:"q1",revision:2,session_id:"s"})]);
   await f.db.exec("SET session_replication_role=replica");
   await expectCode(f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_revision=current_revision+1 WHERE record_id='q1'"),/binding refused|slot regression refused|coverage mismatch/);
   await expectCode(f.db.query("DELETE FROM sessions.ingest_ack_vectors"),"immutable");
   await expectCode(f.db.query("INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch) VALUES(repeat('1',64),'[]',1,repeat('2',64),'claude','s',0)"),"refused in mode v2");
   await f.db.exec("SET session_replication_role=origin");
   const tg=(await f.db.query("SELECT tgname,tgenabled FROM pg_trigger WHERE tgname LIKE 'ack_%' AND NOT tgisinternal ORDER BY 1")).rows;
   expect(tg.every((r:any)=>r.tgenabled==="A")).toBe(true);expect(tg.length).toBe(18);
  }finally{await f.close()}
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

 test("P13 golden vectors: ≥12 attempt preimages (real + synthetic edge rows) and the clock TS twin",async()=>{
  const f=await fixture(kind,"bridge-v2");try{
   // synthetic edge rows through the SQL function on an explicit composite (no table write)
   const edge=[
    {claim_token:"00000000-0000-0000-0000-000000000000",org_id:"o",workspace_id:"",user_id:"u",origin_id:"x",batch_id:"b",digest:"0".repeat(64),reset_epoch:"0",schema_generation:null,source_control_version:null,source_identity_sha256:null,source_witness_sha256:null,records:"[]",epochs:"[]",source_processing_ranks:null},
    {claim_token:"ffffffff-ffff-ffff-ffff-ffffffffffff",org_id:"Σİ😀",workspace_id:"ws",user_id:"ü",origin_id:"ö",batch_id:"批",digest:"f".repeat(64),reset_epoch:"9223372036854775807",schema_generation:"a".repeat(64),source_control_version:"9223372036854775807",source_identity_sha256:"c".repeat(64),source_witness_sha256:"d".repeat(64),records:'[{"a": 1}]',epochs:"[]",source_processing_ranks:"[0]"},
    {claim_token:"12345678-1234-1234-1234-123456789abc",org_id:"o",workspace_id:"w",user_id:"u",origin_id:"x",batch_id:"b",digest:"1".repeat(64),reset_epoch:"-1",schema_generation:"",source_control_version:"0",source_identity_sha256:"",source_witness_sha256:"",records:'{"z": [1, 2], "a": "x"}',epochs:'[{"epoch": 0}]',source_processing_ranks:"[]"},
   ];
   for(const ed of edge)for(const recInput of [ed.records,ed.records.replace(/"a": 1/,'"a":1')]){   // reordered/whitespace JSON input must give identical bytes
    const r=(await f.db.query(`SELECT encode(sessions.ack_attempt_sha(p),'hex') sql,p.records::text records,p.epochs::text epochs,p.source_processing_ranks::text source_processing_ranks
     FROM jsonb_populate_record(NULL::sessions.ingest_claim_attempts,$1::jsonb) p`,[JSON.stringify({...ed,records:JSON.parse(recInput),epochs:JSON.parse(ed.epochs),source_processing_ranks:ed.source_processing_ranks===null?null:JSON.parse(ed.source_processing_ranks)})])).rows[0];
    expect(attemptSha({...ed,records:r.records,epochs:r.epochs,source_processing_ranks:r.source_processing_ranks})).toBe(r.sql)}
   const clocks=[{control_epoch:"3".repeat(64),source_identity_sha256:"c".repeat(64),proof:"d".repeat(64),initial_generation:"a".repeat(64),allocated_high_water:"9007199254740993"},
    {control_epoch:"0".repeat(64),source_identity_sha256:"1".repeat(64),proof:null,initial_generation:"2".repeat(64),allocated_high_water:"0"},
    {control_epoch:"f".repeat(64),source_identity_sha256:"e".repeat(64),proof:"Σ",initial_generation:"b".repeat(64),allocated_high_water:"9223372036854775806"}];
   for(const c of clocks){const r=(await f.db.query(`SELECT encode(sessions.ack_clock_sha_of(x),'hex') h FROM jsonb_populate_record(NULL::sessions.ingest_receipt_seat_clock,$1::jsonb) x`,
    [JSON.stringify({control_epoch:c.control_epoch,source_identity_sha256:c.source_identity_sha256,initial_generation:c.initial_generation,allocated_high_water:c.allocated_high_water,baseline:c.proof===null?{}:{sourceProofSha256:c.proof}})])).rows[0];
    expect(clockSha(c)).toBe(r.h)}
   const live=(await f.db.query("SELECT encode(sessions.ack_clock_sha(),'hex') h,control_epoch,source_identity_sha256,baseline->>'sourceProofSha256' proof,initial_generation,allocated_high_water::text FROM sessions.ingest_receipt_seat_clock")).rows[0];
   expect(clockSha(live)).toBe(live.h);
   await f.send("g1",[record({record_id:"Σİ😀",session_id:"s"}),record({record_id:"plain",session_id:null as any})]);
   await f.send("g2",[record({record_id:"plain",revision:2,session_id:null as any})]);
   const rows=(await f.db.query(`SELECT a.claim_token::text,a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest,a.reset_epoch::text,a.schema_generation,
    a.source_control_version::text,a.source_identity_sha256,a.source_witness_sha256,a.records::text records,a.epochs::text epochs,a.source_processing_ranks::text source_processing_ranks,
    encode(sessions.ack_attempt_sha(a),'hex') sql,encode(v.attempt_sha256,'hex') stored FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_ack_vectors v USING(claim_token)`)).rows;
   expect(rows.length).toBe(2);
   for(const r of rows){expect(attemptSha(r)).toBe(r.sql);expect(r.stored).toBe(r.sql)}
  }finally{await f.close()}
 });

 test("P14 max seat (review v9 HIGH 1): format flips at last_seat=2^63-1 in both directions, terminal-era zero-allocation ACKs, exhaustion",async()=>{
  for(const start of ["bridge-v2","bridge-v1"] as Mode[]){
   const f=await fixture(kind,start,{highWater:"9223372036854775805"});try{
    await f.send("x1",[record({record_id:"a",session_id:"s"}),record({record_id:"b",session_id:"s"})]);   // allocates ...806, ...807 (= 2^63-1)
    expect((await f.db.query("SELECT last_seat::text s FROM sessions.ingest_receipt_seat_clock")).rows[0].s).toBe("9223372036854775807");
    for(const to of start==="bridge-v2"?["v1","v2","v1"]:["v2","v1","v2"]){
     await f.db.query("SELECT sessions.ingest_ack_format_set($1)",[to]);                    // must not overflow
     await f.send("z"+to+Math.random().toString(36).slice(2,6),[record({record_id:"a",session_id:"s"})]);   // stale re-send: zero allocation
     await expectCode(f.send("n"+to+Math.random().toString(36).slice(2,6),[record({record_id:"fresh-"+to+Math.random().toString(36).slice(2,6),session_id:"s"})]),"ACK seats exhausted",f);
    }
    const eras=(await f.db.query("SELECT mode,first_seat::text fs FROM sessions.ingest_ack_format_epochs ORDER BY epoch")).rows;
    expect(eras.at(-1).fs).toBe("9223372036854775808");                                     // empty terminal era boundary
   }finally{await f.close()}
  }
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
