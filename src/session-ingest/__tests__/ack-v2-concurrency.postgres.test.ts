/**
 * TEST-ONLY real-PG17 interleaving evidence for the ledger v8 Phase 1 prototype.
 * Opt in with ACK_V2_TEST_PG_URL (owned, disposable container). Iterations: ACK_V2_ITER (default 100).
 * The "old body" override below exists only in this test; it simulates a session still executing the
 * pre-bridge v1 function body.
 */
import {describe,test,expect,setDefaultTimeout} from "bun:test";
import pg from "pg";
import {fixture,record,type F} from "./ack-v2-support.ts";
setDefaultTimeout(3600000);
const PROBE_PW=crypto.randomUUID();
const url=process.env.ACK_V2_TEST_PG_URL,N=Number(process.env.ACK_V2_ITER??100);
const sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function client(u:string){const c=new pg.Client({connectionString:u});await c.connect();return c}
async function waitFor(q:pg.Client,pid:number,kind:"advisory"|"any"){for(let i=0;i<400;i++){
 const r=(await q.query("SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid=$1",[pid])).rows[0];
 if(r?.wait_event_type==="Lock"&&(kind==="any"||r.wait_event==="advisory"))return;await sleep(5)}throw Error("not waiting: "+pid)}

export async function invariants(q:{query(s:string):Promise<{rows:any[]}>}){
 const r=(await q.query(`WITH fmt AS(SELECT * FROM sessions.ingest_ack_format WHERE singleton),
  v1 AS(SELECT seat FROM sessions.ingest_receipt_seats),
  v2 AS(SELECT generate_series(seat_lo,seat_hi) seat FROM sessions.ingest_ack_vectors WHERE allocated_count>0),
  s AS(SELECT seat FROM v1 UNION ALL SELECT seat FROM v2),c AS(SELECT * FROM sessions.ingest_receipt_seat_clock WHERE singleton)
  SELECT (SELECT count(*) FROM s)::text total,(SELECT count(DISTINCT seat) FROM s)::text distinct_seats,
   (SELECT (last_seat-allocated_high_water)::text FROM c) clock_span,
   (SELECT coalesce(min(seat),0)::text FROM s) mn,(SELECT (allocated_high_water+1)::text FROM c) hw1,
   (SELECT coalesce(max(seat),0)::text FROM s) mx,(SELECT last_seat::text FROM c) last,
   (SELECT count(*) FROM v1,fmt WHERE fmt.mode='v2' AND v1.seat>fmt.cutover_seat)::int v1_above_cutover,
   (SELECT count(*) FROM sessions.ingest_ack_vectors v,fmt WHERE v.allocated_count>0 AND v.seat_lo<=fmt.cutover_seat)::int v2_at_or_below_cutover,
   (SELECT count(*) FROM sessions.ingest_claim_attempts a WHERE EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=a.claim_token)
     AND EXISTS(SELECT 1 FROM sessions.ingest_ack_vectors v WHERE v.claim_token=a.claim_token))::int both_formats,
   (SELECT count(*) FROM sessions.ingest_claim_attempts a WHERE EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=a.claim_token) AND a.ack_format IS DISTINCT FROM 1)::int v1_unstamped,
   (SELECT count(*) FROM sessions.ingest_claim_attempts a WHERE EXISTS(SELECT 1 FROM sessions.ingest_ack_vectors v WHERE v.claim_token=a.claim_token) AND a.ack_format IS DISTINCT FROM 2)::int v2_unstamped,
   (SELECT count(*) FROM sessions.ingest_receipt_seats x WHERE NOT EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.seat_key_sha256=x.key_sha256))::int orphan_v1_seats,
   (SELECT count(*) FROM sessions.ingest_batches b WHERE b.state='committed' AND b.buffer_receipt_kind='staged' AND NOT EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a
     JOIN sessions.ingest_stage_intents t ON t.claim_token=a.claim_token WHERE a.batch_id=b.batch_id AND a.ack_format IS NOT NULL))::int committed_without_format`)).rows[0];
 expect(r.total).toBe(r.distinct_seats);expect(r.total).toBe(r.clock_span);
 if(r.total!=="0"){expect(r.mn).toBe(r.hw1);expect(r.mx).toBe(r.last)}
 for(const k of ["v1_above_cutover","v2_at_or_below_cutover","both_formats","v1_unstamped","v2_unstamped","orphan_v1_seats","committed_without_format"])expect([k,r[k]]).toEqual([k,0]);
 // every live stage remains readable by both readers (decoder + bindings run)
 const st=(await q.query("SELECT o.batch_id,sessions.ingest_receipt_seats_for_stage(o.active_stage_token) s,sessions.ingest_buffer_gc_ordinals(o.active_stage_token) g FROM sessions.ingest_buffer_outbox o WHERE o.state='buffered'")).rows;
 return{...r,stages:st.length};
}

/** Test-only override: sessions that SET ackv2test.old_body=1 run a copy of the pre-bridge v1 body with a pause. */
async function installOldBody(f:F,where:"before-clock"|"after-seat"){
 const def=(await f.db.query("SELECT pg_get_functiondef('sessions.ingest_lazy_ack_batch_v1(jsonb)'::regprocedure) d")).rows[0].d as string;
 let body=def.replace("sessions.ingest_lazy_ack_batch_v1(","sessions.ackv2test_old_body(");
 const pause="PERFORM pg_advisory_lock(4242001);PERFORM pg_advisory_unlock(4242001);";
 if(where==="before-clock")body=body.replace(" SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;"," "+pause+" SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;");
 else body=body.replace("RETURNING * INTO s;\n  END IF;","RETURNING * INTO s;"+pause+"\n  END IF;");
 if(!body.includes(pause))throw Error("injection point not found");
 await f.db.exec(body);
 const bridge=(await f.db.query("SELECT pg_get_functiondef('sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure) d")).rows[0].d as string;
 await f.db.exec(bridge.replace("BEGIN\n PERFORM sessions.ack_require_rc();","BEGIN\n IF current_setting('ackv2test.old_body',true)='1' THEN PERFORM sessions.ackv2test_old_body(p);RETURN;END IF;\n PERFORM sessions.ack_require_rc();"));
}

describe.skipIf(!url)("ack v2 prototype interleavings on real PG17",()=>{
 test(`P1 randomized cutover under concurrent real-path ACKs (${N} iterations)`,async()=>{
  const stats={iterations:0,sendsOk:0,sendsErr:0,errs:{} as Record<string,number>};
  for(let it=0;it<N;it++){
   const f=await fixture("pg","bridge-v1");const cs=await Promise.all([0,1,2,3].map(()=>client(f.url!)));
   try{
    const senders=cs.map(c=>f.senderOn(c));
    const work=senders.map((send,w)=>(async()=>{for(let b=0;b<4;b++){
     const recs=Array.from({length:3+((it+w+b)%5)},(_,i)=>record({record_id:"k"+((w*7+b*3+i)%12),revision:1+((it+w+b+i)%3),session_id:"s"+(i%2)}));
     try{await send(`i${it}-w${w}-b${b}`,recs);stats.sendsOk++}catch(e:any){stats.sendsErr++;const k=String(e?.cause_code??e?.code);stats.errs[k]=(stats.errs[k]??0)+1}}})());
    const flip=(async()=>{await sleep(Math.floor(Math.random()*60));await f.db.query("SELECT sessions.ingest_ack_format_set('v2')")})();
    await Promise.all([...work,flip]);
    expect((await f.db.query("SELECT mode FROM sessions.ingest_ack_format")).rows[0].mode).toBe("v2");
    await invariants(f.db);stats.iterations++;
   }finally{for(const c of cs)await c.end();await f.close()}
  }
  console.log("P1",JSON.stringify(stats));
  expect(stats.sendsOk).toBeGreaterThan(N*8);
 });

 for(const where of ["after-seat","before-clock"] as const)test(`P9b paused old v1 body (${where}) vs flip (${N} iterations)`,async()=>{
  const out={iterations:0,oldCommitted:0,oldRefused:0};
  for(let it=0;it<N;it++){
   const f=await fixture("pg","bridge-v1");const ctl=await client(f.url!),a=await client(f.url!),flipper=await client(f.url!);
   try{
    await installOldBody(f,where);
    await f.send("warm",[record({record_id:"w"+it,session_id:"s"})]);
    await ctl.query("SELECT pg_advisory_lock(4242001)");
    await a.query("SET ackv2test.old_body='1'");
    const apid=(await a.query("SELECT pg_backend_pid() p")).rows[0].p;
    const sendA=f.senderOn(a)("old",[record({record_id:"o1",session_id:"s"}),record({record_id:"o2",session_id:"s"})]).then(()=>"committed",(e:any)=>"refused:"+String(e?.cause_code??e));
    await waitFor(ctl,apid,"advisory");
    const fpid=(await flipper.query("SELECT pg_backend_pid() p")).rows[0].p;
    const flip=flipper.query("SELECT sessions.ingest_ack_format_set('v2')").then(()=>"flipped",(e:any)=>"flip-failed:"+e.message);
    if(where==="after-seat")await waitFor(ctl,fpid,"any");        // flip must queue behind A's clock lock
    else{expect(await flip).toBe("flipped")}                      // A holds nothing yet: flip completes first
    await ctl.query("SELECT pg_advisory_unlock(4242001)");
    const ra=await sendA;const rf=await flip;expect(rf).toBe("flipped");
    if(where==="after-seat"){expect(ra).toBe("committed");out.oldCommitted++;
     expect((await f.db.query("SELECT a.ack_format FROM sessions.ingest_claim_attempts a WHERE batch_id='old'")).rows.map(r=>r.ack_format)).toContain(1)}
    else{expect(ra).toMatch(/^refused/);out.oldRefused++;
     expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_batches WHERE batch_id='old' AND state='committed'")).rows[0].n).toBe(0);
     await a.query("RESET ackv2test.old_body");await f.senderOn(a)("old",[record({record_id:"o1",session_id:"s"}),record({record_id:"o2",session_id:"s"})]); // retry via bridge succeeds in v2
     expect((await f.db.query("SELECT a.ack_format FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_ack_vectors v USING(claim_token) WHERE batch_id='old'")).rows.length).toBe(1)}
    await invariants(f.db);
    // P14-style zero-retention: no advisory lock remains in this database
    expect((await f.db.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())")).rows[0].n).toBe(0);
    out.iterations++;
   }finally{for(const c of [ctl,a,flipper])await c.end().catch(()=>{});await f.close()}
  }
  console.log("P9b",where,JSON.stringify(out));
 });

 test("P12 drain: an older open transaction (superuser and non-superuser login) blocks the flip; privilege required",async()=>{
  const admin=await client(url!);
  await admin.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='ackv2_probe') THEN CREATE ROLE ackv2_probe LOGIN;END IF;END$$");
  await admin.query("ALTER ROLE ackv2_probe PASSWORD '"+PROBE_PW+"'");
  for(const who of ["superuser","non-superuser"]){
   let held:pg.Client|null=null;
   const f=await fixture("pg","bridge-v1",{beforeFixture:async(u)=>{const x=new URL(u!);if(who==="non-superuser"){x.username="ackv2_probe";x.password=PROBE_PW}
    held=await client(x.toString());await held.query("BEGIN");await held.query("SELECT 1");await sleep(20)}});
   try{
    let err="";try{await f.db.query("SELECT sessions.ingest_ack_format_set('v2')")}catch(e:any){err=e.message}
    expect(err).toContain("bridge drain incomplete");
    await held!.query("COMMIT");
    await f.db.query("SELECT sessions.ingest_ack_format_set('v2')");
   }finally{await held!.end().catch(()=>{});await f.close()}
  }
  // without pg_read_all_stats the flip refuses before taking any lock
  const f=await fixture("pg","bridge-v1");try{
   await f.db.query("GRANT USAGE ON SCHEMA sessions TO ackv2_probe;GRANT EXECUTE ON FUNCTION sessions.ingest_ack_format_set(text),sessions.ack_require_rc() TO ackv2_probe");
   const x=new URL(f.url!);x.username="ackv2_probe";x.password=PROBE_PW;const p=await client(x.toString());
   let err="";try{await p.query("SELECT sessions.ingest_ack_format_set('v2')")}catch(e:any){err=e.message}await p.end();
   expect(err).toContain("requires pg_read_all_stats");
  }finally{await f.close();await admin.end()}
 });
});
