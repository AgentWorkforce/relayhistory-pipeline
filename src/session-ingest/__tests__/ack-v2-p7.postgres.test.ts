/**
 * TEST-ONLY P7 latency harness (design v8 §13.2 + §14) for the ledger Phase 1 prototype, real PG17 only.
 * Lock hold = t_rel − t_resume, all server clock_timestamp():
 *  - A (ACK) parks right after the clock FOR UPDATE on a unique session advisory key K (0..2^63-1) held by the
 *    harness, then on wake immediately pg_advisory_unlock(K) (asserted true) and records t_resume;
 *  - observer O calls p7_fixture.lock_clock_then_time() (PERFORM … FOR UPDATE, then clock_timestamp()) and is
 *    confirmed waiting on A before A is released;
 *  - zero retained advisory locks (database, classid, objid, objsubid=1) is asserted after every sample.
 * Calibration (A commits immediately after t_resume) is subtracted. Total ACK-transaction latency is a separate
 * observer-free, barrier-free series with 4 concurrent writers.
 * Sizes: ACK_V2_P7_RUNS (3), ACK_V2_P7_HOLD (commit samples/run/format), ACK_V2_P7_TOTAL (claims/run/format).
 */
import {test,expect,setDefaultTimeout} from "bun:test";
import pg from "pg";
import {randomBytes} from "node:crypto";
import {fixture,record,type F,type Mode} from "./ack-v2-support.ts";
setDefaultTimeout(14400000);
const url=process.env.ACK_V2_TEST_PG_URL;
const RUNS=Number(process.env.ACK_V2_P7_RUNS??3),HOLD=Number(process.env.ACK_V2_P7_HOLD??200),ROLLBACK=Number(process.env.ACK_V2_P7_ROLLBACK??Math.ceil(HOLD/2)),
 CAL=Number(process.env.ACK_V2_P7_CAL??100),TOTAL=Number(process.env.ACK_V2_P7_TOTAL??400),WARM=Number(process.env.ACK_V2_P7_WARM??50),REC=150;
const sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function client(u:string){const c=new pg.Client({connectionString:u});await c.connect();return c}
const key63=()=>{const b=randomBytes(8);b[0]!&=0x7f;return b.readBigInt64BE(0)};   // 0..2^63-1
async function install(f:F){
 await f.db.exec(`CREATE SCHEMA p7_fixture;
 CREATE FUNCTION p7_fixture.lock_clock_then_time() RETURNS timestamptz LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
 BEGIN PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE; RETURN clock_timestamp(); END $$;`);
 const bridge=(await f.db.query("SELECT pg_get_functiondef('sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure) d")).rows[0].d as string;
 const at=" PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;\n";
 if(!bridge.includes(at))throw Error("bridge injection point");
 const barrier=at+` DECLARE_P7;
 IF nullif(current_setting('ackv2test.p7_key',true),'') IS NOT NULL THEN
  DECLARE k bigint:=current_setting('ackv2test.p7_key')::bigint;ta timestamptz;tr timestamptz;
  BEGIN ta:=clock_timestamp();PERFORM pg_advisory_lock(k);
   IF NOT pg_advisory_unlock(k) THEN RAISE EXCEPTION 'p7: advisory unlock failed';END IF;tr:=clock_timestamp();
   RAISE NOTICE 'p7 % % %',k,(extract(epoch FROM ta)*1e6)::bigint,(extract(epoch FROM tr)*1e6)::bigint;
   IF current_setting('ackv2test.p7_calib',true)='1' THEN RAISE EXCEPTION 'p7 calibration abort';END IF;
  END;
 END IF;
`.replace(" DECLARE_P7;\n","");
 let body=bridge.replace(at,barrier);
 // rollback series: abort after the real ACK work
 body=body.replace("PERFORM sessions.ingest_lazy_ack_batch_v1(p);END IF;","PERFORM sessions.ingest_lazy_ack_batch_v1(p);END IF;\n IF current_setting('ackv2test.p7_rollback',true)='1' THEN RAISE EXCEPTION 'p7 rollback after work';END IF;");
 if(!body.includes("p7 rollback after work"))throw Error("rollback injection point");
 await f.db.exec(body);
}
function stats(xs:number[]){const s=[...xs].sort((a,b)=>a-b),q=(p:number)=>s[Math.min(s.length-1,Math.floor(p*s.length))]!;return{n:s.length,p50:q(0.5),p99:q(0.99),mean:s.reduce((a,b)=>a+b,0)/s.length}}
function boot(xs:number[],p:number,R=10000){const out:number[]=[];for(let i=0;i<R;i++){const s:number[]=[];for(let j=0;j<xs.length;j++)s.push(xs[Math.floor(Math.random()*xs.length)]!);s.sort((a,b)=>a-b);out.push(s[Math.min(s.length-1,Math.floor(p*s.length))]!)}out.sort((a,b)=>a-b);return[out[Math.floor(0.025*R)]!,out[Math.floor(0.975*R)]!]}
let ids=0;const pool:string[]=[];const revs=new Map<string,number>();
function claimRecords(r:()=>number){const recs:any[]=[];const used=new Set<string>();
 for(let i=0;i<REC;i++){const x=r();let id:string,rev:number;
  if(x<0.89||pool.length<300){id="p"+(ids++);pool.push(id);rev=1}else{id=pool[Math.floor(r()*pool.length)]!;const c=revs.get(id)??1;rev=x<0.99?c+1:Math.max(1,c-1)}
  if(used.has(id))continue;used.add(id);if(rev>(revs.get(id)??0))revs.set(id,rev);
  recs.push(record({record_id:id,revision:rev,session_id:"s"+(i%8),payload:{text:"y".repeat(24)}}))}
 return recs}
async function heldAdvisory(q:pg.Client,k:bigint){return(await q.query(`SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
 AND classid=$1::bigint::oid AND objid=$2::bigint::oid AND objsubid=1`,[String(k>>32n),String(k&0xffffffffn)])).rows[0].n}
async function waitLock(ctl:pg.Client,pid:number,ev?:string){for(let i=0;i<20000;i++){const r=(await ctl.query("SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid=$1",[pid])).rows[0];
 if(r?.wait_event_type==="Lock"&&(!ev||r.wait_event===ev))return;await sleep(1)}throw Error("p7: not waiting "+pid)}

async function holdSample(f:F,a:pg.Client,ctl:pg.Client,obs:pg.Client,recs:any[],flavor:"commit"|"rollback"|"calib",id:string){
 const k=key63();let marks:{ta:bigint;tr:bigint}|null=null;
 const onNotice=(m:any)=>{const g=/^p7 (\d+) (\d+) (\d+)$/.exec(m.message);if(g&&BigInt(g[1]!)===k)marks={ta:BigInt(g[2]!),tr:BigInt(g[3]!)}};a.on("notice",onNotice);
 await ctl.query("SELECT pg_advisory_lock($1::bigint)",[String(k)]);
 await a.query("SELECT set_config('ackv2test.p7_key',$1,false),set_config('ackv2test.p7_rollback',$2,false),set_config('ackv2test.p7_calib',$3,false)",[String(k),flavor==="rollback"?"1":"0",flavor==="calib"?"1":"0"]);
 const apid=(await a.query("SELECT pg_backend_pid() p")).rows[0].p,opid=(await obs.query("SELECT pg_backend_pid() p")).rows[0].p;
 const send=f.senderOn(a)(id,recs).then(()=>true,()=>false);
 await waitLock(ctl,apid,"advisory");
 await obs.query("BEGIN");const o=obs.query("SELECT (extract(epoch FROM p7_fixture.lock_clock_then_time())*1e6)::bigint t").then(r=>BigInt(r.rows[0].t));
 await waitLock(ctl,opid);
 const un=(await ctl.query("SELECT pg_advisory_unlock($1::bigint) u",[String(k)])).rows[0].u;if(un!==true)throw Error("p7: harness unlock not true");
 const ok=await send;const tRel=await o;await obs.query("ROLLBACK");a.off("notice",onNotice);
 await a.query("SELECT set_config('ackv2test.p7_key','',false)");
 if(await heldAdvisory(ctl,k)!==0)throw Error("p7: advisory K retained");
 if(!marks)throw Error("p7: no marks");if(flavor==="commit"&&!ok)throw Error("p7: commit sample failed");if(flavor!=="commit"&&ok)throw Error("p7: expected abort");
 return{holdUs:Number(tRel-marks!.tr),barrierUs:Number(marks!.tr-marks!.ta)};
}

test.skipIf(!url)("P7 lock-hold (queued observer) and total ACK latency, v1 vs v2, alternating runs",async()=>{
 const R=(()=>{let s=7>>>0;return()=>{s=(s*1664525+1013904223)>>>0;return s/4294967296}})();
 const res:any={params:{RUNS,HOLD,ROLLBACK,CAL,TOTAL,WARM,recordsPerClaim:REC,writers:4},runs:[]};
 const all:Record<string,Record<string,number[]>>={};
 for(let run=0;run<RUNS;run++)for(const m of (run%2? ["bridge-v2","bridge-v1"]:["bridge-v1","bridge-v2"]) as Mode[]){
  const f=await fixture("pg",m);ids=0;pool.length=0;revs.clear();
  const fmt=m==="bridge-v1"?"v1":"v2"; // both use the same bridge + barrier path; only the format differs
  await install(f);
  const a=await client(f.url!),ctl=await client(f.url!),obs=await client(f.url!),ws=await Promise.all([0,1,2,3].map(()=>client(f.url!)));
  try{
   for(let i=0;i<WARM;i++)await f.senderOn(ws[i%4]!)(`w${i}`,claimRecords(R));
   const cal:number[]=[],hold:number[]=[],rb:number[]=[],bar:number[]=[];
   for(let i=0;i<CAL;i++)cal.push((await holdSample(f,a,ctl,obs,claimRecords(R),"calib",`c${i}`)).holdUs);
   for(let i=0;i<HOLD;i++){const x=await holdSample(f,a,ctl,obs,claimRecords(R),"commit",`h${i}`);hold.push(x.holdUs);bar.push(x.barrierUs)}
   for(let i=0;i<ROLLBACK;i++)rb.push((await holdSample(f,a,ctl,obs,claimRecords(R),"rollback",`r${i}`)).holdUs);
   const calMed=stats(cal).p50;
   const total:number[]=[],totalRb:number[]=[];
   const per=Math.ceil(TOTAL/4);
   await Promise.all(ws.map((c,w)=>(async()=>{const send=f.senderOn(c,(sql,ms,ok)=>{if(sql.includes("ingest_commit_buffered_batch")&&ok)total.push(ms*1000)});
    for(let i=0;i<per;i++)await send(`t${w}-${i}`,claimRecords(R))})()));
   const cor=(xs:number[])=>xs.map(x=>Math.max(0,x-calMed));
   const row={run,format:fmt,calibrationUs:stats(cal),holdCommitUs:stats(cor(hold)),holdRollbackUs:stats(cor(rb)),barrierUs:stats(bar),totalCommitUs:stats(total)};
   res.runs.push(row);console.log("P7 run",JSON.stringify(row));
   for(const [k,v] of Object.entries({holdCommit:cor(hold),holdRollback:cor(rb),total})){(all[fmt]??={})[k]=[...((all[fmt]??{})[k]??[]),...v]}
  }finally{for(const c of [a,ctl,obs,...ws])await c.end().catch(()=>{});await f.close()}
 }
 const gate:any={};
 for(const k of ["holdCommit","holdRollback","total"]){const v1=all.v1![k]!,v2=all.v2![k]!;
  const s1=stats(v1),s2=stats(v2);
  gate[k]={v1:{...s1,p50ci:boot(v1,0.5),p99ci:boot(v1,0.99)},v2:{...s2,p50ci:boot(v2,0.5),p99ci:boot(v2,0.99)},
   p50Pass:s2.p50<=s1.p50,p99Pass:s2.p99<=1.10*s1.p99}}
 const med=(f:string)=>res.runs.filter((r:any)=>r.format===f).map((r:any)=>r.holdCommitUs.p50);
 const cv=(xs:number[])=>{const m=xs.reduce((a,b)=>a+b,0)/xs.length;return Math.sqrt(xs.reduce((a,b)=>a+(b-m)**2,0)/xs.length)/m};
 res.v1MedianCV=+cv(med("v1")).toFixed(3);res.validRun=res.v1MedianCV<=0.15;res.gate=gate;
 res.specCompliant=HOLD>=5000&&RUNS>=3;
 console.log("P7 RESULT "+JSON.stringify(res));
});
