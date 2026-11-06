/**
 * TEST-ONLY storage measurement for the ledger v8 Phase 1 prototype on real PG17 (ACK_V2_TEST_PG_URL).
 * Representative synthetic distribution, calibrated to the read-only production measurement of 2026-11-06:
 *  - records/claim drawn from a mixture with mean ≈148 (max 500)
 *  - ≈55% new native identities, ≈44% re-touch with a higher revision (accepted), ≈1% stale re-send (not accepted)
 *  - re-touches concentrate on a hot set (Zipf-like) so lineage grows on few keys
 *  - ≈5% of new identities arrive with legacy source custody (witness `current`)
 * Same deterministic workload for baseline v1 and bridge v2; reports allocated bytes and per-ACK growth.
 */
import {test,expect,setDefaultTimeout} from "bun:test";
import {fixture,record,meta,type Mode} from "./ack-v2-support.ts";
setDefaultTimeout(7200000);
const url=process.env.ACK_V2_TEST_PG_URL,CLAIMS=Number(process.env.ACK_V2_SIZE_CLAIMS??400);
function rng(seed:number){return()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296}}
const SIZES=[[10,0.15],[50,0.2],[100,0.2],[150,0.15],[250,0.15],[400,0.1],[500,0.05]] as const; // mean ≈ 149
function workload(){
 const r=rng(20261106),claims:{records:any[];legacy:string[]}[]=[];let next=0;const rev=new Map<string,number>();const sess=new Map<string,string>();
 for(let c=0;c<CLAIMS;c++){
  let u=r(),n=10;for(const [s,w] of SIZES){if(u<w){n=s;break}u-=w}
  const ids=new Set<string>(),recs:any[]=[],legacy:string[]=[];
  for(let i=0;i<n;i++){
   const x=r();let id:string,revision:number;
   if(x<0.55||next<200){id="n"+(next++);revision=1;if(r()<0.05){legacy.push(id);revision=2}}
   else{const hot=Math.floor(Math.pow(r(),3)*next);id="n"+hot;const cur=rev.get(id)??1;revision=x<0.99?cur+1:Math.max(1,cur-1)}
   if(ids.has(id))continue;ids.add(id);
   if(!sess.has(id))sess.set(id,"s"+(Number(id.slice(1))%40));
   if(revision>(rev.get(id)??0))rev.set(id,revision);
   recs.push(record({record_id:id,revision,session_id:sess.get(id)!,payload:{text:"x".repeat(32),ts_ms:1_761_955_200_000+i}}));
  }
  claims.push({records:recs,legacy});
 }
 return claims;
}
const REL=["ingest_receipt_seats","ingest_lazy_ack_decisions","ingest_ack_vectors","ingest_lazy_native_slots","ingest_claim_attempts","ingest_custody_records"];
async function sizes(f:any){const out:any={};for(const t of REL){const r=(await f.db.query("SELECT CASE WHEN to_regclass($1) IS NULL THEN 0 ELSE pg_total_relation_size(to_regclass($1)) END::bigint b,CASE WHEN to_regclass($1) IS NULL THEN 0 ELSE (SELECT count(*) FROM pg_class WHERE false) END n",["sessions."+t])).rows[0];out[t]=Number(r.b)}return out}
test.skipIf(!url)(`allocated bytes and per-ACK growth, v1 vs v2 (${CLAIMS} claims each)`,async()=>{
 const w=workload(),recs=w.reduce((a,c)=>a+c.records.length,0);
 const res:any={claims:CLAIMS,records:recs,meanRecordsPerClaim:+(recs/CLAIMS).toFixed(1)};
 for(const m of ["baseline","bridge-v2"] as Mode[]){
  const f=await fixture("pg",m);try{
   await f.db.query("VACUUM ANALYZE");const before=await sizes(f);const t0=Date.now();
   for(let i=0;i<w.length;i++){const c=w[i]!;const cur:any={};for(const id of c.legacy){const rr=c.records.find((x:any)=>x.record_id===id);cur[id]=await meta({...rr,revision:1,revision_id:id+":r1",payload:{text:"old"}})}
    // legacy source current at revision 1; candidate revision 2 is accepted over source-baseline custody (≈5% of new ids, as measured)
    await f.send("z"+i,c.records,cur)}
   const elapsed=Date.now()-t0;const raw=await sizes(f);await f.db.query("VACUUM ANALYZE");const vac=await sizes(f);
   const counts=(await f.db.query(`SELECT (SELECT count(*) FROM sessions.ingest_lazy_native_slots)::int slots,(SELECT count(*) FROM sessions.ingest_receipt_seats)::int seats,
    (SELECT count(*) FROM sessions.ingest_lazy_ack_decisions)::int decisions,
    (SELECT last_seat-allocated_high_water FROM sessions.ingest_receipt_seat_clock)::int seats_allocated,
    (SELECT count(*) FROM sessions.ingest_custody_records)::int custody`)).rows[0];
   counts.vectors=m==="baseline"?0:(await f.db.query("SELECT count(*)::int n FROM sessions.ingest_ack_vectors")).rows[0].n;
   const delta=(s:any)=>Object.fromEntries(REL.map(t=>[t,s[t]-before[t]]));
   const ledger=(d:any)=>d.ingest_receipt_seats+d.ingest_lazy_ack_decisions+d.ingest_ack_vectors;
   const dr=delta(raw),dv=delta(vac);
   res[m]={elapsedMs:elapsed,counts,deltaBytesRaw:dr,deltaBytesAfterVacuum:dv,
    ackLedgerBytes:{raw:ledger(dr),vacuumed:ledger(dv),perClaim:+(ledger(dv)/CLAIMS).toFixed(1),perRecord:+(ledger(dv)/recs).toFixed(2)},
    slotBytesPerSlot:+(dv.ingest_lazy_native_slots/counts.slots).toFixed(1),
    totalPerRecordVacuumed:+(Object.values(dv).reduce((a:number,b:any)=>a+b,0)/recs).toFixed(1)};
  }finally{await f.close()}
 }
 res.ratioAckLedger=+(res.baseline.ackLedgerBytes.vacuumed/res["bridge-v2"].ackLedgerBytes.vacuumed).toFixed(1);
 console.log("SIZE "+JSON.stringify(res));
 expect(res["bridge-v2"].counts.seats).toBe(0);expect(res["bridge-v2"].counts.decisions).toBe(0);
 expect(res["bridge-v2"].counts.seats_allocated).toBe(res.baseline.counts.seats_allocated);
});
