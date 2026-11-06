/**
 * TEST-ONLY: one native slot changing several times inside one claim (and across claims).
 * For every case, baseline v1 and bridge v2 must agree on: per-step outcome (incl. SQL refusal text), stage
 * seats JSON, GC keep-sets, the semantic slot projection, AND the complete per-ordinal decision list
 * (ord, bodyAccepted, seat, native_lineage, first_digest) in processing order — i.e. no intermediate decision
 * is dropped and coverage follows the effective v1 processing order, not array/ordinal order.
 * Backends: ACK_V2_BACKENDS=pglite[,pg].
 */
import {describe,test,expect,setDefaultTimeout} from "bun:test";
import {fixture,record,meta,TENANT,type F,type Mode} from "./ack-v2-support.ts";
setDefaultTimeout(600000);
const backends=(process.env.ACK_V2_BACKENDS??"pglite").split(",") as ("pglite"|"pg")[];
const T2={orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:"user-2"};
const R=(rev:number,o:any={})=>record({record_id:"R",revision:rev,session_id:"sess-A",...o});

/** decisions per claim in processing order: v1 rows, or decoded v2 entries; identical shape */
async function decisions(f:F,mode:Mode){
 const sql=mode==="bridge-v2"
  ?`SELECT a.batch_id,e.idx,e.ord,e.accepted,e.seat::text seat,e.lineage::text lineage,e.first_digest FROM sessions.ingest_ack_vectors v JOIN sessions.ingest_claim_attempts a USING(claim_token)
     CROSS JOIN LATERAL sessions.ingest_ack_entries(v.claim_token) e ORDER BY a.batch_id,e.idx`
  :`SELECT a.batch_id,row_number() OVER(PARTITION BY a.claim_token ORDER BY (a.source_processing_ranks->>d.ord)::int)::int idx,d.ord,d.body_accepted accepted,s.seat::text seat,d.native_lineage::text lineage,d.first_digest
     FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_claim_attempts a USING(claim_token) JOIN sessions.ingest_receipt_seats s ON s.key_sha256=d.seat_key_sha256 ORDER BY a.batch_id,idx`;
 return(await f.db.query(sql)).rows.map(r=>[r.batch_id,r.idx,r.ord,r.accepted,r.seat,r.lineage,r.first_digest]);
}
type Step=(f:F)=>Promise<unknown>;
const CASES:Record<string,Step[]>={
 "higher,higher in prepared order":[f=>f.send("c1",[R(1),R(2),R(3)])],
 "reversed processing order (ranks 2,1,0)":[f=>f.send("c1",[R(1),R(2),R(3)],{},TENANT,undefined as any,[2,1,0] as any)],
 "witness rank rule over shuffled ordinals":[f=>f.send("c1",[R(3),R(1),R(2)],{},TENANT,undefined as any,"witness" as any)],
 "lower after higher in one claim":[f=>f.send("c1",[R(3),R(1)])],
 "lower after higher across claims (cross-claim owner reuse)":[f=>f.send("c1",[R(2),R(3)]),f=>f.send("c2",[R(1),R(2),R(4)])],
 "equal revision, same revision_id, same digest":[f=>f.send("c1",[R(1),R(1)])],
 "equal revision, different revision_id, same digest":[f=>f.send("c1",[R(1),R(1,{revision_id:"R:alt"})])],
 "equal revision, same revision_id, different digest":[f=>f.send("c1",[R(1),R(1,{payload:{text:"other"}})])],
 "equal revision, different revision_id and digest (twin)":[f=>f.send("c1",[R(1),R(1,{revision_id:"R:alt",payload:{text:"other"}})])],
 "aliases: same record in two sessions":[f=>f.send("c1",[R(1,{session_id:"sess-A"}),R(2,{session_id:"sess-B"}),R(2,{session_id:"sess-C",revision_id:"R:c"})])],
 "legacy source current then several revisions":[async f=>f.send("c1",[R(1),R(5),R(7)],{R:await meta(R(5,{payload:{text:"legacy"}}))})],
 "cross-user negative after multi-revision":[f=>f.send("c1",[R(1),R(2)]),f=>f.send("c2",[R(3),R(4)],{},T2)],
 "reset between multi-revision claims":[f=>f.send("c1",[R(4),R(5)]),f=>f.control("reset",{request_id:"rq"}),f=>f.send("c2",[R(1),R(2)])],
 "deletion (selection disable) before multi-revision":[f=>f.send("c0",[R(1)]),f=>f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"sess-A"}],enabled:false}),f=>f.send("c1",[R(2),R(3)])],
 "selection clear then re-enable and lower revisions":[f=>f.send("c1",[R(5),R(6)]),f=>f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"sess-A"}],enabled:false}),
   f=>f.control("selection",{origin_id:"origin-1",sessions:[{source:"claude",session_id:"sess-A"}],enabled:true}),f=>f.send("c2",[R(1),R(2)])],
};
async function run(kind:"pglite"|"pg",mode:Mode,steps:Step[]){
 const f=await fixture(kind,mode);const out:any[]=[];try{
  for(const [i,s] of steps.entries()){try{const r:any=await s(f);out.push([i,"ok",r?.kind??null])}catch(e:any){out.push([i,"err",[e?.code,e?.cause_code,f.lastSqlError()].filter(Boolean).join(" ")])}}
  const batches=(await f.db.query("SELECT batch_id FROM sessions.ingest_buffer_outbox ORDER BY 1")).rows.map(r=>r.batch_id);
  for(const b of batches){try{out.push(["seats",b,await f.seats(b)])}catch(e:any){out.push(["seats",b,"err",String(e?.message)])}}
  out.push(["gc",await f.gcOrdinals()],["slots",await f.slots()],["decisions",await decisions(f,mode)]);
  if(mode==="bridge-v2"){const d=(await f.db.query("SELECT (SELECT count(*) FROM sessions.ingest_lazy_ack_decisions)::int d,(SELECT count(*) FROM sessions.ingest_receipt_seats)::int s")).rows[0];expect(d).toEqual({d:0,s:0})}
  return out;
 }finally{await f.close()}
}
for(const kind of backends)describe("multi-revision on "+kind,()=>{
 for(const [name,steps] of Object.entries(CASES))test(name,async()=>{
  const v1=await run(kind,"baseline",steps),v2=await run(kind,"bridge-v2",steps);
  expect(v2).toEqual(v1);
  const dec=v1.find(x=>x[0]==="decisions")![1] as any[];
  console.log(name,"|",JSON.stringify(v1.filter(x=>typeof x[0]==="number")),"| decisions",JSON.stringify(dec.map(d=>[d[0],d[2],d[3],d[5]])));
 });
});
