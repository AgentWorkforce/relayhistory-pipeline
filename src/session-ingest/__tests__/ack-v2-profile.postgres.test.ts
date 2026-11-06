/**
 * TEST-ONLY P7 profiler (real PG17, ACK_V2_TEST_PG_URL). Installs instrumented COPIES of the bridge, writers,
 * validator and decoder (text-patched from pg_get_functiondef; the fixture itself is unchanged) that RAISE NOTICE
 * server clock_timestamp() marks. Breaks down time spent while the seat clock is held, per ACK, for v1 and v2.
 * ACK_V2_PROF_CLAIMS (default 200) sequential 150-record claims per format, after ACK_V2_PROF_WARM (20) warmup.
 */
import {test,expect,setDefaultTimeout} from "bun:test";
import pg from "pg";
import {fixture,record,type F,type Mode} from "./ack-v2-support.ts";
setDefaultTimeout(7200000);
const url=process.env.ACK_V2_TEST_PG_URL,CLAIMS=Number(process.env.ACK_V2_PROF_CLAIMS??200),WARM=Number(process.env.ACK_V2_PROF_WARM??20),REC=150;
const mark=(l:string)=>`RAISE NOTICE 'prof ${l} %',(extract(epoch FROM clock_timestamp())*1e6)::bigint;`;
async function def(f:F,sig:string){return(await f.db.query("SELECT pg_get_functiondef($1::regprocedure) d",[sig])).rows[0].d as string}
function patch(src:string,pairs:[string,string][]){for(const [a,b] of pairs){if(!src.includes(a))throw Error("anchor: "+a.slice(0,60));src=src.replace(a,b)}return src}
function patchLast(src:string,a:string,b:string){const i=src.lastIndexOf(a);if(i<0)throw Error("anchor: "+a.slice(0,60));return src.slice(0,i)+b+src.slice(i+a.length)}
async function instrument(f:F){
 let b=await def(f,"sessions.ingest_lazy_ack_batch(jsonb)");
 b=patch(b,[[" PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;\n"," PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;"+mark("acq")+"\n"]]);
 await f.db.exec(b);
 let v2=await def(f,"sessions.ingest_lazy_ack_batch_v2(jsonb)");
 v2=patch(v2,[[" v_vec:='\\x0101'::bytea"," "+mark("loop_end")+" v_vec:='\\x0101'::bytea"],[" -- one set-based slot UPDATE"," "+mark("vec_ins")+" -- one set-based slot UPDATE"],
  [" IF v_want<>v_want_d"," "+mark("cov_end")+" IF v_want<>v_want_d"]]);
 v2=patchLast(v2,"PERFORM sessions.ingest_lazy_attempt(t.claim_token);\nEND","PERFORM sessions.ingest_lazy_attempt(t.claim_token);"+mark("end")+"\nEND");
 await f.db.exec(v2);
 let v1=await def(f,"sessions.ingest_lazy_ack_batch_v1(jsonb)");
 v1=patch(v1,[[" UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;"," "+mark("loop_end")+" UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;"]]);
 v1=patchLast(v1,"PERFORM sessions.ingest_lazy_attempt(t.claim_token);\nEND","PERFORM sessions.ingest_lazy_attempt(t.claim_token);"+mark("end")+"\nEND");
 await f.db.exec(v1);
 let val=await def(f,"sessions.ack_slot_validate()");
 val=patch(val,[["IF NOT EXISTS(SELECT 1 FROM nr WHERE current_seat IS NOT NULL) THEN RETURN NULL;END IF;","IF NOT EXISTS(SELECT 1 FROM nr WHERE current_seat IS NOT NULL) THEN RETURN NULL;END IF;"+mark("val_s")],
  [" IF v_bad>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: slot current seat binding refused';END IF;"," IF v_bad>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: slot current seat binding refused';END IF;"+mark("val_bound")]]);
 val=patchLast(val," RETURN NULL;\nEND"," "+mark("val_e")+" RETURN NULL;\nEND");
 await f.db.exec(val);
 let dec=await def(f,"sessions.ingest_ack_entries(uuid)");
 dec=patch(dec,[[" b:=v.vec;len:=octet_length(b);"," "+mark("dec_s")+" b:=v.vec;len:=octet_length(b);"],[" IF a.ack_format IS DISTINCT FROM 2"," "+mark("dec_hashed")+" IF a.ack_format IS DISTINCT FROM 2"],
  [" IF pos<>len THEN"," "+mark("dec_parsed")+" IF pos<>len THEN"]]);
 dec=patchLast(dec," END LOOP;\nEND"," END LOOP;"+mark("dec_e")+"\nEND");
 await f.db.exec(dec);
}
let ids=0;const pool:string[]=[];const revs=new Map<string,number>();let seed=11>>>0;const R=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296};
function claimRecords(){const recs:any[]=[];const used=new Set<string>();
 for(let i=0;i<REC;i++){const x=R();let id:string,rev:number;
  if(x<0.89||pool.length<300){id="p"+(ids++);pool.push(id);rev=1}else{id=pool[Math.floor(R()*pool.length)]!;const c=revs.get(id)??1;rev=x<0.99?c+1:Math.max(1,c-1)}
  if(used.has(id))continue;used.add(id);if(rev>(revs.get(id)??0))revs.set(id,rev);
  recs.push(record({record_id:id,revision:rev,session_id:"s"+(i%8),payload:{text:"y".repeat(24)}}))}return recs}
const q=(xs:number[],p:number)=>{const s=[...xs].sort((a,b)=>a-b);return s[Math.min(s.length-1,Math.floor(p*s.length))]??NaN};
test.skipIf(!url)(`P7 profile: time under the clock lock by phase, v1 vs v2 (${CLAIMS} claims each)`,async()=>{
 const out:any={claims:CLAIMS,recordsPerClaim:REC};
 for(const m of ["bridge-v1","bridge-v2"] as Mode[]){
  ids=0;pool.length=0;revs.clear();seed=11>>>0;
  const f=await fixture("pg",m);const c=new pg.Client({connectionString:f.url});await c.connect();
  try{
   await instrument(f);
   let cur:Record<string,number[]>={};const acks:Record<string,number[]>[]=[];
   c.on("notice",(n:any)=>{const g=/^prof (\w+) (\d+)$/.exec(n.message);if(!g)return;if(g[1]==="acq"){cur={};acks.push(cur)}(cur[g[1]!]??=[]).push(Number(g[2]))});
   const send=f.senderOn(c);
   for(let i=0;i<WARM+CLAIMS;i++)await send(`q${i}`,claimRecords());
   const rows=acks.slice(WARM).filter(a=>a.acq&&a.end);
   const d=(a:Record<string,number[]>,x:string,y:string,i=0,j=0)=>(a[y]?.[j]??NaN)-(a[x]?.[i]??NaN);
   const series:Record<string,number[]>={total:[],loop:[]};
   for(const a of rows){series.total!.push(d(a,"acq","end"));series.loop!.push(d(a,"acq","loop_end"));
    if(m==="bridge-v2"){for(const [k,v] of Object.entries({encode_hash_insert:d(a,"loop_end","vec_ins"),coverage_stmt:d(a,"vec_ins","cov_end"),after_coverage:d(a,"cov_end","end"),
      validator:d(a,"val_s","val_e"),validator_binding:d(a,"val_s","val_bound"),validator_monotonic:d(a,"val_bound","val_e"),
      decode1:d(a,"dec_s","dec_e",0,0),decode1_hash:d(a,"dec_s","dec_hashed",0,0),decode1_parse:d(a,"dec_hashed","dec_parsed",0,0),decode1_join:d(a,"dec_parsed","dec_e",0,0),
      decode2:d(a,"dec_s","dec_e",1,1),decode2_hash:d(a,"dec_s","dec_hashed",1,1),decodes_per_ack:(a.dec_s?.length??0)}))(series[k]??=[]).push(v)}
    else (series.after_loop??=[]).push(d(a,"loop_end","end"))}
   out[m]=Object.fromEntries(Object.entries(series).map(([k,v])=>[k,{n:v.length,p50_us:q(v,0.5),p90_us:q(v,0.9),p99_us:q(v,0.99),mean_us:Math.round(v.reduce((x,y)=>x+y,0)/v.length)}]));
  }finally{await c.end();await f.close()}
 }
 console.log("PROFILE "+JSON.stringify(out));
 expect(out["bridge-v2"].total.n).toBe(CLAIMS);
});
