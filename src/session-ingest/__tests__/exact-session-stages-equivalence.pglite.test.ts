/** Exact 044090 detail reference vs the installed 047010 RPC across privacy transitions. */
import {test,expect} from "bun:test";
import {fixture} from "./exact-session-stages.fixture.ts";
import {TENANT} from "./support.ts";
import {TURN_ORIGIN_ID} from "../turns.ts";
type F=Awaited<ReturnType<typeof fixture>>;
const S=(n:number)=>`7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a${String(n).padStart(2,"0")}`;
const LIST_FN="sessions.buffered_sessions_for",STAGES_FN="sessions.buffered_session_stages_for_target";
const binds:Array<[string,{org?:string;user?:string}]>=[["user-1",{}],["user-2",{user:"user-2"}],["org-b",{org:"org-b"}]];
const keys:Array<[string,string]>=[];for(const src of ["claude","codex"])for(const n of [...Array(16).keys(),90])keys.push([src,S(n)]);
/** Walk the whole list with page size `size`, recording each page (first page and every continued page). */
async function pages(f:F,fn:string,size:number,bind={}){
 const out:any[][]=[];let c:Array<string|null>=[null,null,null];
 for(let i=0;i<200;i++){const r=await f.list(fn,size,c,bind);if(!r.length)break;out.push(r);const l=r.at(-1);c=[l.cursor_ts,l.session_id,l.source]}
 return out;
}
/** Every binding: the whole list (limit 100), pages of 1, 2, 3 and 5, and the detail of every key; returns user-1's counts. */
async function same(f:F,label:string,stats:{lists:number;pages:number;details:number}){
 const out:Record<string,[number,number]>={};
 for(const [who,bind] of binds){
  const [a,b]=await Promise.all([f.list(LIST_FN,100,undefined,bind),f.list("ref044090.list",100,undefined,bind)]);
  expect([label,who,a]).toEqual([label,who,b]);stats.lists++;
  for(const size of [1,2,3,5]){
   const [pa,pb]=[await pages(f,LIST_FN,size,bind),await pages(f,"ref044090.list",size,bind)];
   expect([label,who,size,pa]).toEqual([label,who,size,pb]);
   expect(pa.flat()).toEqual(b);stats.pages+=pa.length;
  }
  let n=0;
  for(const [src,session] of keys){
   const [x,y]=await Promise.all([f.stages(STAGES_FN,src,session,bind),f.stages("ref044090.stages",src,session,bind)]);
   expect([label,who,src,session,x]).toEqual([label,who,src,session,y]);n+=x.length;stats.details++;
  }
  out[who]=[a.length,n];
 }
 return out;
}

test("(1) gate off: the target reader retains the frozen fallback, with the writer off, and with the writer on and the backfill complete",async()=>{
 const f=await fixture();
 await f.deliver([["claude",S(1),["r1-a"]],["claude",S(2),["r2-a"]],["codex",S(1),["c1-a"]]]);
 await f.upload(S(3));await f.upload(S(4),3);
 await f.prove();
 const stats={lists:0,pages:0,details:0};
 expect(await f.policy()).toEqual({w:false,r:false,c:false});
 expect((await same(f,"writer off",stats))["user-1"]).toEqual([5,5]);
 await f.writer(true);await f.backfill();
 expect(await f.policy()).toEqual({w:true,r:false,c:true});
 // The side table is complete, but read_enabled is off: still 044040's query (no side-table read can change it).
 await f.db.query("DELETE FROM sessions.ingest_claim_sessions WHERE session_id=$1",[S(1)]);
 expect((await same(f,"writer on, reader off, side rows missing",stats))["user-1"]).toEqual([5,5]);
 expect(stats.details).toBeGreaterThan(50);
});

test("(2)(3) gate on: list rows, order and cursor on the first and every continued page, and every detail, equal the frozen readers",async()=>{
 const f=await fixture();
 const U2={...TENANT,userId:"user-2"},OB={...TENANT,orgId:"org-b"};
 const stats={lists:0,pages:0,details:0};const trail:Array<[string,number,number]>=[];
 const check=async(label:string)=>{const o=await same(f,label,stats);trail.push([label,...o["user-1"]!]);return o};
 // History accepted before the writer exists (backfill-only side rows): one hidden by the reset below, then
 // several sessions per batch (their last_ts tie), a superseded record, turn batches.
 await f.upload(S(90));
 await f.db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id,reset_at) VALUES($1,$2,$3,'r-1',now()-interval '1 hour')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 await f.deliver([["claude",S(1),["r1-a","r1-b"]],["claude",S(2),["r2-a","r2-b","r2-c"]],["codex",S(1),["c1-a"]]]);
 await f.deliver([["claude",S(1),["r1-c"]],["claude",S(3),["r3-a","r3-b"]]]);
 await f.deliver([["claude",S(2),["r2-d"]],["claude",S(4),["r4-a"]],["codex",S(3),["c3-a","c3-b"]]]);
 await f.deliver([["claude",S(1),["r1-a"]],["claude",S(5),["r5-a","r5-b"]]]);
 await f.deliver([["claude",U2.userId,["u2-a"]],["claude",S(7),["u2-b"]]],U2);
 await f.deliver([["claude",S(1),["ob-a"]]],OB);
 await f.enableAll();
 // Accepted with the writer on (writer-inserted rows, accepted_at from the commit refresh): more sessions
 // than a page, one batch of six tied sessions, and a session that already had rows.
 for(const n of [6,7,8])await f.upload(S(n),1);
 await f.deliver([["claude",S(9),["t9"]],["claude",S(10),["t10"]],["codex",S(10),["t10c"]],["claude",S(11),["t11"]],["codex",S(12),["t12"]],["claude",S(13),["t13"]]]);
 await f.upload(S(2),2,"claude",0);await f.upload(S(14),2,"codex");
 await f.deliver([["claude",S(1),["u2-c"]]],U2);
 await f.prove();await f.prove({user:"user-2"});await f.prove({org:"org-b"});
 // Every side row carries its batch's committed_at (backfilled or refreshed), none NULL.
 expect((await f.db.query("SELECT count(*)::int n,count(*) FILTER (WHERE cs.accepted_at IS DISTINCT FROM b.committed_at)::int bad FROM sessions.ingest_claim_sessions cs JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id)")).rows[0]).toMatchObject({bad:0});
 const base=await check("base");
 expect(base).toEqual({"user-1":[17,21],"user-2":[3,2],"org-b":[1,1]});
 // A deletion (any origin) of one session, re-proven.
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','claude',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(4)]);
 await check("deletion, stale proof");await f.prove();
 await check("deletion");
 // A frozen source deletion (rule 5(a)) of a recent session: hidden without a new proof.
 await f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','claude',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(11)]);
 await check("frozen deletion");
 // A fenced attempt: the newest claim's admission version moves on.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET source_control_version=5 WHERE batch_id=(SELECT max(batch_id COLLATE \"C\") FROM sessions.ingest_claim_attempts WHERE user_id=$1 AND records::text LIKE '%'||$2||'%')",[TENANT.userId,S(14)]);
 await check("fenced attempt");
 // A live publication head on the newest tied sessions and on an old one: excluded from the list, walk goes on.
 const pub=(source:string,session:string,state:string,key:string|null)=>f.db.query("INSERT INTO sessions.ingest_publication_heads(org_id,workspace_id,user_id,origin_id,source,session_id,physical_workspace,physical_source,physical_session,machine_id,generation,fence_epoch,reset_epoch,pointer_revision,current_key,state) VALUES($1,$2,$3,$4,$5,$6,$2,$5,$6,'m-1',gen_random_uuid(),1,0,1,$7,$8)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,source,session,key,state]);
 await pub("claude",S(9),"live","k/9");await pub("codex",S(10),"live","k/10");await pub("claude",S(13),"building",null);await pub("claude",S(3),"live","k/3");
 await check("publication heads");
 // A lazy-ACK decision at another admission version hides that claim's records.
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions d SET source_control_version=5 FROM sessions.ingest_claim_attempts a WHERE a.claim_token=d.claim_token AND a.batch_id='b-4'");
 await check("lazy ack");
 // A missed commit refresh (NULL accepted_at under a committed batch) on old and recent rows: the pre-pass evaluates them.
 await f.db.query("UPDATE sessions.ingest_claim_sessions SET accepted_at=NULL WHERE batch_id IN('b-3','b-12') AND user_id=$1",[TENANT.userId]);
 await check("missed refresh");
 // Duplicate record ordinal (recn) on a claim.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=records||jsonb_build_array(records->0) WHERE batch_id='b-5'");
 await check("duplicate record ordinal");
 // The reset moves past b-3's acceptance: everything accepted at or before it hides.
 const acc=((await f.db.query("SELECT committed_at::text t FROM sessions.ingest_batches WHERE batch_id='b-3'")).rows[0] as any).t;
 await f.db.query("UPDATE sessions.upload_resets SET reset_at=$1::timestamptz WHERE request_id='r-1'",[acc]);
 await check("reset, stale proof");await f.prove();
 await check("reset");
 expect(trail.map((t)=>t[0])).toEqual(["base","deletion, stale proof","deletion","frozen deletion","fenced attempt","publication heads","lazy ack","missed refresh","duplicate record ordinal","reset, stale proof","reset"]);
 // Every state but two changed what user-1 sees: a missed refresh must change nothing, and the reset already
 // applied under the stale proof (the re-proof keeps it). A stale proof after a deletion closes the gate.
 expect(trail).toEqual([["base",17,21],["deletion, stale proof",0,0],["deletion",16,20],["frozen deletion",15,19],["fenced attempt",14,18],["publication heads",11,18],["lazy ack",10,16],["missed refresh",10,16],["duplicate record ordinal",10,15],["reset, stale proof",8,10],["reset",8,10]]);
 expect(stats).toEqual({lists:33,pages:372,details:1122}); // 11 states x 3 bindings; pages of 1,2,3,5; 34 keys
});
