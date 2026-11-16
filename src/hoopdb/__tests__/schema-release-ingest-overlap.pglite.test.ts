/**
 * The guarded schema release's buffered-ingest overlap gate (src/hoopdb/schema-release-overlap.ts) on PGlite
 * over EVERY migration (the real 044030 overlap table and owner functions, the real lazy policy): the plan
 * and the apply refuse release_ingest_overlap_required when a release would move readiness off the policy's
 * generation G without an open, unexpired window G -> targetGeneration H with at least 15 minutes left and
 * the release sha's reviewed PRIVATE_INGEST_OVERLAP {G, H}; a database without the lazy policy is a recorded
 * no-op. The wiring into plan, fail-fast apply and the lock-held beforeWrite recheck is pinned by
 * schema-rehearsal.test.ts and schema-release.test.ts.
 */
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {SESSION_BOOTSTRAP_FILES} from "../session-bootstrap-chain.ts";
import {openGenerationOverlap} from "../ingest-generation-overlap.ts";
import {rebindIngestGeneration} from "../ingest-generation-rebind.ts";
import {
  checkIngestOverlap,
  RELEASE_INGEST_OVERLAP_REQUIRED,
  RELEASE_OVERLAP_MIN_REMAINING_SECONDS,
  readIngestOverlapState,
  type ReleaseOverlapContract,
} from "../schema-release-overlap.ts";
setDefaultTimeout(120000);
const root=join(import.meta.dir,"../../.."),G="a".repeat(64),H="f".repeat(64),C="e".repeat(64);
const pair:ReleaseOverlapContract={from:G,to:H};
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(opts:{policy?:boolean}={}){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+G+"','pass')");
 if(opts.policy!==false)await db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)",[JSON.stringify({baselineMode:"lazy-current",generation:G,sourceIdentitySha256:"c".repeat(64),controlEpoch:"3".repeat(64),privacyBaselineSha256:"a".repeat(64),retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"})]);
 const client={query:async(text:string,params?:unknown[])=>(params?.length?db.query(text,params as never):db.query(text)) as never};
 const marker=(generation:string,status:"pending"|"pass")=>db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status=$2",[generation,status]);
 // A window as the owner open writes it, with an arbitrary lifetime (the SQL open enforces 60 s .. 4 h from now).
 const window=(to:string,expiresIn:string)=>db.query(`INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'test',clock_timestamp()-interval '1 minute',clock_timestamp()+interval '${expiresIn}')`,[G,to,"9".repeat(64)]);
 const logs:string[]=[];
 const gate=(input:{targetGeneration?:string;contract?:ReleaseOverlapContract}={})=>checkIngestOverlap(client as never,{targetGeneration:input.targetGeneration??H,contract:"contract" in input?input.contract:pair},l=>logs.push(l));
 const refusal=async(input:Parameters<typeof gate>[0]={})=>{try{await gate(input);return null}catch(e){return (e as {code?:string}).code??"thrown"}};
 return{db,client,marker,window,gate,refusal,logs};
}

test("no lazy policy (fresh database / rehearsal without R7-pre): a recorded no-op, even with no window and a dormant contract",async()=>{
 const f=await fixture({policy:false});
 const e=await f.gate({contract:null});
 expect(e).toMatchObject({gate:"no_lazy_policy",reasons:[],policyGeneration:null,targetGeneration:H,window:{present:false},contract:{state:"dormant",match:false},minRemainingSeconds:900});
 expect(f.logs).toEqual([]);
});

test("refuses without a window: the policy is at G, the marker PASS at G and the release moves readiness to H",async()=>{
 const f=await fixture();
 expect(await f.refusal()).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
 expect(f.logs).toEqual([RELEASE_INGEST_OVERLAP_REQUIRED+" no_window"]);
 // A failed apply's retry (marker pending at H) still moves readiness and still needs the window.
 await f.marker(H,"pending");
 expect(await f.refusal()).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
});

test("refuses a window to another generation, and one from a generation the policy is not at",async()=>{
 const f=await fixture();
 await f.window(C,"2 hours");
 expect(await f.refusal()).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
 expect(f.logs.at(-1)).toBe(RELEASE_INGEST_OVERLAP_REQUIRED+" window_to_mismatch");
 // The same window G -> C is exactly right for a release whose target is C (with the contract {G, C}).
 expect((await f.gate({targetGeneration:C,contract:{from:G,to:C}})).gate).toBe("overlap_ready");
 await f.db.query("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:G,to:C,reason:"owner"})]);
 // A window G -> H while the policy has moved to C (owner write in the fixture): the window does not cover the policy.
 await f.window(H,"2 hours");
 await f.db.query("UPDATE sessions.ingest_lazy_policy SET generation=$1",[C]);
 expect(await f.refusal({contract:{from:C,to:H}})).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
 expect(f.logs.at(-1)).toBe(RELEASE_INGEST_OVERLAP_REQUIRED+" window_from_mismatch");
});

test("refuses an expired window and one with less than the 15-minute minimum left; accepts exactly enough",async()=>{
 const f=await fixture();
 expect(RELEASE_OVERLAP_MIN_REMAINING_SECONDS).toBe(900);
 await f.db.query(`INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'test',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 second')`,[G,H,"9".repeat(64)]);
 expect(await f.refusal()).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
 expect(f.logs.at(-1)).toBe(RELEASE_INGEST_OVERLAP_REQUIRED+" window_expired");
 await f.db.query("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:G,to:H,reason:"owner"})]);
 await f.window(H,"10 minutes");
 expect(await f.refusal()).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
 expect(f.logs.at(-1)).toBe(RELEASE_INGEST_OVERLAP_REQUIRED+" window_ttl_short");
 await f.db.query("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:G,to:H,reason:"owner"})]);
 await f.window(H,"16 minutes");
 const e=await f.gate();
 expect(e.gate).toBe("overlap_ready");
 if(!e.window.present)throw new Error("window");
 expect(e.window.remainingSeconds).toBeGreaterThanOrEqual(900);
 expect(e.window.remainingSeconds).toBeLessThanOrEqual(960);
});

test("refuses a contract that is not exactly {from: G, to: H}: mismatched, reversed, dormant or unreadable",async()=>{
 const f=await fixture();
 await openGenerationOverlap(f.client as never,{from:G,to:H,ttlSeconds:3600,admitDuringApply:false,approvalSha256:"9".repeat(64)});
 for(const [contract,reason] of [[{from:G,to:C},"contract_mismatch"],[{from:C,to:H},"contract_mismatch"],[{from:H,to:G},"contract_mismatch"],[null,"contract_dormant"],[undefined,"contract_unreadable"]] as const){
  expect(await f.refusal({contract})).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
  expect(f.logs.at(-1)).toBe(RELEASE_INGEST_OVERLAP_REQUIRED+" "+reason);
 }
 // Every failing condition is reported, the window's and the contract's together.
 await f.marker(G,"pass");
 expect(await f.refusal({targetGeneration:C,contract:null})).toBe(RELEASE_INGEST_OVERLAP_REQUIRED);
 expect(f.logs.at(-1)).toBe(RELEASE_INGEST_OVERLAP_REQUIRED+" window_to_mismatch,contract_dormant");
});

test("passes with the owner-opened window G -> H and the contract {G, H}; the evidence carries generations and window shape only",async()=>{
 const f=await fixture();
 const w=await openGenerationOverlap(f.client as never,{from:G,to:H,ttlSeconds:3600,admitDuringApply:false,approvalSha256:"9".repeat(64)});
 const e=await f.gate();
 expect(e).toMatchObject({gate:"overlap_ready",reasons:[],targetGeneration:H,policyGeneration:G,marker:{generation:G,status:"pass"},
  window:{present:true,from:G,to:H,admitDuringApply:false,expired:false},contract:{state:"pair",from:G,to:H,match:true}});
 if(!e.window.present)throw new Error("window");
 expect(e.window.expiresAt).toBe(new Date(w.expiresAt).toISOString());
 expect(e.window.remainingSeconds).toBeGreaterThan(3500);
 // No window id, approval digest or opener in the artifact section.
 expect(Object.keys(e.window).sort()).toEqual(["admitDuringApply","expired","expiresAt","from","present","remainingSeconds","to"]);
 expect(JSON.stringify(e)).not.toMatch(/windowId|approval|opened_by|"9{64}"/);
 expect(f.logs).toEqual([]);
 // Read-only: nothing changed.
 expect((await readIngestOverlapState(f.client as never)).window).toMatchObject({from:G,to:H,expired:false});
});

test("no readiness move (marker already PASS at H) and an already rebound policy are recorded no-ops",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 expect(await f.gate({contract:null})).toMatchObject({gate:"no_generation_move",reasons:[]});
 // After the rebind (policy at H, window closed) a later release whose target is H again moves nothing either.
 await rebindIngestGeneration(f.client as never,{from:G,to:H});
 await f.marker(H,"pending");
 expect(await f.gate({contract:null})).toMatchObject({gate:"policy_at_target",policyGeneration:H,reasons:[]});
});
