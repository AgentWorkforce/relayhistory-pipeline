/**
 * The CI overlap operator (scripts/ingest-generation-overlap-ci.ts) on PGlite over EVERY migration: the
 * reviewed pair opens, reads back and closes through the 044030 owner functions; it refuses unless readiness
 * is PASS at the old generation and the custody policy is still there; a foreign window blocks it; an
 * expired window admits nothing and closes as 'expired'; the rebind closes the window it covered.
 * rebind-dry-run / rebind (the post-apply recovery): typed confirmations, step-1 contract only, a dry run
 * that changes nothing, and a committed rebind of the policy and admission rows with no window.
 */
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {SESSION_BOOTSTRAP_FILES} from "../session-bootstrap-chain.ts";
import {rebindIngestGeneration} from "../ingest-generation-rebind.ts";
import {generationOverlapStatus} from "../ingest-generation-overlap.ts";
import {overlapOperatorConfirmation,overlapOperatorInputs,runOverlapOperator,runRebindOperator} from "../ingest-generation-overlap-operator.ts";
import type {PrivateIngestGenerationContract} from "../../session-ingest/private-ingest-release.ts";
setDefaultTimeout(120000);
const root=join(import.meta.dir,"../../.."),A="a".repeat(64),B="f".repeat(64),C="e".repeat(64),sha="1".repeat(40);
const atA:PrivateIngestGenerationContract={generation:A,overlap:{from:A,to:B}};
const atB:PrivateIngestGenerationContract={generation:B,overlap:{from:A,to:B}};
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+A+"','pass')");
 await db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)",[JSON.stringify({baselineMode:"lazy-current",generation:A,sourceIdentitySha256:"c".repeat(64),controlEpoch:"3".repeat(64),privacyBaselineSha256:"a".repeat(64),retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"})]);
 const client={query:async(text:string,params?:unknown[])=>(params?.length?db.query(text,params as never):db.query(text)) as never};
 const marker=(generation:string,status:"pending"|"pass")=>db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status=$2",[generation,status]);
 const expiredWindow=(to:string)=>db.query("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'test',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 second')",[A,to,"9".repeat(64)]);
 const liveWindow=(to:string)=>db.query("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'test',clock_timestamp(),clock_timestamp()+interval '10 minutes')",[A,to,"9".repeat(64)]);
 const op=async(mode:"open"|"readback"|"close",contract:PrivateIngestGenerationContract=atA,ttl="600")=>runOverlapOperator(client as never,overlapOperatorInputs({
  OVERLAP_MODE:mode,OVERLAP_SHA:sha,OVERLAP_TTL_SECONDS:mode==="open"?ttl:"",
  OVERLAP_CONFIRMATION:mode==="open"?`OPEN-OVERLAP ${A} ${B} TTL ${ttl} SHA ${sha}`:mode==="close"?`CLOSE-OVERLAP ${A} ${B} SHA ${sha}`:""},contract));
 const rebindEnv=(mode:"rebind-dry-run"|"rebind",dry="95")=>({OVERLAP_MODE:mode,OVERLAP_SHA:sha,OVERLAP_TTL_SECONDS:"",
  OVERLAP_DRY_RUN_ID:mode==="rebind"?dry:"",
  OVERLAP_CONFIRMATION:mode==="rebind"?`REBIND ${A} ${B} SHA ${sha} DRY ${dry}`:`REBIND-DRY-RUN ${A} ${B} SHA ${sha}`});
 const rebindOp=(mode:"rebind-dry-run"|"rebind",contract:PrivateIngestGenerationContract=atA)=>runRebindOperator(client as never,overlapOperatorInputs(rebindEnv(mode),contract));
 const admissions=async()=>(await db.query<{schema_generation:string;n:number}>("SELECT schema_generation,count(*)::int AS n FROM sessions.ingest_buffer_admission GROUP BY 1 ORDER BY 1")).rows;
 const policyGeneration=async()=>(await db.query<{generation:string}>("SELECT generation FROM sessions.ingest_lazy_policy WHERE singleton")).rows[0]?.generation;
 return{db,client,marker,expiredWindow,liveWindow,op,rebindEnv,rebindOp,admissions,policyGeneration};
}
const code=async(p:Promise<unknown>)=>{try{await p;return null}catch(e){return (e as {code?:string}).code??String((e as Error).message)}};

test("open -> readback -> close for the reviewed pair, one owner transaction each, with an exact TTL and no admit-during-apply",async()=>{
 const f=await fixture();
 expect(await f.op("readback")).toMatchObject({status:"OVERLAP_WINDOW_READBACK_PASS",window:"none",policyGeneration:A,marker:{generation:A,status:"pass"}});
 const opened=await f.op("open",atA,"600");
 expect(opened).toMatchObject({status:"OVERLAP_WINDOW_OPENED",window:"open",from:A,to:B});
 expect(Date.parse(opened.expiresAt!)-Date.parse(opened.openedAt!)).toBe(600000);
 expect(opened.approvalSha256).toMatch(/^[0-9a-f]{64}$/);
 expect((await generationOverlapStatus(f.client as never))).toMatchObject({from:A,to:B,admitDuringApply:false,expired:false});
 expect(await f.op("readback")).toMatchObject({status:"OVERLAP_WINDOW_READBACK_PASS",window:"open",windowId:opened.windowId});
 expect(await code(f.op("open"))).toBe("overlap_operator_already_open");
 expect(await f.op("close")).toMatchObject({status:"OVERLAP_WINDOW_CLOSED",window:"none",closedReason:"owner"});
 expect(await generationOverlapStatus(f.client as never)).toBeNull();
 // Closing again is idempotent.
 expect(await f.op("close")).toMatchObject({status:"OVERLAP_WINDOW_NONE_OPEN",window:"none",closedReason:null});
 // TTL bounds are the SQL bounds.
 expect(await f.op("open",atA,"60")).toMatchObject({status:"OVERLAP_WINDOW_OPENED"});
 await f.op("close");
 expect(await f.op("open",atA,"14400")).toMatchObject({status:"OVERLAP_WINDOW_OPENED"});
 await f.op("close");
 expect(await code(f.op("open",atA,"59"))).toBe("overlap_operator_ttl");
 expect(await code(f.op("open",atA,"14401"))).toBe("overlap_operator_ttl");
});

test("open refuses unless readiness is PASS at G and the custody policy is at G; refusals leave nothing behind",async()=>{
 const f=await fixture();
 await f.marker(B,"pending");
 expect(await code(f.op("open"))).toBe("overlap_operator_marker_not_pass_at_from");
 await f.marker(A,"pending");
 expect(await code(f.op("open"))).toBe("overlap_operator_marker_not_pass_at_from");
 await f.marker(B,"pass");
 expect(await code(f.op("open"))).toBe("overlap_operator_marker_not_pass_at_from");
 await f.marker(A,"pass");
 expect(await generationOverlapStatus(f.client as never)).toBeNull();
 // A contract that already moved to H can no longer open.
 expect(await code(f.op("open",atB))).toBe("overlap_operator_stale");
 // The custody policy moved off G (owner write in the fixture): open refuses before the SQL does.
 await f.db.query("UPDATE sessions.ingest_lazy_policy SET generation=$1",[C]);
 expect(await code(f.op("open"))).toBe("overlap_operator_policy_not_at_from");
 expect(await generationOverlapStatus(f.client as never)).toBeNull();
});

test("a foreign window blocks open, readback and close; an expired one admits nothing and is swept or closed as expired",async()=>{
 const f=await fixture();
 await f.liveWindow(C);
 expect(await code(f.op("open"))).toBe("overlap_operator_foreign_window");
 expect(await code(f.op("readback"))).toBe("overlap_operator_foreign_window");
 expect(await code(f.op("close"))).toBe("overlap_operator_foreign_window");
 expect(await generationOverlapStatus(f.client as never)).toMatchObject({from:A,to:C,expired:false});
 await f.db.query("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:A,to:C,reason:"owner"})]);
 expect(await generationOverlapStatus(f.client as never)).toBeNull();
 // Expired foreign window: admits nothing, so open sweeps it and proceeds.
 await f.expiredWindow(C);
 expect(await f.op("readback")).toMatchObject({status:"OVERLAP_WINDOW_READBACK_PASS",window:"none"});
 expect(await f.op("open")).toMatchObject({status:"OVERLAP_WINDOW_OPENED",window:"open"});
 await f.op("close");
 // Expired own window: readback reports it as expired, close records the reason 'expired'.
 await f.expiredWindow(B);
 expect(await f.op("readback")).toMatchObject({window:"expired"});
 expect(await f.op("close")).toMatchObject({status:"OVERLAP_WINDOW_CLOSED",window:"none",closedReason:"expired"});
});

test("rebind closes the operator's window; afterwards the contract at H reads back none and can no longer open",async()=>{
 const f=await fixture();
 await f.op("open");
 await f.marker(B,"pass");
 expect(await rebindIngestGeneration(f.client as never,{from:A,to:B})).toMatchObject({policy:"rebound",overlapClosed:true});
 expect(await f.op("readback",atB)).toMatchObject({status:"OVERLAP_WINDOW_READBACK_PASS",window:"none",policyGeneration:B});
 expect(await f.op("close",atB)).toMatchObject({status:"OVERLAP_WINDOW_NONE_OPEN"});
 expect(await code(f.op("open",atB))).toBe("overlap_operator_stale");
});

test("rebind modes: exact confirmations, refusals for dormant, stale, unknown mode and missing/invalid DRY",async()=>{
 const f=await fixture();
 expect(overlapOperatorConfirmation("rebind-dry-run",A,B,sha)).toBe(`REBIND-DRY-RUN ${A} ${B} SHA ${sha}`);
 expect(overlapOperatorConfirmation("rebind",A,B,sha,"","95")).toBe(`REBIND ${A} ${B} SHA ${sha} DRY 95`);
 expect(overlapOperatorInputs(f.rebindEnv("rebind-dry-run"),atA)).toMatchObject({mode:"rebind-dry-run",from:A,to:B,ttlSeconds:null,dryRunId:null});
 expect(overlapOperatorInputs(f.rebindEnv("rebind"),atA)).toMatchObject({mode:"rebind",from:A,to:B,dryRunId:"95"});
 const refusal=(env:Record<string,string>,contract:PrivateIngestGenerationContract=atA)=>{try{overlapOperatorInputs(env,contract);return null}catch(e){return (e as {code?:string}).code}};
 for(const mode of ["rebind-dry-run","rebind"] as const){
  expect(refusal(f.rebindEnv(mode),{generation:A,overlap:null})).toBe("overlap_operator_dormant");
  // Stale: the contract already moved to H (the rebind is offered only while it is still bound to G).
  expect(refusal(f.rebindEnv(mode),atB)).toBe("overlap_operator_stale");
  expect(refusal({...f.rebindEnv(mode),OVERLAP_TTL_SECONDS:"600"})).toBe("overlap_operator_ttl");
 }
 expect(refusal({...f.rebindEnv("rebind"),OVERLAP_DRY_RUN_ID:""})).toBe("overlap_operator_dry_run_id");
 for(const bad of ["0","01","-1","1.5","abc"," 95","9".repeat(21)])
  expect(refusal(f.rebindEnv("rebind",bad))).toBe("overlap_operator_dry_run_id");
 expect(refusal({...f.rebindEnv("rebind-dry-run"),OVERLAP_DRY_RUN_ID:"95"})).toBe("overlap_operator_dry_run_id");
 expect(refusal({...f.rebindEnv("rebind"),OVERLAP_CONFIRMATION:`REBIND ${A} ${B} SHA ${sha} DRY 96`})).toBe("overlap_operator_confirmation");
 expect(refusal({...f.rebindEnv("rebind"),OVERLAP_CONFIRMATION:`REBIND ${A} ${B} SHA ${sha}`})).toBe("overlap_operator_confirmation");
 expect(refusal({...f.rebindEnv("rebind-dry-run"),OVERLAP_CONFIRMATION:`REBIND-DRY-RUN ${A} ${C} SHA ${sha}`})).toBe("overlap_operator_confirmation");
 expect(refusal({...f.rebindEnv("rebind-dry-run"),OVERLAP_MODE:"rebind-now"})).toBe("overlap_operator_inputs");
 // Each runner refuses the other's modes.
 expect(await code(runOverlapOperator(f.client as never,overlapOperatorInputs(f.rebindEnv("rebind"),atA)))).toBe("overlap_operator_inputs");
 const openInput=overlapOperatorInputs({OVERLAP_MODE:"open",OVERLAP_SHA:sha,OVERLAP_TTL_SECONDS:"600",OVERLAP_CONFIRMATION:`OPEN-OVERLAP ${A} ${B} TTL 600 SHA ${sha}`},atA);
 expect(await code(runRebindOperator(f.client as never,openInput))).toBe("overlap_operator_inputs");
 expect(await code(runRebindOperator(f.client as never,{...overlapOperatorInputs(f.rebindEnv("rebind"),atA),dryRunId:null}))).toBe("overlap_operator_inputs");
 expect(await f.policyGeneration()).toBe(A);
});

test("rebind-dry-run changes nothing; rebind moves the policy and admission rows with no window; refusals while readiness is not PASS at H",async()=>{
 const f=await fixture();
 // Readiness still at G: both modes refuse in SQL and nothing moves.
 expect(await code(f.rebindOp("rebind-dry-run"))).toBe("readiness_not_pass_for_target");
 expect(await code(f.rebindOp("rebind"))).toBe("readiness_not_pass_for_target");
 await f.marker(B,"pending");
 expect(await code(f.rebindOp("rebind"))).toBe("readiness_not_pass_for_target");
 // The post-apply state: marker PASS at H, policy at G, no window.
 await f.marker(B,"pass");
 expect(await generationOverlapStatus(f.client as never)).toBeNull();
 const before=await f.admissions();
 const dry=await f.rebindOp("rebind-dry-run");
 expect(dry).toMatchObject({status:"INGEST_REBIND_DRY_RUN_PASS",mode:"rebind-dry-run",from:A,to:B,dryRunId:null,
  rebind:{policy:"rebound",dryRun:true,overlapClosed:false},marker:{generation:B,status:"pass"},policyGeneration:A});
 expect(dry.approvalSha256).toMatch(/^[0-9a-f]{64}$/);
 expect(await f.policyGeneration()).toBe(A);
 expect(await f.admissions()).toEqual(before);
 const done=await f.rebindOp("rebind");
 expect(done).toMatchObject({status:"INGEST_REBIND_COMMITTED",mode:"rebind",dryRunId:"95",
  rebind:{policy:"rebound",dryRun:false,overlapClosed:false},marker:{generation:B,status:"pass"},policyGeneration:B});
 expect(done.rebind.admissionsRebound).toBe(done.rebind.admissionsTotal);
 expect(await f.policyGeneration()).toBe(B);
 expect((await f.admissions()).every((r)=>r.schema_generation===B)).toBe(true);
 // The receipt holds counts and fixed fields only.
 expect(Object.keys(done.rebind).sort()).toEqual(["admissionsRebound","admissionsTotal","dryRun","from","overlapClosed","policy","to"]);
 // Rerunning against the still-G contract is idempotent ("already"); readback at the moved contract sees no window.
 expect(await f.rebindOp("rebind")).toMatchObject({status:"INGEST_REBIND_COMMITTED",rebind:{policy:"already",admissionsRebound:0}});
 expect(await f.op("readback",atB)).toMatchObject({status:"OVERLAP_WINDOW_READBACK_PASS",window:"none",policyGeneration:B});
 // A live foreign window blocks the rebind.
 const g=await fixture();
 await g.marker(B,"pass");
 await g.liveWindow(C);
 expect(await code(g.rebindOp("rebind-dry-run"))).toBe("overlap_window_mismatch");
 expect(await g.policyGeneration()).toBe(A);
});
