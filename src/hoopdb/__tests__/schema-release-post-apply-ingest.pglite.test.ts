/**
 * The guarded schema release's POST-apply buffered-ingest check (src/hoopdb/schema-release-post-apply-ingest.ts):
 * the pure classifier for every verdict, the emitted annotations/summary/evidence, and the live read on PGlite
 * over EVERY migration (the real lazy policy, admission table and 044030 overlap table). The wiring into the
 * apply and the workflow is pinned in schema-release.test.ts; the rehearsal's use in schema-rehearsal.test.ts.
 */
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {mkdtempSync,readFileSync,readdirSync,rmSync,writeFileSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {SESSION_BOOTSTRAP_FILES} from "../session-bootstrap-chain.ts";
import {openGenerationOverlap} from "../ingest-generation-overlap.ts";
import {rebindIngestGeneration} from "../ingest-generation-rebind.ts";
import {inspectPostApplyIngest,releaseErrorCode} from "../schema-release-inspect.ts";
import {SCHEMA_RELEASE as P} from "../schema-release-contract.ts";
import {releaseApplyPostCheck} from "../../../scripts/schema-release-database.ts";
import {
 classifyPostApplyIngest,
 INGEST_FENCED_POST_APPLY,
 INGEST_POST_APPLY_UNEXPECTED,
 postApplyIngestReport,
 readPostApplyIngestState,
 runPostApplyIngestCheck,
 type PostApplyIngestRecord,
 type PostApplyIngestState,
} from "../schema-release-post-apply-ingest.ts";
setDefaultTimeout(120000);
const root=join(import.meta.dir,"../../.."),G="a".repeat(64),H="f".repeat(64),C="e".repeat(64);

// ---- Pure classifier.
const win={from:G,to:H,admitDuringApply:false,expiresAt:"2026-11-16T12:00:00.000Z",expired:false,remainingSeconds:3000};
const st=(o:Partial<PostApplyIngestState>={}):PostApplyIngestState=>({policyGeneration:G,marker:{generation:H,status:"pass"},overlapInstalled:true,window:win,admission:{rows:2,offPolicy:0},...o});
const v=(o:Partial<PostApplyIngestState>={},target=H)=>classifyPostApplyIngest(st(o),target);

test("classifier: ok verdicts (no policy, policy at target, open window G -> H)",()=>{
 expect(v({policyGeneration:null,window:null,overlapInstalled:false,admission:null})).toMatchObject({verdict:"no_lazy_policy",ok:true,warn:false,reasons:[]});
 // No lazy policy is ok whatever the rest looks like: buffered ingest is not installed.
 expect(v({policyGeneration:null,marker:null}).verdict).toBe("no_lazy_policy");
 expect(v({policyGeneration:H,window:null})).toMatchObject({verdict:"ingest_at_target",ok:true,warn:false});
 expect(v()).toMatchObject({verdict:"ingest_overlap_rebind_pending",ok:true,warn:false,reasons:[],window:{present:true,remainingSeconds:3000}});
 // Under 15 minutes left: still ok, but a warning.
 expect(v({window:{...win,remainingSeconds:899}})).toMatchObject({verdict:"ingest_overlap_rebind_pending",ok:true,warn:true});
 expect(v({window:{...win,remainingSeconds:900}}).warn).toBe(false);
});

test("classifier: fenced post-apply when the policy is behind and no usable window G -> H",()=>{
 const fenced=(o:Partial<PostApplyIngestState>)=>{const e=v(o);expect(e.verdict).toBe(INGEST_FENCED_POST_APPLY);expect(e.ok).toBe(false);return e.reasons};
 expect(fenced({overlapInstalled:false,window:null})).toEqual(["overlap_not_installed"]);
 expect(fenced({window:null})).toEqual(["no_window"]);
 expect(fenced({window:{...win,expired:true,remainingSeconds:0}})).toEqual(["window_expired"]);
 expect(fenced({window:{...win,remainingSeconds:0}})).toEqual(["window_expired"]);
 expect(fenced({window:{...win,from:C}})).toEqual(["window_from_mismatch"]);
 expect(fenced({window:{...win,to:C}})).toEqual(["window_to_mismatch"]);
 // The policy moved somewhere else than the window's from.
 expect(fenced({policyGeneration:C})).toEqual(["window_from_mismatch"]);
});

test("classifier: anything unexpected fails closed with ingest_post_apply_unexpected",()=>{
 const bad=(o:Partial<PostApplyIngestState>,target=H)=>{const e=v(o,target);expect(e.verdict).toBe(INGEST_POST_APPLY_UNEXPECTED);expect(e.ok).toBe(false);return e.reasons};
 expect(bad({marker:{generation:H,status:"pending"}})).toEqual(["marker_not_pass_at_target"]);
 expect(bad({marker:{generation:G,status:"pass"}})).toEqual(["marker_not_pass_at_target"]);
 expect(bad({marker:null})).toEqual(["marker_not_pass_at_target"]);
 expect(bad({},"not-a-generation")).toEqual(["target_generation_invalid"]);
 expect(bad({admission:{rows:3,offPolicy:1}})).toEqual(["admission_off_policy"]);
 expect(bad({policyGeneration:H,admission:{rows:3,offPolicy:3}})).toEqual(["admission_off_policy"]);
 expect(bad({admission:null})).toEqual(["admission_table_missing"]);
});

test("report: fixed-token annotations; the fenced error names the design doc's Post-apply recovery; no generations",()=>{
 const fenced=postApplyIngestReport(v({window:null}));
 expect(fenced.log[0]).toBe("release_post_apply_ingest ingest_fenced_post_apply no_window");
 expect(fenced.log[1]).toStartWith("::error title=ingest_fenced_post_apply::");
 expect(fenced.log[1]).toContain('docs/design/zero-503-schema-windows.md "Post-apply recovery"');
 const pending=postApplyIngestReport(v({window:{...win,remainingSeconds:600}}));
 expect(pending.log).toEqual([
  "release_post_apply_ingest ingest_overlap_rebind_pending",
  "::notice title=ingest_overlap_rebind_pending::rebind required before window expires at 2026-11-16T12:00:00.000Z (600s remaining)",
  "::warning title=ingest_overlap_rebind_pending::window has fewer than 900s left: run the rebind now",
 ]);
 expect(pending.summary.join("\n")).toContain("rebind required before window expires at 2026-11-16T12:00:00.000Z");
 expect(postApplyIngestReport(v({marker:null})).log[1]).toStartWith("::error title=ingest_post_apply_unexpected::");
 expect(postApplyIngestReport(v({policyGeneration:H})).log).toEqual(["release_post_apply_ingest ingest_at_target"]);
 for(const r of [fenced,pending])for(const l of [...r.log,...r.summary]){expect(l).not.toContain(G);expect(l).not.toContain(H)}
});

const sink=()=>{const out={log:[] as string[],summary:[] as string[],evidence:[] as PostApplyIngestRecord[]};
 return{out,sink:{log:(l:string)=>{out.log.push(l)},summary:async(l:string[])=>{out.summary.push(...l)},evidence:async(e:PostApplyIngestRecord)=>{out.evidence.push(e)}}}};
const code=(p:Promise<unknown>)=>p.then(()=>null,(e:{code?:string})=>e.code??"thrown");

test("run: records evidence before failing; refuses with the verdict; an unreadable state fails closed",async()=>{
 const a=sink();
 expect(await code(runPostApplyIngestCheck(async()=>v({window:null}),a.sink))).toBe(INGEST_FENCED_POST_APPLY);
 expect(a.out.evidence).toHaveLength(1);
 expect(a.out.evidence[0]).toMatchObject({verdict:INGEST_FENCED_POST_APPLY,ok:false,policyGeneration:G,targetGeneration:H,admission:{rows:2,offPolicy:0}});
 expect(a.out.log.some(l=>l.startsWith("::error title=ingest_fenced_post_apply::"))).toBe(true);
 const b=sink();
 expect(await code(runPostApplyIngestCheck(async()=>{throw new Error("connection details here")},b.sink))).toBe(INGEST_POST_APPLY_UNEXPECTED);
 expect(b.out.evidence).toEqual([{check:"post_apply_ingest",verdict:INGEST_POST_APPLY_UNEXPECTED,ok:false,reasons:["state_unreadable"]}]);
 expect(b.out.log.join("\n")).not.toContain("connection details");
 const c=sink();
 expect((await runPostApplyIngestCheck(async()=>v(),c.sink)).verdict).toBe("ingest_overlap_rebind_pending");
 expect(c.out.log.some(l=>l.startsWith("::error"))).toBe(false);
});

// ---- Live read on PGlite over every migration.
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
 const window=(expiresIn:string)=>db.query(`INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'test',clock_timestamp()-interval '1 minute',clock_timestamp()+interval '${expiresIn}')`,[G,H,"9".repeat(64)]);
 const admission=(org:string,generation:string)=>db.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,$2,$3,$4,now()+interval '1 hour',$5)",[org,"a".repeat(64),"b".repeat(64),generation,"3".repeat(64)]);
 const check=async()=>classifyPostApplyIngest(await readPostApplyIngestState(client as never),H);
 return{db,client,marker,window,admission,check};
}

test("pglite: no lazy policy is no_lazy_policy",async()=>{
 const f=await fixture({policy:false});
 await f.marker(H,"pass");
 expect(await f.check()).toMatchObject({verdict:"no_lazy_policy",ok:true,policyGeneration:null,admission:{rows:0,offPolicy:0}});
});

test("pglite: the incident shape (marker PASS at H, policy and admission at G, no window) is ingest_fenced_post_apply; a usable window makes it rebind-pending; the rebind makes it ingest_at_target",async()=>{
 const f=await fixture();
 await f.admission("org-test-1",G);
 // Opened while readiness is PASS at G, as the runbook requires.
 await openGenerationOverlap(f.client as never,{from:G,to:H,ttlSeconds:3600,admitDuringApply:false,approvalSha256:"9".repeat(64)});
 await f.marker(H,"pass");
 const pending=await f.check();
 expect(pending).toMatchObject({verdict:"ingest_overlap_rebind_pending",ok:true,warn:false,policyGeneration:G,marker:{generation:H,status:"pass"},admission:{rows:1,offPolicy:0},window:{present:true,from:G,to:H,expired:false}});
 if(!pending.window.present)throw new Error("window");
 expect(pending.window.remainingSeconds).toBeGreaterThan(3500);
 // The evidence carries counts, generations and flags only: no org id, window id or approval digest.
 const json=JSON.stringify(pending);
 expect(json).not.toContain("org-test-1");
 expect(json).not.toContain("9".repeat(64));
 // The window closed (owner) without a rebind: fenced.
 await f.db.query("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:G,to:H,reason:"owner"})]);
 expect(await f.check()).toMatchObject({verdict:INGEST_FENCED_POST_APPLY,ok:false,reasons:["no_window"]});
 // A short window: still ok, with the warning.
 await f.window("5 minutes");
 expect(await f.check()).toMatchObject({verdict:"ingest_overlap_rebind_pending",ok:true,warn:true});
 // The rebind moves the policy and the admission rows to H and closes the window.
 const r=await rebindIngestGeneration(f.client as never,{from:G,to:H});
 expect(r).toMatchObject({policy:"rebound",overlapClosed:true});
 expect(await f.check()).toMatchObject({verdict:"ingest_at_target",ok:true,policyGeneration:H,admission:{rows:1,offPolicy:0},window:{present:false}});
});

test("pglite: an expired window is fenced; a marker not PASS at H or a split admission fails closed",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 await f.db.query(`INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'test',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 second')`,[G,H,"9".repeat(64)]);
 expect(await f.check()).toMatchObject({verdict:INGEST_FENCED_POST_APPLY,reasons:["window_expired"]});
 await f.db.query("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:G,to:H,reason:"owner"})]);
 await f.window("1 hour");
 await f.admission("org-test-1",G);
 await f.admission("org-test-2",H);
 expect(await f.check()).toMatchObject({verdict:INGEST_POST_APPLY_UNEXPECTED,reasons:["admission_off_policy"],admission:{rows:2,offPolicy:1}});
 await f.db.query("DELETE FROM sessions.ingest_buffer_admission WHERE org_id='org-test-2'");
 await f.marker(H,"pending");
 expect(await f.check()).toMatchObject({verdict:INGEST_POST_APPLY_UNEXPECTED,reasons:["marker_not_pass_at_target"]});
 await f.marker(H,"pass");
 expect((await f.check()).verdict).toBe("ingest_overlap_rebind_pending");
});

test("pglite: the read is read-only (runs inside a READ ONLY transaction)",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 await f.db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
 const e=await f.check();
 await f.db.query("ROLLBACK");
 expect(e.verdict).toBe(INGEST_FENCED_POST_APPLY);
});

test("pglite: no admission table fails closed with its own reason",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 await f.db.exec("DROP TABLE sessions.ingest_buffer_admission CASCADE");
 expect(await f.check()).toMatchObject({verdict:INGEST_POST_APPLY_UNEXPECTED,reasons:["admission_table_missing"],admission:null});
});

// ---- The real transaction wrapper (inspectPostApplyIngest) and the apply's post-apply entry
// (releaseApplyPostCheck). PGlite's user is a superuser, so the owner-identity row alone is shimmed
// (requireReleaseOwner is pinned in schema-release.test.ts); every other statement is real.
const ownerRow={me:P.owner,session:P.owner,db:P.database,rolcanlogin:true,rolsuper:false,rolcreaterole:true,rolcreatedb:true,rolbypassrls:true,rolreplication:true,schema_owner:P.owner};
const releaseLike=(db:PGlite,hook:(text:string)=>Promise<void>|void=()=>{})=>({query:async(text:string,params?:unknown[])=>{
 await hook(text);
 if(text.startsWith("SELECT current_user AS me"))return{rows:[ownerRow]};
 return params?.length?db.query(text,params as never):db.query(text);
}}) as never;
const advisoryLocks=async(db:PGlite)=>Number((await db.query<{n:number}>("SELECT count(*)::int AS n FROM pg_locks WHERE locktype='advisory'")).rows[0]!.n);
// A write succeeds only outside any (read-only or aborted) transaction.
const idle=async(db:PGlite)=>{await db.exec("CREATE TEMP TABLE idle_probe(x int);DROP TABLE idle_probe");return true};

test("wrapper: holds the advisory lock in a READ ONLY transaction while reading, then rolls back and unlocks",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 const seen:{locks:number;ro:string}[]=[];
 const client=releaseLike(f.db,async(t)=>{if(t.includes("ingest_buffer_admission"))seen.push({locks:await advisoryLocks(f.db),ro:(await f.db.query<{r:string}>("SELECT current_setting('transaction_read_only') AS r")).rows[0]!.r})});
 expect((await inspectPostApplyIngest(client,{targetGeneration:H})).verdict).toBe(INGEST_FENCED_POST_APPLY);
 expect(seen).toEqual([{locks:1,ro:"on"},{locks:1,ro:"on"}]);
 expect(await advisoryLocks(f.db)).toBe(0);
 expect(await idle(f.db)).toBe(true);
});

test("wrapper: an exception mid-read leaves the connection idle, no transaction open, the advisory lock released",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 const client=releaseLike(f.db,(t)=>{if(t.includes("FROM sessions.ingest_buffer_admission"))throw Object.assign(new Error("injected"),{code:"XX000"})});
 expect(await code(inspectPostApplyIngest(client,{targetGeneration:H}))).toBe("XX000");
 expect(await advisoryLocks(f.db)).toBe(0);
 expect(await idle(f.db)).toBe(true);
 // A SQL error inside the transaction (aborted state) is rolled back the same way.
 const sqlFail=releaseLike(f.db,async(t)=>{if(t.includes("FROM sessions.ingest_buffer_admission"))await f.db.query("SELECT 1/0")});
 expect(await code(inspectPostApplyIngest(sqlFail,{targetGeneration:H}))).toBe("22012");
 expect(await advisoryLocks(f.db)).toBe(0);
 expect(await idle(f.db)).toBe(true);
});

test("apply entry: a fenced state throws ingest_fenced_post_apply (exit 1) with the apply evidence present and the post-apply record appended",async()=>{
 const f=await fixture();
 await f.marker(H,"pass");
 const dir=mkdtempSync(join(process.env.TMPDIR??tmpdir(),"post-apply-"));open.push(()=>rmSync(dir,{recursive:true,force:true}));
 const summary=join(dir,"summary.md");writeFileSync(summary,"");
 const prev=process.env.GITHUB_STEP_SUMMARY;process.env.GITHUB_STEP_SUMMARY=summary;open.push(()=>{if(prev===undefined)delete process.env.GITHUB_STEP_SUMMARY;else process.env.GITHUB_STEP_SUMMARY=prev});
 const logs:string[]=[];const log=console.log;console.log=(l:string)=>{logs.push(String(l))};
 let err:unknown;
 try{await releaseApplyPostCheck(releaseLike(f.db) as never,{held:[],expectedPending:["upstream:x.sql","upstream:y.sql"],targetGeneration:H},join(dir,"release-apply"))}catch(e){err=e}finally{console.log=log}
 expect(releaseErrorCode(err)).toBe(INGEST_FENCED_POST_APPLY);
 const ev=JSON.parse(readFileSync(join(dir,"release-apply/evidence.json"),"utf8"));
 expect(ev).toMatchObject({version:1,apply:{verified:true,held:0,applied:2,targetGeneration:H},postApplyIngest:{verdict:INGEST_FENCED_POST_APPLY,ok:false,reasons:["no_window"],policyGeneration:G}});
 expect(logs.some(l=>l.startsWith("::error title=ingest_fenced_post_apply::"))).toBe(true);
 expect(readFileSync(summary,"utf8")).toContain("ingest_fenced_post_apply");
 expect(await advisoryLocks(f.db)).toBe(0);
 // An unreadable state: the apply evidence stays, the record says state_unreadable, exit 1.
 const broken=releaseLike(f.db,(t)=>{if(t.includes("ingest_lazy_policy"))throw new Error("boom")});
 console.log=()=>{};
 try{err=undefined;await releaseApplyPostCheck(broken,{held:[],expectedPending:[],targetGeneration:H},join(dir,"release-apply"))}catch(e){err=e}finally{console.log=log}
 expect(releaseErrorCode(err)).toBe(INGEST_POST_APPLY_UNEXPECTED);
 expect(JSON.parse(readFileSync(join(dir,"release-apply/evidence.json"),"utf8"))).toMatchObject({apply:{verified:true},postApplyIngest:{verdict:INGEST_POST_APPLY_UNEXPECTED,reasons:["state_unreadable"]}});
});
