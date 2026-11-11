/**
 * 20261105047020 paged exact-session stages (design B, default off). Synthetic data and PGlite only.
 * sessions.buffered_session_stages_for_target_paged returns one envelope per call: at most
 * p_page_claims (clamped 1..4) candidate claims of the target session are expanded, a keyset cursor
 * resumes strictly after the last INSPECTED candidate, and later pages only admit claims committed at
 * or before the first page's snapshot_at. Concatenated pages must equal 047010's
 * buffered_session_stages_for_target and the frozen 044090 reader row for row.
 */
import {test,expect} from "bun:test";
import {readFileSync,readdirSync} from "node:fs";
import {fixture} from "./exact-session-stages.fixture.ts";
import {TENANT} from "./support.ts";

const epoch="1".repeat(64),baseline="a".repeat(64);
const HP=[epoch,null,null,null,null,false,baseline];
const MIGRATION="supabase/migrations/20261105047020_buffered_session_stages_paged.sql";
const ROLLBACK="supabase/rollbacks/20261105047020_buffered_session_stages_paged.down.sql";
// The landed exact-session reader (047010, #526). The paged migration requires it, so it must sort first
// and be the only file with this stem.
const READERS=readdirSync("supabase/migrations").filter(n=>n.endsWith("_buffered_exact_session_stages.sql"));
const READER_LEDGER="upstream:"+READERS[0];
const SIGNATURE="sessions.buffered_session_stages_for_target_paged(text,text,text,text,text,text,text,text,boolean,text,jsonb,integer)";
const TARGET_SIGNATURE="sessions.buffered_session_stages_for_target(text,text,text,text,text,text,text,text,boolean,text)";
const PAGED="SELECT stages,cursor,has_more,snapshot_at FROM sessions.buffered_session_stages_for_target_paged($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12)";
// Same projection as the fixture's STAGES so paged rows compare with f.stages(...) exactly.
const EXPAND=`SELECT batch_id,stage_key,stage_sha256,stage_bytes::text AS stage_bytes,group_indexes,group_ordinals,kept_ordinals,reset_epoch::text AS reset_epoch,accepted_at,admission_epoch,admission_version
  FROM ROWS FROM (jsonb_to_recordset($1::jsonb) AS (batch_id text,stage_key text,stage_sha256 text,stage_bytes bigint,group_indexes integer[],group_ordinals jsonb,kept_ordinals integer[],reset_epoch bigint,accepted_at timestamptz,admission_epoch text,admission_version text)) WITH ORDINALITY AS x(batch_id,stage_key,stage_sha256,stage_bytes,group_indexes,group_ordinals,kept_ordinals,reset_epoch,accepted_at,admission_epoch,admission_version,ordinality)
  ORDER BY x.ordinality`;
type Page={stages:any[];cursor:any;has_more:boolean;snapshot_at:Date};
type F=Awaited<ReturnType<typeof fixture>>;

const page=async(f:F,session:string,cursor:unknown,p:unknown,source="droid"):Promise<Page>=>{
 const rows=await f.asReader(PAGED,[TENANT.workspaceId,source,session,...HP,cursor===null?null:JSON.stringify(cursor),p]);
 expect(rows).toHaveLength(1);
 return rows[0] as Page;
};
const expand=async(f:F,stages:unknown[])=>(await f.db.query(EXPAND,[JSON.stringify(stages)])).rows as any[];
/** Full client traversal, as the Worker does: cursor NULL, then each returned cursor. */
async function traverse(f:F,session:string,p:unknown,between?:(n:number)=>Promise<void>,source="droid"){
 const pages:Page[]=[];let cursor:unknown=null;
 for(;;){
  if(pages.length>64)throw Error("runaway traversal");
  const pg=await page(f,session,cursor,p,source);pages.push(pg);
  if(pages.length>1)expect(pg.snapshot_at).toEqual(pages[0]!.snapshot_at);
  expect(pg.cursor===null).toBe(!pg.has_more);
  if(!pg.has_more)break;
  expect(pg.cursor.snapshot_at).toBe(typeof cursor==="object"&&cursor?(cursor as any).snapshot_at:pg.cursor.snapshot_at);
  if(cursor)expect(JSON.stringify(pg.cursor)).not.toBe(JSON.stringify(cursor));
  cursor=pg.cursor;
  if(between)await between(pages.length);
 }
 return {pages,rows:await expand(f,pages.flatMap(p=>p.stages))};
}
async function wide(f:F){
 let unrelatedGroup=0;
 for(let batch=0;batch<17;batch++){
  // Synthetic values, aggregate incident shape only (as 047010's regression): 16*10+17 target records.
  const groups:Array<[string,string,string[]]>=[["droid","fixture-wide-target",Array.from({length:batch===16?17:10},(_,i)=>`${batch}-target-${i}`)]];
  for(let session=0;session<9;session++){
   const count=unrelatedGroup++<30?45:44;
   groups.push([session%2?"codex":"droid",`fixture-wide-other-${session}`,Array.from({length:count},(_,record)=>`${batch}-${session}-${record}`)]);
  }
  await f.deliver(groups);
 }
 await f.enableAll();await f.prove();
}
/** n claims whose every record belongs to the target session. */
async function allTarget(f:F,n:number,session="fixture-all-target",tag="r"){
 for(let batch=0;batch<n;batch++)await f.deliver([["droid",session,Array.from({length:3},(_,i)=>`${session}-${tag}-${batch}-${i}`)]]);
}

test("047020 (a): 17 claims / 170 groups / 6939 records, p=2 -> 9 pages equal to 047010 and 044090",async()=>{
 const f=await fixture();await wide(f);
 expect((await f.db.query("SELECT count(*)::int claims,sum(jsonb_array_length(records))::int records FROM sessions.ingest_claim_attempts")).rows).toEqual([{claims:17,records:6939}]);
 expect((await f.db.query("SELECT sum(jsonb_array_length(group_ordinals))::int groups FROM sessions.ingest_stage_intents")).rows).toEqual([{groups:170}]);
 const frozen=await f.stages("ref044090.stages","droid","fixture-wide-target");
 const target=await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-wide-target");
 expect(target).toEqual(frozen);
 const {pages,rows}=await traverse(f,"fixture-wide-target",2);
 expect(pages).toHaveLength(9);
 expect(pages.map(p=>p.has_more)).toEqual([true,true,true,true,true,true,true,true,false]);
 expect(pages.map(p=>p.stages.length)).toEqual([2,2,2,2,2,2,2,2,1]);
 expect(pages.at(-1)!.cursor).toBeNull();
 expect(Object.keys(pages[0]!.cursor).sort()).toEqual(["accepted_at","batch_id","snapshot_at","stage_key","stage_token","v"]);
 expect(rows).toEqual(target);
 expect(rows).toHaveLength(17);
 expect(rows.reduce((n,r)=>n+r.kept_ordinals.length,0)).toBe(177);
 // Same column names as the _target function (jsonb stores object keys in its canonical order; the
 // row order and every column value are what EXPAND proves above, column order is restored by name).
 const cols=(await f.db.query("SELECT array_agg(a ORDER BY o) cols FROM pg_proc p CROSS JOIN LATERAL unnest(p.proargnames) WITH ORDINALITY AS u(a,o) WHERE p.oid=$1::regprocedure AND p.proargmodes[o]='t'",[TARGET_SIGNATURE])).rows[0] as any;
 expect(pages.flatMap(p=>p.stages).every((r:any)=>JSON.stringify(Object.keys(r).sort())===JSON.stringify([...cols.cols].sort()))).toBe(true);
 expect(cols.cols).toEqual(Object.keys(target[0]));
 // An unrelated session and another owner see their own (here: empty / no) stages through the same pages.
 const other=await traverse(f,"fixture-wide-other-0",2);
 expect(other.rows).toEqual(await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-wide-other-0"));
 const foreign=await f.asReader(PAGED,[TENANT.workspaceId,"droid","fixture-wide-target",...HP,null,2],{user:"fixture-other-user"});
 expect(foreign.map((r:any)=>[r.stages,r.cursor,r.has_more])).toEqual([[[],null,false]]);
});

test("047020 (b)+(d): all-target claims, per-page expansion bounded by p_page_claims, clamp 1..4",async()=>{
 const f=await fixture();await allTarget(f,7);await f.enableAll();await f.prove();
 const unpaged=await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-all-target");
 expect(unpaged).toEqual(await f.stages("ref044090.stages","droid","fixture-all-target"));
 expect(unpaged).toHaveLength(7);
 const sizes:Record<string,number[]>={};
 for(const [p,want] of [[1,[1,1,1,1,1,1,1]],[2,[2,2,2,1]],[3,[3,3,1]],[4,[4,3]],[0,[1,1,1,1,1,1,1]],[-5,[1,1,1,1,1,1,1]],[99,[4,3]],[null,[2,2,2,1]]] as Array<[number|null,number[]]>){
  const {pages,rows}=await traverse(f,"fixture-all-target",p);
  expect(rows).toEqual(unpaged);
  sizes[String(p)]=pages.map(x=>x.stages.length);
  expect([p,sizes[String(p)]]).toEqual([p,want]);
 }
 // Counter (b): a poison claim. Claim k's records are corrupted so expanding them raises 22P02 in
 // rec's jsonb_to_recordset. A page that does not expand claim k succeeds; only the page that owns
 // it fails. With p=2 the page holding k as its (p+1)th has_more probe must still succeed, which
 // proves the probe row is never expanded. PGlite cannot show plan nodes inside a plpgsql body, so
 // the poison sentinel is the counter used here.
 const claims=(await f.db.query(`SELECT a.claim_token,a.records FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id) ORDER BY b.committed_at,a.batch_id`)).rows as any[];
 expect(claims).toHaveLength(7);
 for(let k=0;k<claims.length;k++){
  const poison=claims[k].records.map((r:any,i:number)=>i===1?{...r,ord:"not-an-integer"}:r);
  await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$1::jsonb WHERE claim_token=$2",[JSON.stringify(poison),claims[k].claim_token]);
  const failingPage=Math.floor(k/2)+1;let cursor:unknown=null;let n=0;let failed:number|null=null;
  for(;;){
   n++;
   try{const pg=await page(f,"fixture-all-target",cursor,2);if(!pg.has_more)break;cursor=pg.cursor}
   catch(e){expect((e as any).code).toBe("22P02");failed=n;break}
  }
  expect([k,failed]).toEqual([k,failingPage]);
  await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$1::jsonb WHERE claim_token=$2",[JSON.stringify(claims[k].records),claims[k].claim_token]);
 }
 console.log(JSON.stringify({qualification:"synthetic PGlite; counter = poison-claim sentinel + per-page stage counts (each all-target claim yields exactly one stage)",pageSizes:sizes}));
});

test("047020 (c): fully privacy-filtered pages still advance the cursor",async()=>{
 const f=await fixture();await allTarget(f,7);await f.enableAll();await f.prove();
 const claims=(await f.db.query(`SELECT a.claim_token FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id) ORDER BY b.committed_at,a.batch_id`)).rows as any[];
 // Claims 2..5 (pages 2 and 3 at p=2): every ACK admission moves, so every target record is hidden.
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=5 WHERE claim_token=ANY($1::uuid[])",[claims.slice(2,6).map(c=>c.claim_token)]);
 const unpaged=await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-all-target");
 expect(unpaged).toEqual(await f.stages("ref044090.stages","droid","fixture-all-target"));
 expect(unpaged).toHaveLength(3);
 const {pages,rows}=await traverse(f,"fixture-all-target",2);
 expect(pages.map(p=>[p.stages.length,p.has_more])).toEqual([[2,true],[0,true],[0,true],[1,false]]);
 expect(rows).toEqual(unpaged);
 // Every page filtered (another owner's binding keeps nothing; ACK moved on all claims): terminates.
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=5");
 const none=await traverse(f,"fixture-all-target",1);
 expect(none.pages.map(p=>p.stages.length)).toEqual([0,0,0,0,0,0,0]);
 expect(none.rows).toEqual(await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-all-target"));
});

test("047020 (e): later pages admit only claims committed at or before snapshot_at",async()=>{
 const f=await fixture();await allTarget(f,3);await f.enableAll();await f.prove();
 const {pages,rows}=await traverse(f,"fixture-all-target",1,async(n)=>{if(n===1){await allTarget(f,1,"fixture-all-target","late");await f.prove()}});
 expect(pages.map(p=>p.stages.length)).toEqual([1,1,1]);
 expect(rows).toHaveLength(3);
 const late=(await f.db.query("SELECT max(committed_at) AS t FROM sessions.ingest_batches")).rows[0] as any;
 expect(late.t.getTime()).toBeGreaterThan(pages[0]!.snapshot_at.getTime());
 // A FRESH traversal (new snapshot, as the Worker's recheck) sees the late claim.
 const fresh=await traverse(f,"fixture-all-target",1);
 expect(fresh.rows).toHaveLength(4);
 expect(fresh.rows).toEqual(await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-all-target"));
 // A cursor cannot claim a snapshot from the future or lie about ordering.
 const c=pages[0]!.cursor;
 for(const bad of [{...c,snapshot_at:"2999-01-01T00:00:00+00:00"},{...c,accepted_at:"2999-01-01T00:00:00+00:00"},{...c,v:2},{...c,extra:"x"},{...c,stage_token:"x"},{...c,accepted_at:"not-a-time"},[],"x"]){
  await expect(page(f,"fixture-all-target",bad,1)).rejects.toMatchObject({code:"22023"});
 }
});

test("047020 (f): static SQL: page CTE with LIMIT is MATERIALIZED before any expansion; only the new function",()=>{
 const sql=readFileSync(MIGRATION,"utf8");
 const code=sql.split("\n").map(l=>l.replace(/--.*$/,"")).join("\n");
 expect([...code.matchAll(/CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([\w.]+)/g)].map(m=>m[1])).toEqual(["sessions.buffered_session_stages_for_target_paged"]);
 expect(code).not.toMatch(/\b(?:ALTER|DROP)\s+FUNCTION\b/);
 expect(code).not.toMatch(/\bOFFSET\b/i);
 const body=code.slice(code.indexOf("  WITH ctl AS MATERIALIZED ("));
 const ctes=[...body.matchAll(/^\s*(?:WITH|\),)\s+(\w+)\s+AS\s+(?:MATERIALIZED\s+)?\(/gm)].map(m=>m[1]);
 expect(ctes.slice(0,3)).toEqual(["ctl","page","stg"]);
 const pageAt=body.indexOf("), page AS MATERIALIZED (");
 const pageSql=body.slice(pageAt,body.indexOf("), stg AS MATERIALIZED ("));
 expect(pageAt).toBeGreaterThan(0);
 expect(pageSql).toContain("FROM sessions.ingest_claim_sessions AS cs");
 expect(pageSql).toContain("LIMIT v_page + 1");
 expect(pageSql).toContain("b.committed_at <= v_snapshot");
 expect(pageSql).not.toMatch(/jsonb_to_recordset|jsonb_array_elements|records|epochs/);
 const firstExpand=code.search(/jsonb_to_recordset|jsonb_array_elements\(st\./);
 expect(firstExpand).toBeGreaterThan(code.indexOf("), page AS MATERIALIZED ("));
 // stg (the only source of records/epochs for every expansion) is driven by page and excludes the probe row.
 const stg=body.slice(body.indexOf("), stg AS MATERIALIZED ("),body.indexOf("), grp AS"));
 expect(stg).toMatch(/FROM page AS pg\b/);
 expect(stg).toContain("WHERE pg.n <= v_page");
 expect(code).toContain("v_page integer := least(greatest(coalesce(p_page_claims, 2), 1), 4)");
});

test("047020 requires the exact owned 047010 reader, refuses a prior paged RPC, grants only the read role",async()=>{
 const f=await fixture();
 const attrs=(await f.db.query(`SELECT p.prosecdef,p.provolatile,p.proconfig,l.lanname,
  (SELECT array_agg(r.rolname ORDER BY r.rolname) FROM aclexplode(p.proacl) a JOIN pg_roles r ON r.oid=a.grantee WHERE a.grantee<>p.proowner) AS grantees
  FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=$1::regprocedure`,[SIGNATURE])).rows;
 expect(attrs).toEqual([{prosecdef:true,provolatile:"s",proconfig:["search_path=pg_catalog, sessions, pg_temp"],lanname:"plpgsql",grantees:["bulls96_read_api"]}]);
 const migration=readFileSync(MIGRATION,"utf8");
 await expect(f.db.exec(migration)).rejects.toMatchObject({code:"55000"});
 await f.db.exec("DROP FUNCTION "+SIGNATURE);
 await f.db.exec("CREATE FUNCTION sessions.buffered_session_stages_for_target_paged(integer) RETURNS integer LANGUAGE sql AS 'SELECT 1'");
 await expect(f.db.exec(migration)).rejects.toMatchObject({code:"55000"});
 await f.db.exec("DROP FUNCTION sessions.buffered_session_stages_for_target_paged(integer)");
 await f.db.exec("ALTER FUNCTION "+TARGET_SIGNATURE+" VOLATILE");
 await expect(f.db.exec(migration)).rejects.toMatchObject({code:"55000"});
 expect((await f.db.query("SELECT to_regprocedure($1) IS NULL AS absent",[SIGNATURE])).rows).toEqual([{absent:true}]);
 await f.db.exec("ALTER FUNCTION "+TARGET_SIGNATURE+" STABLE");
 await f.db.exec(migration);
 expect((await f.db.query("SELECT to_regprocedure($1) IS NULL AS absent",[SIGNATURE])).rows).toEqual([{absent:false}]);
});

test("047020 gate: closed gate is an empty envelope; gate change mid-traversal fails closed",async()=>{
 const f=await fixture();await allTarget(f,3);
 // Reader gate off: 047010's fallback, one exhausted envelope equal to the unpaged function.
 await f.writer(true);await f.prove();
 const off=await traverse(f,"fixture-all-target",1);
 expect(off.pages).toHaveLength(1);
 expect(off.rows).toEqual(await f.stages("sessions.buffered_session_stages_for_target","droid","fixture-all-target"));
 await f.writer(false);await f.enableAll();await f.prove();
 const first=await page(f,"fixture-all-target",null,1);
 expect(first.has_more).toBe(true);
 await f.reader(false);
 await expect(page(f,"fixture-all-target",first.cursor,1)).rejects.toMatchObject({code:"55000"});
 await f.reader(true);
 // Scope (owner_buffered_read_scope) and argument errors are 047010's, code for code.
 const code=async(q:Promise<unknown>)=>{try{await q;return "ok"}catch(e){return [(e as any).code,(e as any).message]}};
 for(const [ws,source,session] of [["fixture-other-workspace","droid","fixture-all-target"],[TENANT.workspaceId,"","fixture-all-target"],[TENANT.workspaceId,"droid",null]]){
  const want=await code(f.asReader("SELECT * FROM sessions.buffered_session_stages_for_target($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",[ws,source,session,...HP]));
  expect(want).not.toBe("ok");
  expect(await code(f.asReader(PAGED,[ws,source,session,...HP,null,1]))).toEqual(want);
 }
});

test("047020 sorts after the exact-session reader it requires (apply order is by file name)",()=>{
 expect(READERS).toEqual(["20261105047010_buffered_exact_session_stages.sql"]);
 expect(READERS[0]!<MIGRATION.slice("supabase/migrations/".length)).toBe(true);
});
test("047020 (g): down requires its ledger, exact checksum/body and no later migration; leaves 047010 intact",async()=>{
 const f=await fixture();
 const down=readFileSync(ROLLBACK,"utf8");
 const migration=readFileSync(MIGRATION,"utf8");
 const sha=new Bun.CryptoHasher("sha256").update(migration).digest("hex");
 const own="upstream:20261105047020_buffered_session_stages_paged.sql";
 const refuse=async()=>{await expect(f.db.exec(down)).rejects.toMatchObject({code:"55000"});await f.db.exec("ROLLBACK");};
 await refuse();
 await f.db.exec("CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text)");
 await f.db.query("INSERT INTO sessions.__96bulls_migrations VALUES($1,$2)",[own,"wrong"]);await refuse();
 await f.db.query("UPDATE sessions.__96bulls_migrations SET sha256=$1",[sha]);
 await f.db.exec("ALTER FUNCTION "+SIGNATURE+" VOLATILE");await refuse();
 await f.db.exec("ALTER FUNCTION "+SIGNATURE+" STABLE");
 await f.db.exec("INSERT INTO sessions.__96bulls_migrations VALUES('upstream:20990101000000_later.sql','fixture')");await refuse();
 await f.db.exec("DELETE FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:2099%'");
 await f.db.query("INSERT INTO sessions.__96bulls_migrations VALUES($1,'fixture-reader')",[READER_LEDGER]);
 const before=(await f.db.query("SELECT oid::regprocedure::text AS fn,prosrc FROM pg_proc WHERE oid IN ($1::regprocedure,'sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)'::regprocedure) ORDER BY 1",[TARGET_SIGNATURE])).rows;
 await f.db.exec(down);
 expect((await f.db.query("SELECT to_regprocedure($1) IS NULL AS absent",[SIGNATURE])).rows).toEqual([{absent:true}]);
 expect((await f.db.query("SELECT name FROM sessions.__96bulls_migrations")).rows).toEqual([{name:READER_LEDGER}]);
 expect((await f.db.query("SELECT oid::regprocedure::text AS fn,prosrc FROM pg_proc WHERE oid IN ($1::regprocedure,'sessions.buffered_session_stages_for(text,text,text,text,text,text,text,text,boolean,text)'::regprocedure) ORDER BY 1",[TARGET_SIGNATURE])).rows).toEqual(before);
 // Re-apply after revert is clean.
 await f.db.exec(migration);
 expect((await f.db.query("SELECT to_regprocedure($1) IS NULL AS absent",[SIGNATURE])).rows).toEqual([{absent:false}]);
});
