/** Derive ONLY620 functions from their SQL authority; retain all composed ACLs. */
import {readFile,writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
const root=new URL("..",import.meta.url);
const file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261104062000_canonical_history_lineage.sql"),"utf8");
const defs=new Map<string,{secdef:boolean}>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const types=m[2]!.trim()?m[2]!.split(",").map(p=>p.trim().replace(/\s+DEFAULT\s+[\s\S]*$/i,"").split(/\s+/).slice(1).join(" ").replace(/^timestamptz$/,"timestamp with time zone").replace(/^int$/,"integer")).join(","):"";
 defs.set(m[1]+"("+types+")",{secdef:/SECURITY DEFINER/i.test(m[3]!)});
}
const grants=new Map<string,string[]>();
for(const m of sql.matchAll(/GRANT EXECUTE ON FUNCTION([\s\S]*?)TO\s+(bulls96_session_ingest|service_role|session_directory_search_worker)\s*;/gi))
 for(const raw of m[1]!.replace(/\s+/g,"").split(/,(?=sessions\.)/)){
  const sig=raw.replace(/timestamptz/g,"timestamp with time zone");
  if(!defs.has(sig))throw Error("grant outside publication SQL authority: "+sig);
  grants.set(sig,[...(grants.get(sig)??[]),m[2]+":EXECUTE"]);
 }
if(defs.size<9||grants.size<5)throw Error("History function inventory incomplete");

const modelSQLFile=file("supabase/migrations/20261104063000_history_model_work.sql");
let modelSQL:string|undefined;
try{modelSQL=await readFile(modelSQLFile,"utf8")}catch(e){if((e as NodeJS.ErrnoException).code!=="ENOENT")throw e}
if(modelSQL)for(const m of modelSQL.matchAll(/GRANT EXECUTE ON FUNCTION\s+(sessions\.\w+\([^)]*\))\s+TO bulls96_session_model\s*;/gi)){
 const sig=m[1]!.replace(/\s+/g,"");
 if(defs.has(sig))grants.set(sig,[...(grants.get(sig)??[]),"bulls96_session_model:EXECUTE"]);
}

const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const key of defs.keys())if(key.startsWith("sessions.ingest_history_")||key.startsWith("sessions.session_history_")||key.startsWith("sessions.session_directory_history_"))delete acl.functions[key];
for(const key of defs.keys()){
 if((key.startsWith("sessions.ingest_history_")||key.startsWith("sessions.session_history_")||key.startsWith("sessions.session_directory_history_")))
  acl.functions[key]=["<owner>:EXECUTE",...(grants.get(key)??[])];
 else if(!acl.functions[key])throw Error("replacement function missing original ACL "+key);
}
acl.functions=Object.fromEntries(Object.entries(acl.functions).sort(([a],[b])=>a<b?-1:a>b?1:0));
let wrapper=await readFile(file("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql"),"utf8");
wrapper=wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,
 ()=>"v_manifest jsonb := $manifest$"+JSON.stringify(acl.functions)+"$manifest$::jsonb;");
const secdefBlock=wrapper.match(/v_secdef text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/);
if(!secdefBlock)throw Error("boundary SECDEF source missing");
const secdefs=[...secdefBlock[1]!.matchAll(/'([^']+)'/g)].map(m=>m[1]!).filter(k=>!defs.has(k));
for(const [sig,def]of defs)if(def.secdef)secdefs.push(sig);
wrapper=wrapper.replace(secdefBlock[0],"v_secdef text[] := ARRAY[\n"+[...new Set(secdefs)].sort().map(k=>"    '"+k+"'").join(",\n")+"\n  ]::text[];");
const runtimeNew=[...grants.entries()].filter(([,v])=>v.includes("service_role:EXECUTE")).map(([k])=>k);
const runtimeBlock=wrapper.match(/v_runtime_fns text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/);
if(!runtimeBlock)throw Error("boundary runtime manifest source missing");
const runtime=[...runtimeBlock[1]!.matchAll(/'([^']+)'/g)].map(m=>m[1]!).filter(k=>!k.startsWith("sessions.ingest_history_")&&!k.startsWith("sessions.session_history_")&&!k.startsWith("sessions.session_directory_history_"));
wrapper=wrapper.replace(runtimeBlock[0],"v_runtime_fns text[] := ARRAY[\n"+[...new Set([...runtime,...runtimeNew])].sort().map(k=>"    '"+k+"'").join(",\n")+"\n  ]::text[];");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];
if(!body)throw Error("boundary body missing");
const bodySHA=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");
const guardRuntime=guards.match(/export const RUNTIME_APPLICATION_FUNCTIONS = \[([\s\S]*?)\] as const;/);
if(!guardRuntime)throw Error("runtime application source missing");
const guardSigs=[...guardRuntime[1]!.matchAll(/"([^"]+)"/g)].map(m=>m[1]!).filter(k=>!k.startsWith("sessions.ingest_history_")&&!k.startsWith("sessions.session_history_")&&!k.startsWith("sessions.session_directory_history_"));
guards=guards.replace(guardRuntime[0],"export const RUNTIME_APPLICATION_FUNCTIONS = [\n"+[...new Set([...guardSigs,...runtimeNew])].sort().map(k=>"  "+JSON.stringify(k)).join(",\n")+",\n] as const;");
guards=guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/,"$1"+JSON.stringify(bodySHA));
let verify=await readFile(file("supabase/bootstrap/session_ingest_verify.sql"),"utf8");
verify=verify.replace(/v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
 "v_allowed text[] := ARRAY[\n"+Object.entries(acl.functions).filter(([,v])=>(v as string[]).includes("bulls96_session_ingest:EXECUTE")).map(([k])=>k).sort().map(k=>" '"+k+"'").join(",\n")+"\n ]::text[];");

let workerVerify=await readFile(file("supabase/bootstrap/session_directory_search_worker_verify.sql"),"utf8");
if(!/v_approved text\[\] := ARRAY\[[\s\S]*?\];/.test(workerVerify))throw Error("History worker verifier source missing");
workerVerify=workerVerify.replace(/v_approved text\[\] := ARRAY\[[\s\S]*?\];/,
 "v_approved text[] := ARRAY[\n"+Object.entries(acl.functions).filter(([,v])=>(v as string[]).includes("session_directory_search_worker:EXECUTE")).map(([k])=>k).sort().map(k=>" '"+k+"'").join(",\n")+"\n ];");
const prior=await readFile(file("supabase/migrations/20261104056000_session_replay_pages.sql"),"utf8");
const pointers=await readFile(file("supabase/migrations/20261102010000_session_directory_canonical_ids.sql"),"utf8");
const briefs=await readFile(file("supabase/migrations/20261102020000_session_brief_state.sql"),"utf8");
const large=await readFile(file("supabase/migrations/20261104057000_session_large_read_jobs.sql"),"utf8");
const ordinary=await readFile(file("supabase/migrations/20261104060000_ingest_canonical_publication.sql"),"utf8");
const restore=["session_replay_source_snapshot","publish_session_replay_pages","session_publication_is_managed","claim_session_large_read","capture_session_replay_pages","session_brief","session_directory_pointers","session_directory_replay_pages"].map(name=>{
 const pattern=new RegExp("CREATE(?: OR REPLACE)? FUNCTION sessions\\."+name+"\\([\\s\\S]*?AS\\s+\\$([a-z_]+)\\$[\\s\\S]*?\\$\\1\\$;","i");
 const match=(name==="session_publication_is_managed"?ordinary:name==="claim_session_large_read"?large:name==="session_brief"?briefs:name==="session_directory_pointers"?pointers:prior).match(pattern);if(!match)throw Error("History rollback original missing "+name);
 return match[0].replace(/^CREATE FUNCTION/,"CREATE OR REPLACE FUNCTION");
});
const ownSig=[...defs.keys()].filter(k=>k.startsWith("sessions.ingest_history_")||k.startsWith("sessions.session_history_")||k.startsWith("sessions.session_directory_history_")).reverse();
const ownTables=[...sql.matchAll(/CREATE TABLE\s+(sessions\.ingest_history_\w+)\(/g)].map(m=>m[1]!).reverse();
const ownTriggers=[...sql.matchAll(/CREATE TRIGGER\s+(\w+)[\s\S]*?\bON\s+(sessions\.\w+)/g)].map(m=>"DROP TRIGGER IF EXISTS "+m[1]+" ON "+m[2]+";");
const down=[
 "-- Generated from620 owned definitions + authoritative560 originals. Stop all620/630 workers first.",
 "-- Operational rollback retains installed authority/object ledgers; never forget late PUT obligations.",
 "DO $rollback_guard$ BEGIN",
 " IF EXISTS(SELECT 1 FROM sessions.ingest_history_plans)OR EXISTS(SELECT 1 FROM sessions.ingest_history_reconcile_outbox)",
 "  OR EXISTS(SELECT 1 FROM sessions.ingest_history_privacy_outbox)OR EXISTS(SELECT 1 FROM sessions.ingest_history_required_scopes)OR EXISTS(SELECT 1 FROM sessions.ingest_history_scope_versions)",
 "  OR EXISTS(SELECT 1 FROM sessions.ingest_history_derived_objects)OR to_regprocedure('sessions.session_history_model_claim(jsonb)')IS NOT NULL",
 " THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='history: retain authority/object ledgers; stop consumers for operational rollback';END IF;",
 "END $rollback_guard$;",...ownTriggers,...restore,
 "DROP FUNCTION IF EXISTS\n "+ownSig.join(",\n ")+";",
 "DROP TABLE IF EXISTS\n "+ownTables.join(",\n ")+";",
 "DROP SEQUENCE IF EXISTS sessions.ingest_history_inventory_epoch;",
 "ALTER TABLE sessions.session_large_read_jobs DROP COLUMN history_generation;",
].join("\n")+"\n";

const outputs={
 "supabase/rollbacks/20261104062000_canonical_history_lineage.down.sql":down,
 "src/hoopdb/sessions-function-acl.json":JSON.stringify(acl,null,2)+"\n",
 "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql":wrapper,
 "src/hoopdb/migrate-guards.ts":guards,
 "supabase/bootstrap/session_ingest_verify.sql":verify,
 "supabase/bootstrap/session_directory_search_worker_verify.sql":workerVerify,
};
const check=process.argv.includes("--check");
let drift=false;
for(const[path,expected]of Object.entries(outputs)){
 if(await readFile(file(path),"utf8")===expected)continue;
 if(check){console.error("history boundary drift: "+path);drift=true}else await writeFile(file(path),expected);
}
if(drift)process.exitCode=1;
else console.log(JSON.stringify({functions:defs.size,publicationRPCs:grants.size,boundarySHA:bodySHA,check}));
