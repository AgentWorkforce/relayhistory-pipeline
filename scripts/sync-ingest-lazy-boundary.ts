/** Derive private550 capabilities; preserve all existing public RPC ACLs. */
import {readFile,writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
const root=new URL("..",import.meta.url),file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261104055000_lazy_current_custody.sql"),"utf8");
const defs=new Map<string,boolean>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.ingest_\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const args=m[2]!.trim()?m[2]!.split(",").map(x=>x.trim().split(/\s+/).slice(1).join(" ")).join(","):"";
 defs.set(m[1]+"("+args+")",/SECURITY DEFINER/i.test(m[3]!));
}
const priv=["ingest_lazy_policy_set(jsonb)","ingest_lazy_user(jsonb,text,jsonb)","ingest_lazy_begin(jsonb,text)","ingest_lazy_records(jsonb)",
 "ingest_lazy_guard(text)","ingest_lazy_ack(jsonb,text)","ingest_lazy_attempt(uuid)","ingest_lazy_attempt_ack(uuid)",
 "ingest_lazy_ack_batch(jsonb)","ingest_lazy_stage_seats(uuid)","ingest_lazy_clear(jsonb,text)","ingest_buffer_gc_ordinals(uuid)","ingest_buffer_gc_groups(jsonb,jsonb)"].map(x=>"sessions."+x);
const publicSd=["ingest_claim_buffered_batch","ingest_prepare_buffer_stage","ingest_commit_buffered_batch","ingest_set_session_uploads","ingest_reset_uploaded_sessions","ingest_buffer_gc_claim","ingest_buffer_gc_prepare","ingest_buffer_gc_swap"].map(x=>"sessions."+x+"(jsonb)");
if(priv.some(k=>defs.get(k)!==false)||defs.size!==28||[...defs].some(([k,sd])=>sd!==publicSd.includes(k))||/\bGRANT\s+/i.test(sql))throw Error("unexpected lazy capability");
const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const name of priv)acl.functions[name]=["<owner>:EXECUTE"];
for(const name of defs.keys())if(!acl.functions[name])throw Error("unclassified function "+name);
acl.functions=Object.fromEntries(Object.entries(acl.functions).sort(([a],[b])=>a<b?-1:a>b?1:0));
let wrapper=await readFile(file("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql"),"utf8");
wrapper=wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,
 ()=>"v_manifest jsonb := $manifest$"+JSON.stringify(acl.functions)+"$manifest$::jsonb;");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];if(!body)throw Error("boundary body missing");
const sha=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");
guards=guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/,"$1"+JSON.stringify(sha));
const outputs={"src/hoopdb/sessions-function-acl.json":JSON.stringify(acl,null,2)+"\n","supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql":wrapper,"src/hoopdb/migrate-guards.ts":guards};
let drift=false;for(const[path,out]of Object.entries(outputs)){if(await readFile(file(path),"utf8")===out)continue;if(process.argv.includes("--check")){console.error("lazy custody drift: "+path);drift=true}else await writeFile(file(path),out)}
if(drift)process.exitCode=1;else console.log(JSON.stringify({functions:defs.size,newPrivate:priv.length,newRuntimeGrants:0,boundarySHA:sha}));
