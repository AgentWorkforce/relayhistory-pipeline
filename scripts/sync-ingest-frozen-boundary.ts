/** P1 frozen-source custody: register its functions in the ACL manifest and refresh the boundary wrapper. */
import {readFile,writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
const root=new URL("..",import.meta.url),file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261105010000_frozen_source_custody.sql"),"utf8");
const defs=new Map<string,boolean>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.ingest_\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const args=m[2]!.trim()?m[2]!.split(",").map(x=>x.trim().split(/\s+/).slice(1).join(" ")).join(","):"";
 defs.set(m[1]+"("+args+")",/SECURITY DEFINER/i.test(m[3]!));
}
const priv=["ingest_frozen_source_privacy(text,text,text)","ingest_frozen_source_policy()"].map(x=>"sessions."+x);
const runtime=["ingest_frozen_source_receipt(jsonb)","ingest_frozen_source_witness(jsonb)","ingest_source_control_begin(jsonb)","ingest_source_control_complete(jsonb)"].map(x=>"sessions."+x);
if(defs.size!==6||priv.some(k=>defs.get(k)!==false)||runtime.some(k=>defs.get(k)!==true))throw Error("unexpected frozen capability "+JSON.stringify([...defs]));
const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const name of priv)acl.functions[name]=["<owner>:EXECUTE"];
for(const name of runtime)acl.functions[name]=["<owner>:EXECUTE","bulls96_session_ingest:EXECUTE"];
acl.functions=Object.fromEntries(Object.entries(acl.functions).sort(([a],[b])=>a<b?-1:a>b?1:0));
let wrapper=await readFile(file("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql"),"utf8");
wrapper=wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,
 ()=>"v_manifest jsonb := $manifest$"+JSON.stringify(acl.functions)+"$manifest$::jsonb;");
const secdefBlock=wrapper.match(/v_secdef text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/);
if(!secdefBlock)throw Error("boundary SECDEF source missing");
const secdefs=[...secdefBlock[1]!.matchAll(/'([^']+)'/g)].map(m=>m[1]!).filter(k=>!defs.has(k));
for(const [sig,sd]of defs)if(sd)secdefs.push(sig);
wrapper=wrapper.replace(secdefBlock[0],"v_secdef text[] := ARRAY[\n"+[...new Set(secdefs)].sort().map(k=>"    '"+k+"'").join(",\n")+"\n  ]::text[];");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];if(!body)throw Error("boundary body missing");
const sha=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");
guards=guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/,"$1"+JSON.stringify(sha));
let verify=await readFile(file("supabase/bootstrap/session_ingest_verify.sql"),"utf8");
verify=verify.replace(/v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
 "v_allowed text[] := ARRAY[\n"+Object.entries(acl.functions).filter(([,v])=>(v as string[]).includes("bulls96_session_ingest:EXECUTE")).map(([k])=>k).sort().map(k=>" '"+k+"'").join(",\n")+"\n ]::text[];");
const outputs={"src/hoopdb/sessions-function-acl.json":JSON.stringify(acl,null,2)+"\n","supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql":wrapper,"src/hoopdb/migrate-guards.ts":guards,"supabase/bootstrap/session_ingest_verify.sql":verify};
let drift=false;for(const[path,out]of Object.entries(outputs)){if(await readFile(file(path),"utf8")===out)continue;if(process.argv.includes("--check")){console.error("frozen custody drift: "+path);drift=true}else await writeFile(file(path),out)}
if(drift)process.exitCode=1;else console.log(JSON.stringify({functions:defs.size,private:priv.length,runtime:runtime.length,boundarySHA:sha}));
