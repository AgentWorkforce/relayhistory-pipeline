/** Derive GC ACL/boundary from040510; ingest phase2 RPCs from the combined manifest. */
import {readFile,writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
const root=new URL("..",import.meta.url);
const file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261104051000_ingest_buffer_gc.sql"),"utf8");
const defs=new Map<string,{secdef:boolean}>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.ingest_buffer_gc_\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const types=m[2]!.trim()?m[2]!.split(",").map(p=>p.trim().split(/\s+/).slice(1).join(" ")).join(","):"";
 defs.set(m[1]+"("+types+")",{secdef:/SECURITY DEFINER/i.test(m[3]!)});
}
const bufferSql=await readFile(file("supabase/migrations/20261104050000_buffered_upload_receipts.sql"),"utf8");
const shared=bufferSql.match(/CREATE FUNCTION\s+(sessions\.ingest_require_generation)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/i);
if(!shared)throw Error("generation helper source missing");
defs.set(shared[1]+"("+shared[2]!.trim().split(/\s+/).slice(1).join(" ")+")",{secdef:/SECURITY DEFINER/i.test(shared[3]!)});
if(defs.size!==7)throw Error("GC SQL function inventory incomplete");
const grants=new Set<string>();
for(const m of sql.matchAll(/GRANT EXECUTE ON FUNCTION([\s\S]*?)TO\s+bulls96_session_ingest\s*;/gi))
 for(const sig of m[1]!.replace(/\s+/g,"").split(/,(?=sessions\.)/)){
  if(!defs.has(sig))throw Error("grant outside GC SQL authority");
  grants.add(sig);
 }
if(grants.size!==4)throw Error("unexpected GC RPC count");
const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const key of Object.keys(acl.functions))if(key.startsWith("sessions.ingest_buffer_gc_"))delete acl.functions[key];
for(const key of defs.keys())acl.functions[key]=["<owner>:EXECUTE",...(grants.has(key)?["bulls96_session_ingest:EXECUTE"]:[])];
acl.functions=Object.fromEntries(Object.entries(acl.functions).sort(([a],[b])=>a<b?-1:a>b?1:0));
let wrapper=await readFile(file("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql"),"utf8");
wrapper=wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,
 ()=>"v_manifest jsonb := $manifest$"+JSON.stringify(acl.functions)+"$manifest$::jsonb;");
const secdefBlock=wrapper.match(/v_secdef text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/);
if(!secdefBlock)throw Error("boundary SECDEF source missing");
const secdefs=[...secdefBlock[1]!.matchAll(/'([^']+)'/g)].map(m=>m[1]!).filter(k=>!k.startsWith("sessions.ingest_buffer_gc_"));
for(const [sig,def]of defs)if(def.secdef)secdefs.push(sig);
wrapper=wrapper.replace(secdefBlock[0],"v_secdef text[] := ARRAY[\n"+[...new Set(secdefs)].sort().map(k=>"    '"+k+"'").join(",\n")+"\n  ]::text[];");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];
if(!body)throw Error("boundary body missing");
const bodySHA=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");
guards=guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/,"$1"+JSON.stringify(bodySHA));
let verify=await readFile(file("supabase/bootstrap/session_ingest_verify.sql"),"utf8");
verify=verify.replace(/v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
 "v_allowed text[] := ARRAY[\n"+Object.entries(acl.functions).filter(([,v])=>(v as string[]).includes("bulls96_session_ingest:EXECUTE")).map(([k])=>k).sort().map(k=>" '"+k+"'").join(",\n")+"\n ]::text[];");
const outputs={
 "src/hoopdb/sessions-function-acl.json":JSON.stringify(acl,null,2)+"\n",
 "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql":wrapper,
 "src/hoopdb/migrate-guards.ts":guards,
 "supabase/bootstrap/session_ingest_verify.sql":verify,
};
const check=process.argv.includes("--check");
let drift=false;
for(const[path,expected]of Object.entries(outputs)){
 if(await readFile(file(path),"utf8")===expected)continue;
 if(check){console.error("GC boundary drift: "+path);drift=true}else await writeFile(file(path),expected);
}
if(drift)process.exitCode=1;
else console.log(JSON.stringify({functions:defs.size,gcRPCs:grants.size,boundarySHA:bodySHA,check}));
