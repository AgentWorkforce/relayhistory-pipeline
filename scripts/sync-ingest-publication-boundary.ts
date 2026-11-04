/** Derive publisher ACL/boundary from040600; ingest phase2 RPCs from combined manifest. */
import {readFile,writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
const root=new URL("..",import.meta.url);
const file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261104060000_ingest_canonical_publication.sql"),"utf8");
const defs=new Map<string,{secdef:boolean}>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const types=m[2]!.trim()?m[2]!.split(",").map(p=>p.trim().split(/\s+/).slice(1).join(" ").replace(/^timestamptz$/,"timestamp with time zone").replace(/^int$/,"integer")).join(","):"";
 defs.set(m[1]+"("+types+")",{secdef:/SECURITY DEFINER/i.test(m[3]!)});
}
const grants=new Map<string,string[]>();
for(const m of sql.matchAll(/GRANT EXECUTE ON FUNCTION([\s\S]*?)TO\s+(bulls96_session_ingest|service_role)\s*;/gi))
 for(const raw of m[1]!.replace(/\s+/g,"").split(/,(?=sessions\.)/)){
  const sig=raw.replace(/timestamptz/g,"timestamp with time zone");
  if(!defs.has(sig))throw Error("grant outside publication SQL authority: "+sig);
  grants.set(sig,[...(grants.get(sig)??[]),m[2]+":EXECUTE"]);
 }
if(defs.size<20||grants.size<12)throw Error("publication function inventory incomplete");
const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const key of Object.keys(acl.functions))if(key.startsWith("sessions.ingest_publication_")||key.startsWith("sessions.session_publication_")||key.startsWith("sessions.set_session_state_for_publication("))delete acl.functions[key];
for(const key of defs.keys()){
 if(key.startsWith("sessions.ingest_publication_")||key.startsWith("sessions.session_publication_")||key.startsWith("sessions.set_session_state_for_publication("))
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
const runtimeNew=["sessions.session_publication_probe(jsonb)","sessions.session_publication_lock(jsonb)","sessions.session_publication_is_managed(text,text,text,text,text)","sessions.session_publication_register_object(jsonb)","sessions.set_session_state_for_publication(text,text,text,text,text,text,bigint,text,timestamp with time zone,text)"];
const runtimeBlock=wrapper.match(/v_runtime_fns text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/);
if(!runtimeBlock)throw Error("boundary runtime manifest source missing");
const runtime=[...runtimeBlock[1]!.matchAll(/'([^']+)'/g)].map(m=>m[1]!);
wrapper=wrapper.replace(runtimeBlock[0],"v_runtime_fns text[] := ARRAY[\n"+[...new Set([...runtime,...runtimeNew])].sort().map(k=>"    '"+k+"'").join(",\n")+"\n  ]::text[];");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];
if(!body)throw Error("boundary body missing");
const bodySHA=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");
const guardRuntime=guards.match(/export const RUNTIME_APPLICATION_FUNCTIONS = \[([\s\S]*?)\] as const;/);
if(!guardRuntime)throw Error("runtime application source missing");
const guardSigs=[...guardRuntime[1]!.matchAll(/"([^"]+)"/g)].map(m=>m[1]!);
guards=guards.replace(guardRuntime[0],"export const RUNTIME_APPLICATION_FUNCTIONS = [\n"+[...new Set([...guardSigs,...runtimeNew])].sort().map(k=>"  "+JSON.stringify(k)).join(",\n")+",\n] as const;");
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
 if(check){console.error("publication boundary drift: "+path);drift=true}else await writeFile(file(path),expected);
}
if(drift)process.exitCode=1;
else console.log(JSON.stringify({functions:defs.size,publicationRPCs:grants.size,boundarySHA:bodySHA,check}));
