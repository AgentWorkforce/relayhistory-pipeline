/**630 capabilities derive from actual SQL+the generated literal role grants.
 *620's generator independently derives its own3 added grants from same SQL. */
import{readFile,writeFile}from"node:fs/promises";import{createHash}from"node:crypto";
import{MODEL_WORKER_ROLE,MODEL_WORKER_FUNCTIONS}from"../src/shared/model-worker-contract.ts";
const root=new URL("../",import.meta.url),file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261104063000_history_model_work.sql"),"utf8");
const defs=new Map<string,boolean>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const args=m[2]!.trim()?m[2]!.split(",").map(x=>x.trim().split(/\s+/).slice(1).join(" ")).join(","):"";
 defs.set(m[1]+"("+args+")",/SECURITY DEFINER/i.test(m[3]!));
}
const grants=[...sql.matchAll(new RegExp("GRANT EXECUTE ON FUNCTION (sessions\\.[a-z_]+\\(jsonb\\)) TO "+MODEL_WORKER_ROLE+";","g"))].map(m=>m[1]!).sort();
if(JSON.stringify(grants)!==JSON.stringify([...MODEL_WORKER_FUNCTIONS].sort())||defs.size!==9||[...defs].some(([f,sd])=>sd!==MODEL_WORKER_FUNCTIONS.includes(f as any)))throw Error("model capability shape drift");
const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const[f,sd]of defs)acl.functions[f]=["<owner>:EXECUTE",...(sd?[MODEL_WORKER_ROLE+":EXECUTE"]:[])];
for(const f of MODEL_WORKER_FUNCTIONS){if(!acl.functions[f])throw Error("model source helper not classified");acl.functions[f]=[...new Set([...acl.functions[f],MODEL_WORKER_ROLE+":EXECUTE"])].sort();}
acl.functions=Object.fromEntries(Object.entries(acl.functions).sort(([a],[b])=>a<b?-1:a>b?1:0));
let wrapper=await readFile(file("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql"),"utf8");
wrapper=wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,()=>"v_manifest jsonb := $manifest$"+JSON.stringify(acl.functions)+"$manifest$::jsonb;");
const block=wrapper.match(/v_secdef text\[\] := ARRAY\[([\s\S]*?)\]::text\[\];/);
if(!block)throw Error("SECDEF inventory missing");
const secdefs=[...block[1]!.matchAll(/'([^']+)'/g)].map(m=>m[1]!).filter(k=>!defs.has(k));
for(const[f,sd]of defs)if(sd)secdefs.push(f);
wrapper=wrapper.replace(block[0],"v_secdef text[] := ARRAY[\n"+[...new Set(secdefs)].sort().map(f=>"    '"+f+"'").join(",\n")+"\n  ]::text[];");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];if(!body)throw Error("boundary missing");
const sha=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");guards=guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/,"$1"+JSON.stringify(sha));
const outputs={"src/hoopdb/sessions-function-acl.json":JSON.stringify(acl,null,2)+"\n","supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql":wrapper,"src/hoopdb/migrate-guards.ts":guards};
let drift=false;for(const[p,out]of Object.entries(outputs)){if(await readFile(file(p),"utf8")===out)continue;if(process.argv.includes("--check")){console.error("model boundary drift: "+p);drift=true}else await writeFile(file(p),out)}
if(drift)process.exitCode=1;else console.log(JSON.stringify({functions:defs.size,role:MODEL_WORKER_ROLE,capabilities:grants.length,boundarySHA:sha}));
