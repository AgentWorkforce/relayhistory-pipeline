/** Derive private first-ACK seat capabilities from040530, preserving combined ACL. */
import {readFile,writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
const root=new URL("..",import.meta.url),file=(p:string)=>new URL(p,root);
const sql=await readFile(file("supabase/migrations/20261104053000_ingest_first_ack_seats.sql"),"utf8");
const defs=new Map<string,boolean>();
for(const m of sql.matchAll(/CREATE(?: OR REPLACE)? FUNCTION\s+(sessions\.ingest_receipt_\w+)\(([^)]*)\)([\s\S]*?)\s+AS\s+\$[a-z_]+\$/gi)){
 const args=m[2]!.trim()?m[2]!.split(",").map(x=>x.trim().split(/\s+/).slice(1).join(" ")).join(","):"";
 defs.set(m[1]+"("+args+")",/SECURITY DEFINER/i.test(m[3]!));
}
const expected=["sessions.ingest_receipt_seat_initialize(jsonb)","sessions.ingest_receipt_seat_records(uuid,jsonb)","sessions.ingest_receipt_seat_ack()","sessions.ingest_receipt_seats_for_stage(uuid)"];
if(defs.size!==expected.length||expected.some(k=>defs.get(k)!==false)||/GRANT\s+/i.test(sql))throw Error("unexpected seat capability");
const acl=JSON.parse(await readFile(file("src/hoopdb/sessions-function-acl.json"),"utf8"));
for(const name of expected)acl.functions[name]=["<owner>:EXECUTE"];
acl.functions=Object.fromEntries(Object.entries(acl.functions).sort(([a],[b])=>a<b?-1:a>b?1:0));
let wrapper=await readFile(file("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql"),"utf8");
wrapper=wrapper.replace(/v_manifest jsonb := \$manifest\$[\s\S]*?\$manifest\$::jsonb;/,
 ()=>"v_manifest jsonb := $manifest$"+JSON.stringify(acl.functions)+"$manifest$::jsonb;");
const body=wrapper.match(/v_body text := \$boundary\$([\s\S]*?)\$boundary\$;/)?.[1];if(!body)throw Error("boundary body missing");
const sha=createHash("sha256").update(body).digest("hex");
let guards=await readFile(file("src/hoopdb/migrate-guards.ts"),"utf8");
guards=guards.replace(/(export const SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256 =\s*)"[a-f0-9]{64}"/,"$1"+JSON.stringify(sha));
const outputs={"src/hoopdb/sessions-function-acl.json":JSON.stringify(acl,null,2)+"\n","supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql":wrapper,"src/hoopdb/migrate-guards.ts":guards};
let drift=false;for(const[path,out]of Object.entries(outputs)){if(await readFile(file(path),"utf8")===out)continue;if(process.argv.includes("--check")){console.error("receipt seat drift: "+path);drift=true}else await writeFile(file(path),out)}
if(drift)process.exitCode=1;else console.log(JSON.stringify({functions:defs.size,newGrants:0,boundarySHA:sha}));
