/** Offline real-router bundle. Only DB connection construction is replaced.
 * All loaded RHC business source is checked against the explicit git head.
 */
import {createRequire} from "node:module";
import {resolve,relative,extname} from "node:path";
import {readFile,mkdir,writeFile} from "node:fs/promises";
import {execFileSync} from "node:child_process";
import {createHash} from "node:crypto";
const rhc=process.argv[2],head=process.argv[3],out=process.argv[4],wip=process.argv.includes("--allow-wip");
if(!rhc||!head||!/^[a-f0-9]{40}$/.test(head)||!out||!resolve(out).startsWith("/home/khaliqgant/relay-briefs/ordered-router-fixture-"))throw Error("explicit_owned_router_source_required");
const require=createRequire(resolve(rhc,"package.json")),esbuild=require("esbuild");
const hashes:Record<string,string>={},dirty:string[]=[],sha=(b:Uint8Array|string)=>createHash("sha256").update(b).digest("hex");
await mkdir(out,{recursive:false,mode:0o700});
await esbuild.build({stdin:{contents:`
import {Hono} from "hono";
import {PgDialect} from "drizzle-orm/pg-core";
import {deliveryRoutes} from "./packages/relayhistory/src/routes/delivery.ts";
export {CURRENT_CUSTODY_AUTHORITY_SQL,readCurrentCustodyWitness} from "./packages/relayhistory/src/lib/delivery-custody-witness.ts";
export {prepareDelivery} from "./packages/relayhistory/src/lib/delivery.ts";
export function createOwnedRouter(execute,auth){
 const dialect=new PgDialect();
 globalThis[Symbol.for("ordered-owned-source-db")]={execute(q){const p=dialect.sqlToQuery(q);return execute(p.sql,p.params)}};
 const app=new Hono();app.use("*",async(c,next)=>{c.set("auth",auth);c.set("correlationId","owned-ordered-proof");await next()});
 app.route("/v1",deliveryRoutes);return app;
}`,resolveDir:rhc,sourcefile:"owned-router-entry.ts",loader:"ts"},
 bundle:true,platform:"node",target:"node24",format:"esm",outfile:resolve(out,"router.mjs"),logLevel:"silent",plugins:[{
 name:"owned-source-connection-only",
 setup(build:any){
  build.onResolve({filter:/\/db\/client\.js$/},()=>({path:"owned-source-client",namespace:"owned-source"}));
  build.onLoad({filter:/.*/,namespace:"owned-source"},()=>({contents:'export function createDb(url){if(url!=="postgres://fixture.original.invalid/owned-source")throw Error("nonfixture_database");const d=globalThis[Symbol.for("ordered-owned-source-db")];if(!d)throw Error("fixture_driver_missing");return d}',loader:"js"}));
  build.onLoad({filter:/\.(?:[cm]?js|tsx?|json)$/},async(args:any)=>{
   if(args.path.includes("/node_modules/"))return;
   const rel=relative(rhc,args.path);if(rel.startsWith(".."))throw Error("source_outside_worktree");
   const bytes=await readFile(args.path);let expected:Buffer;try{expected=execFileSync("git",["-C",rhc,"show",head+":"+rel],{maxBuffer:8*1024*1024})}catch{throw Error("untracked_source:"+rel)}
   if(!expected.equals(bytes)){dirty.push(rel);if(!wip)throw Error("source_blob_drift:"+rel)}
   hashes[rel]=sha(bytes);return{contents:bytes.toString(),loader:extname(rel)===".json"?"json":/\.tsx?$/.test(rel)?"ts":"js"};
  });
 }
 }]});
await writeFile(resolve(out,"manifest.json"),JSON.stringify({version:1,head,dirty,businessSourceSha256:hashes,bundleSha256:sha(await readFile(resolve(out,"router.mjs"))),dbShim:"connection constructor only; literal owned-source; real PG SQL driver required"},null,2)+"\n");
console.log(JSON.stringify({head,sourceFiles:Object.keys(hashes).length,dirty}));
