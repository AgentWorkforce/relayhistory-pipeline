/** Generate an offline oracle from exact RHC migration source, without credentials.
 * Imports the reviewed authoritative migration generator in a clean child env;
 * no Neon client is constructed or queried. No checked-in absolute import.
 */
import {readFile,mkdir,writeFile} from "node:fs/promises";
import {resolve} from "node:path";
import {createHash} from "node:crypto";
import {execFileSync} from "node:child_process";
const sourceHead="9f57617874a056b7a82d2324cb3c024b8d9e04be";
const rhc=process.argv[2],out=process.argv[3];
if(!rhc||!out||!resolve(out).startsWith("/home/khaliqgant/relay-briefs/ordered-control-neon-source-"))throw Error("explicit_source_and_fixture_required");
const prefix="packages/relayhistory/",helper=prefix+"scripts/apply-neon-migration.mjs",split=prefix+"scripts/split-sql-statements.mjs";
const blob=(path:string)=>execFileSync("git",["-C",rhc,"show",sourceHead+":"+path]);
for(const path of [helper,split])if(!blob(path).equals(await readFile(resolve(rhc,path))))throw Error("generator_blob_drift");
await mkdir(out,{recursive:false,mode:0o700});await mkdir(resolve(out,"migrations"));
const names=execFileSync("git",["-C",rhc,"ls-tree","--name-only",sourceHead+":"+prefix+"db/migrations"],{encoding:"utf8"}).trim().split("\n").filter(x=>x.endsWith(".sql"));
for(const name of names)await writeFile(resolve(out,"migrations",name),blob(prefix+"db/migrations/"+name));
const child=`import {readMigrations,migrationStatements} from ${JSON.stringify("file://"+resolve(rhc,helper))};
const all=readMigrations(${JSON.stringify(resolve(out,"migrations"))});
console.log(JSON.stringify({baselineStatements:migrationStatements(all.filter(x=>!x.name.startsWith("0077_")),"ordered_neon_runtime"),finalGrantStatement:migrationStatements(all,"ordered_neon_runtime").find(x=>x.startsWith("DO $runtime_grants$"))}));`;
const generated=JSON.parse(execFileSync(process.execPath,["--input-type=module","-e",child],{cwd:rhc,env:{PATH:process.env.PATH??""},encoding:"utf8",maxBuffer:16*1024*1024}));
const sha=(x:Uint8Array|string)=>createHash("sha256").update(x).digest("hex");
const source0077=blob(prefix+"db/migrations/0077_delivery_cutover_guard.sql").toString();
await writeFile(resolve(out,"fixture.json"),JSON.stringify({version:1,sourceHead,generatorSha256:sha(blob(helper)),splitterSha256:sha(blob(split)),source0077Sha256:sha(source0077),source0077,...generated},null,2)+"\n");
console.log(JSON.stringify({sourceHead,files:names.length,baselineStatements:generated.baselineStatements.length,source0077Sha256:sha(source0077)}));
