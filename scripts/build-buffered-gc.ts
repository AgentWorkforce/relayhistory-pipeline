/** Build the deployable default-off GC artifact, including compiled schema assets. */
import {build,NODE_CONDITION_NAMES} from "alchemy/Bundle/Bundle";
import * as Effect from "effect/Effect";
import {mkdir,writeFile,readFile} from "node:fs/promises";
import {resolve,dirname} from "node:path";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
import {execFileSync} from "node:child_process";
import {derivedSchemaBuildPlugin} from "../stacks/aws/derived-schema-build.ts";
import {derivedSchemaAssets} from "../src/session-pipeline/derived-schema.ts";
const root=resolve(dirname(fileURLToPath(import.meta.url)),".."),out=process.argv[2];
if(!out||!resolve(out).startsWith("/home/khaliqgant/relay-briefs/"))throw Error("explicit_safe_artifact_directory_required");
const dirty=!!execFileSync("git",["status","--porcelain"],{cwd:root,encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))throw Error("clean_source_required");
await mkdir(resolve(out),{recursive:false,mode:0o700});
await mkdir(resolve(out,"bundle"));
const first=await derivedSchemaAssets();
await Effect.runPromise(build({input:resolve(root,"src/session-ingest/buffered-gc-lambda.ts"),cwd:root,platform:"node",tsconfig:resolve(root,"tsconfig.json"),resolve:{conditionNames:[...NODE_CONDITION_NAMES]},plugins:[derivedSchemaBuildPlugin()]},
 {format:"esm",file:resolve(out,"bundle/index.mjs"),codeSplitting:false,sourcemap:false}));
const final=await derivedSchemaAssets();if(JSON.stringify(first)!==JSON.stringify(final))throw Error("build_inputs_changed");
const bundle=await readFile(resolve(out,"bundle/index.mjs"));
const sha=(b:string|Uint8Array)=>createHash("sha256").update(b).digest("hex");
await writeFile(resolve(out,"bundle/package.json"),'{"type":"module"}\n');
execFileSync("zip",["-q","-X",resolve(out,"buffered-gc.zip"),"index.mjs","package.json"],{cwd:resolve(out,"bundle")});
await writeFile(resolve(out,"manifest.json"),JSON.stringify({version:1,dirty,templateSha256:sha(await readFile(resolve(root,"stacks/aws/buffered-gc.template.json"))),head:execFileSync("git",["rev-parse","HEAD"],{cwd:root,encoding:"utf8"}).trim(),generation:first.expected.generation,entry:"index.handler",node:"24",names:first.expected.names,verifierSha256:Object.fromEntries(Object.entries(first.verifiers).map(([k,v])=>[k,sha(v)])),bundleSha256:sha(bundle),zipSha256:sha(await readFile(resolve(out,"buffered-gc.zip"))),defaultEnabled:false},null,2)+"\n");
console.log(JSON.stringify({built:true,generation:first.expected.generation,bytes:bundle.byteLength}));
