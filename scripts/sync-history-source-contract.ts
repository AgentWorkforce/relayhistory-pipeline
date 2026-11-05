/** Generated Node boundary; RHC owns SQL, role facts, validators and DTOs. */
import{readFile,writeFile}from"node:fs/promises";import{createHash}from"node:crypto";
const root=new URL("..",import.meta.url),file=(p:string)=>new URL(p,root);
const MANIFEST_SHA256="7f87bb63783720816f99472e8288e6cfec0ca04c05db2f28c8576557b1c91c7e";
const manifestRaw=await readFile(file("src/session-ingest/vendor/history-custody-source.json"),"utf8");
if(createHash("sha256").update(manifestRaw).digest("hex")!==MANIFEST_SHA256)throw Error("history source authority manifest drift");
const manifest=JSON.parse(manifestRaw) as {version:number;sourceRepo:string;sourceCommit:string;files:{module:string;sourcePath:string;sha256:string}[]};
const expected=["history-custody-contract","history-custody-node","history-custody-authority-sql","delivery-source-authority-sql","delivery-source-role-facts"];
if(manifest.version!==1||manifest.sourceRepo!=="AgentWorkforce/relayhistory-cloud"||!/^[a-f0-9]{40}$/.test(manifest.sourceCommit)||JSON.stringify(manifest.files.map(f=>f.module))!==JSON.stringify(expected))throw Error("history source authority inventory");
const check=process.argv.includes("--check");let drift=false;
for(const source of manifest.files){
 if(source.sourcePath!=="packages/relayhistory/src/lib/"+source.module+".ts"||!/^[a-f0-9]{64}$/.test(source.sha256))throw Error("history source module authority");
 const raw=await readFile(file("src/session-ingest/vendor/"+source.module+".source.txt"),"utf8");
 if(createHash("sha256").update(raw).digest("hex")!==source.sha256)throw Error("history source raw authority drift: "+source.module);
 let code=raw.replace('"./current-custody-protocol.js"','"./custody-protocol.ts"');
 for(const name of expected)code=code.replaceAll('"./'+name+'.js"','"./'+name+'.ts"');
 const output="// GENERATED from "+manifest.sourceRepo+" "+source.sourcePath+" @ "+manifest.sourceCommit+"\n// SHA256 "+source.sha256+"; regenerate: bun scripts/sync-history-source-contract.ts\n"+code;
 const target=file("src/session-ingest/"+source.module+".ts");let actual="";try{actual=await readFile(target,"utf8")}catch(e){if((e as NodeJS.ErrnoException).code!=="ENOENT")throw e}
 if(actual!==output){if(check){console.error("history generated source drift: "+source.module);drift=true}else await writeFile(target,output)}
}
if(drift)process.exitCode=1;else console.log(JSON.stringify({sourceCommit:manifest.sourceCommit,modules:expected.length,manifestSha256:MANIFEST_SHA256,check}));
