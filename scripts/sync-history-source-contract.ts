/** Generated boundary artifact; upstream owns this contract. */
import{readFile,writeFile}from"node:fs/promises";import{createHash}from"node:crypto";
const SOURCE_COMMIT="89aaf380f8a2b3a26050d352492c6c342fa15484",SOURCE_SHA256="d1fc98e92a902c711d0000cbed82c8e05d414f7cf9b6ad251bb907f909480049";
const root=new URL("..",import.meta.url),raw=await readFile(new URL("src/session-ingest/vendor/history-custody-contract.source.txt",root),"utf8");
if(createHash("sha256").update(raw).digest("hex")!==SOURCE_SHA256)throw Error("history source contract raw authority drift");
const expected="// GENERATED from AgentWorkforce/relayhistory-cloud packages/relayhistory/src/lib/history-custody-contract.ts @ "+SOURCE_COMMIT+"\n// SHA256 "+SOURCE_SHA256+"; regenerate: bun scripts/sync-history-source-contract.ts\n"+raw.replace('"./current-custody-protocol.js"','"./custody-protocol.ts"');
const target=new URL("src/session-ingest/history-custody-contract.ts",root);let actual="";try{actual=await readFile(target,"utf8")}catch{}
if(process.argv.includes("--check")){if(actual!==expected)throw Error("history source generated contract drift")}else if(actual!==expected)await writeFile(target,expected);
console.log(JSON.stringify({sourceCommit:SOURCE_COMMIT,sourceSha256:SOURCE_SHA256,check:process.argv.includes("--check")}));
