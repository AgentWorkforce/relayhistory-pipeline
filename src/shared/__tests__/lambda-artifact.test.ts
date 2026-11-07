import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { readFile, rm, mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { randomUUID, createHash } from "node:crypto";

const root = fileURLToPath(new URL("../../..", import.meta.url));
test("installed Node Lambda archives have deterministic identities, embedded assets and working envelopes without the source checkout", async () => {
  const sha = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  const node = process.env.LAMBDA_PROOF_NODE || "node";
  const version = execFileSync(node, ["--version"], { encoding: "utf8" }).trim();
  expect(Number(version.slice(1).split(".")[0])).toBeGreaterThanOrEqual(24);
  const output = join(root, "dist", `lambda-unit-${randomUUID()}`);
  const environment = { PATH: process.env.PATH!, ...(process.env.SYSTEMROOT ? { SYSTEMROOT: process.env.SYSTEMROOT } : {}) };
  const build = (service: string) => execFileSync(process.execPath, ["scripts/build-lambda-artifact.ts", "--service", service, "--source-sha", sha, "--allow-dirty-test", "--out", join(output, service)], { cwd: root, env: environment, stdio: "pipe" });
  try {
    for (const service of ["pipeline", "read-api"]) {
      build(service);
      const receipt = JSON.parse(await readFile(join(output, service, "receipt.json"), "utf8"));
      expect(receipt.sourceStatus).toContain("test");
      expect(receipt.runtime).toBe("nodejs24.x"); expect(receipt.handler).toBe("index.handler");
      expect(receipt.provider.externalProviderBundled).toBe(false);
      const archive = await readFile(join(output, service, `${service}.zip`));
      expect(createHash("sha256").update(archive).digest("hex")).toBe(receipt.archive.sha256);
      build(service);
      expect(createHash("sha256").update(await readFile(join(output, service, `${service}.zip`))).digest("hex")).toBe(receipt.archive.sha256);
      const installed = join(output, `${service}-installed`);
      await mkdir(installed);
      execFileSync("python3", ["-c", "import sys,zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])", join(output, service, `${service}.zip`), installed]);
      // The actual module must import and expose the embedded generation even
      // when its copied SQL asset directory is absent at runtime.
      await rm(join(installed, "assets"), { recursive: true });
      const script = `import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const m=await import(pathToFileURL(process.argv[1]).href);
assert.equal(typeof m.handler,'function');
const schema=await m.derivedSchemaAssets();
assert.equal(schema.expected.generation,process.argv[2]);
assert.ok(schema.expected.names.length>0);assert.ok(schema.verifiers.searchWorker.length>0);
if(process.argv[3]==='pipeline'){
 assert.deepEqual(await m.handler({brief:true}),{ok:true});
 const handler=m.createPipelineLambdaHandler({loadRuntime:async()=>{throw Error('synthetic unavailable')}});
 assert.deepEqual(await handler({Records:[{messageId:'a',body:'invalid'},{messageId:'b',body:'invalid'}]}),{batchItemFailures:[{itemIdentifier:'a'},{itemIdentifier:'b'}]});
}else{
 const event={rawPath:'/health',requestContext:{http:{method:'GET'}}};
 const result=await m.handleUrlEvent({fetch:async req=>new Response(req.method,{status:200})},event);
 assert.equal(result.statusCode,200);assert.equal(result.body,'GET');
 assert.equal((await m.handleUrlEvent({fetch:()=>{throw Error('must not call')}},{...event,requestContext:{http:{method:'POST'}}})).statusCode,405);
 assert.equal((await m.handler(event)).statusCode,503);
}
console.log(JSON.stringify({node:process.version,platform:process.platform,arch:process.arch,service:process.argv[3],embeddedSchema:schema.expected.generation,ok:true}));`;
      const result = execFileSync(node, ["--input-type=module", "-e", script, join(installed, "index.mjs"), receipt.schema.generation, service], { cwd: installed, env: environment, encoding: "utf8", timeout: 15_000 });
      expect(result).toContain('"ok":true');
      expect(await readFile(join(installed, "LICENSE"), "utf8")).toContain("Apache");
      expect(await readFile(join(installed, "NOTICE"), "utf8")).toContain("MIT License. Copyright (c) 2026 Khaliq Gant.");
    }
  } finally { await rm(output, { recursive: true, force: true }); }
}, 90_000);
