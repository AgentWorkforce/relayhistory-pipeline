// The large-worker image ships only the bundle: no supabase/, no node_modules. Build it exactly
// as docker/large-read-worker.Dockerfile does and boot it from an empty directory.
import {expect,test} from 'bun:test';
import {mkdtempSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const root=resolve(import.meta.dir,'../../..');
const run=(cmd:string[],cwd:string,env:Record<string,string>)=>{const p=Bun.spawnSync(cmd,{cwd,env,stdout:'pipe',stderr:'pipe'});return {code:p.exitCode,out:p.stdout.toString(),err:p.stderr.toString()}};
test('Dockerfile builds with the derived-schema build script and the documented heap ceiling',()=>{
 const docker=readFileSync(join(root,'docker/large-read-worker.Dockerfile'),'utf8'),doc=readFileSync(join(root,'docs/canonical-large-worker-runtime.md'),'utf8');
 expect(docker).toContain('RUN bun scripts/build-large-read-worker.ts /out/large-read-main.mjs');
 expect(docker).not.toMatch(/RUN bun build /);
 const heap=docker.match(/CMD \["node", "--max-old-space-size=(\d+)", "\/app\/large-read-main\.mjs"\]/)?.[1];
 expect(heap).toBeDefined();expect(doc).toContain(`node --max-old-space-size=${heap} /app/large-read-main.mjs`);
 // supabase/ is a build-stage input only; the runtime stage copies just the bundle.
 const copies=docker.slice(docker.lastIndexOf('\nFROM ')).split('\n').filter(l=>l.startsWith('COPY'));
 expect(copies).toEqual(['COPY --from=build /out/large-read-main.mjs ./large-read-main.mjs']);
});
test('built bundle embeds schema assets and boots with no supabase/ tree',()=>{
 const dir=mkdtempSync(join(tmpdir(),'large-read-bundle-'));
 try{
  const out=join(dir,'large-read-main.mjs'),PATH=process.env.PATH??'';
  const built=run([process.execPath,join(root,'scripts/build-large-read-worker.ts'),out],root,{PATH});
  expect(built.code).toBe(0);
  const bundle=readFileSync(out,'utf8');
  expect(bundle).not.toMatch(/\.\.\/supabase\//); // no module-relative path into supabase/bootstrap or migrations
  expect(bundle).not.toContain('//#region src/shared/db.ts');
  expect(bundle).not.toMatch(/^import [^;]* from "(?!node:)[^"]+";?$/m); // self-contained: no bare package imports
  const off=run(['node',out],dir,{PATH});
  expect(off.err).toBe('');expect(off.code).toBe(0);
  expect(JSON.parse(off.out)).toMatchObject({message:'large canonical worker',enabled:false,processed:0});
  // Default flip (docs/scrub-engine.md): no TypeScript grant in this image and no relayscrubber
  // artifacts in the bundle, so an enabled run fails closed at initScrubEngine, before the runtime
  // factory (which would fail as worker_error on the missing env) and before any connection.
  const wasm=readFileSync(join(root,'packages/pipeline/scrub-bindgen/generated/relayscrubber_bg.wasm'));
  const pin=JSON.parse(readFileSync(join(root,'packages/pipeline/scrub-bindgen-pin.json'),'utf8'));
  expect(bundle).not.toContain(wasm.subarray(0,48).toString('base64'));
  expect(bundle).not.toContain(wasm.subarray(1024,1072).toString('base64'));
  expect(pin.wasm_sha256).toMatch(/^[0-9a-f]{64}$/);
  expect(bundle).not.toContain(pin.wasm_sha256); // nor the pin
  // lambda.ts (imported for helpers) declares its grant at handler entry only, which this entry
  // never calls: a load-time grant would turn the run below into TypeScript and fail as worker_error.
  const on=run(['node',out],dir,{PATH,SESSION_LARGE_READ_WORKER_ENABLED:'1'});
  expect(on.code).toBe(1);expect(on.out).toBe('');
  expect(on.err).not.toContain('ENOENT');
  expect(JSON.parse(on.err)).toEqual({message:'large canonical worker failed',code:'scrub_engine_unavailable'});
  // SCRUB_ENGINE=ts is refused here too (no grant), never TypeScript.
  const ts=run(['node',out],dir,{PATH,SESSION_LARGE_READ_WORKER_ENABLED:'1',SCRUB_ENGINE:'ts'});
  expect(ts.code).toBe(1);expect(ts.out).toBe('');
  expect(JSON.parse(ts.err)).toEqual({message:'large canonical worker failed',code:'scrub_engine_invalid'});
 }finally{rmSync(dir,{recursive:true,force:true})}
},60000);
