import type { ScrubEngine } from '../../src/lib/scrub-engine/engine.js';
import { bindEngineRawSealer } from '../../src/lib/pipeline/engine-raw-chunks.js';
import { createHash } from 'node:crypto';
import { initScrubEngine } from '../../src/lib/scrub-engine/index.js';
export function fixtureSealer(transform=(text:string)=>text.replaceAll('VISIBLE','SEALED')) {
 const state={calls:0,failed:false,seen:[] as Uint8Array[]};
 const marker=Object.freeze({ruleset:'relayhistory-scrub',version:10,profile:'fixture-v10'});
 const engine:ScrubEngine&{failed:boolean}={kind:'wasm-bindgen',rulesetVersion:10,scrubProfile:marker.profile,marker,
  get failed(){return state.failed;},scrubText:transform,scrubLong:transform,scrubJson:<T>(v:T)=>v,
  scrubRecords(source){state.calls++;state.seen.push(new Uint8Array(source));return {scrubbed:new TextEncoder().encode(transform(new TextDecoder().decode(source))),records:1,scrub:marker};}};
 return {state,engine,sealer:bindEngineRawSealer(engine,{retained:'same-family-up-to-writer'})};
}

/** Exercises the real verified adapter/active singleton with synthetic artifact
 * bytes and injected glue. This is not artifact or ruleset qualification. */
export async function verifiedFixtureEngine(records: () => {text:string;recordCount:number}) {
 const enc=new TextEncoder(),bytes={wasm:enc.encode('synthetic wasm'),glue:enc.encode('synthetic glue'),loader:enc.encode('synthetic loader')};
 const sha=(b:Uint8Array)=>createHash('sha256').update(b).digest('hex');
 const manifest={schema:3,ruleset:'relayhistory-scrub',rulesetVersion:10,rulesetSourceSha256:'a'.repeat(64),crateVersion:'0.0.0-test',abiVersion:2,policy:'secrets',redactionToken:'[REDACTED]',scrubProfile:'fixture-v10'};
 const pin={schema:2,package:'@agentworkforce/relayscrubber',package_version:manifest.crateVersion,release:{source:'github-release',repository:'AgentWorkforce/relayscrubber',tag:'v0.0.0-test',tarball:'test.tgz',tarball_sha256:'e'.repeat(64),sha256sums:{'artifact.json':'f'.repeat(64)}},wasm_sha256:sha(bytes.wasm),glue_sha256:sha(bytes.glue),loader_sha256:sha(bytes.loader),paths:{wasm:'test.wasm',glue:'test.js',loader:'index.js'},scrub_profile:manifest.scrubProfile,manifest};
 const engine=await initScrubEngine({env:{SCRUB_ENGINE:'wasm-bindgen',READER_MAX_RULESET:'10'},bindgen:{pin,readArtifact:async name=>bytes[name],importGlue:async()=>({default:async()=>({}),manifestJson:()=>JSON.stringify(manifest),scrubTextChecked:(s:string)=>s,scrubJson:(s:string)=>JSON.stringify({jsonText:s}),scrubRecords:()=>JSON.stringify(records())})}});
 return engine as ScrubEngine&{readonly failed:boolean};
}
