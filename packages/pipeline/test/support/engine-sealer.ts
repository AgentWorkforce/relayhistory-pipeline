import type { ScrubEngine } from '../../src/lib/scrub-engine/engine.js';
import { bindEngineRawSealer } from '../../src/lib/pipeline/engine-raw-chunks.js';
export function fixtureSealer(transform=(text:string)=>text.replaceAll('VISIBLE','SEALED')) {
 const state={calls:0,failed:false,seen:[] as Uint8Array[]};
 const marker=Object.freeze({ruleset:'relayhistory-scrub',version:10,profile:'fixture-v10'});
 const engine:ScrubEngine&{failed:boolean}={kind:'wasm-bindgen',rulesetVersion:10,scrubProfile:marker.profile,marker,
  get failed(){return state.failed;},scrubText:transform,scrubLong:transform,scrubJson:<T>(v:T)=>v,
  scrubRecords(source){state.calls++;state.seen.push(new Uint8Array(source));return {scrubbed:new TextEncoder().encode(transform(new TextDecoder().decode(source))),records:1,scrub:marker};}};
 return {state,engine,sealer:bindEngineRawSealer(engine,{retained:'same-family-up-to-writer'})};
}
