/** Main-compatible default reads; explicit policy is independently enabled. */
import { test,expect } from "bun:test";
import { mkdtemp,rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { manifestKey } from "../../../packages/object-store/src/index.ts";
import { buildBriefInput } from "../input.ts";
import { briefConfig } from "../config.ts";
import { KEY,loc,mapReader,recordStream,transcript } from "./harness.ts";

for(const format of ["ndjson","jsonl"] as const) test(`${format} brief default still reads version9 and foreign families`,async()=>{
 const root=await mkdtemp(join(tmpdir(),"brief-reader-policy-"));const store=createFsStore({root});
 try{
  const records=["user","assistant"].map((role,n)=>({kind:"session_event",origin_id:"original",record_id:`r${n}`,revision:"1",operation:"upsert",received_at:"2026-10-30T12:00:00Z",payload:{ts_ms:Date.parse(`2026-10-29T10:00:0${n}Z`),role,text:`${role} canonical text`}}));
  const manifest=format==="ndjson"?await recordStream(store,records):await transcript(store,records.map(r=>({type:r.payload.role,message:{role:r.payload.role,content:r.payload.text}})));
  const key=manifestKey(loc(KEY));const deps={reader:mapReader(new Map([[key,manifest]])),store};
  const pointers=[{machineId:"m-1",manifestKey:key,revision:1}];
  const baseline=await buildBriefInput(deps,KEY,pointers);expect(baseline.kind).toBe("ready");
  expect(briefConfig({}).readerMaxRuleset).toBeUndefined();
  for(const marker of [{ruleset:"relayhistory-scrub",version:9},{ruleset:"foreign-family",version:8}]){
   manifest.raw.scrub=marker;
   expect(await buildBriefInput(deps,KEY,pointers)).toEqual(baseline);
   await expect(buildBriefInput({...deps,readerMaxRuleset:8},KEY,pointers)).rejects.toMatchObject({code:"scrub_ruleset_unsupported"});
  }
 }finally{await rm(root,{recursive:true,force:true});}
});
