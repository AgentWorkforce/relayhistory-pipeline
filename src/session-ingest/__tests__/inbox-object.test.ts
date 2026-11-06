import {expect,test} from "bun:test";
import {objectKey, type ObjectStore} from "../../shared/vendor.ts";
import {scratchStore} from "./support.ts";
import {inboxObjectKey,writeInboxObject,type InboxObjectInput} from "../inbox-object.ts";
const input = ():InboxObjectInput => ({tenant:{orgId:"org",workspaceId:"ws",userId:"user"},originId:"origin",batchId:"batch",batchDigest:"a".repeat(64),payload:new TextEncoder().encode("scrubbed synthetic bytes"),receipt:new TextEncoder().encode('{"receiptId":"synthetic"}')});

test("payload and exact receipt persist together without any SQL or queue dependency",async()=>{
 const f=scratchStore();try{
  const i=input(), r=await writeInboxObject(f.store,i);
  expect(r.replay).toBe(false);expect(r.receipt).toEqual(i.receipt);
  const o=await f.store.getObject(objectKey(r.key));
  const bytes=new Uint8Array(await new Response(o.body).arrayBuffer());
  const text=new TextDecoder().decode(bytes);expect(text).toContain('"receiptSha256"');
  expect(text.endsWith('{"receiptId":"synthetic"}scrubbed synthetic bytes')).toBe(true);
  const replay=await writeInboxObject(f.store,input());expect(replay.replay).toBe(true);expect(replay.receipt).toEqual(r.receipt);
 }finally{f.done()}
});

test("ambiguous successful PUT does not fabricate success, retry recovers the persisted receipt",async()=>{
 const f=scratchStore();try{
  const proxy=new Proxy(f.store,{get(t,p){if(p==="put")return async(...a:Parameters<ObjectStore["put"]>)=>{await t.put(...a);throw Error("response_lost")};const v=Reflect.get(t,p);return typeof v==="function"?v.bind(t):v}});
  await expect(writeInboxObject(proxy,input())).rejects.toThrow("response_lost");
  const r=await writeInboxObject(f.store,input());expect(r.replay).toBe(true);expect(r.receipt).toEqual(input().receipt);
 }finally{f.done()}
});

test("concurrent retries converge, changed content or receipt cannot overwrite the accepted batch",async()=>{
 const f=scratchStore();try{
  const results=await Promise.all(Array.from({length:12},()=>writeInboxObject(f.store,input())));
  expect(new Set(results.map(x=>x.key)).size).toBe(1);
  expect(results.filter(x=>!x.replay).length).toBe(1);
  for(const changed of [{payload:new Uint8Array([1])},{receipt:new Uint8Array([2])},{batchDigest:"b".repeat(64)}])
   await expect(writeInboxObject(f.store,{...input(),...changed})).rejects.toThrow();
  expect((await writeInboxObject(f.store,input())).replay).toBe(true);
 }finally{f.done()}
});

test("tenant, origin and batch keys are isolated and property order does not change replay",async()=>{
 const f=scratchStore();try{
  const i=input();await writeInboxObject(f.store,i);
  expect((await writeInboxObject(f.store,{...input(),tenant:{userId:"user",workspaceId:"ws",orgId:"org"}})).replay).toBe(true);
  for(const d of [{tenant:{...i.tenant,userId:"other"}},{tenant:{...i.tenant,workspaceId:"other"}},{tenant:{...i.tenant,orgId:"other"}},{originId:"other"},{batchId:"other"}])expect(inboxObjectKey({...i,...d})).not.toBe(inboxObjectKey(i));
 }finally{f.done()}
});

test("oversized and malformed input never reaches object storage",async()=>{
 let calls=0;const store=new Proxy({} as ObjectStore,{get(){calls++;throw Error("storage_unused")}});
 for(const i of [{...input(),payload:new Uint8Array(4*1024*1024)},{...input(),payload:new Uint8Array()},{...input(),batchDigest:"bad"},{...input(),originId:"\ud800"},{...input(),tenant:{...input().tenant,userId:"\u0000"}}])await expect(writeInboxObject(store,i)).rejects.toThrow();
 expect(calls).toBe(0);
});

test("corrupted stored bytes are not accepted as a replay",async()=>{
 const f=scratchStore();try{
  const i=input();await writeInboxObject(f.store,i);
  await f.store.put(objectKey(inboxObjectKey(i)),new Uint8Array([1,2,3]));
  await expect(writeInboxObject(f.store,i)).rejects.toThrow();
 }finally{f.done()}
});
