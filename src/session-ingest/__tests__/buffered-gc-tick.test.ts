import {test,expect} from "bun:test";
import {bufferedGcTick,BufferedGcError,type BufferedGcLedger} from "../buffered-gc.ts";
import type {BufferedGcTicket} from "../buffered-protocol.ts";
const key=(n:number)=>"96bulls/org-a/buffered-batches/v1/ws=ws-a/m=srv_"+"0".repeat(32)+"/b="+String(n).padStart(32,"0")+"-"+"c".repeat(16)+"-"+"d".repeat(32)+".staged.ndjson.zst";
const uuid=(n:number)=>"00000000-0000-4000-8000-"+String(n).padStart(12,"0");
function cleanupTicket(n:number):BufferedGcTicket{
 return {key:key(n),sha256:"e".repeat(64),bytes:10,groupOrdinals:[[0]],stageToken:uuid(n),claimToken:uuid(1000+n),leaseToken:uuid(2000+n),
  rowVersion:"2",resetEpoch:"0",receiptState:"acknowledged",phase:"cleanup",cleanupState:"retired",keepOrdinals:[],keepGroupIndexes:[]} as BufferedGcTicket;
}
// The barrier already exists: ensureBufferedBarrier reads it back and returns its version.
function barrierStore(){
 return {getObject:async(k:string)=>{
  const n=Number(k.split("/b=")[1]!.slice(0,32));const t=cleanupTicket(n);
  const bytes=new TextEncoder().encode(JSON.stringify({v:1,kind:"buffered-redaction",stage_token:t.stageToken,claim_token:t.claimToken,original_sha256:t.sha256}));
  return {body:new ReadableStream({start(c){c.enqueue(bytes);c.close()}}),meta:{version:{version:"barrier-"+n}}};
 },put:async()=>{throw Error("unexpected_put")}} as any;
}
function fakeLedger(script:Array<BufferedGcTicket[]|Error>){
 const calls:number[]=[],finished:string[]=[];let i=0;
 const ledger:BufferedGcLedger={
  async claim(limit){calls.push(limit);const next=script[i++]??[];if(next instanceof Error)throw next;return next},
  async prepare(){throw Error("unexpected_prepare")},async swap(){throw Error("unexpected_swap")},
  async finish(t,version){finished.push(t.stageToken+"@"+version);return true},
 };
 return {ledger,calls,finished};
}
const deps=(ledger:BufferedGcLedger,erased:string[]=[])=>({ledger,store:barrierStore(),keyRoot:"96bulls" as const,
 eraser:{eraseObjectExcept:async(k:string,v:string)=>{erased.push(k+"@"+v)}} as any});

test("tick issues one single-ticket claim per slot; per-tick ticket bound and coverage are unchanged",async()=>{
 const f=fakeLedger([[],[cleanupTicket(1)],[],[cleanupTicket(2)],[]]),erased:string[]=[];
 expect(await bufferedGcTick(deps(f.ledger,erased),5)).toEqual({examined:2,finished:2,deferred:0});
 expect(f.calls).toEqual([1,1,1,1,1]);
 expect(f.finished).toEqual([uuid(1)+"@barrier-1",uuid(2)+"@barrier-2"]);
 expect(erased).toEqual([key(1)+"@barrier-1",key(2)+"@barrier-2"]);
});
test("a claim statement timeout still fails the tick after earlier committed claims (fail closed, no swallow)",async()=>{
 const f=fakeLedger([[cleanupTicket(1)],[],new BufferedGcError("sql_57014"),[cleanupTicket(4)]]);
 await expect(bufferedGcTick(deps(f.ledger),5)).rejects.toMatchObject({code:"sql_57014"});
 // Work before the failed claim is already durable; nothing after it runs.
 expect(f.calls).toEqual([1,1,1]);expect(f.finished).toEqual([uuid(1)+"@barrier-1"]);
});
test("every claim error code propagates, including the codes that collect defers",async()=>{
 for(const code of ["sql_57014","sql_40001","sql_55P03","sql_failed","sql_tickets"]){
  const f=fakeLedger([new BufferedGcError(code)]);
  await expect(bufferedGcTick(deps(f.ledger),3)).rejects.toMatchObject({code});
 }
});
test("collect deferral rules are unchanged and the limit stays bounded",async()=>{
 const f=fakeLedger([[cleanupTicket(1)]]);
 const d=deps(f.ledger);d.eraser={eraseObjectExcept:async()=>{throw new BufferedGcError("sql_57014")}} as any;
 expect(await bufferedGcTick(d,1)).toEqual({examined:1,finished:0,deferred:1});
 const g=fakeLedger([[cleanupTicket(1)]]);const e=deps(g.ledger);e.eraser={eraseObjectExcept:async()=>{throw new BufferedGcError("barrier_unexpected_content")}} as any;
 await expect(bufferedGcTick(e,1)).rejects.toMatchObject({code:"barrier_unexpected_content"});
 for(const limit of [0,-1,1.5,101,Number.NaN])await expect(bufferedGcTick(deps(fakeLedger([]).ledger),limit)).rejects.toMatchObject({code:"claim_limit"});
});
