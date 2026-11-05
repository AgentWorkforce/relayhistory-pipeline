import {expect,test} from 'bun:test';
import {largeReadMain} from '../large-read-main.ts';
import type {LargeReadRuntime} from '../large-read-main.ts';
function runtime(pages:Array<{busy:boolean;scanned:number;changed:number;cycleComplete:boolean}>) {
 let scans=0,claims=0,closed=0;
 return { factory:()=>({deps:{db:{query:async(sql:string)=>{
  if(sql.includes('discover_session_large_reads'))return {rows:[{r:pages[Math.min(scans++,pages.length-1)]}]};
  if(sql.includes('claim_session_large_read')){claims++;return {rows:[{job:null}]}};
  throw new Error('unexpected SQL');
 }}} as unknown as LargeReadRuntime['deps'],close:async()=>{closed++}}),counts:()=>({scans,claims,closed})};
}
test('default-off boot and invalid bounds never open runtime or read credentials',async()=>{
 let opened=0;const factory=()=>{opened++;throw new Error('runtime must remain closed')};
 expect(await largeReadMain({},factory)).toMatchObject({enabled:false,processed:0});
 await expect(largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1',SESSION_LARGE_READ_MAX_JOBS:'11'},factory)).rejects.toThrow('job bound');
 expect(opened).toBe(0);
});
test('durable discovery advances before claiming and is bounded to16pages',async()=>{
 const f=runtime([{busy:false,scanned:128,changed:1,cycleComplete:false}]);
 expect(await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},f.factory)).toEqual({enabled:true,processed:0,published:0,failed:0,scanned:2048,cycleComplete:false});
 expect(f.counts()).toEqual({scans:16,claims:16,closed:1});
});
test('a complete unchanged cycle stops without spinning and closes',async()=>{
 const f=runtime([{busy:false,scanned:0,changed:0,cycleComplete:true}]);
 expect(await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},f.factory)).toMatchObject({cycleComplete:true});
 expect(f.counts()).toEqual({scans:2,claims:1,closed:1});
});
test('invalid discovery is refused and the pool is closed',async()=>{
 const f=runtime([{busy:false,scanned:129,changed:0,cycleComplete:false}]);
 await expect(largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},f.factory)).rejects.toThrow('invalid discovery');
 expect(f.counts()).toEqual({scans:1,claims:0,closed:1});
});
