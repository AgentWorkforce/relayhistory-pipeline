import{expect,test}from"bun:test";
import{captureHistoryCurrentMetadata,revalidateHistoryCurrentMetadata}from"../history-current-source.ts";
const H="a".repeat(64),P="b".repeat(64),E="c".repeat(64);
const input={orgId:"org",workspaceId:"",expectedSourceAuthoritySha256:H,expectedProviderIdentitySha256:P,controlEpoch:E};
const owner=(userId:string,version="0")=>({userId,version,privacyVersion:version,controlEpoch:E,pending:false,pendingControl:null,privacy:version==="0"?{complete:true,deletions:[],resets:[]}:null});
function port(mutate?:(n:number,p:any)=>void){
 const calls:any[]=[];const api:any={
 snapshot:async(p:any)=>{calls.push(p);const owners=[...new Set([...p.priorOwners,"live-owner"])].map(id=>owner(id as string));const page={protocol:"history-current-metadata-v1",orgId:p.orgId,workspaceId:p.workspaceId,sourceAuthoritySha256:H,providerIdentitySha256:P,controlEpoch:E,complete:true,requestedRecordIds:p.recordIds,rows:[],owners,terminal:{rowCount:0,ownerCount:owners.length,recordPages:1,ownerPages:1,chainSha256:H},lookupSha256:H};mutate?.(calls.length,page);return page},
 revalidate:async(p:any)=>({protocol:"history-current-metadata-v1",sourceAuthoritySha256:H,providerIdentitySha256:P,controlEpoch:E,complete:true,owners:p.owners.map((id:string)=>owner(id)),lookupSha256:P})};return{api,calls};
}
const ids=()=>Array.from({length:501},(_,i)=>"record-"+String(i).padStart(4,"0"));
test("501-key capture carries absent prior and discovered owners through ALL lookups and fresh revalidation",async()=>{
 const f=port(),c=await captureHistoryCurrentMetadata(f.api,input,ids(),["erased-owner"]);
 expect(f.calls.length).toBe(2);expect(f.calls[0].recordIds.length).toBe(500);expect(f.calls[1].recordIds.length).toBe(1);
 expect(f.calls[1].priorOwners).toEqual(["erased-owner","live-owner"]);
 expect([...c.owners.keys()].sort()).toEqual(["erased-owner","live-owner"]);
 expect(await revalidateHistoryCurrentMetadata(f.api,c)).toBe(P);
});
test("cross-lookup owner changes refuse; later context never overwrites earlier rows",async()=>{
 for(const change of[(p:any)=>p.owners[0].version="1",(p:any)=>p.owners[0].pending=true,(p:any)=>p.owners[0].privacy={complete:true,deletions:[{}],resets:[]}]){
  const f=port((n,p)=>{if(n===2)change(p)});
  await expect(captureHistoryCurrentMetadata(f.api,input,ids(),["erased-owner"])).rejects.toThrow("history_source_owner_changed");
 }
});
test("missing prior owner, partial requested keys and fresh owner drift cannot become complete",async()=>{
 const missing=port((n,p)=>{if(n===2){p.owners=p.owners.filter((o:any)=>o.userId!=="erased-owner");p.terminal.ownerCount=p.owners.length}});
 await expect(captureHistoryCurrentMetadata(missing.api,input,ids(),["erased-owner"])).rejects.toThrow("history_source_owner_incomplete");
 const subset=port((n,p)=>p.requestedRecordIds=[]);
 await expect(captureHistoryCurrentMetadata(subset.api,input,["key"],[])).rejects.toThrow("history_source_metadata_incomplete");
 const f=port(),c=await captureHistoryCurrentMetadata(f.api,input,["key"],["erased-owner"]);
 const old=f.api.revalidate;f.api.revalidate=async(p:any)=>{const r=await old(p);r.owners[0].version="1";return r};
 await expect(revalidateHistoryCurrentMetadata(f.api,c)).rejects.toThrow("history_source_owner_changed");
 await expect(captureHistoryCurrentMetadata(f.api,input,[],[])).rejects.toThrow("history_source_no_record_keys");
});
