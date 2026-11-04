/** History carrier evacuation. One verified source pass per physical carrier.
 * Kept lines are never parsed-and-stringified or scrubbed a second time. The
 * caller registers the resulting manifest and atomically swaps ALL references.
 */
import{createHash}from"node:crypto";
import{rawChunkKey,parseUploadGenerationPrefix,type ManifestV1,type ChunkEntry,type ObjectStore,type SessionLocation}from"../../packages/object-store/src/index.ts";
import{computeContentSha256}from"../../packages/object-store/src/index.ts";
import{encodeRawChunkFrame}from"../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import{canonicalChunkLines,streamVerifiedManifestChunks}from"../session-read-api/canonical-projection.ts";
import{nodeReadBudget}from"../session-read-api/node-read-profile.ts";
const E=new TextEncoder(),sha=(v:Uint8Array)=>createHash("sha256").update(v).digest("hex");
const no=(code:string):never=>{throw Object.assign(Error(code),{code})};
export interface HistoryNativeKey{originId:string;recordId:string}
export interface HistoryRawObject{key:string;sha256:string;bytes:number;kind:"raw"}
export interface HistoryRewriteInput{
 manifest:ManifestV1;sourcePrefix:string;location:SessionLocation;remove:readonly HistoryNativeKey[];
 store:ObjectStore;decompress:(stored:Uint8Array,maxOutputLength:number)=>Uint8Array;signal:AbortSignal;
 /** Durable intent ACK must precede write; final metadata CAS remains separate. */
 register:(spec:HistoryRawObject)=>Promise<void>;write:(spec:HistoryRawObject,body:Uint8Array)=>Promise<void>;
 maxRawBytes?:number;maxRecords?:number;
}
export interface HistoryRewriteResult{raw:ManifestV1["raw"];keptRecords:number;removedRecords:number;sourceBytes:number;objects:number}
export async function rewriteVerifiedCarrier(input:HistoryRewriteInput):Promise<HistoryRewriteResult>{
 const{manifest,location,signal}=input,budget=nodeReadBudget("large"),destination=parseUploadGenerationPrefix(location.prefix);
 const sourceBase=parseUploadGenerationPrefix(input.sourcePrefix)?.base??input.sourcePrefix;
 if(!destination||destination.base!==sourceBase||location.prefix===input.sourcePrefix||manifest.raw.format!=="ndjson"||!manifest.raw.scrub||
  location.workspaceId!==manifest.session.workspace_id||location.machineId!==manifest.session.machine_id||location.harness!==manifest.session.harness||location.sessionId!==manifest.session.session_id||location.startDate!==new Date(manifest.session.started_at).toISOString().slice(0,10))no("history_rewrite_identity");
 const maxRawBytes=input.maxRawBytes??budget.sourceBytes,maxRecords=input.maxRecords??budget.sourceRecords;
 if(!Number.isSafeInteger(maxRawBytes)||maxRawBytes<1||maxRawBytes>budget.sourceBytes||!Number.isSafeInteger(maxRecords)||maxRecords<0||maxRecords>budget.sourceRecords)no("history_rewrite_budget");
 const removals=new Set(input.remove.map(k=>{if(!k.originId||!k.recordId)no("history_rewrite_key");return JSON.stringify([k.originId,k.recordId])}));
 const chunks:ChunkEntry[]=[],ends=new Map<number,number>();let keptRecords=0,removedRecords=0,sourceBytes=0,seen=0;
 for await(const old of streamVerifiedManifestChunks(manifest,input.store,input.sourcePrefix,input.decompress,{signal,maxRawBytes})){
  const kept:string[]=[];let records=0;sourceBytes+=old.raw.length;
  for(const line of canonicalChunkLines(old)){
   signal.throwIfAborted();
   if(line.record){if(seen>=maxRecords)no("actual canonical record count exceeds scope limit");seen++;
    if(removals.has(JSON.stringify([line.record.origin_id,line.record.record_id]))){removedRecords++;continue;}
    records++;keptRecords++;
   }
   kept.push(line.raw);
  }
  // Retain an empty epoch's provenance and dense seq with a blank LF, never a
  // fabricated record. This also satisfies existing backfill epoch invariants.
  const raw=E.encode(kept.join("")||"\n"),stored=await encodeRawChunkFrame(raw);signal.throwIfAborted();
  if(raw.length>16*1024**2||stored.length>16*1024**2)no("history_rewrite_chunk_limit");
  const epoch=old.entry.epoch,descriptor=manifest.raw.epochs[epoch];if(!descriptor)no("history_rewrite_epoch");
  const start=ends.get(epoch)??(descriptor.resumes_epoch===undefined?0:ends.get(descriptor.resumes_epoch)??0),hash=sha(raw);
  const entry:ChunkEntry={seq:chunks.length,epoch,start_offset:start,bytes:raw.length,stored_bytes:stored.length,sha256:hash,sha256_8:hash.slice(0,8),stored_sha256:sha(stored),stored_raw_bytes:raw.length,raw_sha256:hash,compression:"zstd",records};
  const spec:HistoryRawObject={key:String(rawChunkKey(location,entry.seq,start,hash,"ndjson")),sha256:entry.stored_sha256,bytes:stored.length,kind:"raw"};
  await input.register(spec);signal.throwIfAborted();await input.write(spec,stored);signal.throwIfAborted();
  chunks.push(entry);ends.set(epoch,start+raw.length);
 }
 const current=manifest.raw.epochs[manifest.raw.epoch];if(!current)no("history_rewrite_epoch");
 const raw:ManifestV1["raw"]={...manifest.raw,chunks,chunk_count:chunks.length,raw_bytes:chunks.reduce((n,c)=>n+c.bytes,0),stored_bytes:chunks.reduce((n,c)=>n+c.stored_bytes,0),next_offset:ends.get(manifest.raw.epoch)??(current.resumes_epoch===undefined?0:ends.get(current.resumes_epoch)??0),content_sha256:await computeContentSha256(chunks)};
 return{raw,keptRecords,removedRecords,sourceBytes,objects:chunks.length};
}
